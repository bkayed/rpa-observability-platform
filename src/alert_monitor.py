"""
RPA alert monitoring module.

This module authenticates with UiPath Orchestrator, extracts operational signals
such as offline robots, stale running jobs, browser extension errors, and unresolved
faulted jobs, then synchronizes alert lifecycle state into SQL Server and optionally
sends consolidated email notifications.
"""
import json
import logging
import os
import smtplib
import urllib.parse
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import pytz
import requests
import urllib3
from sqlalchemy import create_engine, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Local timezone used for alert timestamps and reporting output
LOCAL_TIMEZONE = pytz.timezone("Asia/Amman")



CANONICAL_TYPES = {
    "robotoffline": "RobotOffline",
    "stalejob": "StaleJob",
    "browsererror": "BrowserError",
    "faultedjob": "FaultedJob",
}
def canonical_type(t: str) -> str:
    """
    Returns a consistent, human-friendly alert_type like:
    RobotOffline, StaleJob, BrowserError, FaultedJob

    Any unknown types are returned as trimmed original (no upper).
    """
    raw = (t or "").strip()
    if not raw:
        return ""
    key = raw.replace(" ", "").replace("_", "").lower()
    return CANONICAL_TYPES.get(key, raw)

def load_settings(config_path, sheet_name, key_column, value_column):
    """Load key-value settings from an Excel configuration sheet."""
    df = pd.read_excel(config_path, sheet_name=sheet_name)
    df[key_column] = df[key_column].astype(str)
    df[value_column] = df[value_column].astype(str)
    return df.set_index(key_column)[value_column].to_dict()

def create_logger(fileHandler, loggerName):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers
    if not logger.handlers:
        logger.addHandler(fileHandler)

    return logger


def create_formatter(Format):#
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    return formatter

def create_log_file_handler(logFilePath, formatter):
    fh = logging.FileHandler(logFilePath, encoding="utf-8")  # ✅
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    return fh

# ==============================
# CONFIGURATION
# ==============================

# ==============================
# AUTHENTICATION
# ==============================
# Expected configuration keys:
# - URL_Authenticate
# - TenancyName
# - UiPathUsername
# - UiPathPassword
# - Base_OrchestratorUrl
# - SQL_Server
# - SQL_Database
# - SQL_UID
# - SQL_PWD
# - SenderEmail
# - To
# - CC
# - Server
# - Port
# - OutputDir
# - SEVERITY_MAPPING_PATH
def authenticate(config, logger):
    """Authenticate against UiPath Orchestrator and return a bearer token."""
    try:
        url = config["URL_Authenticate"]
        payload = {
            "TenancyName": config["TenancyName"],
            "UsernameOrEmailAddress": config["UiPathUsername"],
            "Password": config["UiPathPassword"],
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, headers=headers, json=payload, verify=False)
        response.raise_for_status()

        token = response.json().get("result")
        logger.info("Authenticated on UiPath Orchestrator successfully.")
        return token

    except Exception as exc:
        logger.critical(f"Authentication failed: {exc}")
        raise
def build_uipath_headers(auth_token, organization_unit_id):
    """Build standard headers for UiPath Orchestrator API calls."""
    return {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "X-UIPATH-OrganizationUnitId": str(organization_unit_id),
    }    
def get_process_triggers(config, logger, auth_token):
    """
    Extract all process schedules/triggers from UiPath Orchestrator.
    Returns DataFrame with:
    - TriggerId
    - TriggerName
    - ProcessName
    - ReleaseId
    - Enabled
    - StartProcessCron
    - StartProcessCronSummary
    - TimeZoneId
    - NextRunTime
    """
    try:
        logger.info("Start extracting process triggers from Orchestrator")
        # Use config-driven folder context instead of hardcoding the organization unit
        headers = build_uipath_headers(auth_token, config["OrganizationUnitId"])

        url = f"{config['Base_OrchestratorUrl']}/odata/ProcessSchedules?$top=5000"
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()

        schedules = response.json().get("value", [])

        if not schedules:
            logger.info("No process schedules found.")
            return pd.DataFrame(columns=[
                "TriggerId", "TriggerName", "ProcessName", "ReleaseId", "Enabled",
                "StartProcessCron", "StartProcessCronSummary", "TimeZoneId", "NextRunTime"
            ])

        rows = []
        for s in schedules:
            rows.append({
                "TriggerId": s.get("Id"),
                "TriggerName": s.get("Name", ""),
                "ProcessName": s.get("ReleaseName", "") or s.get("ProcessKey", ""),
                "ReleaseId": s.get("ReleaseId"),
                "Enabled": s.get("Enabled"),
                "StartProcessCron": s.get("StartProcessCron", ""),
                "StartProcessCronSummary": s.get("StartProcessCronSummary", ""),
                "TimeZoneId": s.get("TimeZoneId", ""),
                "NextRunTime": convert_to_jordan_time(s.get("NextRunTime"))
            })

        df = pd.DataFrame(rows)
        logger.info(f"Extracted {len(df)} process trigger(s) successfully")
        return df

    except requests.RequestException as e:
        logger.error(f"HTTP error in get_process_triggers: {e}")
        logger.exception("Traceback:")
        return pd.DataFrame(columns=[
            "TriggerId", "TriggerName", "ProcessName", "ReleaseId", "Enabled",
            "StartProcessCron", "StartProcessCronSummary", "TimeZoneId", "NextRunTime"
        ])

    except Exception as e:
        logger.error(f"Unexpected error in get_process_triggers: {e}")
        logger.exception("Traceback:")
        return pd.DataFrame(columns=[
            "TriggerId", "TriggerName", "ProcessName", "ReleaseId", "Enabled",
            "StartProcessCron", "StartProcessCronSummary", "TimeZoneId", "NextRunTime"
        ])
# ==============================
# DATABASE SETUP
# ==============================
def create_sql_engine(config):
    """Create a SQLAlchemy engine using externalized SQL Server settings."""
    db_driver = config.get("SQL_Driver", "ODBC Driver 17 for SQL Server")
    server = config["SQL_Server"]
    database = config["SQL_Database"]
    username = config["SQL_UID"]
    password = config["SQL_PWD"]

    params = urllib.parse.quote_plus(
        f"DRIVER={{{db_driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "Connection Timeout=60;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
def init_sql_db(engine, logger):
    """
    Initialize SQL tables used to store the current alert state and
    alert lifecycle history for reporting, escalation, and MTTR analysis.
    """
    ddl_alerts = """
    IF OBJECT_ID('dbo.alerts', 'U') IS NULL
    CREATE TABLE dbo.alerts (
        id INT IDENTITY(1,1) PRIMARY KEY,
        alert_type NVARCHAR(100) NOT NULL,
        alert_key  NVARCHAR(255) NOT NULL,
        description NVARCHAR(MAX) NULL,
        status NVARCHAR(20) NOT NULL,
        first_seen DATETIME NULL,
        last_seen  DATETIME NULL,
        last_sent  DATETIME NULL,
        CONSTRAINT uq_alert UNIQUE(alert_type, alert_key)
    );
    """

    ddl_events = """
    IF OBJECT_ID('dbo.alerts_events', 'U') IS NULL
    CREATE TABLE dbo.alerts_events (
        event_id INT IDENTITY(1,1) PRIMARY KEY,
        event_time DATETIME NOT NULL,
        alert_type NVARCHAR(100) NOT NULL,
        alert_key  NVARCHAR(255) NOT NULL,
        status NVARCHAR(20) NOT NULL,
        description NVARCHAR(MAX) NULL,
        details NVARCHAR(MAX) NULL,
        severity NVARCHAR(30) NULL,
        source NVARCHAR(50) NULL,
        process_name NVARCHAR(255) NULL,
        robot_name NVARCHAR(255) NULL
    );
    """

    ddl_indexes = """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_events_time' AND object_id=OBJECT_ID('dbo.alerts_events'))
        CREATE INDEX idx_events_time ON dbo.alerts_events(event_time);

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_events_key' AND object_id=OBJECT_ID('dbo.alerts_events'))
        CREATE INDEX idx_events_key ON dbo.alerts_events(alert_type, alert_key);

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_alerts_key' AND object_id=OBJECT_ID('dbo.alerts'))
        CREATE INDEX idx_alerts_key ON dbo.alerts(alert_type, alert_key);
    """

    with engine.begin() as con:
        con.execute(text(ddl_alerts))
        con.execute(text(ddl_events))
        con.execute(text(ddl_indexes))

    logger.info("SQL DB initialized successfully.")


# ==============================
# Load Severity Mapping file
# ==============================
def load_process_mapping(config, logger):
    try:
        df = pd.read_excel(config['SEVERITY_MAPPING_PATH'], sheet_name="UIPATH")
        df.columns = df.columns.str.strip()

        df["process_name"] = df["ProcessName"].astype(str).str.strip()
        df["severity"] = df["Severity"].astype(str).str.upper().str.strip()
        df["source"] = df["Source"].astype(str).str.upper().str.strip()

        for col in ["expected_interval_minutes", "expected_interval_hours", "expected_runs_per_day"]:
            if col not in df.columns:
                df[col] = None

        logger.info("Load Severity Mapping file Successfully")
        return df

    except Exception as e:
        logger.error(f"Load Severity Mapping file Faild: {e}")
        raise

def get_expected_interval_for_process(process_name, map_df, logger=None):
    """
    Returns timedelta or None based on:
    expected_interval_minutes / expected_interval_hours / expected_runs_per_day
    Priority:
      1) expected_interval_minutes
      2) expected_interval_hours
      3) expected_runs_per_day
    """
    try:
        p = (process_name or "").strip()
        hit = map_df[map_df["process_name"] == p]

        if hit.empty:
            hit = map_df[map_df["process_name"] == "UNKNOWN"]

        if hit.empty:
            return None

        row = hit.iloc[0]

        minutes = pd.to_numeric(row.get("expected_interval_minutes"), errors="coerce")
        hours = pd.to_numeric(row.get("expected_interval_hours"), errors="coerce")
        runs_per_day = pd.to_numeric(row.get("expected_runs_per_day"), errors="coerce")

        if pd.notna(minutes) and minutes > 0:
            return timedelta(minutes=float(minutes))

        if pd.notna(hours) and hours > 0:
            return timedelta(hours=float(hours))

        if pd.notna(runs_per_day) and runs_per_day > 0:
            return timedelta(hours=(24 / float(runs_per_day)))

        return None

    except Exception as e:
        if logger:
            logger.warning(f"Could not calculate expected interval for {process_name}: {e}")
        return None
    
def build_alert_key(alert_type: str, process_name: str, robot_name: str, raw_key: str = "") -> str:
    t = canonical_type(alert_type)

    p = (process_name or "").strip() or "UNKNOWN"
    r = (robot_name or "").strip() or "UNKNOWN"

    if t in ["StaleJob", "BrowserError", "FaultedJob"]:
        return f"{p}|{r}"
    if t == "RobotOffline":
        return r
    return (raw_key or "").strip() or f"{p}|{r}"

def get_last_active_event_info(engine, a_type, a_key, logger):
    try:
        q = text("""
            SELECT TOP 1 severity, source, process_name, robot_name
            FROM dbo.alerts_events
            WHERE alert_type=:t AND alert_key=:k AND status='active'
            ORDER BY event_time DESC
        """)
        df = pd.read_sql(q, engine, params={"t": a_type, "k": a_key})

        if not df.empty:
            row = df.iloc[0].fillna("")
            return (row["severity"] or "MAJOR",
                    row["source"] or "UIPATH_PROCESS",
                    row["process_name"] or "UNKNOWN",
                    row["robot_name"] or "UNKNOWN")

        return "MAJOR", "UIPATH_PROCESS", "UNKNOWN", "UNKNOWN"
    except Exception as e:
        logger.error(f"get_last_active_event_info_sql Failed: {e}", exc_info=True)
        return "MAJOR", "UIPATH_PROCESS", "UNKNOWN", "UNKNOWN"

def fetch_active_alerts(engine) -> pd.DataFrame:
    q = """
    SELECT alert_type, alert_key, description, status, first_seen, last_seen, last_sent
    FROM dbo.alerts
    WHERE status='active'
    """
    return pd.read_sql(q, engine)
def mark_alerts_sent(engine, now_dt, keys):
    # keys: iterable of (alert_type, alert_key)
    if not keys:
        return
    with engine.begin() as db:
        for a_type, a_key in keys:
            db.execute(text("""
                UPDATE dbo.alerts
                SET last_sent=:now_dt
                WHERE alert_type=:t AND alert_key=:k
            """), {"now_dt": now_dt, "t": a_type, "k": a_key})

def extract_process_robot(a: dict):
    """
    Returns (process_name, robot_name) from your current_alerts row.
    Works with your existing alert types.
    """
    t = str(a.get("type", ""))
    details = str(a.get("details", ""))

    process_name = ""
    robot_name = ""

    if t == "StaleJob":
        # details: "Release: X, Robot: Y, LastLog: ..."
        if "Release:" in details:
            process_name = details.split("Release:", 1)[1].split(",")[0].strip()
        if "Robot:" in details:
            robot_name = details.split("Robot:", 1)[1].split(",")[0].strip()

    elif t == "FaultedJob":
        # key: "release|robot"
        key = str(a.get("key", ""))
        if "|" in key:
            process_name, robot_name = key.split("|", 1)
            process_name = process_name.strip()
            robot_name = robot_name.strip()

    elif t == "BrowserError":
        # desc: "Job 'REL' on ROBOT ..."
        desc = str(a.get("desc", ""))
        if "Job '" in desc and "' on " in desc:
            process_name = desc.split("Job '", 1)[1].split("'", 1)[0].strip()
            robot_name = desc.split("' on ", 1)[1].split(" logged", 1)[0].strip()

    elif t == "RobotOffline":
        # key is robot name
        robot_name = str(a.get("key", "")).strip()
        process_name = "RobotOffline"

    if not process_name:
        process_name = "UNKNOWN"
    if not robot_name:
        robot_name = "UNKNOWN"

    return process_name, robot_name


def resolve_severity_source(process_name, map_df):
    try:
        p = (process_name or "UNKNOWN").strip()
        hit = map_df[map_df["process_name"] == p]
        if hit.empty:
            hit = map_df[map_df["process_name"] == "UNKNOWN"]

        if hit.empty:
            return "MAJOR", "UIPATH_PROCESS"

        row = hit.iloc[0]
        return row.get("severity", "MAJOR"), row.get("source", "UIPATH_PROCESS")

    except Exception as e:
        return "MAJOR", "UIPATH_PROCESS"



# ==============================
# EMAIL SETUP
# ==============================

def send_email(config, subject, html_body, attachment_path=None, logger=None):
    try:
        sender = config["SenderEmail"]
        to_list = [x.strip() for x in config["To"].split(";") if x.strip()]
        cc_list = [x.strip() for x in config.get("CC", "").split(";") if x.strip()]

        msg = MIMEMultipart("alternative")
        msg["From"] = sender
        msg["To"] = ", ".join(to_list)
        msg["Cc"] = ", ".join(cc_list)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        if attachment_path and os.path.exists(attachment_path):
            part = MIMEBase("application", "octet-stream")
            with open(attachment_path, "rb") as f:
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(part)

        with smtplib.SMTP(config["Server"], int(config["Port"])) as server:
            server.sendmail(sender, to_list + cc_list, msg.as_string())

        if logger:
            logger.info(f"Email sent: {subject}")
        else:
            logger.info(f"Email sent: {subject}")


    except Exception as e:
        if logger:
            logger.error(f"Email error: {e}", exc_info=True)
        else:
            logger.error(f"Email error: {e}", exc_info=True)


# ==============================
# UTILS
# ==============================
def parse_time(time_str):
    if not time_str or time_str.startswith("0001-01-01"):
        return None
    try:
        if "." in time_str:
            base, rest = time_str.split(".", 1)
            micro = (rest.rstrip("Z") + "000000")[:6]
            return datetime.strptime(f"{base}.{micro}Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def to_local(dt):
    if not dt: return None
    return pytz.utc.localize(dt).astimezone(LOCAL_TIMEZONE)



    # --- Convert UTC → Jordan time (as strings)
def convert_to_jordan_time(utc_str):
    try:
        if not utc_str or str(utc_str).startswith("0001-01-01"):
            return None

        utc_str = str(utc_str)

        if "." in utc_str:
            base, rest = utc_str.split(".", 1)
            micro = (rest.rstrip("Z") + "000000")[:6]
            utc_time = datetime.strptime(f"{base}.{micro}Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

        jordan_time = utc_time.replace(tzinfo=pytz.utc).astimezone(LOCAL_TIMEZONE)
        return jordan_time.strftime("%Y-%m-%d %H:%M:%S")

    except Exception:
        return utc_str

# ==============================
# FETCH ALERTS
# ==============================
def get_alerts(config,logger,auth_token):
    try:
        logger.info("get_alerts API Extraction started")
        today_start = datetime.now(LOCAL_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        utc_start = today_start.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
        url = f"{config['Base_OrchestratorUrl']}/odata/alerts?$filter=CreationTime ge {utc_start}"
        headers = build_uipath_headers(auth_token, config["OrganizationUnitId"])
    
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
    
        alerts = response.json().get("value", [])
        logger.info(f" Retrieved {len(alerts)} alerts")
    
        if not alerts:
            logger.info("No alerts found for today.")
            return pd.DataFrame()
    
        df = pd.DataFrame(alerts)
    
        # --- Expand nested JSON (Data column)
        if "Data" in df.columns:
            df["Data"] = df["Data"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip().startswith("{") else {})
            data_expanded = pd.json_normalize(df["Data"])
            df = pd.concat([df.drop(columns=["Data"]), data_expanded], axis=1)
    
        for col in ["CreationTime", "ReportingTime"]:
            if col in df.columns:
                df[col] = df[col].apply(convert_to_jordan_time)
    
        # --- Clean column order for consistency
        columns_order = [
            "NotificationName", "Component", "Severity", "QueueName", "QueueId", "QueueItemId",
            "RobotName", "Reason", "State", "CreationTime", "ReportingTime", "Message",
            "ProcessingExceptionType", "MessageErrorCode", "FolderFullyQualifiedName",]
        
        df = df[[col for col in columns_order if col in df.columns]]
        logger.info("get_alerts completed successfully")
    
        return df
    except requests.RequestException as e:
            logger.error(f"HTTP error in get_alerts: {e}")
            logger.exception("Full traceback:")
            return pd.DataFrame()
    
    except Exception as e:
            logger.error(f"Unexpected error in get_alerts: {e}")
            logger.exception("Full traceback:")
            return pd.DataFrame()
# ==============================
# FETCH STALE JOBS
# ==============================


def load_stale_ignore_list(mapping_xlsx: str, logger, sheet_name="ProcessSeverity") -> set:
    """
    Returns set of process names (lowercase) that should be ignored from stale detection.
    """
    try:
        df = pd.read_excel(mapping_xlsx, sheet_name=sheet_name)
        df.columns = [c.strip() for c in df.columns]

        df["process_name"] = df["process_name"].astype(str).str.strip()
        df["ignore_stale"] = df.get("StaleJobsIgnore", "NO").astype(str).str.upper().str.strip()

        ignored = set(df[df["ignore_stale"].isin(["YES", "TRUE", "1"])]["process_name"].str.lower().tolist())
        logger.info(f"Loaded stale ignore list: {len(ignored)} process(es) from {sheet_name}")
        return ignored
    except Exception as e:
        logger.warning(f"Could not load {sheet_name}: {e}")
        return set()
    
def get_stale_jobs(config,logger,auth_token, threshold_minutes=30):
    """
    Detect running jobs that have no recent log activity beyond the configured threshold.
    Jobs listed in the stale-ignore mapping are excluded from detection.
    """
    try:
        mapping_xlsx = config["SEVERITY_MAPPING_PATH"]  # your mapping file
        ignore_set = set()
        if mapping_xlsx and os.path.exists(mapping_xlsx):
            ignore_set = load_stale_ignore_list(mapping_xlsx, logger)
            logger.info("Checking stale jobs")
            
            
        headers = build_uipath_headers(auth_token, config["OrganizationUnitId"])
    
        # --- 1️⃣ Get all running jobs
        jobs_url = f"{config['Base_OrchestratorUrl']}/odata/Jobs?$filter=State eq 'Running'"
        jobs_response = requests.get(jobs_url, headers=headers, verify=False)
        jobs_response.raise_for_status()
        jobs = jobs_response.json().get("value", [])
        if not jobs:
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])
    
        job_count = len(jobs)
        now = datetime.now(LOCAL_TIMEZONE)
    
        # --- 2️⃣ Fetch recent logs (auto-scaled)
        log_sample = min(max(job_count * 200, 2000), 10000)
    
        logs_url = f"{config['Base_OrchestratorUrl']}/odata/RobotLogs?$top={log_sample}&$orderby=TimeStamp desc"
        logs_response = requests.get(logs_url, headers=headers, verify=False)
        logs_response.raise_for_status()
        logs = logs_response.json().get("value", [])
    
        latest_log = {}
        for log in logs:
            job_key = log.get("JobKey") or log.get("JobId")
            t = parse_time(log.get("TimeStamp"))
            if job_key and t:
                # keep the first (most recent) only
                if job_key not in latest_log:
                    latest_log[job_key] = pytz.utc.localize(t).astimezone(LOCAL_TIMEZONE)
    
        # --- 3️⃣ Detect stale jobs
        stale = []
        for j in jobs:
            job_key = j.get("Key")
            rel = j.get("ReleaseName", "")
            robot = j.get("RobotName") or j.get("HostMachineName", "")
            start_time = j.get("StartTime")
    
            last = latest_log.get(job_key)
            diff = None
            last_time_str = None
            if rel.lower() in ignore_set:
                # ✅ ignore this process completely
                continue
    
            if last:
                diff = (now - last).total_seconds() / 60
                last_time_str = last.strftime("%Y-%m-%d %H:%M:%S")
            elif start_time:
                s = parse_time(start_time)
                if s:
                    s_local = pytz.utc.localize(s).astimezone(LOCAL_TIMEZONE)
                    diff = (now - s_local).total_seconds() / 60
                    last_time_str = s_local.strftime("%Y-%m-%d %H:%M:%S")
    
            if diff and diff > threshold_minutes:

                stale.append({
                                "type": "StaleJob",
                                "key": job_key,  # keep raw key if you want
                                "process_name": rel,
                                "robot_name": robot,
                                "desc": f"Job '{rel}' on {robot}' inactive for {round(diff)} min",
                                "details": f"Release: {rel}, Robot: {robot}, LastLog: {last_time_str}",
                                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
                            })
                                
        df = pd.DataFrame(stale)
        logger.info(f"Stale job detection completed --> Found {len(df)} stale job(s) > {threshold_minutes} min old.")
        return df
    
    except Exception as e:
        logger.error(f"get_stale_jobs failed: {e}")
        logger.exception("Full traceback:")
        return pd.DataFrame(columns=["type","key","desc","details","timestamp"])
# ==============================
# FETCH BROWSER EXTENSION ERROR JOBS
# # ==============================
def get_browser_extension_error_jobs(config,logger,auth_token, lookback_minutes=30):
    """
    Detect recent jobs that logged browser communication failures, regardless of final job state.
    """
    try:
        logger.info("Checking browser extension errors")
        headers = build_uipath_headers(auth_token, config["OrganizationUnitId"])
    
        
        now = datetime.now(LOCAL_TIMEZONE)
        utc_start = (now - timedelta(minutes=lookback_minutes)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
        # 1️⃣ Fetch recent error logs
        logs_url = (
            f"{config['Base_OrchestratorUrl']}/odata/RobotLogs?"
            f"$filter=Level eq 'Error' and TimeStamp ge {utc_start}"
            f"&$orderby=TimeStamp desc&$top=10000"
        )
        resp = requests.get(logs_url, headers=headers, verify=False)
        resp.raise_for_status()
        logs = resp.json().get("value", [])
        logger.info(f"Total error logs fetched: {len(logs)}")
        if not logs:
            logger.info("No Error logs found in the last period.")
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])
    
        # 2️⃣ Filter messages in Python
        target_phrases = ["Cannot communicate with the browser", "browser extension"]
        matching_logs = [
            l for l in logs
            if any(p.lower() in str(l.get("Message", "")).lower() for p in target_phrases)
        ]
        if not matching_logs:
            logger.info(" No browser-related errors found.")
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])
    
        logger.info(f" Found {len(matching_logs)} matching browser-error logs.")
    
        # 3️⃣ Fetch all jobs created recently
        jobs_url = (
            f"{config['Base_OrchestratorUrl']}/odata/Jobs?"
            f"$filter=CreationTime ge {utc_start}&$orderby=CreationTime desc&$top=5000"
        )
        jobs_resp = requests.get(jobs_url, headers=headers, verify=False)
        jobs_resp.raise_for_status()
        jobs = jobs_resp.json().get("value", [])
    
        # Map by JobKey (GUID) instead of numeric Id
        jobs_by_key = {j["Key"]: j for j in jobs if "Key" in j}
    
        # 4️⃣ Build alert entries
        alerts = []
        seen = set()
        for log in matching_logs:
            job_key = log.get("JobKey")
            if not job_key or job_key in seen:
                continue
            seen.add(job_key)
    
            job = jobs_by_key.get(job_key, {})
            rel = job.get("ReleaseName", log.get("ProcessName", "UnknownProcess"))
            robot = job.get("RobotName", log.get("RobotName", "UnknownRobot"))
            state = job.get("State", "Unknown")
            msg = log.get("Message", "")
            ts = parse_time(log.get("TimeStamp"))
            ts_local = pytz.utc.localize(ts).astimezone(LOCAL_TIMEZONE) if ts else now
    
            alerts.append({
                    "type": "BrowserError",
                    "key": str(job_key),
                    "process_name": rel,
                    "robot_name": robot,
                    "desc": f"Job '{rel}' on {robot} logged browser communication error (State: {state})",
                    "details": msg[:300],
                    "timestamp": ts_local.strftime("%Y-%m-%d %H:%M:%S")
                })
    
        df = pd.DataFrame(alerts)
        logger.info(f"Browser error jobs detected: {len(df)}")
        return df
    except requests.RequestException as e:
            logger.error(f"HTTP error in get_browser_extension_error_jobs: {e}")
            logger.exception("Traceback:")
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])

    except Exception as e:
        logger.error(f"Unexpected error in get_browser_extension_error_jobs: {e}")
        logger.exception("Traceback:")
        return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])
# ==============================
# FETCH Offline Robots
# ==============================
def get_offline_robots_unified(alerts_df: pd.DataFrame,logger, recovery_window_minutes: int = 10):
    """
    Detect robots that went offline and did not recover within the configured grace period.
    """
    try:
        logger.info("Start Checking offline robots")
        if alerts_df is None or alerts_df.empty:
            logger.info("No alerts data available.")
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])
    
        now = datetime.now(LOCAL_TIMEZONE)
    
        df = alerts_df.copy()
        df["CreationTime"] = pd.to_datetime(df["CreationTime"], errors="coerce")
        # make CreationTime timezone-aware in Asia/Amman
        df["CreationTime"] = df["CreationTime"].dt.tz_localize(LOCAL_TIMEZONE, nonexistent="shift_forward", ambiguous="NaT")
    
        robots_df = df[df["Component"] == "Robots"].copy()
    
        offline = robots_df[
            (robots_df["Severity"].str.lower() == "fatal") &
            (robots_df["NotificationName"] == "Robot.StatusChanged.Offline")]
        
        available = robots_df[
            (robots_df["Severity"].str.lower() == "info") &
            (robots_df["NotificationName"] == "Robot.StatusChanged.Available")]
    
        rows = []
        for _, r in offline.iterrows():
            robot = r.get("RobotName", "Unknown")
            t_off = r.get("CreationTime")
            if pd.isna(t_off):
                continue
            if now < t_off + timedelta(minutes=recovery_window_minutes):
                continue
            recovered = available[
                (available["RobotName"] == robot) &
                (available["CreationTime"] > t_off) &
                (available["CreationTime"] <= t_off + timedelta(minutes=recovery_window_minutes))]
            
            if recovered.empty:
                rows.append({
                    "type": "RobotOffline",
                    "key": robot,
                    "process_name": "RobotOffline",
                    "robot_name": robot,
                    "desc": f"Robot '{robot}' offline since {t_off.strftime('%Y-%m-%d %H:%M:%S')} (no recovery ≥{recovery_window_minutes} min)",
                    "details": f"Offline at: {t_off.strftime('%Y-%m-%d %H:%M:%S')}",
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
                })
        df = pd.DataFrame(rows, columns=["type", "key", "desc", "process_name","robot_name","details", "timestamp"])
        logger.info(f"Offline robots detected: {len(df)}")
        return df

    except Exception as e:
            logger.error(f"Error in get_offline_robots_unified: {e}")
            logger.exception("Traceback:")
            return pd.DataFrame(columns=["type", "key", "desc", "details", "timestamp"])


# ==============================
# Save output daily file
# ==============================


def should_send_email(engine, now_dt, new_alerts, resolved, reminder_minutes=120):
    if new_alerts or resolved:
        return True, set(new_alerts)  # send at least new ones

    active = fetch_active_alerts(engine)
    if active.empty:
        return False, set()

    active["alert_type"] = active["alert_type"].astype(str).apply(canonical_type).str.strip()
    active["alert_key"] = active["alert_key"].astype(str).str.strip()

    active["last_sent"] = pd.to_datetime(active["last_sent"], errors="coerce")
    due = active[(active["last_sent"].isna()) | (active["last_sent"] <= (now_dt - timedelta(minutes=reminder_minutes)))]

    # normalize for keys (case-insensitive)
    due_keys = set((t.lower(), k.lower()) for t, k in zip(due["alert_type"], due["alert_key"]))
    return (len(due_keys) > 0), due_keys


    
# ==============================
# get_faulted_process_robot_unresolved
# ==============================   

def get_process_schedule_row(process_name, map_df):
    try:
        p = (process_name or "").strip()
        df = map_df.copy()
        df["process_name"] = df["process_name"].astype(str).str.strip()

        hit = df[df["process_name"] == p]
        if hit.empty:
            hit = df[df["process_name"] == "UNKNOWN"]

        if hit.empty:
            return None

        return hit.iloc[0]
    except Exception:
        return None


def is_enabled_process(process_name, map_df):
    row = get_process_schedule_row(process_name, map_df)

    if row is None:
        return True

    raw_val = row.get("Enabled", True)

    if pd.isna(raw_val):
        return True

    # handle numeric values from Excel: 1, 1.0, 0, 0.0
    if isinstance(raw_val, (int, float)):
        return float(raw_val) != 0

    val = str(raw_val).strip().lower()

    true_values = {"true", "1", "1.0", "yes", "enabled", "y"}
    false_values = {"false", "0", "0.0", "no", "disabled", "n"}

    if val in true_values:
        return True
    if val in false_values:
        return False

    return True


def parse_active_hours(active_hours_text):
    """
    Converts:
    '07,08,09,10'
    -> {7,8,9,10}
    """
    try:
        if pd.isna(active_hours_text) or not str(active_hours_text).strip():
            return set(range(24))

        vals = set()
        for x in str(active_hours_text).split(","):
            x = x.strip()
            if x != "":
                vals.add(int(x))
        return vals if vals else set(range(24))
    except Exception:
        return set(range(24))


def parse_active_days(active_days_text):
    """
    Converts:
    'SUN,MON,TUE'
    -> {'SUN','MON','TUE'}
    """
    try:
        if pd.isna(active_days_text) or not str(active_days_text).strip():
            return {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}

        vals = set()
        for x in str(active_days_text).split(","):
            x = x.strip().upper()
            if x:
                vals.add(x)

        return vals if vals else {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}
    except Exception:
        return {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}


def get_day_name(dt_obj):
    days = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    return days[dt_obj.weekday()]


def is_now_in_active_window(process_name, map_df, now_dt):
    row = get_process_schedule_row(process_name, map_df)
    if row is None:
        return True

    active_hours = parse_active_hours(row.get("active_hours"))
    active_days = parse_active_days(row.get("active_days"))

    current_hour = now_dt.hour
    current_day = get_day_name(now_dt)

    return (current_hour in active_hours) and (current_day in active_days)


def get_faulted_process_robot_unresolved(config, logger, auth_token, map_df, lookback_hours=12):
    """
    Detect unresolved process faults by checking whether a faulted execution has been followed
    by a successful execution within the expected operational window.
    """
    try:
        logger.info(f"Checking unresolved faulted processes (last {lookback_hours} hours)")

        headers = build_uipath_headers(auth_token, config["OrganizationUnitId"])

        now = datetime.now(LOCAL_TIMEZONE)
        utc_start = (now - timedelta(hours=lookback_hours)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        jobs_url = (
            f"{config['Base_OrchestratorUrl']}/odata/Jobs?"
            f"$filter=CreationTime ge {utc_start}"
            f"&$orderby=CreationTime desc&$top=5000"
        )

        resp = requests.get(jobs_url, headers=headers, verify=False)
        resp.raise_for_status()
        jobs = resp.json().get("value", [])
        logger.info(f"Jobs fetched: {len(jobs)}")

        if not jobs:
            return pd.DataFrame(columns=["type", "key", "process_name", "robot_name", "desc", "details", "timestamp"])

        df = pd.DataFrame(jobs)

        for c in ["ReleaseName", "RobotName", "State", "CreationTime", "EndTime", "Key", "Info", "HostMachineName"]:
            if c not in df.columns:
                df[c] = ""

        df["RobotNameNorm"] = df["RobotName"].astype(str).str.strip()
        df.loc[df["RobotNameNorm"].isin(["", "None", "nan"]), "RobotNameNorm"] = None

        df["HostMachineName"] = df["HostMachineName"].astype(str).str.strip()
        df.loc[df["HostMachineName"].isin(["", "None", "nan"]), "HostMachineName"] = None

        df["RobotNameNorm"] = df["RobotNameNorm"].fillna(df["HostMachineName"]).fillna("UnknownRobot")

        def parse_any(s):
            t = parse_time(s)
            return pytz.utc.localize(t).astimezone(LOCAL_TIMEZONE) if t else None

        df["CreationDT"] = df["CreationTime"].apply(parse_any)
        df["EndDT"] = df["EndTime"].apply(parse_any)

        df = df[df["CreationDT"].notna()].sort_values("CreationDT", ascending=False)

        rows = []
        grouped = df.groupby(["ReleaseName", "RobotNameNorm"], dropna=False)

        for (release, robot), g in grouped:
            if not release:
                continue

            g = g.sort_values("CreationDT", ascending=False)

            gf = g[g["State"].astype(str).str.lower() == "faulted"]
            if gf.empty:
                continue

            last_fault = gf.iloc[0]
            fault_time = last_fault["EndDT"] if pd.notna(last_fault["EndDT"]) else last_fault["CreationDT"]

            if fault_time is None:
                continue

            fault_time_str = fault_time.strftime("%Y-%m-%d %H:%M:%S")

            # 1) If success happened after fault => resolved
            success_after_fault = g[
                (g["State"].astype(str).str.lower() == "successful") &
                (
                    ((g["EndDT"].notna()) & (g["EndDT"] > fault_time)) |
                    ((g["EndDT"].isna()) & (g["CreationDT"] > fault_time))
                )
            ].sort_values("CreationDT", ascending=False)

            if not success_after_fault.empty:
                success_time = success_after_fault.iloc[0]["EndDT"]
                if pd.isna(success_time):
                    success_time = success_after_fault.iloc[0]["CreationDT"]

                logger.info(
                    f"[FAULT RESOLVED BY SUCCESS] release={release} | robot={robot} | "
                    f"fault_time={fault_time_str} | success_time={success_time}"
                )
                continue

            # 2) If process is disabled => don't keep unresolved by schedule
            row = get_process_schedule_row(release, map_df)
            
            if not is_enabled_process(release, map_df):
                logger.info(
                    f"[FAULT SKIPPED - PROCESS DISABLED] release={release} | robot={robot} | fault_time={fault_time_str}"
                )
                continue

            # 3) Get expected interval
            expected_delta = get_expected_interval_for_process(release, map_df, logger)

            if expected_delta is None:
                logger.info(
                    f"[FAULT OPEN - NO EXPECTED INTERVAL] release={release} | robot={robot} | fault_time={fault_time_str}"
                )

                info = last_fault.get("Info", "") or ""
                job_key = last_fault.get("Key", "")

                rows.append({
                    "type": "FaultedJob",
                    "key": f"{release}|{robot}",
                    "process_name": release,
                    "robot_name": robot,
                    "desc": f"Unresolved fault: '{release}' on {robot}",
                    "details": f"LastFaultTime: {fault_time_str} | FaultedJobKey: {job_key} | Info: {str(info)[:200]}",
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
                })
                continue

            # 4) If now is outside active schedule window, wait
            if not is_now_in_active_window(release, map_df, now):
                logger.info(
                    f"[FAULT WAITING ACTIVE WINDOW] release={release} | robot={robot} | "
                    f"fault_time={fault_time_str} | now={now.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                continue

            # 5) If still inside expected waiting period, wait
            next_expected_time = fault_time + expected_delta

            if now < next_expected_time:
                logger.info(
                    f"[FAULT WAITING NEXT RUN] release={release} | robot={robot} | "
                    f"fault_time={fault_time_str} | next_expected_time={next_expected_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                continue

            # 6) Otherwise still unresolved
            logger.info(
                f"[FAULT STILL OPEN AFTER EXPECTED WINDOW] release={release} | robot={robot} | "
                f"fault_time={fault_time_str} | next_expected_time={next_expected_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            info = last_fault.get("Info", "") or ""
            job_key = last_fault.get("Key", "")

            rows.append({
                "type": "FaultedJob",
                "key": f"{release}|{robot}",
                "process_name": release,
                "robot_name": robot,
                "desc": f"Unresolved fault: '{release}' on {robot}",
                "details": (
                    f"LastFaultTime: {fault_time_str} | "
                    f"NextExpectedRun: {next_expected_time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"FaultedJobKey: {job_key} | Info: {str(info)[:200]}"
                ),
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
            })

        result_df = pd.DataFrame(
            rows,
            columns=["type", "key", "process_name", "robot_name", "desc", "details", "timestamp"]
        )

        logger.info(f"Unresolved faulted processes detected: {len(result_df)}")
        return result_df

    except requests.RequestException as e:
        logger.error(f"HTTP error in get_faulted_process_robot_unresolved: {e}")
        logger.exception("Traceback:")
        return pd.DataFrame(columns=["type", "key", "process_name", "robot_name", "desc", "details", "timestamp"])

    except Exception as e:
        logger.error(f"Unexpected error in get_faulted_process_robot_unresolved: {e}")
        logger.exception("Traceback:")
        return pd.DataFrame(columns=["type", "key", "process_name", "robot_name", "desc", "details", "timestamp"])




def db_sync_alerts(engine, current_alerts: pd.DataFrame, map_df: pd.DataFrame, logger):
    """
    Synchronize current alerts with the SQL state store and record lifecycle transitions
    for newly active and resolved alerts.
    """
    now_dt = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None)

    # --- current keys ---
    cur = current_alerts.copy()
    cur["alert_type"] = cur["type"].apply(canonical_type)
    cur["process_name"] = cur.get("process_name", "").astype(str).fillna("")
    cur["robot_name"] = cur.get("robot_name", "").astype(str).fillna("")
    cur["desc"] = cur.get("desc", "").astype(str).fillna("")
    cur["details"] = cur.get("details", "").astype(str).fillna("")
    cur["stable_key"] = cur.apply(lambda r: build_alert_key(r["type"], r["process_name"], r["robot_name"], r.get("key","")), axis=1)

    # --- prev keys ---
    prev = pd.read_sql("SELECT alert_type, alert_key FROM dbo.alerts WHERE status='active'", engine)

    prev["alert_type"] =  prev["alert_type"].astype(str).apply(canonical_type).str.strip()
    prev["alert_key"]  = prev["alert_key"].astype(str).str.strip()

    prev_keys = set(zip(prev["alert_type"], prev["alert_key"]))
    cur_keys  = set(zip(cur["alert_type"], cur["stable_key"]))

    new_alerts = cur_keys - prev_keys
    resolved   = prev_keys - cur_keys

    # --- UPSERT active alerts (but do NOT spam last_sent here) ---
    with engine.begin() as db:
        for _, a in cur.iterrows():
            db.execute(text("""
                MERGE dbo.alerts AS t
                USING (SELECT :alert_type AS alert_type, :alert_key AS alert_key) AS s
                ON (t.alert_type = s.alert_type AND t.alert_key = s.alert_key)
                WHEN MATCHED THEN
                    UPDATE SET
                        description = :description,
                        status      = 'active',
                        last_seen   = :now_dt
                WHEN NOT MATCHED THEN
                    INSERT (alert_type, alert_key, description, status, first_seen, last_seen, last_sent)
                    VALUES (:alert_type, :alert_key, :description, 'active', :now_dt, :now_dt, NULL);
            """), {
                "alert_type": a["alert_type"],
                "alert_key": a["stable_key"],
                "description": a["desc"],
                "now_dt": now_dt
            })

    # --- resolve missing ---
    with engine.begin() as db:
        for a_type, a_key in resolved:
            db.execute(text("""
                UPDATE dbo.alerts
                SET status='resolved', last_seen=:now_dt
                WHERE alert_type=:t AND alert_key=:k
            """), {"now_dt": now_dt, "t": a_type, "k": a_key})

    # --- insert events only for NEW and RESOLVED ---
    sev, src = resolve_severity_source(a["process_name"], map_df)

    with engine.begin() as db:
        # NEW -> active events
        for _, a in cur.iterrows():
            key_tuple = (a["alert_type"], a["stable_key"])
            if key_tuple in new_alerts:
                sev, src = resolve_severity_source(a["process_name"], map_df)
                db.execute(text("""
                    INSERT INTO dbo.alerts_events(
                        event_time, alert_type, alert_key, status,
                        description, details, severity, source,
                        process_name, robot_name
                    ) VALUES (
                        :event_time, :alert_type, :alert_key, :status,
                        :description, :details, :severity, :source,
                        :process_name, :robot_name
                    )
                """), {
                    "event_time": now_dt,
                    "alert_type": a["alert_type"],
                    "alert_key": a["stable_key"],
                    "status": "active",
                    "description": a["desc"],
                    "details": a["details"],
                    "severity": sev,
                    "source": src,
                    "process_name": a["process_name"] or "UNKNOWN",
                    "robot_name": a["robot_name"] or "UNKNOWN",
                })

        # RESOLVED -> resolved events
        for a_type, a_key in resolved:
                sev, src, proc, rob = get_last_active_event_info(engine, a_type, a_key, logger)            
                db.execute(text("""
                INSERT INTO dbo.alerts_events(
                    event_time, alert_type, alert_key, status,
                    description, details, severity, source,
                    process_name, robot_name
                ) VALUES (
                    :event_time, :alert_type, :alert_key, :status,
                    :description, :details, :severity, :source,
                    :process_name, :robot_name
                )
            """), {
                "event_time": now_dt,
                "alert_type": a_type,
                "alert_key": a_key,
                "status": "resolved",
                "description": "Resolved (not detected in current run)",
                "details": "--resolved",
                "severity": sev,
                "source": src,
                "process_name": proc,
                "robot_name": rob,
            })

    logger.info(f"DB sync done | new={len(new_alerts)} resolved={len(resolved)} active_now={len(cur_keys)}")
    return now_dt, cur, new_alerts, resolved


        # Ensure unified columns exist
def ensure_alert_columns(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=["type","key","desc","details","timestamp","process_name","robot_name"])
    for c in ["process_name","robot_name","desc","details","key","type","timestamp"]:
        if c not in df.columns:
            df[c] = ""
    return df
# ==============================
# MAIN LOGIC
# ==============================
# -----------------------------------------------------------------------------
# Monitoring cycle orchestration
# -----------------------------------------------------------------------------
# 1. Load secure external configuration
# 2. Authenticate with Orchestrator
# 3. Extract operational signals
# 4. Normalize and merge alerts
# 5. Persist lifecycle state in SQL
# 6. Send notifications only when needed
# -----------------------------------------------------------------------------
def run_monitor():
    """Execute one monitoring cycle: extract signals, sync alert state, and send notifications."""
    engine = None
    fileHandler = None

    try:
        # Read the config file path from an environment variable or fallback demo path
        config_path = os.getenv("RPA_MONITOR_CONFIG_PATH", "./config/config.xlsx")

        # ===== Load Config =====
        config = load_settings(config_path, 'Settings', 'Name', 'Value')

        logFilePath = (
            config['logFilePath']
            + "log_Alarm_Uipath/"
            + f"Fault_Automation_Alarm_Uipath__"
            + datetime.now().strftime('%m-%d-%Y.log')
        )

        formatter = create_formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileHandler = create_log_file_handler(logFilePath, formatter)

        # ===== Setup Loggers =====
        for n in ['___main___', '___Init___', '___Processing___']:
            logging.getLogger(n).handlers.clear()

        logger_main = logging.getLogger('___main___')
        logger_init = logging.getLogger('___Init___')
        logger = logging.getLogger('___Processing___')

        for lg in [logger_main, logger_init, logger]:
            lg.setLevel(logging.DEBUG)
            lg.addHandler(fileHandler)
            lg.propagate = False

        logger_main.info("=======================================")
        logger_main.info("Fault Automation Alarm Started")
        logger_main.info("=======================================")

        # ===== INIT SQL SERVER =====
        engine = create_sql_engine(config)
        init_sql_db(engine, logger)

        now_dt = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None)

        # ===== AUTH =====
        token = authenticate(config, logger)

        # ===== FETCH DATA =====
        process_triggers_df = get_process_triggers(config, logger, token)
        
        alerts_df = get_alerts(config, logger, token)
        stale_df = get_stale_jobs(config, logger, token)
        map_df = load_process_mapping(config, logger)
        offline_recovery_minutes = int(config.get("OfflineRecoveryMinutes", 10))
        browser_error_lookback_minutes = int(config.get("BrowserErrorLookbackMinutes", 30))
        fault_lookback_hours = int(config.get("FaultLookbackHours", 12))
        reminder_minutes = int(config.get("ReminderMinutes", 120))

        offline_df = get_offline_robots_unified(alerts_df, logger, offline_recovery_minutes)
        browser_err_df = get_browser_extension_error_jobs(config, logger, token, browser_error_lookback_minutes)
        faulted_df = get_faulted_process_robot_unresolved(config, logger, token, map_df, fault_lookback_hours)




        stale_df = ensure_alert_columns(stale_df)
        offline_df = ensure_alert_columns(offline_df)
        browser_err_df = ensure_alert_columns(browser_err_df)
        faulted_df = ensure_alert_columns(faulted_df)
        # Combine all detected operational issues into one normalized alert dataset
        current_alerts = pd.concat(
            [offline_df, stale_df, browser_err_df, faulted_df],
            ignore_index=True
        ).fillna("")

        # ===== Build stable alert_key =====
        def build_key(row):
            return build_alert_key(
                row.get("type",""),
                row.get("process_name",""),
                row.get("robot_name",""),
                row.get("key","")
            )

        current_alerts["stable_key"] = [
    build_alert_key(t, p, r, k)
    for t, p, r, k in zip(
        current_alerts["type"],
        current_alerts["process_name"],
        current_alerts["robot_name"],
        current_alerts["key"]
    )
]


        # ===== DB SYNC (ONE SOURCE OF TRUTH) =====
        now_dt, cur, new_alerts, resolved = db_sync_alerts(engine, current_alerts, map_df, logger_main)
        
        # normalize key tuples for should_send_email (lower)
        new_norm = set((t.lower(), k.lower()) for (t, k) in new_alerts)
        res_norm = set((t.lower(), k.lower()) for (t, k) in resolved)
        # Send immediately for new or resolved alerts, otherwise send periodic reminders for long-lived active alerts
        send_it, keys_to_mark = should_send_email(engine, now_dt, new_norm, res_norm, reminder_minutes=reminder_minutes)

        logger_main.info("SQL DB sync completed successfully.")

        # ===== EMAIL (keep your existing) =====
        offline_count = len(offline_df)
        stale_count = len(stale_df)
        browser_err_count = len(browser_err_df)
        faulted_count = len(faulted_df)
        
        if offline_count == 0 and stale_count == 0 and browser_err_count == 0 and faulted_count == 0:
            subject = "UiPath Monitoring – All Clear ✅"
        else:
            subject = (f"UiPath Monitoring – "
                       f"{offline_count} offline robot(s), "
                       f"{stale_count} stale job(s), "
                       f"{browser_err_count} browser error(s), "
                       f"{faulted_count} faulted job(s)")
        
        # Build HTML sections
        html_parts = ["<p>Dear Team,</p>"]
        
        html_parts.append("<h3>Offline Robots (≥10 minutes)</h3>")
        if offline_count:
            html_parts.append(pd.DataFrame(offline_df).to_html(index=False, border=1, justify='center'))
        else:
            html_parts.append("<p>No offline robots detected.</p>")
        
        html_parts.append(f"<h3>Stale Running Jobs (>30 minutes inactivity )</h3>")
        
        if not stale_df.empty:
            html_parts.append(stale_df.to_html(index=False, border=1, justify='center'))
        else:
            html_parts.append("<p>No stale running jobs detected.</p>")
        html_parts.append("<h3>Browser Extension Errors (last 30 min)</h3>")
        if not browser_err_df.empty:
            html_parts.append(browser_err_df.to_html(index=False, border=1, justify='center'))
        else:
            html_parts.append("<p>No browser-extension errors detected.</p>")
        html_parts.append("<h3>Faulted Jobs (recent)</h3>")
        if not faulted_df.empty:
            html_parts.append(faulted_df.to_html(index=False, border=1, justify='center'))
        else:
            html_parts.append("<p>No faulted jobs detected.</p>")
        html_parts.append("<p>Full details in the attached snapshot (sheets: <i>Alerts</i>, <i>Stale_Running_Jobs</i>).</p>")
        html_parts.append("<p>Regards,<br>RPA Monitoring System 🤖</p>")
        combined_body = "\n".join(html_parts)

        if send_it and (offline_count or stale_count or browser_err_count or faulted_count):
            send_email(config, subject, combined_body)
        
            # After sending: update last_sent ONLY for what we decided to mark
            # IMPORTANT: keys_to_mark is lowercased tuples, but DB stores pretty case.
            # We'll re-fetch active alerts and mark those that match case-insensitively.
        
            active = fetch_active_alerts(engine)
            if not active.empty:
                active["alert_type_c"] = active["alert_type"].astype(str).apply(canonical_type).str.strip()
                active["alert_key_c"]  = active["alert_key"].astype(str).str.strip()
        
                to_mark = []
                for _, r in active.iterrows():
                    tup = (r["alert_type_c"].lower(), r["alert_key_c"].lower())
                    if tup in keys_to_mark:
                        to_mark.append((r["alert_type"], r["alert_key"]))  # original stored values
        
                mark_alerts_sent(engine, now_dt, to_mark)
        


        logger_main.info("Monitoring cycle completed successfully.")

    except Exception as e:
        if fileHandler:
            logger = logging.getLogger('___main___')
            logger.error(f"FATAL ERROR: {e}")
            logger.exception("Full traceback:")

    finally:
        try:
            if engine:
                engine.dispose()
        except Exception:
            pass

        if fileHandler:
            fileHandler.close()
            logging.shutdown()
            
            
if __name__ == "__main__":
    run_monitor()