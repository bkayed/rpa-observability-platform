"""
Ticket engine module.

This module reads active alerts from the monitoring database, applies severity
and repetition-based ticketing rules, opens or resolves tickets, and produces
history and agent-facing outputs for downstream technical workflows.
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime
import pytz
import urllib.parse
from sqlalchemy import create_engine, text

# Local timezone used for ticket timestamps and state transitions
LOCAL_TIMEZONE = pytz.timezone("Asia/Amman")

# =========================================================
# CONFIG + LOGGER HELPERS (same style you use)
# =========================================================
def load_settings(config_path, sheet_name, key_column, value_column):
    """Load key-value settings from an Excel configuration sheet."""
    df = pd.read_excel(config_path, sheet_name=sheet_name, dtype=str)
    df[key_column] = df[key_column].astype(str).str.strip()
    df[value_column] = df[value_column].astype(str).str.strip()
    return df.set_index(key_column)[value_column].to_dict()

def create_formatter(_format_string):
    return logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def create_log_file_handler(log_file_path, formatter):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    return file_handler

def setup_loggers(log_file_path):
    formatter = create_formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler = create_log_file_handler(log_file_path, formatter)

    for name in ["___main___", "___Init___", "___Processing___"]:
        logging.getLogger(name).handlers.clear()

    logger_main = logging.getLogger("___main___")
    logger_init = logging.getLogger("___Init___")
    logger_process = logging.getLogger("___Processing___")

    for logger in [logger_main, logger_init, logger_process]:
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            logger.addHandler(file_handler)
        logger.propagate = False

    return file_handler, logger_main, logger_init, logger_process
CANONICAL_TYPES = {
    "robotoffline": "RobotOffline",
    "stalejob": "StaleJob",
    "browsererror": "BrowserError",
    "faultedjob": "FaultedJob",
    "taskschedulerfailed": "TaskSchedulerFailed",
}

def canonical_type(t: str) -> str:
    raw = (t or "").strip()
    if not raw:
        return ""
    key = raw.replace(" ", "").replace("_", "").lower()
    return CANONICAL_TYPES.get(key, raw)
# =========================================================
# SQL ENGINE
# =========================================================
def create_sql_engine(config):
    """Create a SQLAlchemy engine using externalized SQL Server settings."""    
    DB_Driver = config.get("SQL_Driver", "ODBC Driver 17 for SQL Server")
    Server = config["SQL_Server"]
    Database = config["SQL_Database"]
    UID = config["SQL_UID"]
    PWD = config["SQL_PWD"]

    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_Driver}}};"
        f"SERVER={Server};"
        f"DATABASE={Database};"
        f"UID={UID};"
        f"PWD={PWD};"
        "TrustServerCertificate=yes;"
        "Connection Timeout=60;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=1800
    )
    return engine
# =========================================================
# init tickets table 
# =========================================================

def init_ticket_tables(engine, logger_process):
    """
    Initialize the SQL ticket table used to persist open and resolved alarm-driven tickets.
    """
    
    with engine.begin() as conn:
        conn.execute(text("""
        IF OBJECT_ID('dbo.tickets','U') IS NULL
        CREATE TABLE dbo.tickets (
            ticket_id INT IDENTITY(1,1) PRIMARY KEY,
            alert_type NVARCHAR(100) NOT NULL,
            alert_key  NVARCHAR(300) NOT NULL,
            source NVARCHAR(50) NOT NULL,
            severity NVARCHAR(50) NOT NULL,
            process_name NVARCHAR(200) NOT NULL,
            robot_name NVARCHAR(200) NOT NULL,
            status NVARCHAR(20) NOT NULL,
            ticket_reason NVARCHAR(MAX) NULL,
            down_count_24h INT NULL,
            ticket_threshold_count INT NULL,
            first_seen DATETIME2 NOT NULL,
            last_seen  DATETIME2 NOT NULL,
            opened_at  DATETIME2 NOT NULL,
            resolved_at DATETIME2 NULL,
            last_down_time DATETIME2 NULL,
            last_resolved_time DATETIME2 NULL,
            CONSTRAINT UQ_ticket UNIQUE(alert_type, alert_key)
        );
        """))
    logger_process.info("Tickets table ready.")
    
# =========================================================
# load currently open tickets from DB
# =========================================================
def get_open_ticket_keys(engine) -> set:
    q = """
    SELECT alert_type, alert_key
    FROM dbo.tickets
    WHERE status = 'Open'
    """
    df = pd.read_sql(q, engine)
    df["alert_type"] = df["alert_type"].astype(str).apply(canonical_type).str.strip()
    df["alert_key"]  = df["alert_key"].astype(str).str.strip()
    return set((df["alert_type"] + "|" + df["alert_key"]).tolist())

# =========================================================
# upsert OPEN tickets
# =========================================================
def upsert_open_tickets(engine, new_open_df: pd.DataFrame, logger_process):
    if new_open_df is None or new_open_df.empty:
        return 0

    now = datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for _, r in new_open_df.iterrows():
        rows.append({
            "alert_type": r["alert_type"],
            "alert_key": r["alert_key"],
            "source": r["source"],
            "severity": r["severity"],
            "process_name": r["process_name"],
            "robot_name": r["robot_name"],
            "ticket_reason": r.get("ticket_reason", ""),
            "down_count_24h": int(r.get("down_count_24h", 0)),
            "ticket_threshold_count": int(r.get("ticket_threshold_count", 0)),
            "first_seen": now,
            "last_seen": now,
            "opened_at": now,
            "last_down_time": r.get("last_down_time", None),
        })

    sql = text("""
    MERGE dbo.tickets AS T
    USING (SELECT
        :alert_type AS alert_type,
        :alert_key AS alert_key
    ) AS S
    ON (T.alert_type = S.alert_type AND T.alert_key = S.alert_key)
    WHEN MATCHED THEN
        UPDATE SET
            status = 'Open',
            last_seen = :last_seen,
            source = :source,
            severity = :severity,
            process_name = :process_name,
            robot_name = :robot_name,
            ticket_reason = :ticket_reason,
            down_count_24h = :down_count_24h,
            ticket_threshold_count = :ticket_threshold_count,
            last_down_time = :last_down_time,
            resolved_at = NULL
    WHEN NOT MATCHED THEN
        INSERT (alert_type, alert_key, source, severity, process_name, robot_name,
                status, ticket_reason, down_count_24h, ticket_threshold_count,
                first_seen, last_seen, opened_at, last_down_time)
        VALUES (:alert_type, :alert_key, :source, :severity, :process_name, :robot_name,
                'Open', :ticket_reason, :down_count_24h, :ticket_threshold_count,
                :first_seen, :last_seen, :opened_at, :last_down_time);
    """)

    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)

    logger_process.info(f"Upserted OPEN tickets: {len(rows)}")
    return len(rows)
def get_current_active_alerts(engine, logger_process):
    try:
        q = """
        SELECT
            alert_type,
            alert_key,
            description,
            status
        FROM dbo.alerts
        WHERE LOWER(LTRIM(RTRIM(status))) = 'active'
        """
        df = pd.read_sql(q, engine)

        if df.empty:
            logger_process.info("No current active alerts found in dbo.alerts.")
            return df

        df["alert_type"] = df["alert_type"].astype(str).apply(canonical_type).str.strip()
        df["alert_key"] = df["alert_key"].astype(str).str.strip()

        logger_process.info(f"Current active alerts fetched: {len(df)} rows")
        return df

    except Exception as e:
        logger_process.error(f"get_current_active_alerts failed: {e}", exc_info=True)
        return pd.DataFrame()
# =========================================================
# update RESOLVED tickets
# =========================================================
def resolve_tickets(engine, resolved_df: pd.DataFrame, logger_process):
    """Mark previously open tickets as resolved using the latest resolved alert events."""
    if resolved_df is None or resolved_df.empty:
        return 0
    df = resolved_df.copy()
    logger_process.info(f"Resolved ticket candidates received: {len(df)}")
    df["alert_type"] = df["alert_type"].astype(str).apply(canonical_type).str.strip()
    df["alert_key"]  = df["alert_key"].astype(str).str.strip()

    sql = text("""
UPDATE dbo.tickets
SET
    status = 'Resolved',
    resolved_at = :resolved_at,
    last_resolved_time = :resolved_at,
    last_seen = :resolved_at
WHERE
    LOWER(LTRIM(RTRIM(alert_type))) = LOWER(:alert_type)
    AND LTRIM(RTRIM(alert_key)) = :alert_key
    AND status = 'Open'
""")

    with engine.begin() as conn:
        for _, r in df.iterrows():
            resolved_at = pd.to_datetime(r.get("resolved_time"), errors="coerce")

            if pd.isna(resolved_at):
                resolved_at = datetime.now(LOCAL_TIMEZONE)
            else:
                resolved_at = resolved_at.tz_localize(None) if resolved_at.tzinfo else resolved_at

            resolved_at = resolved_at.strftime("%Y-%m-%d %H:%M:%S")

    logger_process.info(f"Resolved tickets updated: {len(df)}")
    return len(df)



# =========================================================
# UNIFIED CATALOG (same as yours, keep)
# =========================================================
def load_unified_catalog(mapping_path: str, logger_process) -> pd.DataFrame:
    frames = []

    # UiPath catalog
    try:
        df_u = pd.read_excel(mapping_path, sheet_name="ProcessSeverity")
        df_u.columns = [c.strip() for c in df_u.columns]

        df_u["process_name"] = df_u["process_name"].astype(str).str.strip()
        df_u["source"] = df_u.get("source", "UIPATH_PROCESS").astype(str).str.upper().str.strip()
        df_u["severity"] = df_u.get("severity", "MAJOR").astype(str).str.upper().str.strip()

        df_u["robot_name"] = ""  # wildcard
        df_u["expected_runs_per_day"] = pd.to_numeric(df_u.get("expected_runs_per_day", 1), errors="coerce").fillna(1).astype(int)
        df_u["critical_once_daily"] = df_u.get("critical_once_daily", "NO").astype(str).str.upper().str.strip()
        df_u["ticket_threshold_count"] = pd.to_numeric(df_u.get("ticket_threshold_count", 3), errors="coerce").fillna(3).astype(int)

        df_u = df_u[["source", "process_name", "robot_name", "severity",
                     "critical_once_daily", "ticket_threshold_count", "expected_runs_per_day"]]
        frames.append(df_u)
        logger_process.info(f"Loaded ProcessSeverity: {len(df_u)} rows")
    except Exception as e:
        logger_process.warning(f"Could not load ProcessSeverity sheet: {e}")

    # Task Scheduler catalog
    try:
        df_t = pd.read_excel(mapping_path, sheet_name="ProcessCatalog")
        df_t.columns = [c.strip() for c in df_t.columns]

        df_t["process_name"] = df_t["process_name"].astype(str).str.strip()
        df_t["robot_name"] = df_t.get("robot_name", "").astype(str).str.strip()
        df_t["source"] = df_t.get("source", "TASK_SCHEDULER").astype(str).str.upper().str.strip()
        df_t["severity"] = df_t.get("severity", "MAJOR").astype(str).str.upper().str.strip()

        df_t["expected_runs_per_day"] = pd.to_numeric(df_t.get("expected_runs_per_day", 1), errors="coerce").fillna(1).astype(int)
        df_t["critical_once_daily"] = df_t.get("critical_once_daily", "NO").astype(str).str.upper().str.strip()
        df_t["ticket_threshold_count"] = pd.to_numeric(df_t.get("ticket_threshold_count", 3), errors="coerce").fillna(3).astype(int)

        df_t = df_t[["source", "process_name", "robot_name", "severity",
                     "critical_once_daily", "ticket_threshold_count", "expected_runs_per_day"]]
        frames.append(df_t)
        logger_process.info(f"Loaded ProcessCatalog: {len(df_t)} rows")
    except Exception as e:
        logger_process.warning(f"Could not load ProcessCatalog sheet: {e}")

    if not frames:
        logger_process.error("Unified catalog is empty (both sheets failed).")
        return pd.DataFrame(columns=[
            "source", "process_name", "robot_name", "severity",
            "critical_once_daily", "ticket_threshold_count", "expected_runs_per_day"
        ])

    catalog = pd.concat(frames, ignore_index=True).fillna("")
    logger_process.info(f"Unified catalog total rows: {len(catalog)}")
    return catalog

# =========================================================
# 1) DOWN COUNTS (MUST include alert_type + alert_key)
# =========================================================
def get_down_count_last_24h(engine, logger_process):
    try:
        query = """
        SELECT
            alert_type,
            alert_key,
            process_name,
            robot_name,
            UPPER(LTRIM(RTRIM(source))) AS source,
            COUNT(*) AS down_count_24h,
            MAX(event_time) AS last_down_time
        FROM dbo.alerts_events
        WHERE status = 'active'
          AND event_time >= DATEADD(HOUR, -24, SYSDATETIME())
        GROUP BY alert_type, alert_key, process_name, robot_name, source
        """
        df = pd.read_sql(query, engine)
        logger_process.info(f"Down counts fetched (last 24h): {len(df)} rows")
        return df
    except Exception as e:
        logger_process.error(f"get_down_count_last_24h failed: {e}", exc_info=True)
        return pd.DataFrame()

# =========================================================
# 2) APPLY RULES BEFORE WRITING (returns tickets_to_open)
# =========================================================
def build_tickets_df(engine, catalog_df: pd.DataFrame, logger_process):
    """
    Build the set of tickets that should be opened based on current active alerts,
    alert repetition counts, and business severity rules.
    """    
    counts = get_down_count_last_24h(engine, logger_process)
    active_alerts = get_current_active_alerts(engine, logger_process)

    if active_alerts.empty:
        logger_process.info("No current active alerts. No tickets to keep open.")
        return pd.DataFrame()

    # normalize counts
    if counts is None or counts.empty:
        counts = pd.DataFrame(columns=[
            "alert_type", "alert_key", "process_name", "robot_name",
            "source", "down_count_24h", "last_down_time"
        ])

    for c in ["source", "process_name", "robot_name", "alert_type", "alert_key"]:
        if c in counts.columns:
            counts[c] = counts[c].fillna("").astype(str).str.strip()
    if "source" in counts.columns:
        counts["source"] = counts["source"].str.upper()

    # normalize catalog
    catalog = catalog_df.copy()
    for c in ["source", "process_name", "robot_name"]:
        if c in catalog.columns:
            catalog[c] = catalog[c].fillna("").astype(str).str.strip()
    catalog["source"] = catalog["source"].str.upper()

    # counts joined only for CURRENTLY ACTIVE alerts
    base_current = active_alerts.merge(
        counts,
        on=["alert_type", "alert_key"],
        how="left",
        suffixes=("", "_cnt")
    )

    # fallback values
    for col in ["process_name", "robot_name", "source"]:
        cnt_col = f"{col}_cnt"
        if cnt_col in base_current.columns:
            base_current[col] = base_current.get(col, "").replace("", pd.NA).fillna(base_current[cnt_col])

    if "down_count_24h" not in base_current.columns:
        base_current["down_count_24h"] = 1
    else:
        base_current["down_count_24h"] = pd.to_numeric(base_current["down_count_24h"], errors="coerce").fillna(1).astype(int)

    # clean blanks
    for c in ["process_name", "robot_name", "source"]:
        if c not in base_current.columns:
            base_current[c] = ""
        base_current[c] = base_current[c].fillna("").astype(str).str.strip()

    base_current["source"] = base_current["source"].str.upper()

    # Task Scheduler strict join
    c_task = base_current[base_current["source"] == "TASK_SCHEDULER"].copy()
    cat_task = catalog[catalog["source"] == "TASK_SCHEDULER"].copy()
    tickets_task = pd.DataFrame()
    if not c_task.empty and not cat_task.empty:
        tickets_task = c_task.merge(cat_task, on=["source", "process_name", "robot_name"], how="left")

    # UiPath join process+source only
    c_ui = base_current[base_current["source"] == "UIPATH_PROCESS"].copy()
    cat_ui = catalog[catalog["source"] == "UIPATH_PROCESS"].copy()
    tickets_ui = pd.DataFrame()
    if not c_ui.empty and not cat_ui.empty:
        tickets_ui = c_ui.merge(cat_ui.drop(columns=["robot_name"]), on=["source", "process_name"], how="left")

    base = pd.concat([tickets_task, tickets_ui], ignore_index=True).fillna("")
    if base.empty:
        logger_process.info("No rows after joining current active alerts with catalog.")
        return pd.DataFrame()

    base["severity"] = base["severity"].replace("", "MAJOR").astype(str).str.upper().str.strip()
    base["critical_once_daily"] = base["critical_once_daily"].replace("", "NO").astype(str).str.upper().str.strip()
    base["ticket_threshold_count"] = pd.to_numeric(base["ticket_threshold_count"], errors="coerce").fillna(3).astype(int)
    base["down_count_24h"] = pd.to_numeric(base["down_count_24h"], errors="coerce").fillna(1).astype(int)

    cond_critical = (base["critical_once_daily"] == "YES") & (base["severity"] == "MAJOR")
    cond_major_repeated = (base["critical_once_daily"] != "YES") & (base["down_count_24h"] >= base["ticket_threshold_count"])

    base["ticket_required"] = cond_critical | cond_major_repeated
    base["ticket_reason"] = ""
    base.loc[cond_critical, "ticket_reason"] = "CRITICAL daily process down."
    base.loc[cond_major_repeated, "ticket_reason"] = "Down occurred multiple times in last 24h (MAJOR)."

    tickets = base[base["ticket_required"]].copy()
    if tickets.empty:
        logger_process.info("No tickets (rules not met for current active alerts).")
        return pd.DataFrame()

    tickets["generated_at"] = datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    tickets["status"] = "Open"
    tickets["alert_type"] = tickets["alert_type"].astype(str).apply(canonical_type).str.strip()
    tickets["alert_key"] = tickets["alert_key"].astype(str).str.strip()

    logger_process.info(f"Tickets to open after rules (current active only): {len(tickets)}")

    return tickets[[
        "alert_type", "alert_key",
        "source", "severity", "process_name", "robot_name",
        "down_count_24h", "ticket_threshold_count",
        "ticket_reason", "last_down_time", "generated_at", "status"
    ]]
# =========================================================
# 3) RESOLVED EVENTS (incremental by event_id)
# =========================================================
def load_last_event_id(state_path: str, logger_process) -> int:
    try:
        if os.path.exists(state_path):
            with open(state_path, "r", encoding="utf-8") as file:
                value = file.read().strip()
            if value.isdigit():
                last_id = int(value)
                logger_process.info(f"Last event_id loaded: {last_id}")
                return last_id
    except Exception as exc:
        logger_process.warning(f"Could not read last event_id: {exc}")

    logger_process.info("Last event_id missing -> fallback to 0")
    return 0

def save_last_event_id(state_path: str, last_id: int, logger_process):
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        f.write(str(int(last_id)))
    logger_process.info(f"Last event_id saved: {last_id}")

def get_resolved_events_after_id(engine, last_event_id: int, logger_process) -> pd.DataFrame:
    """
    Retrieve newly resolved alert events incrementally using the last processed event ID.
    """

    try:
        q = text("""
        SELECT
            event_id,
            event_time AS resolved_time,
            alert_type,
            alert_key,
            UPPER(LTRIM(RTRIM(ISNULL(source,'')))) AS source,
            UPPER(LTRIM(RTRIM(ISNULL(severity,'MAJOR')))) AS severity,
            ISNULL(NULLIF(LTRIM(RTRIM(process_name)),''),'UNKNOWN') AS process_name,
            ISNULL(NULLIF(LTRIM(RTRIM(robot_name)),''),'UNKNOWN') AS robot_name
        FROM dbo.alerts_events
        WHERE event_id > :last_id
          AND LOWER(LTRIM(RTRIM(status))) = 'resolved'
        ORDER BY event_id ASC
        """)
        df = pd.read_sql(q, engine, params={"last_id": last_event_id})
        if not df.empty:
            df["status"] = "Resolved"
        logger_process.info(f"Resolved events after event_id {last_event_id}: {len(df)}")
        return df
    except Exception as e:
        logger_process.error(f"get_resolved_events_after_id failed: {e}", exc_info=True)
        return pd.DataFrame()
# Optional helper for full high-watermark tracking
def get_max_event_id(engine, logger_process) -> int:
    try:
        q = text("SELECT ISNULL(MAX(event_id), 0) AS max_id FROM dbo.alerts_events")
        df = pd.read_sql(q, engine)
        return int(df.iloc[0]["max_id"])
    except Exception as e:
        logger_process.error(f"get_max_event_id failed: {e}", exc_info=True)
        return 0

# =========================================================
# 4) OPEN STATE (persistent, NOT daily)
# =========================================================
def make_key(alert_type: str, alert_key: str) -> str:
    return f"{str(alert_type).strip()}|{str(alert_key).strip()}"
# JSON state is used for lightweight local tracking (stateless runs),
# while SQL-backed tickets provide the persistent, authoritative lifecycle state
def load_open_state(json_path: str, logger_process) -> set:
    try:
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return set(data)
    except Exception as e:
        logger_process.warning(f"Failed to load open_state.json: {e}")
    return set()

def save_open_state(json_path: str, open_keys: set, logger_process):
    try:
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sorted(list(open_keys)), f, ensure_ascii=False, indent=2)
        logger_process.info(f"Open state saved: {json_path} | open_count={len(open_keys)}")
    except Exception as e:
        logger_process.warning(f"Failed to save open_state.json: {e}")

# =========================================================
# 5) HISTORY + AGENT FILE WRITERS
# =========================================================
def append_ticket_history(new_open_df, new_resolved_df, history_path, logger_process):
    """
    Append newly opened and newly resolved ticket lifecycle entries to the ticket history log.
    """
    os.makedirs(os.path.dirname(history_path), exist_ok=True)

    lines = []

    def _append(df, status_label):
        if df is None or df.empty:
            return
        for _, r in df.iterrows():
            ts = datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
            process_name = str(r.get("process_name", "")).strip()
            source = str(r.get("source", "")).strip()
            robot = str(r.get("robot_name", "")).strip()
            severity = str(r.get("severity", "MAJOR")).strip()
            reason = str(r.get("ticket_reason", "")).strip()

            line = (
                f"{ts} | Alarm text:{process_name} - {source} - {robot} , "
                f"Severity:{severity} , Status:{status_label}"
            )
            # if status_label == "Active" and reason:
            #     line += f" , Reason:{reason}"
            lines.append(line)

    _append(new_open_df, "Active")
    _append(new_resolved_df, "Resolved")

    if not lines:
        logger_process.info("No new history lines to append.")
        return 0

    with open(history_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    logger_process.info(f"History appended: {history_path} | New lines={len(lines)}")
    return len(lines)

def write_agent_txt(open_df, resolved_df, txt_path, logger_process):
    """
    Write a deterministic agent-facing text file summarizing newly active and resolved tickets.
    """
    os.makedirs(os.path.dirname(txt_path), exist_ok=True)

    lines = []

    def _write(df, status_label):
        if df is None or df.empty:
            return
        for _, r in df.iterrows():
            process_name = str(r.get("process_name", "")).strip()
            source = str(r.get("source", "")).strip()
            robot = str(r.get("robot_name", "")).strip()
            severity = str(r.get("severity", "MAJOR")).strip()
            lines.append(
                f"Alarm text:{process_name} - {source} - {robot} , "
                f"Severity:{severity} , "
                f"Status:{status_label}"
            )

    _write(open_df, "Active")
    _write(resolved_df, "Resolved")

    # ✅ overwrite every run (deterministic)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger_process.info(f"Agent TXT written: {txt_path} | Open={0 if open_df is None else len(open_df)} | Close={0 if resolved_df is None else len(resolved_df)}")

# =========================================================
# 6) STATE APPLICATION (decide NEW_OPEN / NEW_RESOLVED)
# =========================================================
def apply_state_and_prepare_outputs(tickets_to_open_df, resolved_events_df, open_state_keys, logger_process):
    """
    Compare the current ticket candidates and resolved events against the persisted open state
    to determine newly opened and newly resolved ticket outputs.
    """
    # --- new open: rule-based opens not already open
    new_open = pd.DataFrame()
    if tickets_to_open_df is not None and not tickets_to_open_df.empty:
        tickets_to_open_df = tickets_to_open_df.copy()
        tickets_to_open_df["__k"] = tickets_to_open_df.apply(lambda r: make_key(r["alert_type"], r["alert_key"]), axis=1)
        new_open = tickets_to_open_df[~tickets_to_open_df["__k"].isin(open_state_keys)].copy()

    # --- new resolved: resolved events whose key is currently open
    new_resolved = pd.DataFrame()
    if resolved_events_df is not None and not resolved_events_df.empty:
        resolved_events_df = resolved_events_df.copy()
        resolved_events_df["__k"] = resolved_events_df.apply(lambda r: make_key(r["alert_type"], r["alert_key"]), axis=1)
        new_resolved = resolved_events_df[resolved_events_df["__k"].isin(open_state_keys)].copy()

    # update state
    if not new_open.empty:
        open_state_keys.update(new_open["__k"].tolist())
    if not new_resolved.empty:
        for k in new_resolved["__k"].tolist():
            open_state_keys.discard(k)

    logger_process.info(f"NEW_OPEN={len(new_open)} | NEW_RESOLVED={len(new_resolved)} | OPEN_STATE_NOW={len(open_state_keys)}")

    # drop helper key
    for df in [new_open, new_resolved]:
        if df is not None and not df.empty and "__k" in df.columns:
            df.drop(columns=["__k"], inplace=True)

    return new_open, new_resolved, open_state_keys

# Compare current ticket candidates against the previously open set to derive newly opened and newly resolved tickets
def apply_state_from_current_snapshot(tickets_to_open_df, open_state_keys, logger_process):
    current_open_df = pd.DataFrame()
    current_open_keys = set()

    if tickets_to_open_df is not None and not tickets_to_open_df.empty:
        current_open_df = tickets_to_open_df.copy()
        current_open_df["__k"] = current_open_df.apply(
            lambda r: make_key(r["alert_type"], r["alert_key"]), axis=1
        )
        current_open_keys = set(current_open_df["__k"].tolist())

    new_open_keys = current_open_keys - open_state_keys
    resolved_keys = open_state_keys - current_open_keys

    new_open_df = pd.DataFrame()
    if not current_open_df.empty:
        new_open_df = current_open_df[current_open_df["__k"].isin(new_open_keys)].copy()

    new_resolved_df = pd.DataFrame()
    if resolved_keys:
        parts = []
        for k in resolved_keys:
            a_type, a_key = k.split("|", 1)
            parts.append({
                "alert_type": a_type,
                "alert_key": a_key,
                "resolved_time": datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                "status": "Resolved"
            })
        new_resolved_df = pd.DataFrame(parts)

    logger_process.info(
        f"NEW_OPEN={len(new_open_df)} | NEW_RESOLVED={len(new_resolved_df)} | OPEN_STATE_NOW={len(current_open_keys)}"
    )


    if not new_open_df.empty and "__k" in new_open_df.columns:
        new_open_df.drop(columns=["__k"], inplace=True)

    return new_open_df, new_resolved_df, current_open_keys
# =========================================================
# MAIN
# =========================================================

def run_ticket_engine():
    fileHandler = None
    engine = None
    # Read configuration from an environment variable or fallback demo path
    config_path = os.getenv("RPA_TICKET_ENGINE_CONFIG_PATH", "./config/config.xlsx")

    try:
        config = load_settings(config_path, "Settings", "Name", "Value")
        required_keys = [
        "SQL_Server",
        "SQL_Database",
        "SQL_UID",
        "SQL_PWD",
        "SEVERITY_MAPPING_PATH",
        "logFilePath",
    ]

        missing = [k for k in required_keys if k not in config or str(config[k]).strip() == ""]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")
        # ✅ flags from config (proper booleans)
        debug_excel = str(config.get("DebugExcel", "TRUE")).strip().upper() == "TRUE"
        use_db_tickets = str(config.get("UseDbTickets", "TRUE")).strip().upper() == "TRUE"

        log_file_path  = os.path.join(
            config["logFilePath"],"log_Ticket_Writer",f"Ticket_WriterV2__{datetime.now().strftime('%m-%d-%Y')}.log")
        fileHandler, logger_main, logger_init, logger_process = setup_loggers(log_file_path )

        logger_main.info("=======================================")
        logger_main.info("Ticket Writer Started (Rule-Based History + Resolved from DB)")
        logger_main.info("=======================================")

        mapping_xlsx = config["SEVERITY_MAPPING_PATH"]
        output_dir = config.get("OutputDir", "").strip() or "./outputs"
        os.makedirs(output_dir, exist_ok=True)

        # outputs
        history_txt = os.path.join(output_dir, "tickets_history.txt")      # append lifecycle
        agent_txt = os.path.join(output_dir, "tickets_to_agent.txt")  # overwrite every run

        # states
        open_state_json   = os.path.join(output_dir, "ticket_writer_open_state.json")     # persistent
        last_event_id_path = os.path.join(output_dir, "ticket_writer_last_event_id.txt")  # incremental resolved

        # connect db
        engine = create_sql_engine(config)

        # 1) Catalog
        catalog_df = load_unified_catalog(mapping_xlsx, logger_process)

        # ✅ for debug: raw down counts
        down_counts_df = get_down_count_last_24h(engine, logger_process)

        # 2) Rule-based opens
        tickets_to_open_df = build_tickets_df(engine, catalog_df, logger_process)

        # 3) Resolved incremental
        last_id = load_last_event_id(last_event_id_path, logger_process)
        resolved_df = get_resolved_events_after_id(engine, last_id, logger_process)
        

        # advance last_id to max in DB each run (safe)
        # max_id = get_max_event_id(engine, logger_process)
        # save_last_event_id(last_event_id_path, max_id, logger_process)
        
        new_last_id = last_id
        if resolved_df is not None and not resolved_df.empty:
            new_last_id = int(resolved_df["event_id"].max())
        save_last_event_id(last_event_id_path, new_last_id, logger_process)

        # 4) Apply JSON open_state (test mode)
        open_keys = load_open_state(open_state_json, logger_process)
        new_open_df, new_resolved_df, open_keys = apply_state_and_prepare_outputs(
            tickets_to_open_df, resolved_df, open_keys, logger_process
        )
        #new_open_df, new_resolved_df, open_keys =  apply_state_from_current_snapshot(tickets_to_open_df, open_keys, logger_process)
        save_open_state(open_state_json, open_keys, logger_process)

        # 4B) DB Ticket Mode
        new_open_db_df = pd.DataFrame()
        new_resolved_db_df = pd.DataFrame()

        if use_db_tickets:
            logger_process.info("DB Ticket Mode ENABLED")

            init_ticket_tables(engine, logger_process)

            # Load DB open tickets
            open_keys_db = get_open_ticket_keys(engine)

            # Apply state against DB open tickets
            # new_open_db_df, new_resolved_db_df, _ = apply_state_and_prepare_outputs(
            #     tickets_to_open_df, resolved_df, open_keys_db, logger_process 
            # )
            new_open_db_df, new_resolved_db_df, _ = apply_state_from_current_snapshot(tickets_to_open_df, open_keys_db, logger_process)

            # Update persistent SQL ticket state
            upsert_open_tickets(engine, new_open_db_df, logger_process)
            resolve_tickets(engine, new_resolved_db_df, logger_process)
            

        else:
            logger_process.info("DB Ticket Mode DISABLED (JSON only)")

        # 5) Write history (append only NEW)
        append_ticket_history(new_open_df, new_resolved_df, history_txt, logger_process)

        # 6) Write agent file (overwrite each run)
        
        write_agent_txt(new_open_df, new_resolved_df, agent_txt, logger_process)

        # =====================================================
        # DEBUG EXCEL
        # =====================================================
        if debug_excel:
            try:
                debug_dir = os.path.join(output_dir, "DEBUG_TICKETS")
                os.makedirs(debug_dir, exist_ok=True)

                debug_path = os.path.join(
                    debug_dir,
                    f"DEBUG_TICKETS_{datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
                )

                with pd.ExcelWriter(debug_path, engine="openpyxl") as writer:

                    if not catalog_df.empty:
                        catalog_df.to_excel(writer, sheet_name="UnifiedCatalog", index=False)

                    if down_counts_df is not None and not down_counts_df.empty:
                        down_counts_df.to_excel(writer, sheet_name="DownCounts_Last24h", index=False)

                    if tickets_to_open_df is not None and not tickets_to_open_df.empty:
                        tickets_to_open_df.to_excel(writer, sheet_name="Tickets_To_Open_Raw", index=False)

                    if resolved_df is not None and not resolved_df.empty:
                        resolved_df.to_excel(writer, sheet_name="Resolved_Events_Raw", index=False)

                    if new_open_df is not None and not new_open_df.empty:
                        new_open_df.to_excel(writer, sheet_name="New_Open_JSON", index=False)

                    if new_resolved_df is not None and not new_resolved_df.empty:
                        new_resolved_df.to_excel(writer, sheet_name="New_Resolved_JSON", index=False)

                    if use_db_tickets:
                        if new_open_db_df is not None and not new_open_db_df.empty:
                            new_open_db_df.to_excel(writer, sheet_name="New_Open_DB", index=False)

                        if new_resolved_db_df is not None and not new_resolved_db_df.empty:
                            new_resolved_db_df.to_excel(writer, sheet_name="New_Resolved_DB", index=False)

                        # current DB open state
                        open_keys_db_current = get_open_ticket_keys(engine)
                        if open_keys_db_current:
                            pd.DataFrame({"open_key": list(open_keys_db_current)}).to_excel(writer, sheet_name="OpenState_DB", index=False)

                    if open_keys:
                        pd.DataFrame({"open_key": list(open_keys)}).to_excel(writer, sheet_name="OpenState_JSON", index=False)

                logger_main.info(f"Debug Excel written: {debug_path}")

            except Exception as e:
                logger_process.error(f"Debug Excel failed: {e}", exc_info=True)

        logger_main.info("Ticket Writer Finished ✅")

    except Exception as e:
        try:
            logging.getLogger('___main___').error(f"FATAL in Ticket Writer: {e}", exc_info=True)
        except Exception:
            print("FATAL in Ticket Writer:", e)

    finally:
        if engine:
            try:
                engine.dispose()
            except Exception:
                pass
        if fileHandler:
            try:
                fileHandler.close()
            except Exception:
                pass
            logging.shutdown()
            
            
            
if __name__ == "__main__":
    run_ticket_engine()