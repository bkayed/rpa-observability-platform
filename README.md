# 🚀 RPA Observability Platform

An end-to-end **Automation Observability & Incident Management Platform** designed to monitor, analyze, and manage RPA processes across UiPath  environment.

---

## 📌 Overview

This project provides a **centralized monitoring classification layer** that:

* Detects failures and anomalies in RPA processes
* Classifies alerts based on business severity
* Generates and manages tickets automatically
* Tracks full alert lifecycle (NEW → ACTIVE → RESOLVED)
* Builds a complete **process health intelligence layer**
* Feeds operational dashboards (Power BI / ELK)

---

## 🧠 Architecture

The platform is built around **three core engines**:

### 1️⃣ Alert Engine

* Extracts data from:

  * UiPath Orchestrator API / DB
* Detects:

  * Faulted jobs
  * Stale jobs
  * Robot offline
  * Browser errors
* Normalizes alerts into a unified format

---

### 2️⃣ Ticket Engine

* Applies business rules:

  * Severity (CRITICAL / MAJOR / MINOR)
  * Threshold-based escalation
* Generates tickets automatically
* Tracks lifecycle:

  * NEW → ACTIVE → RESOLVED
* Stores tickets in SQL for persistence
* Produces agent-ready outputs

---

### 3️⃣ Process Health Engine

* Builds full inventory of all processes
* Enriches with:

  * Business mapping
  * Scheduling logic (cron parsing)
  * Queue activity
  * Execution history
* Calculates:

  * Health score
  * Process state (Healthy / Warning / At Risk / Stopped)
* Outputs datasets for dashboards and analytics

---

## 📊 Key Features

* 🔍 **Real-time alert monitoring**
* 🧾 **Automated ticket generation**
* 📈 **Process health scoring**
* 🔄 **Incremental event processing**
* 🧠 **SLA-aware classification**
* 🗂 **Full process inventory (not only active ones)**
* ⚙️ **Config-driven architecture (no hardcoding)**
* 🔗 **Multi-source integration (API + DB + Logs)**

---

## 📂 Project Structure

```
rpa-observability-platform/
│
├── src/
│   ├── alert_monitor.py        # Alert detection & normalization
│   ├── ticket_engine.py        # Ticket lifecycle & rules engine
│   └── process_health.py       # Process health ETL & analytics
│
├── config/
│   └── config.xlsx             # Environment & system configuration
│
├── data/
│   └── business_mapping.xlsx   # Business severity mapping
│
├── outputs/
│   ├── process_health/
│   └── tickets/
│
├── docs/
│   └── architecture.png        # (to be added)
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚙️ Configuration

All sensitive and environment-specific values are externalized:

* SQL Server connection
* Orchestrator API URLs
* Output paths
* Logging directories

Example:

```ini
SQL_Server=xxxx
SQL_Database=xxxx
SQL_UID=xxxx
SQL_PWD=xxxx
BASE_DATA_LAKE=./data_lake
```

---

## ▶️ How to Run

### 1️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

---

### 2️⃣ Run Alert Engine

```bash
python src/alert_monitor.py
```

---

### 3️⃣ Run Ticket Engine

```bash
python src/ticket_engine.py
```

---

### 4️⃣ Run Process Health ETL

```bash
python src/process_health.py
```

---

## 📊 Outputs

The platform generates:

* `UiPath_Process_Health.csv`
* `Queues_Summary.csv`
* `tickets_to_agent.txt`
* `tickets_history.csv`

These can be used in:

* Power BI dashboards
* ELK stack
* Internal monitoring tools

---

## 🏢 Real-World Use Case

Designed and implemented for enterprise RPA environments to:

* Reduce incident detection time
* Improve automation reliability
* Provide full visibility over automation landscape
* Enable proactive monitoring instead of reactive fixes

---

## 🔮 Future Enhancements

* 🔔 Real-time streaming (Kafka / Webhooks)
* 📊 Advanced anomaly detection (ML models)
* 🤖 LLM-based root cause analysis
* 🌐 Web dashboard (Flask / React)
* 🔗 Integration with ServiceNow / Jira

---

## 👤 Author

**Batool Kayed**
AI Engineer – LLM & Automation

---

## ⭐ If you find this useful

Give the repo a star ⭐ and feel free to connect!
