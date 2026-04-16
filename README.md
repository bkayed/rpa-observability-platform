# rpa-observability-platform

A Python-based monitoring and observability platform for RPA  UiPath processes.

## Overview
This project monitors RPA process execution, detects operational issues, classifies alert severity, tracks alert lifecycle, and generates ticket-ready outputs for technical teams.

## Key Features
- Monitor RPA process failures and operational anomalies
- Detect robot offline, stale jobs, browser communication errors, and unresolved faulted jobs
- Persist alert lifecycle (NEW / ACTIVE / RESOLVED)
- Open and resolve tickets based on severity and repeat thresholds
- Build process health using schedules, queue activity, and execution history
- Support secure config-driven API/DB connectivity
- Run in demo mode using dummy data

## Project Modules
- `alert_monitor.py`: alert extraction and lifecycle tracking
- `ticket_engine.py`: ticket decision logic
- `process_health.py`: process health and master sheet generation
- `db.py`: database helpers
- `config_loader.py`: config/env loading

## Security
This public version does not include internal credentials, real server names, or production data.

## Demo Inputs
Sample data is included in `data/raw/` and `config/`.

## Future Enhancements
- predictive alerting
- LLM-based root cause summaries
- dashboard integration
