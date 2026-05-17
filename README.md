# Aperture ETL

Python-based ETL pipeline for the Aperture Analytics Platform.

This ETL system pulls analytics data from external systems such as Splunk, processes and transforms the data, and loads it into PostgreSQL for dashboard reporting and insights.

---

# Architecture Overview

ETL Flow:

Splunk / APIs / Logs
        ↓
    Extract
        ↓
   Transform
        ↓
      Load
        ↓
 PostgreSQL
        ↓
 Dashboard APIs
        ↓
 React Dashboard UI

---

# Features

- Pull analytics data from Splunk
- Process heatmaps, crashes, and metrics
- Aggregate and validate records
- Load processed data into PostgreSQL
- Manual ETL execution
- Scheduler/Cron-based execution
- Spring Boot-triggered ETL execution
- Parallel developer execution support

---

# Repository

```bash
https://github.com/2026aperture/aperture-etl
