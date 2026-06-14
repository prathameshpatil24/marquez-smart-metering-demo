# Marquez — Data Lineage in Action
### Practical Demo: Tracking a Smart Metering Pipeline with OpenLineage

> **Applied Programming Exam** — FOM Hochschule Essen  
> **Topic:** Marquez (Data Lineage Platform, Observability Layer)  
> **Student:** Prathamesh Patil (Mat. Nr. 838598)  
> **Program:** M.Sc. Big Data & Business Analytics  
> **Semester:** Summer Semester 2026

---

## Overview

This project demonstrates **Marquez** — the reference implementation of **OpenLineage** — by building a real-world data pipeline that processes actual energy consumption data, runs data quality checks, and emits lineage events to Marquez at every stage.

Unlike typical Marquez tutorials that only send fake metadata via curl commands, this demo:

1. **Processes real data** — 43,000+ readings from the UCI ML Repository
2. **Runs 7 data quality checks** to detect actual sensor anomalies
3. **Tracks lineage end-to-end** — every stage emits OpenLineage events with schema, SQL, and quality facets
4. **Demonstrates failure lineage** — corrupted data triggers a FAIL event, and Marquez shows the blast radius of downstream datasets that were never produced

## Dataset

**UCI Individual Household Electric Power Consumption**  
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/235)  
- **Size:** 2,075,259 minute-by-minute readings (126 MB)  
- **Period:** December 2006 – November 2010  
- **Location:** Sceaux, France  
- **Features:** Active/reactive power, voltage, current intensity, 3 sub-metering circuits  
- **Natural quality issues:** ~1.25% missing values from real sensor gaps  

The notebook uses the **last 30 days** (43,201 readings) for the demo pipeline.

## Pipeline Architecture

```
[UCI Raw Data]
      │
      ▼
┌─────────────────────────┐
│  Stage 1: INGEST        │  Read CSV, parse timestamps
│  Job: ingest-meter-     │  
│       readings          │  
└────────────┬────────────┘
             │
      [staged_readings]
             │
             ▼
┌─────────────────────────┐
│  Stage 2: VALIDATE      │  7 data quality checks
│  Job: validate-readings │  ← ❌ FAILS here if error
│                         │     rate exceeds 20%
└────────────┬────────────┘
             │
      [validated_readings]
             │
             ▼
┌─────────────────────────┐
│  Stage 3: TRANSFORM     │  Calculate energy (kWh),
│  Job: calculate-        │  cost (EUR), CO₂ (kg)
│       consumption-      │  
│       metrics           │  
└────────────┬────────────┘
             │
      [consumption_metrics]
             │
             ▼
┌─────────────────────────┐
│  Stage 4: AGGREGATE     │  Daily sustainability
│  Job: generate-         │  report by building
│       sustainability-   │  
│       report            │  
└────────────┬────────────┘
             │
      [daily_sustainability_report]
```

## Data Quality Checks (Stage 2)

| # | Check | What it catches | Threshold |
|---|-------|----------------|-----------|
| 1 | Null `Global_active_power` | Missing primary reading | Any null |
| 2 | Null `Voltage` | Missing voltage measurement | Any null |
| 3 | Null sub-metering values | Incomplete circuit data | Any null |
| 4 | Negative power readings | Physically impossible | < 0 kW |
| 5 | Voltage out of range | Grid anomaly / sensor fault | < 200V or > 260V |
| 6 | Power exceeds 11 kW | Residential overload | > 11 kW |
| 7 | Invalid timestamps | Parse failure / corrupt data | Any null |

**Rule:** If more than **20%** of rows have issues → pipeline **FAILS** and emits a `FAIL` event to Marquez with detailed error message.

## Two Pipeline Runs

| Run | Data | Result | What Marquez Shows |
|-----|------|--------|-------------------|
| Run 1 | Real UCI data (clean) | ✅ All 4 stages COMPLETE | Full lineage graph, green runs |
| Run 2 | Corrupted data (injected faults) | ❌ FAILS at Stage 2 | Red FAIL event with error details, missing downstream datasets |

### Injected Faults (Run 2)

- **30% null** `Global_active_power` values
- **20 negative** power readings
- **15 voltage spikes** (280–400V range)
- **Error rate: ~38%** → exceeds 20% threshold → FAIL

## Transformation Logic (Stage 3)

| Metric | Formula | Value |
|--------|---------|-------|
| Energy (kWh) | `Global_active_power (kW) × 1/60` | Per-minute reading |
| Cost (EUR) | `energy_kwh × €0.35` | German electricity price |
| CO₂ (kg) | `energy_kwh × 0.38` | German grid mix factor |

## OpenLineage Concepts Demonstrated

| Concept | Where in Demo |
|---------|---------------|
| `START` event | Emitted when each stage begins |
| `COMPLETE` event | Emitted when a stage succeeds |
| `FAIL` event | Emitted when validation fails with error details |
| Schema facet | Column names + types on every dataset |
| SQL facet | Transformation query attached to each job |
| Data quality facet | Row counts on output datasets |
| Data source facet | UCI Repository URI on raw input |
| Error message facet | Detailed failure reason on FAIL events |
| Namespace | All jobs grouped under `smart-metering-pipeline` |
| Lineage graph | Full DAG visible in Marquez Web UI |

## Prerequisites

- **Docker Desktop** (v17.05+) with Docker Compose
- **Python 3.8+** with pip
- **~2 GB** free disk space for container images
- **~150 MB** for the UCI dataset (auto-downloaded by the notebook)

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/prathameshpatil24/marquez-smart-metering-demo.git
cd marquez-smart-metering-demo

# 2. Start Marquez (PostgreSQL + API + Web UI)
docker-compose up -d

# 3. Install Python dependencies
pip install pandas requests matplotlib

# 4. Run the notebook
jupyter notebook smart_metering_pipeline.ipynb

# 5. Run all cells (Shift+Enter or Kernel → Restart & Run All)

# 6. Explore lineage in the Marquez UI
open http://localhost:3000

# 7. Cleanup when done
docker-compose down -v
```

## What to Explore in the Marquez UI

1. **Search `generate-sustainability-report`** → Full pipeline lineage graph (4 stages, all green)
2. **Search `validate-readings-faulty`** → Failed run (red) with error message facet
3. **Click any dataset** → See schema (column names, types) and which jobs read/write it
4. **Click `ingest-meter-readings`** → See two runs (clean + faulty data)
5. **Compare lineage graphs** → Clean run has all downstream datasets; faulty run stops at validation

## Project Structure

```
marquez-smart-metering-demo/
├── docker-compose.yml              # Marquez stack (API + Web UI + PostgreSQL)
├── smart_metering_pipeline.ipynb   # Main demo notebook (all pipeline stages)
├── README.md                       # This file
├── .gitignore                      # Excludes UCI dataset + outputs
├── data/
│   └── household_power_consumption.txt  # UCI dataset (auto-downloaded, gitignored)
└── output/                         # Generated by the notebook
    ├── consumption_metrics.csv
    ├── daily_sustainability_report.csv
    ├── hourly_consumption_pattern.csv
    └── energy_dashboard.png
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Lineage Platform | Marquez (v0.49+) |
| Lineage Standard | OpenLineage (v1.0.5) |
| Metadata Storage | PostgreSQL 14 |
| Pipeline | Python 3.9 + pandas |
| Visualization | matplotlib |
| Infrastructure | Docker + Docker Compose |
| Web UI | Marquez Web (React) |

## Sources

- [Marquez Project](https://marquezproject.ai/)
- [OpenLineage Specification](https://openlineage.io/)
- [OpenLineage Getting Started](https://openlineage.io/getting-started/)
- [MarquezProject/marquez — GitHub](https://github.com/MarquezProject/marquez)
- [LF AI & Data Foundation — Marquez](https://lfaidata.foundation/projects/marquez/)
- [UCI Individual Household Electric Power Consumption Dataset](https://archive.ics.uci.edu/dataset/235)
