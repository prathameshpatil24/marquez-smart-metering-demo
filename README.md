# Marquez Demo — Smart Metering Pipeline with Data Quality Monitoring

> **Applied Programming Exam** — FOM Hochschule Essen  
> **Topic:** Marquez (Data Lineage Platform, Observability Layer)  
> **Student:** Prathamesh Patil (Nr. 28)

## Overview

This demo implements a **real data processing pipeline** for IoT smart meter readings (heat, water, electricity) that:

1. **Processes actual CSV data** — not just fake metadata events
2. **Runs 7 data quality checks** at the validation stage
3. **Emits OpenLineage events** to Marquez at every stage
4. **Demonstrates failure lineage** — when faulty sensor data is detected, the pipeline fails and Marquez records the failure with error details and blast radius

## Pipeline Architecture

```
                                    ┌─────────────────┐
                                    │  raw_meter_      │
                                    │  readings (CSV)  │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                          Stage 1   │  INGEST           │
                                    │  Read CSV data    │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │  staged_readings  │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                          Stage 2   │  VALIDATE         │  ← ❌ Fails here if
                                    │  7 quality checks │     error rate > 20%
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │  validated_       │
                                    │  readings         │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                          Stage 3   │  TRANSFORM        │
                                    │  Cost + CO₂ calc  │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │  consumption_     │
                                    │  metrics          │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                          Stage 4   │  AGGREGATE        │
                                    │  Building report  │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │  building_        │
                                    │  sustainability_  │
                                    │  report           │
                                    └─────────────────┘
```

## Data Quality Checks (Stage 2)

| # | Check | What it catches |
|---|-------|----------------|
| 1 | Null `meter_id` | Unidentified sensor |
| 2 | Null `building_id` | Orphaned reading |
| 3 | Null `reading_value` | Missing measurement |
| 4 | Negative readings | Physically impossible |
| 5 | Reading > 10,000 | Sensor malfunction |
| 6 | Future timestamps | Clock sync error |
| 7 | Invalid `reading_type` | Unknown meter type |

**Rule:** If more than 20% of rows have issues → pipeline **FAILS** and emits a `FAIL` event to Marquez with the error details.

## Two Demo Runs

| Run | Input File | Expected Result |
|-----|-----------|-----------------|
| Run 1 | `data/meter_readings_clean.csv` | ✅ All 4 stages COMPLETE |
| Run 2 | `data/meter_readings_faulty.csv` | ❌ FAILS at Stage 2 (Validate) |

### Faulty Data Issues (meter_readings_faulty.csv)

- 1 row with **null meter_id**
- 1 row with **null building_id**
- 1 row with **null reading_value**
- 2 rows with **negative readings** (-45.2, -120.0)
- 2 rows with **impossibly high readings** (99999.0, 72000.0)
- 1 row with **future timestamp** (2029-01-15)
- 1 row with **invalid reading_type** ("INVALIDTYPE")

**Error rate: ~60% → exceeds 20% threshold → FAIL**

## Prerequisites

- Docker + Docker Compose
- Python 3.8+
- pip (for pandas, requests)

## Quick Start

```bash
# 1. Clone or navigate to this directory
cd marquez-demo

# 2. Run the full demo (starts Marquez + runs both pipelines)
chmod +x run_demo.sh
./run_demo.sh

# 3. Open Marquez UI
open http://localhost:3000

# 4. Cleanup when done
docker-compose down -v
```

## Manual Step-by-Step

```bash
# Start Marquez
docker-compose up -d
# Wait ~20 seconds

# Install Python dependencies
pip install pandas requests

# Run with clean data
python3 pipeline/pipeline.py --input data/meter_readings_clean.csv

# Run with faulty data
python3 pipeline/pipeline.py --input data/meter_readings_faulty.csv

# Open UI
open http://localhost:3000
```

## What to Show in the Presentation

### Screenshot Opportunities

1. **Terminal output** — showing quality checks passing vs failing
2. **Marquez lineage graph** — search `generate-sustainability-report` for full pipeline view
3. **Run history** — showing COMPLETE (green) vs FAIL (red) runs for `validate-readings`
4. **Error details** — click on the failed run to see the error message facet
5. **Schema view** — click on `consumption_metrics` to see enriched schema (cost_eur, co2_kg)
6. **Blast radius** — in the faulty run, `consumption_metrics` and `building_sustainability_report` were never produced

## OpenLineage Concepts Used

| Concept | Implementation |
|---------|---------------|
| `START` event | Emitted when each stage begins |
| `COMPLETE` event | Emitted when a stage succeeds |
| `FAIL` event | Emitted when validation fails (with error message) |
| Schema facet | Column names + types for every dataset |
| SQL facet | The transformation query for each job |
| Data quality facet | Row counts on output datasets |
| Data source facet | Source URI on raw input dataset |
| Error message facet | Detailed failure reason on FAIL events |
| Namespace | All jobs grouped under `smart-metering-pipeline` |

## Project Structure

```
marquez-demo/
├── docker-compose.yml          # Marquez stack (API + Web UI + PostgreSQL)
├── requirements.txt            # Python dependencies
├── run_demo.sh                 # One-click demo runner
├── data/
│   ├── meter_readings_clean.csv    # Valid sensor data (18 rows)
│   └── meter_readings_faulty.csv   # Faulty sensor data (15 rows)
├── pipeline/
│   ├── __init__.py
│   ├── lineage_emitter.py      # OpenLineage event helper class
│   └── pipeline.py             # Main 4-stage pipeline
└── output/                     # Generated reports (after running)
    ├── consumption_metrics.csv
    ├── building_sustainability_report.csv
    └── building_totals.csv
```

## Sources

- [Marquez Project](https://marquezproject.ai/)
- [OpenLineage Specification](https://openlineage.io/)
- [OpenLineage Getting Started Guide](https://openlineage.io/getting-started/)
- [MarquezProject/marquez (GitHub)](https://github.com/MarquezProject/marquez)
- [LF AI & Data Foundation — Marquez](https://lfaidata.foundation/projects/marquez/)
