# Marquez — Smart Metering Pipeline Demo

Practical demo for **Applied Programming (FOM Hochschule Essen)** showing how [Marquez](https://marquezproject.ai/) tracks data lineage across a 4-stage energy data pipeline with data quality monitoring.

## Quick Start

1. Start Marquez: `docker-compose up -d`
2. Open notebook: `jupyter notebook smart_metering_pipeline.ipynb`
3. Run all cells
4. Explore lineage: http://localhost:3000

## What it does

- Ingests real UCI energy data (2M+ readings)
- Runs 7 data quality checks
- Calculates consumption cost and CO2 emissions
- Emits OpenLineage events to Marquez at every stage
- Demonstrates FAIL events for corrupted sensor data

## Stack

Marquez - OpenLineage - Python - pandas - Docker - PostgreSQL
