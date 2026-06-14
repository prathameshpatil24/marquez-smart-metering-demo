#!/usr/bin/env python3
"""
Smart Metering Data Pipeline with Data Quality Monitoring
==========================================================
A 4-stage pipeline that processes IoT smart meter readings,
validates data quality, calculates consumption metrics, and
generates building-level sustainability reports.

Each stage emits OpenLineage events to Marquez, including
FAIL events when data quality checks detect faulty sensor data.

Pipeline:
  INGEST → VALIDATE → TRANSFORM → AGGREGATE

Usage:
  python pipeline.py --input data/meter_readings_clean.csv
  python pipeline.py --input data/meter_readings_faulty.csv
"""

import argparse
import os
import sys
import pandas as pd
from datetime import datetime, timezone

# Add parent dir to path so we can import from pipeline package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.lineage_emitter import LineageEmitter


# ── Configuration ────────────────────────────────────────────────────────────

# Energy prices (EUR) and CO2 factors (kg CO2 per unit)
ENERGY_PRICES = {
    "HEAT": 0.12,         # EUR per kWh
    "ELECTRICITY": 0.35,  # EUR per kWh
    "WATER": 4.50,        # EUR per m³
}

CO2_FACTORS = {
    "HEAT": 0.20,         # kg CO2 per kWh (gas heating)
    "ELECTRICITY": 0.38,  # kg CO2 per kWh (German grid mix 2026)
    "WATER": 0.003,       # kg CO2 per m³
}

# Data quality thresholds
MAX_READING_VALUE = 10000   # Any reading above this = sensor malfunction
MAX_ERROR_RATE = 0.20       # Pipeline fails if >20% of rows have issues
VALID_READING_TYPES = {"HEAT", "WATER", "ELECTRICITY"}


# ── Stage 1: INGEST ─────────────────────────────────────────────────────────

def stage_ingest(input_file, emitter):
    """Read raw meter readings from CSV into a staged DataFrame."""
    job_name = "ingest-meter-readings"
    run_id = emitter.new_run_id()

    print("\n" + "=" * 60)
    print("📥 STAGE 1: INGEST — Reading raw meter data")
    print("=" * 60)

    # ── Emit START event
    emitter.emit_start(
        run_id, job_name,
        input_datasets=[{
            "name": "raw_meter_readings",
            "facets": emitter.merge_facets(
                emitter.schema_facet([
                    ("meter_id", "VARCHAR"),
                    ("building_id", "VARCHAR"),
                    ("unit_id", "VARCHAR"),
                    ("reading_type", "VARCHAR"),
                    ("reading_value", "DOUBLE"),
                    ("reading_unit", "VARCHAR"),
                    ("timestamp", "TIMESTAMP"),
                    ("meter_status", "VARCHAR"),
                ]),
                emitter.datasource_facet(
                    "iot-meter-gateway",
                    f"file://{os.path.abspath(input_file)}"
                ),
            ),
        }],
    )

    # ── Read CSV
    df = pd.read_csv(input_file)
    row_count = len(df)
    file_size = os.path.getsize(input_file)

    print(f"  📄 Source file:  {input_file}")
    print(f"  📊 Rows loaded:  {row_count}")
    print(f"  💾 File size:    {file_size:,} bytes")
    print(f"  📋 Columns:      {', '.join(df.columns)}")

    # ── Emit COMPLETE event
    emitter.emit_complete(
        run_id, job_name,
        input_datasets=[{"name": "raw_meter_readings"}],
        output_datasets=[{
            "name": "staged_readings",
            "facets": emitter.merge_facets(
                emitter.schema_facet([
                    ("meter_id", "VARCHAR"),
                    ("building_id", "VARCHAR"),
                    ("unit_id", "VARCHAR"),
                    ("reading_type", "VARCHAR"),
                    ("reading_value", "DOUBLE"),
                    ("reading_unit", "VARCHAR"),
                    ("timestamp", "TIMESTAMP"),
                    ("meter_status", "VARCHAR"),
                ]),
                emitter.quality_facet(row_count, file_size),
            ),
        }],
    )

    print("  ✅ Ingest complete — data staged successfully")
    return df


# ── Stage 2: VALIDATE ───────────────────────────────────────────────────────

def stage_validate(df, emitter):
    """Run data quality checks on staged meter readings."""
    job_name = "validate-readings"
    run_id = emitter.new_run_id()

    print("\n" + "=" * 60)
    print("🔍 STAGE 2: VALIDATE — Running data quality checks")
    print("=" * 60)

    # ── Emit START event
    emitter.emit_start(
        run_id, job_name,
        input_datasets=[{"name": "staged_readings"}],
    )

    # ── Run quality checks
    issues = []
    total_rows = len(df)

    # Check 1: Null meter IDs
    null_meter = df["meter_id"].isna().sum()
    if null_meter > 0:
        issues.append(f"NULL meter_id: {null_meter} rows")
        print(f"  ❌ Check 1 — Null meter_id:        {null_meter} rows FAILED")
    else:
        print(f"  ✅ Check 1 — Null meter_id:        PASSED")

    # Check 2: Null building IDs
    null_building = df["building_id"].isna().sum()
    if null_building > 0:
        issues.append(f"NULL building_id: {null_building} rows")
        print(f"  ❌ Check 2 — Null building_id:     {null_building} rows FAILED")
    else:
        print(f"  ✅ Check 2 — Null building_id:     PASSED")

    # Check 3: Null or missing reading values
    null_values = df["reading_value"].isna().sum()
    if null_values > 0:
        issues.append(f"NULL reading_value: {null_values} rows")
        print(f"  ❌ Check 3 — Null reading_value:   {null_values} rows FAILED")
    else:
        print(f"  ✅ Check 3 — Null reading_value:   PASSED")

    # Check 4: Negative readings (physically impossible)
    negative = (df["reading_value"].fillna(0) < 0).sum()
    if negative > 0:
        issues.append(f"Negative readings: {negative} rows")
        print(f"  ❌ Check 4 — Negative readings:    {negative} rows FAILED")
    else:
        print(f"  ✅ Check 4 — Negative readings:    PASSED")

    # Check 5: Impossibly high readings (sensor malfunction)
    too_high = (df["reading_value"].fillna(0) > MAX_READING_VALUE).sum()
    if too_high > 0:
        issues.append(f"Readings > {MAX_READING_VALUE}: {too_high} rows (sensor malfunction)")
        print(f"  ❌ Check 5 — Sensor malfunction:   {too_high} rows > {MAX_READING_VALUE} FAILED")
    else:
        print(f"  ✅ Check 5 — Sensor malfunction:   PASSED")

    # Check 6: Future timestamps
    df["_ts"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=False)
    now = pd.Timestamp.now()
    future = (df["_ts"] > now).sum()
    if future > 0:
        issues.append(f"Future timestamps: {future} rows")
        print(f"  ❌ Check 6 — Future timestamps:    {future} rows FAILED")
    else:
        print(f"  ✅ Check 6 — Future timestamps:    PASSED")

    # Check 7: Invalid reading types
    invalid_types = (~df["reading_type"].isin(VALID_READING_TYPES)).sum()
    if invalid_types > 0:
        issues.append(f"Invalid reading_type: {invalid_types} rows")
        print(f"  ❌ Check 7 — Invalid reading_type: {invalid_types} rows FAILED")
    else:
        print(f"  ✅ Check 7 — Invalid reading_type: PASSED")

    # ── Calculate error rate
    # Count rows with ANY issue
    problem_mask = (
        df["meter_id"].isna()
        | df["building_id"].isna()
        | df["reading_value"].isna()
        | (df["reading_value"].fillna(0) < 0)
        | (df["reading_value"].fillna(0) > MAX_READING_VALUE)
        | (df["_ts"] > pd.Timestamp.now())
        | (~df["reading_type"].isin(VALID_READING_TYPES))
    )
    error_count = problem_mask.sum()
    error_rate = error_count / total_rows

    print(f"\n  📊 Quality Summary:")
    print(f"     Total rows:    {total_rows}")
    print(f"     Clean rows:    {total_rows - error_count}")
    print(f"     Problem rows:  {error_count}")
    print(f"     Error rate:    {error_rate:.1%} (threshold: {MAX_ERROR_RATE:.0%})")

    # ── Decision: PASS or FAIL
    if error_rate > MAX_ERROR_RATE:
        error_msg = (
            f"Data quality check FAILED — error rate {error_rate:.1%} exceeds "
            f"threshold {MAX_ERROR_RATE:.0%}. Issues found: {'; '.join(issues)}"
        )
        print(f"\n  🚨 PIPELINE HALTED — Error rate too high!")
        print(f"     {error_msg}")

        # ── Emit FAIL event
        emitter.emit_fail(
            run_id, job_name,
            input_datasets=[{"name": "staged_readings"}],
            error_message=error_msg,
        )
        return None  # Signal failure

    # ── Filter out bad rows and continue
    df_clean = df[~problem_mask].drop(columns=["_ts"]).copy()
    clean_count = len(df_clean)

    print(f"\n  ✅ Validation PASSED — {clean_count} clean rows forwarded")

    # ── Emit COMPLETE event
    emitter.emit_complete(
        run_id, job_name,
        input_datasets=[{"name": "staged_readings"}],
        output_datasets=[{
            "name": "validated_readings",
            "facets": emitter.merge_facets(
                emitter.schema_facet([
                    ("meter_id", "VARCHAR"),
                    ("building_id", "VARCHAR"),
                    ("unit_id", "VARCHAR"),
                    ("reading_type", "VARCHAR"),
                    ("reading_value", "DOUBLE"),
                    ("reading_unit", "VARCHAR"),
                    ("timestamp", "TIMESTAMP"),
                    ("meter_status", "VARCHAR"),
                ]),
                emitter.quality_facet(clean_count),
            ),
        }],
        job_facets=emitter.sql_facet(
            "SELECT * FROM staged_readings "
            "WHERE meter_id IS NOT NULL "
            "AND building_id IS NOT NULL "
            "AND reading_value > 0 "
            f"AND reading_value <= {MAX_READING_VALUE} "
            "AND reading_type IN ('HEAT', 'WATER', 'ELECTRICITY') "
            "AND timestamp <= NOW()"
        ),
    )

    return df_clean


# ── Stage 3: TRANSFORM ──────────────────────────────────────────────────────

def stage_transform(df, emitter):
    """Calculate consumption cost and CO2 emissions per reading."""
    job_name = "calculate-consumption-metrics"
    run_id = emitter.new_run_id()

    print("\n" + "=" * 60)
    print("⚙️  STAGE 3: TRANSFORM — Calculating consumption metrics")
    print("=" * 60)

    # ── Emit START event
    emitter.emit_start(
        run_id, job_name,
        input_datasets=[{"name": "validated_readings"}],
    )

    # ── Calculate cost and CO2
    df["cost_eur"] = df.apply(
        lambda row: round(row["reading_value"] * ENERGY_PRICES.get(row["reading_type"], 0), 2),
        axis=1,
    )

    df["co2_kg"] = df.apply(
        lambda row: round(row["reading_value"] * CO2_FACTORS.get(row["reading_type"], 0), 3),
        axis=1,
    )

    total_cost = df["cost_eur"].sum()
    total_co2 = df["co2_kg"].sum()
    row_count = len(df)

    print(f"  💰 Total cost calculated:  €{total_cost:,.2f}")
    print(f"  🌱 Total CO₂ emissions:    {total_co2:,.2f} kg")
    print(f"  📊 Rows enriched:          {row_count}")

    # ── Save intermediate output
    output_path = "output/consumption_metrics.csv"
    os.makedirs("output", exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"  💾 Saved to: {output_path}")

    # ── Emit COMPLETE event
    emitter.emit_complete(
        run_id, job_name,
        input_datasets=[{"name": "validated_readings"}],
        output_datasets=[{
            "name": "consumption_metrics",
            "facets": emitter.merge_facets(
                emitter.schema_facet([
                    ("meter_id", "VARCHAR"),
                    ("building_id", "VARCHAR"),
                    ("unit_id", "VARCHAR"),
                    ("reading_type", "VARCHAR"),
                    ("reading_value", "DOUBLE"),
                    ("reading_unit", "VARCHAR"),
                    ("timestamp", "TIMESTAMP"),
                    ("meter_status", "VARCHAR"),
                    ("cost_eur", "DOUBLE"),
                    ("co2_kg", "DOUBLE"),
                ]),
                emitter.quality_facet(row_count),
            ),
        }],
        job_facets=emitter.sql_facet(
            "SELECT *, "
            "reading_value * price_per_unit AS cost_eur, "
            "reading_value * co2_factor AS co2_kg "
            "FROM validated_readings v "
            "JOIN energy_prices p ON v.reading_type = p.type"
        ),
    )

    print("  ✅ Transform complete")
    return df


# ── Stage 4: AGGREGATE ──────────────────────────────────────────────────────

def stage_aggregate(df, emitter):
    """Generate building-level sustainability report."""
    job_name = "generate-sustainability-report"
    run_id = emitter.new_run_id()

    print("\n" + "=" * 60)
    print("📊 STAGE 4: AGGREGATE — Building sustainability report")
    print("=" * 60)

    # ── Emit START event
    emitter.emit_start(
        run_id, job_name,
        input_datasets=[{"name": "consumption_metrics"}],
    )

    # ── Aggregate by building and reading type
    report = df.groupby(["building_id", "reading_type"]).agg(
        total_readings=("reading_value", "count"),
        total_consumption=("reading_value", "sum"),
        avg_consumption=("reading_value", "mean"),
        total_cost_eur=("cost_eur", "sum"),
        total_co2_kg=("co2_kg", "sum"),
    ).round(2).reset_index()

    # ── Building-level totals
    building_totals = df.groupby("building_id").agg(
        units_count=("unit_id", "nunique"),
        total_cost_eur=("cost_eur", "sum"),
        total_co2_kg=("co2_kg", "sum"),
    ).round(2).reset_index()

    row_count = len(report)

    print(f"\n  🏢 Building Sustainability Summary:")
    print(f"  {'─' * 55}")
    for _, row in building_totals.iterrows():
        print(
            f"  {row['building_id']:20s} | "
            f"{row['units_count']} units | "
            f"€{row['total_cost_eur']:>8,.2f} | "
            f"{row['total_co2_kg']:>8,.2f} kg CO₂"
        )
    print(f"  {'─' * 55}")
    print(
        f"  {'TOTAL':20s} | "
        f"{building_totals['units_count'].sum()} units | "
        f"€{building_totals['total_cost_eur'].sum():>8,.2f} | "
        f"{building_totals['total_co2_kg'].sum():>8,.2f} kg CO₂"
    )

    # ── Save final report
    report_path = "output/building_sustainability_report.csv"
    report.to_csv(report_path, index=False)

    totals_path = "output/building_totals.csv"
    building_totals.to_csv(totals_path, index=False)

    print(f"\n  💾 Detailed report: {report_path}")
    print(f"  💾 Building totals: {totals_path}")

    # ── Emit COMPLETE event
    emitter.emit_complete(
        run_id, job_name,
        input_datasets=[{"name": "consumption_metrics"}],
        output_datasets=[{
            "name": "building_sustainability_report",
            "facets": emitter.merge_facets(
                emitter.schema_facet([
                    ("building_id", "VARCHAR"),
                    ("reading_type", "VARCHAR"),
                    ("total_readings", "INTEGER"),
                    ("total_consumption", "DOUBLE"),
                    ("avg_consumption", "DOUBLE"),
                    ("total_cost_eur", "DOUBLE"),
                    ("total_co2_kg", "DOUBLE"),
                ]),
                emitter.quality_facet(row_count),
            ),
        }],
        job_facets=emitter.sql_facet(
            "SELECT building_id, reading_type, "
            "COUNT(*) AS total_readings, "
            "SUM(reading_value) AS total_consumption, "
            "AVG(reading_value) AS avg_consumption, "
            "SUM(cost_eur) AS total_cost_eur, "
            "SUM(co2_kg) AS total_co2_kg "
            "FROM consumption_metrics "
            "GROUP BY building_id, reading_type"
        ),
    )

    print("  ✅ Sustainability report generated")
    return report


# ── Main Orchestrator ────────────────────────────────────────────────────────

def run_pipeline(input_file):
    """Run the full 4-stage pipeline with lineage tracking."""
    emitter = LineageEmitter()

    print("\n" + "╔" + "═" * 58 + "╗")
    print("║  Smart Metering Pipeline — Data Quality Monitoring Demo  ║")
    print("╚" + "═" * 58 + "╝")
    print(f"\n  Input: {input_file}")
    print(f"  Time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Stage 1: Ingest
    df = stage_ingest(input_file, emitter)

    # Stage 2: Validate
    df = stage_validate(df, emitter)
    if df is None:
        print("\n" + "🚨" * 20)
        print("  PIPELINE FAILED at VALIDATE stage")
        print("  Downstream datasets NOT produced:")
        print("    ✗ consumption_metrics")
        print("    ✗ building_sustainability_report")
        print("  → Check Marquez UI for failure details and blast radius")
        print("🚨" * 20 + "\n")
        return False

    # Stage 3: Transform
    df = stage_transform(df, emitter)

    # Stage 4: Aggregate
    stage_aggregate(df, emitter)

    print("\n" + "✅" * 20)
    print("  PIPELINE COMPLETED SUCCESSFULLY")
    print("  → Check Marquez UI at http://localhost:3000")
    print("  → Search for 'generate-sustainability-report'")
    print("✅" * 20 + "\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Smart Metering Pipeline with Data Quality Monitoring"
    )
    parser.add_argument(
        "--input", required=True,
        help="Path to meter readings CSV (clean or faulty)"
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"❌ File not found: {args.input}")
        sys.exit(1)

    success = run_pipeline(args.input)
    sys.exit(0 if success else 1)
