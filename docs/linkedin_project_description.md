# LinkedIn Project Description

## Title
NYC Yellow Taxi — End-to-End Azure Data Engineering Pipeline

## Short description (under Featured projects)
Built a production-grade Medallion Architecture pipeline on Azure:
ADF ingestion → Databricks PySpark (Bronze→Silver) → dbt Gold layer.
Cleaned 36M+ monthly trip records, quarantined bad data with reason codes,
and delivered 5 analytics-ready Gold tables with dbt tests and Airflow orchestration.

---

## Long description (for LinkedIn post announcing the project)

Just shipped a full data engineering portfolio project — NYC Yellow Taxi pipeline on Azure.

Here's what the pipeline actually does:

INGEST: Azure Data Factory pulls monthly Parquet files from the NYC TLC API
into ADLS Gen2 Bronze. Parameterised pipeline handles any month dynamically.

TRANSFORM (Bronze → Silver) via PySpark on Databricks:
→ Merged 12 monthly files with source tagging
→ Caught date infiltrations (Jan 2024 file had rows from 2009 and 2023 mixed in)
→ Validated causality (pickup before dropoff), negative fares, missing location IDs
→ Deduplicated using ROW_NUMBER() window — deterministic, auditable
→ Engineered 10+ new columns: trip_duration_min, avg_speed_mph, tip_pct, is_weekend
→ Joined zone lookup twice (pickup + dropoff) to enrich with borough and zone names
→ Wrote rejected rows to a quarantine Delta table with typed reason codes
→ OPTIMIZE + ZORDER on pickup_datetime + borough for query performance

MODEL (Silver → Gold) via dbt:
→ dim_date: 366-row calendar with US holidays, WoW/MTD helpers
→ dim_zone: 265 TLC zones with airport and major hub flags
→ fact_trips: 36M row partitioned fact table with FK integrity tests
→ agg_hourly_zone: demand heatmap — revenue and trips per hour per zone
→ agg_daily_borough: daily KPIs with WoW % change and MTD window functions
→ agg_payment_trends: cash vs credit trends and tipping behaviour by borough

ORCHESTRATE: Airflow DAG schedules the full run monthly, polls ADF and
Databricks, runs dbt tests as a quality gate, and alerts on failure.

Tech stack: Python · PySpark · dbt · Delta Lake · ADF · ADLS Gen2 · Databricks · Airflow · Azure

GitHub: [your-github-link]

#DataEngineering #Azure #PySpark #dbt #Databricks #Airflow #DeltaLake
