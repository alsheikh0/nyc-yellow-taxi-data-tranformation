# NYC Yellox Taxi Data Transformation - PySpark + DBT

End-to-end data engineering project using NYC TLC Yellow Taxi data (2024).
This repository contains the **dbt Gold layer** that sits on top of the PySpark
Bronze→Silver pipeline.

---

## Architecture

```
ADLS Gen2 (bronze/)        ADLS Gen2 (silver/)        ADLS Gen2 (gold/)
  yellow_taxi/*.parquet  →  yellow_taxi/ (Delta)  →  dim_date
  taxi_zone_lookup.csv   →  taxi_zone_lookup/     →  dim_zone
                         →  rejected/ (quarantine) →  fact_trips
                                                   →  agg_hourly_zone
                                                   →  agg_daily_borough
                                                   →  agg_payment_trends
```

**Tools:** ADF · Databricks (PySpark) · Delta Lake · dbt-databricks · Azure ADLS Gen2

---

## Gold Layer Models

### Dimensions
| Model | Rows | Description |
|---|---|---|
| `dim_date` | 366 | Calendar dimension with time-intelligence columns |
| `dim_zone` | 265 | TLC taxi zone hierarchy: zone → borough |

### Facts
| Model | Rows (approx) | Description |
|---|---|---|
| `fact_trips` | ~36M | Trip-grain fact. Partitioned by `pickup_month`. |

### Aggregates
| Model | Grain | Description |
|---|---|---|
| `agg_hourly_zone` | date × hour × zone | Demand heatmap — trips and revenue per hour per zone |
| `agg_daily_borough` | date × borough | Daily KPIs with WoW % change and MTD running totals |
| `agg_payment_trends` | month × borough × weekend × payment | Tipping and payment method mix analysis |

---

## Key Business Questions Answered

- Which pickup zones generate the most revenue between 6–9pm on weekdays?
- Is cash payment declining month-over-month through 2024?
- Which borough had the highest week-over-week trip volume growth in Q3?
- Do airport trips generate higher revenue-per-mile than non-airport trips?
- Which hours on weekends have the longest average trip duration?

---

## dbt Commands

```bash
# Install dependencies
pip install dbt-databricks

# Set environment variables
export DATABRICKS_HOST="<your-workspace>.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<personal-access-token>"

# Run all models
dbt run

# Run only Gold layer
dbt run --select gold

# Run tests
dbt test

# Run tests for Gold only
dbt test --select gold

# Generate and serve docs
dbt docs generate
dbt docs serve

# See model lineage
dbt ls --select +fact_trips   # all upstream deps
dbt ls --select fact_trips+   # all downstream deps
```

---

## Data Quality Tests

Every model has schema tests in `models/gold/schema.yml`:

| Test | Type | What it checks |
|---|---|---|
| `unique` + `not_null` on PKs | Schema test | No duplicate or null keys |
| `relationships` on FKs | Schema test | Every trip joins to a valid date and zone |
| `accepted_values` on categoricals | Schema test | Payment type labels are in the allowed set |
| `assert_no_negative_fares` | Singular test | No negative fares leak into Gold |
| `assert_pickup_before_dropoff` | Singular test | No causality violations reach Gold |

---

## Folder Structure

```
nyc_taxi_dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   └── stg_silver_trips.sql      ← view on Silver Delta table
│   └── gold/
│       ├── schema.yml                ← docs + tests for all Gold models
│       ├── dimensions/
│       │   ├── dim_date.sql
│       │   └── dim_zone.sql
│       ├── facts/
│       │   └── fact_trips.sql
│       └── aggregates/
│           ├── agg_hourly_zone.sql
│           ├── agg_daily_borough.sql
│           └── agg_payment_trends.sql
├── tests/
│   ├── assert_no_negative_fares.sql
│   └── assert_pickup_before_dropoff.sql
└── macros/
    └── revenue_metrics.sql
```

---

## Sample Analytical Queries

### Top 10 pickup zones by revenue on Friday evenings
```sql
select
    pickup_zone,
    pickup_borough,
    sum(total_revenue) as revenue,
    sum(total_trips)   as trips
from gold.agg_hourly_zone
where pickup_hour between 17 and 21
  and pickup_day_of_week = 6   -- Friday (Spark dayofweek: 1=Sun)
group by pickup_zone, pickup_borough
order by revenue desc
limit 10;
```

### Week-over-week trip growth by borough
```sql
select
    pickup_date,
    pickup_borough,
    total_trips,
    trips_wow_pct_change
from gold.agg_daily_borough
where pickup_date >= '2024-06-01'
order by pickup_date, pickup_borough;
```

### Cash vs credit card share by month
```sql
select
    pickup_month,
    payment_type_label,
    sum(trip_count)           as trips,
    avg(payment_type_share_pct) as avg_share_pct
from gold.agg_payment_trends
group by pickup_month, payment_type_label
order by pickup_month, trips desc;
```
