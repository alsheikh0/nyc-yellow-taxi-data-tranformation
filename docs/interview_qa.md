# Interview Q&A — NYC Taxi Data Engineering Project

These are the questions a mid-level DE interviewer will ask when you present
this project. Every answer references the actual code you built.

---

## Architecture & Design

**Q1. Walk me through your end-to-end pipeline architecture.**

> The pipeline has four layers. Raw Parquet files from the NYC TLC website land
> in ADLS Gen2 Bronze via an ADF Copy Activity that's parameterised by year and
> month. A Databricks PySpark job reads those files, runs six transformation
> steps, and writes a clean Delta table to the Silver container partitioned by
> `pickup_month`. dbt then reads that Silver table through a staging view and
> produces the Gold layer — two dimension tables, one fact table, and three
> pre-aggregated tables. The whole thing is orchestrated by an Airflow DAG that
> runs on the 3rd of every month, since TLC data has a roughly two-month lag.

---

**Q2. Why did you choose the Medallion Architecture? What does each layer actually give you?**

> Bronze is immutable — it's the raw source of truth. If I make a mistake in
> Silver, I can reprocess from Bronze without going back to the API. Silver is
> where I apply business logic: date validation, causality checks, deduplication,
> and feature engineering. It's the layer data scientists can query safely.
> Gold is optimised for specific business questions — pre-aggregated by hour,
> zone, and borough — so dashboards hit a ~100K-row agg table instead of 36
> million fact rows. The separation also means I can rerun just the Gold models
> in dbt without rerunning the expensive PySpark job.

---

**Q3. Why Delta Lake instead of plain Parquet for Silver and Gold?**

> Delta gives me three things Parquet alone cannot. First, ACID transactions —
> if the Silver write fails halfway through, I don't end up with a corrupt table.
> Second, time travel — I can query `SELECT * FROM delta.\`path\` VERSION AS OF 3`
> to see what Silver looked like before last month's run, which is invaluable for
> debugging. Third, `OPTIMIZE` with `ZORDER BY` — I ZORDER Silver on
> `pickup_datetime` and `pickup_borough` because those are the two most common
> filter columns in dashboard queries. Without ZORDER, Spark reads all Parquet
> files even for a narrow date range. With it, it skips irrelevant files using
> the Delta log statistics.

---

## PySpark & Transformations

**Q4. What was the hardest transformation problem in this dataset and how did you handle it?**

> Date infiltration. The January 2024 file contains rows from 2009, 2023, and
> February 2024 mixed in with genuine January rows. If you just filter
> `WHERE YEAR = 2024` without checking which file a row came from, you silently
> drop data you should keep and keep data you should drop. My approach was to
> tag each row with `source_month` extracted from the filename using
> `input_file_name()` and `regexp_extract()`, then separately validate
> `YEAR(pickup_datetime) = 2024`. That way the rejection reason is recorded
> clearly and I can audit exactly how many rows came from each source file.

---

**Q5. How did you handle deduplication? Why not just `dropDuplicates()`?**

> Plain `dropDuplicates()` is non-deterministic — when there are two identical
> rows it picks one arbitrarily, and you can't control which. I used a window
> function: `ROW_NUMBER() OVER (PARTITION BY pickup_datetime, dropoff_datetime,
> PULocationID, DOLocationID, fare_amount ORDER BY source_month)` and kept only
> `row_num = 1`. This is deterministic — it always keeps the row from the
> earliest source month, which is the original submission rather than a late
> update. It also means I can explain exactly which row survived and why.

---

**Q6. You wrote a quarantine table. Why not just filter bad rows silently?**

> Silent drops are a data quality debt that compounds. If Silver has 5% fewer
> rows than Bronze this month and nobody knows why, analysts lose trust in the
> data. The quarantine table gives me three things: an audit trail showing exactly
> which rows were rejected and why, a count I can put in the pipeline run log to
> monitor rejection rate over time, and the ability to reprocess specific records
> if the business rules change. The `reject_reason` column uses a compound
> `WHEN` chain so each row has exactly one labelled reason — I can query
> `GROUP BY reject_reason` to see whether rejections are spiking in a particular
> category.

---

**Q7. Why did you partition Silver by `pickup_month`?**

> Two reasons. Dashboard queries almost always filter to a recent time window —
> "show me last month's data" — so partitioning means Spark only reads 1/12 of
> the files instead of all of them. Second, when I run the pipeline in
> incremental mode I write `mode="append"` to the Silver table. Without
> partitioning, appending a new month would require Spark to scan all existing
> data to avoid writing duplicates. With month-level partitioning, the new
> month's files land in a new partition directory and don't interact with
> existing ones at all.

---

## dbt & SQL

**Q8. What is the role of the staging layer in dbt? Why not just read Silver directly in Gold models?**

> The staging model is a view — zero compute cost — that does two things.
> First, it aliases raw column names like `PULocationID` and `tpep_pickup_datetime`
> to readable names like `pickup_location_id` and `pickup_datetime`. Second, it
> generates the surrogate key using `dbt_utils.generate_surrogate_key()`. If the
> Silver schema ever changes — say TLC renames a column — I fix it in one place
> (the staging model) and all four Gold models automatically pick up the change.
> Without staging, a column rename would require updating every Gold model
> individually.

---

**Q9. Explain the window functions you used in `agg_daily_borough`.**

> Three window functions, all over `PARTITION BY pickup_borough ORDER BY pickup_date`.
> First, `LAG(total_trips, 7)` to get last week's trip count for the same borough
> on the same day of week — the offset is 7 because I want the same weekday, not
> just seven rows back. The WoW % change is then `(current - prior) / prior * 100`.
> Second, `SUM(total_trips) OVER (...ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
> for the running MTD total — I reset it per `pickup_month` by adding that to the
> partition clause. Third, `RANK() OVER (PARTITION BY pickup_date ORDER BY total_trips DESC)`
> to rank boroughs by trip volume on each specific date. RANK not ROW_NUMBER because
> if two boroughs tie on a given date I want them both ranked 1.

---

**Q10. How do your dbt tests create a data quality safety net?**

> I have two layers of tests. Schema tests in `schema.yml` run automatically on
> every `dbt test` — `unique` and `not_null` on primary keys, `relationships`
> tests that enforce FK integrity between `fact_trips` and both dimension tables,
> and `accepted_values` on `payment_type_label`. These catch structural problems.
> Then I have two singular tests — custom SQL queries that return rows only if
> something is wrong. `assert_no_negative_fares` catches any fares that slipped
> through Silver cleaning. `assert_pickup_before_dropoff` catches causality
> violations. dbt treats any row returned from a singular test as a test failure,
> so the Airflow DAG will mark the `run_dbt_tests` task failed and send an alert
> before bad data reaches analysts.

---

## Orchestration & Production

**Q11. Why does the Airflow DAG run on the 3rd of every month, not the 1st?**

> TLC publishes data with roughly a two-month lag, and publication doesn't happen
> on the exact 1st — it varies. Running on the 3rd gives a buffer so the previous
> month's file is reliably available. The `trigger_adf` task also calculates the
> target month as `logical_date - 60 days`, so if the DAG runs on March 3rd it
> targets January's data. If TLC hasn't published yet, the ADF Copy Activity
> will fail, the `poll_adf` task will retry up to 30 times at 5-minute intervals,
> and if it ultimately fails the `notify_failure` task fires an email before the
> DAG marks itself failed.

**Q12. What does `max_active_runs=1` do and why is it important here?**

> It prevents two instances of the DAG from running simultaneously. Without it,
> if a backfill is running and the scheduled run triggers, both jobs would try
> to write to the same Silver Delta partition at the same time. Delta Lake handles
> concurrent writes with optimistic concurrency control, but the `OVERWRITE` mode
> I use in full-rebuild runs would cause one job to overwrite the other's output.
> One active run at a time eliminates that race condition entirely.

---

## Azure & Cloud

**Q13. How does Databricks connect to ADLS Gen2 securely? You mentioned Managed Identity.**

> In the Databricks cluster configuration I assign a Managed Identity to the
> cluster and grant that identity the `Storage Blob Data Contributor` role on the
> ADLS Gen2 account via Azure RBAC. The code then uses the `abfss://` protocol
> which triggers Managed Identity authentication automatically — no credentials
> are stored in the notebook or passed via environment variables. The alternative
> is Service Principal with a client secret stored in Azure Key Vault and
> referenced via a Databricks secret scope, which is also secure but adds more
> moving parts. Managed Identity is simpler when the cluster and storage are in
> the same Azure subscription.

---

**Q14. How would you make this pipeline handle schema evolution — e.g. if TLC adds a new column?**

> Delta Lake handles this natively via schema evolution. I use
> `.option("overwriteSchema", "true")` on the Silver write, which allows schema
> changes on full rebuilds. For incremental runs, Delta's `mergeSchema` option
> will automatically add new columns from incoming data without breaking existing
> rows — existing rows get `NULL` for the new column. In dbt I set
> `on_schema_change = 'sync_all_columns'` in the `fact_trips` config, which
> tells dbt to add new columns to the Gold table rather than failing. The
> staging model is the single place that maps Silver columns to Gold names, so
> if TLC renames a column I fix it in `stg_silver_trips.sql` only.

---

**Q15. How would you extend this project to handle real-time data instead of monthly batch?**

> The Medallion Architecture supports streaming natively. I would replace the
> ADF monthly copy with an Event Hub or Kafka topic that receives trip records
> in near real-time from the TLC feed. The Databricks job would become a
> Structured Streaming job using `spark.readStream` with a `foreach` sink to
> the Silver Delta table — Delta's transaction log makes streaming writes ACID-
> safe. The dbt Gold layer would stay as-is but run on a shorter schedule —
> hourly instead of monthly. The `agg_hourly_zone` model is actually designed
> for this: it aggregates at the hour + zone grain, which is exactly the
> granularity you need for a live demand dashboard.
