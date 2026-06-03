

dbutils.widgets.text("target_month", "2024-01", "Target Month (YYYY-MM or 'all')")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"], "Run Mode")

TARGET_MONTH = dbutils.widgets.get("target_month")
MODE         = dbutils.widgets.get("mode")

print(f"target_month = {TARGET_MONTH}")
print(f"mode         = {MODE}")


# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract, lit, when,
    to_timestamp, year, month, hour, dayofweek,
    unix_timestamp, round as spark_round,
    date_format, row_number, current_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName(f"bronze_to_silver_yellow_taxi_{TARGET_MONTH}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Storage paths — update to your ADLS account name
STORAGE_ACCOUNT = "nyctaxistorageacc"   # <-- change this
BRONZE_PATH      = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/yellow_taxi/"
ZONE_PATH        = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/lookup/taxi_zone_lookup.csv"
SILVER_PATH      = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/yellow_taxi/"
QUARANTINE_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/rejected/yellow_taxi/"

# Read Bronze files

if TARGET_MONTH == "all" or MODE == "full":
    file_pattern = BRONZE_PATH + "*.parquet"
    print("Reading ALL monthly files (full rebuild)")
else:
    file_pattern = BRONZE_PATH + f"yellow_tripdata_{TARGET_MONTH}.parquet"
    print(f"Reading single month: {TARGET_MONTH}")

df_raw = (
    spark.read.parquet(file_pattern)
    .withColumn("source_file",  input_file_name())
    .withColumn("source_month", regexp_extract("source_file", r"(\d{4}-\d{2})\.parquet", 1))
    .withColumn("ingested_at",  current_timestamp())
)

raw_count = df_raw.count()
print(f"Raw rows read: {raw_count:,}")


# Parse timestamps + date infiltration filter

df_typed = (
    df_raw
    .withColumn("pickup_datetime",  to_timestamp("tpep_pickup_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
)

df_tagged = df_typed.withColumn(
    "reject_reason",
    when(year("pickup_datetime")  != 2024, lit("date_infiltration: pickup year != 2024"))
   .when(year("dropoff_datetime") != 2024, lit("date_infiltration: dropoff year != 2024"))
   .otherwise(lit(None).cast("string"))
)

df_date_valid    = df_tagged.filter(col("reject_reason").isNull()).drop("reject_reason")
df_date_rejected = df_tagged.filter(col("reject_reason").isNotNull())

date_reject_count = df_date_rejected.count()
print(f"Date infiltration rejects: {date_reject_count:,}")


#  Business rule validation

df_validated = df_date_valid.withColumn(
    "reject_reason",
    when(col("pickup_datetime") >= col("dropoff_datetime"),
         lit("causality_violation: pickup >= dropoff"))
   .when(col("trip_distance") < 0,
         lit("invalid_distance: negative"))
   .when(col("fare_amount") < 0,
         lit("invalid_fare: negative"))
   .when(col("passenger_count").isNull() | (col("passenger_count") <= 0),
         lit("invalid_passengers: null or zero"))
   .when(col("PULocationID").isNull() | col("DOLocationID").isNull(),
         lit("missing_location_id"))
   .otherwise(lit(None).cast("string"))
)

df_clean     = df_validated.filter(col("reject_reason").isNull()).drop("reject_reason")
df_rule_bad  = df_validated.filter(col("reject_reason").isNotNull())

rule_reject_count = df_rule_bad.count()
print(f"Business rule rejects: {rule_reject_count:,}")

# Write all rejects to quarantine (union both reject sets)
df_all_rejects = df_date_rejected.unionByName(df_rule_bad, allowMissingColumns=True)

(df_all_rejects
    .write.format("delta")
    .mode("overwrite" if MODE == "full" else "append")
    .option("overwriteSchema", "true")
    .save(QUARANTINE_PATH)
)
print(f"Quarantine written: {df_all_rejects.count():,} rows")


# Deduplication
# ─────────────────────────────────────────────────────────────────────────────
dedup_window = Window.partitionBy(
    "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount"
).orderBy("source_month")

df_deduped = (
    df_clean
    .withColumn("_rn", row_number().over(dedup_window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

dupe_count = df_clean.count() - df_deduped.count()
print(f"Duplicates removed: {dupe_count:,}")


# Derived columns

df_enriched = (
    df_deduped
    .withColumn("trip_duration_min",
        spark_round(
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60, 2))
    .withColumn("avg_speed_mph",
        when(col("trip_duration_min") > 0,
             spark_round(col("trip_distance") / (col("trip_duration_min") / 60), 2))
        .otherwise(lit(None).cast("double")))
    .withColumn("pickup_hour",         hour("pickup_datetime"))
    .withColumn("pickup_day_of_week",  dayofweek("pickup_datetime"))
    .withColumn("pickup_month",        month("pickup_datetime"))
    .withColumn("pickup_date",         col("pickup_datetime").cast("date"))
    .withColumn("is_weekend",
        col("pickup_day_of_week").isin([1, 7]).cast("boolean"))
    .withColumn("total_cost",
        spark_round(
            col("fare_amount") + col("extra") + col("mta_tax") +
            col("tip_amount") + col("tolls_amount") + col("improvement_surcharge"), 2))
    .withColumn("tip_pct",
        when(col("fare_amount") > 0,
             spark_round(col("tip_amount") / col("fare_amount") * 100, 1))
        .otherwise(lit(None).cast("double")))
    .withColumn("payment_type_label",
        when(col("payment_type") == 1, "credit_card")
        .when(col("payment_type") == 2, "cash")
        .when(col("payment_type") == 3, "no_charge")
        .when(col("payment_type") == 4, "dispute")
        .otherwise("unknown"))
    .withColumn("is_speed_outlier",
        (col("avg_speed_mph") > 100).cast("boolean"))
)

# Zone enrichment join
df_zones = (
    spark.read.csv(ZONE_PATH, header=True, inferSchema=True)
    .select(
        col("LocationID").cast("integer"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone_name"),
        col("service_zone")
    )
)

df_silver = (
    df_enriched
    .join(df_zones.alias("pu"), col("PULocationID") == col("pu.LocationID"), "left")
    .withColumnRenamed("borough",      "pickup_borough")
    .withColumnRenamed("zone_name",    "pickup_zone")
    .withColumnRenamed("service_zone", "pickup_service_zone")
    .drop(col("pu.LocationID"))

    .join(df_zones.alias("do"), col("DOLocationID") == col("do.LocationID"), "left")
    .withColumnRenamed("borough",      "dropoff_borough")
    .withColumnRenamed("zone_name",    "dropoff_zone")
    .withColumnRenamed("service_zone", "dropoff_service_zone")
    .drop(col("do.LocationID"))
)

# Write Silver Delta table

write_mode = "overwrite" if MODE == "full" else "append"

(df_silver
    .write
    .format("delta")
    .mode(write_mode)
    .option("overwriteSchema", "true")
    .partitionBy("pickup_month")
    .save(SILVER_PATH)
)

silver_count = df_silver.count()
print(f"\n{'='*60}")
print(f"Silver write complete ({write_mode})")
print(f"  Raw rows read       : {raw_count:,}")
print(f"  Date rejects        : {date_reject_count:,}")
print(f"  Rule rejects        : {rule_reject_count:,}")
print(f"  Duplicates removed  : {dupe_count:,}")
print(f"  Silver rows written : {silver_count:,}")
print(f"{'='*60}")

# Run OPTIMIZE + ZORDER on Silver for query performance
# ─────────────────────────────────────────────────────────────────────────────
# ZORDERing on pickup_datetime and pickup_borough dramatically speeds up
# the most common dashboard queries (time range + borough filter combos)
print("Running OPTIMIZE + ZORDER on Silver table...")
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    ZORDER BY (pickup_datetime, pickup_borough)
""")
print("OPTIMIZE complete.")

# Log pipeline run stats to Delta metadata table

from pyspark.sql import Row

run_log = spark.createDataFrame([Row(
    pipeline_name   = "bronze_to_silver_yellow_taxi",
    target_month    = TARGET_MONTH,
    run_mode        = MODE,
    raw_rows        = raw_count,
    silver_rows     = silver_count,
    quarantine_rows = date_reject_count + rule_reject_count,
    dupes_removed   = dupe_count,
    run_timestamp   = datetime.utcnow().isoformat(),
)])

METADATA_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/pipeline_run_log/"
run_log.write.format("delta").mode("append").save(METADATA_PATH)
print("Run log written.")
