-- models/gold/facts/fact_trips.sql
-- -------------------------------------------------------
-- Trip-grain fact table. One row = one taxi trip.
-- Foreign keys to dim_date and dim_zone (x2).
-- All heavy computation already done in Silver by PySpark.
-- This model selects, renames, and links to dimensions.
-- Partitioned by pickup_month for query performance.
-- -------------------------------------------------------

{{
    config(
        materialized        = 'table',
        schema              = 'gold',
        partition_by        = 'pickup_month',
        file_format         = 'delta',
        on_schema_change    = 'sync_all_columns'
    )
}}

with trips as (

    select * from {{ ref('stg_silver_trips') }}

),

-- Exclude rows flagged as speed outliers from the fact table
-- (they still exist in Silver for audit purposes)
clean_trips as (

    select * from trips
    where is_speed_outlier = false
      and trip_duration_min > 0
      and trip_distance     > 0

),

final as (

    select
        -- ── surrogate key ─────────────────────────────────
        trip_id,

        -- ── foreign keys to dimensions ────────────────────
        cast(date_format(pickup_date, 'yyyyMMdd') as int)   as date_key,
        pickup_location_id,
        dropoff_location_id,
        pickup_month,           -- partition column

        -- ── time ─────────────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        pickup_hour,
        pickup_day_of_week,
        is_weekend,

        -- ── geography (denormalised for convenience) ──────
        pickup_borough,
        pickup_zone,
        pickup_service_zone,
        dropoff_borough,
        dropoff_zone,
        dropoff_service_zone,

        -- ── trip measures ─────────────────────────────────
        passenger_count,
        trip_distance,
        trip_duration_min,
        avg_speed_mph,

        -- ── financial measures ────────────────────────────
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_cost,
        tip_pct,

        -- ── categorical attributes ────────────────────────
        payment_type_label,
        rate_code_id,
        vendor_id,
        store_and_fwd_flag,

        -- ── derived booleans for easy filtering ───────────
        case
            when pickup_borough != dropoff_borough then true
            else false
        end                                                  as is_cross_borough,

        case
            when pickup_zone    like '%Airport%'
              or dropoff_zone   like '%Airport%'
              or pickup_zone    like '%JFK%'
              or dropoff_zone   like '%JFK%'
              or pickup_zone    like '%LaGuardia%'
              or dropoff_zone   like '%LaGuardia%'
            then true
            else false
        end                                                  as is_airport_trip,

        -- ── lineage ──────────────────────────────────────
        source_month,
        current_timestamp()                                  as loaded_at

    from clean_trips

)

select * from final
