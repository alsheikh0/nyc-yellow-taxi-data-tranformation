-- models/staging/stg_silver_trips.sql
-- -------------------------------------------------------
-- Source: Silver Delta table written by the PySpark job.
-- This staging model is a VIEW — zero compute cost.
-- Its only job is aliasing and light type-casting so that
-- downstream Gold models never reference raw column names.
-- -------------------------------------------------------

with source as (

    select * from {{ source('silver', 'yellow_taxi') }}

),

renamed as (

    select
        -- ── identifiers ─────────────────────────────────
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime', 'dropoff_datetime',
            'PULocationID',    'DOLocationID',
            'fare_amount'
        ]) }}                              as trip_id,

        -- ── timestamps ──────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        pickup_month,
        is_weekend,

        -- ── locations ───────────────────────────────────
        cast(PULocationID as int)          as pickup_location_id,
        cast(DOLocationID as int)          as dropoff_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone,
        dropoff_borough,
        dropoff_zone,
        dropoff_service_zone,

        -- ── trip metrics ─────────────────────────────────
        trip_distance,
        trip_duration_min,
        avg_speed_mph,
        is_speed_outlier,
        passenger_count,

        -- ── financials ───────────────────────────────────
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_cost,
        tip_pct,

        -- ── payment ──────────────────────────────────────
        payment_type,
        payment_type_label,

        -- ── vendor / rate ─────────────────────────────────
        VendorID                           as vendor_id,
        RatecodeID                         as rate_code_id,
        store_and_fwd_flag,

        -- ── lineage ──────────────────────────────────────
        source_month,
        source_file

    from source

)

select * from renamed
