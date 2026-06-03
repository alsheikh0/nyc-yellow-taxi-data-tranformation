-- models/gold/aggregates/agg_hourly_zone.sql
-- -------------------------------------------------------
-- Aggregate: trip volume and revenue per hour per pickup zone.
-- This is the primary table for demand heatmaps and
-- surge-pricing analysis ("which zones are hottest at 6pm?").
-- Pre-aggregated so dashboards never hit fact_trips directly.
-- -------------------------------------------------------

{{
    config(
        materialized = 'table',
        schema       = 'gold',
        file_format  = 'delta'
    )
}}

with trips as (

    select * from {{ ref('fact_trips') }}

),

hourly_zone as (

    select
        -- ── dimensions ───────────────────────────────────
        pickup_date,
        pickup_month,
        pickup_hour,
        is_weekend,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone,

        -- ── volume metrics ────────────────────────────────
        count(*)                                             as total_trips,
        sum(passenger_count)                                 as total_passengers,

        -- ── revenue metrics ───────────────────────────────
        round(sum(total_cost),   2)                          as total_revenue,
        round(avg(total_cost),   2)                          as avg_fare_per_trip,
        round(sum(tip_amount),   2)                          as total_tips,
        round(avg(tip_pct),      1)                          as avg_tip_pct,

        -- ── trip distance / duration ─────────────────────
        round(avg(trip_distance),    2)                      as avg_trip_distance_mi,
        round(avg(trip_duration_min),1)                      as avg_trip_duration_min,
        round(avg(avg_speed_mph),    1)                      as avg_speed_mph,

        -- ── payment mix ──────────────────────────────────
        count(case when payment_type_label = 'credit_card' then 1 end)  as trips_credit_card,
        count(case when payment_type_label = 'cash'        then 1 end)  as trips_cash,

        -- ── airport / cross-borough flags ─────────────────
        count(case when is_airport_trip    then 1 end)       as airport_trips,
        count(case when is_cross_borough   then 1 end)       as cross_borough_trips,

        -- ── revenue per mile (efficiency signal) ──────────
        round(
            sum(total_cost) / nullif(sum(trip_distance), 0),
            2
        )                                                    as revenue_per_mile,

        -- ── lineage ──────────────────────────────────────
        current_timestamp()                                  as aggregated_at

    from trips

    group by
        pickup_date,
        pickup_month,
        pickup_hour,
        is_weekend,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone

)

select * from hourly_zone
