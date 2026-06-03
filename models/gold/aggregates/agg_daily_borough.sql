-- models/gold/aggregates/agg_daily_borough.sql
-- -------------------------------------------------------
-- Aggregate: daily KPIs rolled up to borough level.
-- Designed for executive dashboards and trend analysis —
-- "how did Brooklyn perform vs Manhattan this week?"
-- Includes WoW and MoM growth window functions.
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

daily_borough_base as (

    select
        pickup_date,
        pickup_month,
        is_weekend,
        pickup_borough,

        count(*)                                              as total_trips,
        round(sum(total_cost),    2)                          as total_revenue,
        round(avg(total_cost),    2)                          as avg_fare,
        round(avg(trip_distance), 2)                          as avg_distance_mi,
        round(avg(trip_duration_min), 1)                      as avg_duration_min,
        round(sum(tip_amount),    2)                          as total_tips,
        round(avg(tip_pct),       1)                          as avg_tip_pct,
        sum(passenger_count)                                  as total_passengers,
        count(case when is_airport_trip  then 1 end)          as airport_trips,
        count(case when is_cross_borough then 1 end)          as cross_borough_trips,
        count(distinct pickup_location_id)                    as active_pickup_zones

    from trips

    group by
        pickup_date,
        pickup_month,
        is_weekend,
        pickup_borough

),

with_growth as (

    select
        *,

        -- Week-over-week trip volume change
        lag(total_trips, 7) over (
            partition by pickup_borough
            order by pickup_date
        )                                                     as trips_wow_prior,

        round(
            100.0 * (
                total_trips - lag(total_trips, 7) over (
                    partition by pickup_borough order by pickup_date
                )
            ) / nullif(
                lag(total_trips, 7) over (
                    partition by pickup_borough order by pickup_date
                ), 0
            ), 1
        )                                                     as trips_wow_pct_change,

        -- Week-over-week revenue change
        round(
            100.0 * (
                total_revenue - lag(total_revenue, 7) over (
                    partition by pickup_borough order by pickup_date
                )
            ) / nullif(
                lag(total_revenue, 7) over (
                    partition by pickup_borough order by pickup_date
                ), 0
            ), 1
        )                                                     as revenue_wow_pct_change,

        -- Running MTD trip total
        sum(total_trips) over (
            partition by pickup_borough, pickup_month
            order by pickup_date
            rows between unbounded preceding and current row
        )                                                     as mtd_trips,

        -- Running MTD revenue total
        round(
            sum(total_revenue) over (
                partition by pickup_borough, pickup_month
                order by pickup_date
                rows between unbounded preceding and current row
            ), 2
        )                                                     as mtd_revenue,

        -- Borough rank by trip volume on this date
        rank() over (
            partition by pickup_date
            order by total_trips desc
        )                                                     as borough_rank_by_trips,

        -- Borough rank by revenue on this date
        rank() over (
            partition by pickup_date
            order by total_revenue desc
        )                                                     as borough_rank_by_revenue

    from daily_borough_base

)

select
    *,
    current_timestamp() as aggregated_at
from with_growth
