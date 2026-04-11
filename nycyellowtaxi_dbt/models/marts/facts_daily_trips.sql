{{ config(materialized='table') }}

select
    pickup_date,
    pickup_borough,
    vendor_id,
    count(*)              as total_daily_trips,
    sum(passenger_count)  as total_passengers,
    sum(trip_distance)    as total_distance_miles,
    sum(fare_amount)      as total_fare_amount,
    sum(total_cost)       as total_revenue,
    avg(total_cost)       as avg_cost_per_trip,
    avg(trip_distance)    as avg_distance_per_trip
from {{ ref('stg_yellow_trips') }}
group by pickup_date, pickup_borough, vendor_id
order by pickup_date desc