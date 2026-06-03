-- macros/revenue_metrics.sql
-- -------------------------------------------------------
-- Reusable macro that generates standard revenue metric
-- expressions. Use in any aggregate model to keep KPI
-- definitions consistent across the Gold layer.
--
-- Usage:
--   {{ revenue_metrics(prefix='') }}
-- -------------------------------------------------------

{% macro revenue_metrics(prefix='') %}

    round(sum({{ prefix }}total_cost),  2)    as total_revenue,
    round(avg({{ prefix }}total_cost),  2)    as avg_fare_per_trip,
    round(sum({{ prefix }}tip_amount),  2)    as total_tips,
    round(avg({{ prefix }}tip_pct),     1)    as avg_tip_pct,
    round(
        sum({{ prefix }}total_cost) /
        nullif(sum({{ prefix }}trip_distance), 0),
        2
    )                                         as revenue_per_mile

{% endmacro %}


-- macros/trip_volume_metrics.sql
{% macro trip_volume_metrics(prefix='') %}

    count(*)                                  as total_trips,
    sum({{ prefix }}passenger_count)          as total_passengers,
    round(avg({{ prefix }}trip_distance), 2)  as avg_distance_mi,
    round(avg({{ prefix }}trip_duration_min), 1) as avg_duration_min,
    round(avg({{ prefix }}avg_speed_mph), 1)  as avg_speed_mph

{% endmacro %}
