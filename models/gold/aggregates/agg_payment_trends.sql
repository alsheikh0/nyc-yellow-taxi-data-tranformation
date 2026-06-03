-- models/gold/aggregates/agg_payment_trends.sql
-- -------------------------------------------------------
-- Aggregate: payment method and tipping trends by month.
-- Answers: "Is cash declining over 2024?"
--          "Which borough tips most generously?"
--          "Do weekend riders tip more than weekday riders?"
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

payment_base as (

    select
        pickup_month,
        pickup_borough,
        is_weekend,
        payment_type_label,

        count(*)                                              as trip_count,
        round(sum(total_cost),  2)                            as total_revenue,
        round(sum(tip_amount),  2)                            as total_tips,
        round(avg(tip_pct),     1)                            as avg_tip_pct,

        -- Tip bands
        count(case when tip_pct = 0                         then 1 end)  as trips_no_tip,
        count(case when tip_pct > 0  and tip_pct <= 15      then 1 end)  as trips_low_tip,
        count(case when tip_pct > 15 and tip_pct <= 25      then 1 end)  as trips_mid_tip,
        count(case when tip_pct > 25                        then 1 end)  as trips_high_tip

    from trips

    group by
        pickup_month,
        pickup_borough,
        is_weekend,
        payment_type_label

),

with_share as (

    select
        *,

        -- Payment type share within this month/borough/weekend segment
        round(
            100.0 * trip_count / nullif(
                sum(trip_count) over (
                    partition by pickup_month, pickup_borough, is_weekend
                ), 0
            ), 1
        )                                                     as payment_type_share_pct

    from payment_base

)

select
    *,
    current_timestamp() as aggregated_at
from with_share
order by
    pickup_month,
    pickup_borough,
    is_weekend,
    trip_count desc
