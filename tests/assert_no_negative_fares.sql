-- tests/assert_no_negative_fares.sql
-- -------------------------------------------------------
-- Custom singular test. Returns rows if any trip in the
-- Gold fact table has a negative fare_amount.
-- dbt fails the test if this query returns any rows.
-- -------------------------------------------------------

select
    trip_id,
    fare_amount,
    pickup_datetime,
    pickup_borough,
    'negative fare in fact_trips' as failure_reason

from {{ ref('fact_trips') }}

where fare_amount < 0
