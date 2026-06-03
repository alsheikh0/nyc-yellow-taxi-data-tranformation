-- tests/assert_pickup_before_dropoff.sql
-- -------------------------------------------------------
-- Custom singular test. Returns rows if any trip has
-- pickup_datetime >= dropoff_datetime (causality violation).
-- Should always return 0 rows after Silver cleaning — but
-- this is the Gold-layer safety net.
-- -------------------------------------------------------

select
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_min,
    pickup_borough,
    'causality violation: pickup >= dropoff' as failure_reason

from {{ ref('fact_trips') }}

where pickup_datetime >= dropoff_datetime
   or trip_duration_min <= 0
