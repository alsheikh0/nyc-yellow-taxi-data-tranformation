-- models/gold/dimensions/dim_zone.sql
-- -------------------------------------------------------
-- Taxi zone dimension table.
-- Source: taxi_zone_lookup.csv loaded to the Bronze layer.
-- Provides the borough → service_zone → zone hierarchy
-- that analysts use to slice trips geographically.
-- -------------------------------------------------------

{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with raw_zones as (

    select
        cast(LocationID  as int)    as location_id,
        Borough                     as borough,
        Zone                        as zone_name,
        service_zone

    from {{ source('silver', 'taxi_zone_lookup') }}

),

enriched as (

    select
        location_id,
        zone_name,
        borough,
        service_zone,

        -- Borough grouping for outer-borough vs Manhattan analysis
        case borough
            when 'Manhattan'    then 'core'
            when 'Brooklyn'     then 'outer'
            when 'Queens'       then 'outer'
            when 'Bronx'        then 'outer'
            when 'Staten Island' then 'outer'
            else 'unknown'
        end                         as borough_type,

        -- Airport zones — useful for airport-run analysis
        case
            when zone_name like '%Airport%'
              or zone_name like '%JFK%'
              or zone_name like '%LaGuardia%'
              or zone_name like '%Newark%'
            then true
            else false
        end                         as is_airport_zone,

        -- Major hub flag
        case
            when zone_name in (
                'Times Sq/Theatre District',
                'Midtown Center',
                'Penn Station/Madison Sq West',
                'Grand Central',
                'Upper East Side North',
                'Upper East Side South',
                'JFK Airport',
                'LaGuardia Airport'
            ) then true
            else false
        end                         as is_major_hub

    from raw_zones

)

select * from enriched
order by location_id
