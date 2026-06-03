-- models/gold/dimensions/dim_date.sql
-- -------------------------------------------------------
-- Calendar dimension covering all dates in 2024.
-- Generated entirely in SQL — no seed file needed.
-- Includes time-intelligence columns analysts need
-- for YTD, MTD, week-over-week comparisons.
-- Materialized as TABLE so it's always fast to join.
-- -------------------------------------------------------

{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with date_spine as (

    -- Generate one row per day in 2024 using a recursive CTE
    -- Databricks / Spark SQL supports this natively
    select explode(
        sequence(
            to_date('2024-01-01'),
            to_date('2024-12-31'),
            interval 1 day
        )
    ) as calendar_date

),

enriched as (

    select
        -- ── primary key ──────────────────────────────────
        cast(date_format(calendar_date, 'yyyyMMdd') as int)  as date_key,
        calendar_date,

        -- ── year / quarter / month ────────────────────────
        year(calendar_date)                                   as year,
        quarter(calendar_date)                                as quarter_num,
        concat('Q', quarter(calendar_date))                   as quarter_label,
        month(calendar_date)                                  as month_num,
        date_format(calendar_date, 'MMMM')                   as month_name,
        date_format(calendar_date, 'MMM')                     as month_abbr,

        -- ── week ─────────────────────────────────────────
        weekofyear(calendar_date)                             as week_of_year,
        date_format(calendar_date, 'EEEE')                   as day_name,
        date_format(calendar_date, 'EEE')                    as day_abbr,
        dayofweek(calendar_date)                              as day_of_week,   -- 1=Sun
        dayofmonth(calendar_date)                             as day_of_month,
        dayofyear(calendar_date)                              as day_of_year,

        -- ── weekend flag ──────────────────────────────────
        case
            when dayofweek(calendar_date) in (1, 7) then true
            else false
        end                                                   as is_weekend,

        -- ── US federal holidays 2024 (relevant for demand spikes) ─
        case
            when calendar_date in (
                '2024-01-01',   -- New Year's Day
                '2024-01-15',   -- MLK Day
                '2024-02-19',   -- Presidents' Day
                '2024-05-27',   -- Memorial Day
                '2024-06-19',   -- Juneteenth
                '2024-07-04',   -- Independence Day
                '2024-09-02',   -- Labor Day
                '2024-10-14',   -- Columbus Day
                '2024-11-11',   -- Veterans Day
                '2024-11-28',   -- Thanksgiving
                '2024-12-25'    -- Christmas
            ) then true
            else false
        end                                                   as is_us_holiday,

        -- ── time intelligence helpers ─────────────────────
        last_day(calendar_date)                               as last_day_of_month,
        date_trunc('week',  calendar_date)                    as week_start_date,
        date_trunc('month', calendar_date)                    as month_start_date,
        date_trunc('quarter', calendar_date)                  as quarter_start_date,

        -- ── relative flags (useful for "current month" filters) ──
        case
            when year(calendar_date) = year(current_date())
             and month(calendar_date) = month(current_date())
            then true else false
        end                                                   as is_current_month,

        case
            when year(calendar_date) = year(current_date())
            then true else false
        end                                                   as is_current_year

    from date_spine

)

select * from enriched
order by calendar_date
