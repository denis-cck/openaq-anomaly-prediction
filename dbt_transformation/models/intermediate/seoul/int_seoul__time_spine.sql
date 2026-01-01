-- ===========================================================================
-- INT_SEOUL__TIME_SPINE.SQL
-- ===========================================================================
-- models/intermediate/seoul/int_seoul__time_spine.sql
--
-- DESCRIPTION:
-- We create a time spine for Seoul measurements at an hourly frequency.
--
{{ config(materialized="view") }}


-- [CTEs] ====================================================================
with
    dates as (
        select *
        from
            unnest(
                generate_timestamp_array(
                    '2024-01-01 00:00:00', current_timestamp(), interval 1 hour
                )
            ) as datetimeto_utc
    )

-- [FINAL QUERY OUTPUT] ======================================================
select datetimeto_utc
from dates
