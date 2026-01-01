-- ===========================================================================
-- INT_SEOUL__MEASUREMENTS_PIVOTED.SQL
-- ===========================================================================
-- models/intermediate/seoul/int_seoul__measurements_pivoted.sql
--
{{ config(materialized="table", cluster_by=["location_id"]) }}


-- [CTEs] ====================================================================
with

    -- We create the master grid (all locations x all possibles timestamps)
    seoul_locations as (
        -- Get every location that has ever reported data
        select distinct location_id
        from {{ ref("int_seoul__measurements_standardized") }}
    ),
    master_spine as (
        -- Cross join locations with the time spine to create the "Heartbeat"
        select locations.location_id, spine.datetimeto_utc
        from seoul_locations locations
        cross join {{ ref("int_seoul__time_spine") }} spine
        where spine.datetimeto_utc >= '2024-04-30 15:00:00'  -- Align measurements_standardized start
    ),

    -- We create a pivot table of the actual measurements
    measurements as (
        -- Get your cleaned standardized data
        select * from {{ ref("int_seoul__measurements_standardized") }}
    ),
    pivoted as (
        select
            location_id,
            datetimeto_utc,
            coalesce(max(concurrent_sensors), 0) as concurrent_sensors,

            -- Pivot the sensors into columns
            max(case when parameter_id = '2' then value end) as pm25_ugm3,
            max(case when parameter_id = '1' then value end) as pm10_ugm3,
            max(case when parameter_id = '7' then value end) as no2_ppm,
            max(case when parameter_id = '10' then value end) as o3_ppm,
            max(case when parameter_id = '9' then value end) as so2_ppm,
            max(case when parameter_id = '8' then value end) as co_ppm,
        from measurements
        group by 1, 2
    ),

    -- FINAL TABLE: join the master spine with the pivoted measurements
    final_table as (
        select
            spine.location_id,
            spine.datetimeto_utc,
            pivot.pm25_ugm3,
            pivot.pm10_ugm3,
            pivot.no2_ppm,
            pivot.o3_ppm,
            pivot.so2_ppm,
            pivot.co_ppm,
            coalesce(pivot.concurrent_sensors, 0) as concurrent_sensors
        from master_spine spine
        left join
            pivoted pivot on spine.location_id = pivot.location_id
            and spine.datetimeto_utc = pivot.datetimeto_utc
    )

-- [FINAL QUERY OUTPUT] ======================================================
select *
from final_table
