-- ===========================================================================
-- INT_SEOUL__MEASUREMENTS__STANDARDIZED.SQL
-- ===========================================================================
-- models/intermediate/seoul/int_seoul__measurements_standardized.sql
--
{{ config(materialized="view") }}


-- [CTEs] ====================================================================
with

    filtered_measurements as (
        select
            location_id,
            sensor_id,
            case when value < 0 or value >= 10000 then null else value end as value,
            sensor_name,

            parameter_id,
            parameter_name,
            parameter_units,
            parameter_displayname,

            datetimeto_utc,
            datetimeto_local,
            timezone,

            location_name,
            location_latitude,
            location_longitude,
            ismobile,
            ismonitor,
            datetimefirst_utc,
            datetimelast_utc,

            country_name,
            owner_name,
            provider_name,

            ingested_at,
            updated_at,
            refreshed_at

        from {{ ref("int_openaq__measurements_standardized") }}

        -- Static filters for Seoul data (from EDA)
        where
            datetimeto_utc >= '2024-04-30 15:00:00'
            -- and value >= 0
            -- and value < 10000
            -- and sensor_name in ('pm25 µg/m³', 'pm10 µg/m³', 'no2 ppm', 'o3 ppm',
            -- 'so2 ppm', 'co ppm')  -- Not great, should use parameter_id instead
            and parameter_id in ('2', '1', '7', '10', '9', '8')
    ),

    seoul_measurements_with_concurrency as (
        select
            *,
            coalesce(
                count(value) over (partition by location_id, datetimeto_utc), 0
            ) as concurrent_sensors,
        -- count(*) over (
        -- partition by location_id, datetimeto_utc
        -- ) as concurrent_sensors
        from filtered_measurements
    )

-- [FINAL QUERY OUTPUT] ======================================================
select
    location_id,
    sensor_id,
    value,
    sensor_name,

    parameter_id,
    parameter_name,
    parameter_units,
    parameter_displayname,
    concurrent_sensors,
    -- concurrent_sensors_safer,
    datetimeto_utc,
    datetimeto_local,
    timezone,

    location_name,
    location_latitude,
    location_longitude,
    ismobile,
    ismonitor,
    datetimefirst_utc,
    datetimelast_utc,

    country_name,
    owner_name,
    provider_name,

    ingested_at,
    updated_at,
    refreshed_at
from seoul_measurements_with_concurrency
