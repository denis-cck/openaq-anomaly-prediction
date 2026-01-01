-- ===========================================================================
-- STG_OPENAQ__LOCATIONS.SQL
-- ===========================================================================
-- models/staging/_openaq/stg_openaq__locations.sql
--
{{ config(materialized="view") }}

-- [FINAL QUERY OUTPUT] ======================================================
select
    id,
    `name` as location_name,
    locality,
    timezone,
    `isMobile` as ismobile,
    `isMonitor` as ismonitor,
    distance,
    bounds_min_lon,
    bounds_min_lat,
    bounds_max_lon,
    bounds_max_lat,
    country_id,
    country_code,
    country_name,
    owner_id,
    owner_name,
    provider_id,
    provider_name,
    coordinates_latitude,
    coordinates_longitude,
    `datetimeFirst_utc` as datetimefirst_utc,
    `datetimeFirst_local` as datetimefirst_local,
    `datetimeLast_utc` as datetimelast_utc,
    `datetimeLast_local` as datetimelast_local,
    ingested_at,
    updated_at,
    refreshed_at,

from {{ source("src_openaq", "src_openaq__locations") }}
