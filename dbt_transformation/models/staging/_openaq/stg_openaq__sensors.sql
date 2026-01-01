-- ===========================================================================
-- STG_OPENAQ__SENSORS.SQL
-- ===========================================================================
-- models/staging/_openaq/stg_openaq__sensors.sql
--
{{ config(materialized="view") }}

-- [FINAL QUERY OUTPUT] ======================================================
select
    id,
    location_id,
    `name` as sensor_name,
    parameter_id,
    parameter_name,
    parameter_units,
    parameter_displayname,
    ingested_at,
    updated_at,
    refreshed_at

from {{ source("src_openaq", "src_openaq__sensors") }}
