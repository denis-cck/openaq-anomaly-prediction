-- ===========================================================================
-- INT_OPENAQ__MEASUREMENTS__STANDARDIZED.SQL
-- ===========================================================================
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "datetimeto_utc",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["location_id", "sensor_name", "datetimeto_utc"],
    )
}}


-- [CTEs] ====================================================================
with
    new_measurements as (
        select *
        from {{ ref("stg_openaq__measurements") }}

        {% if is_incremental() %}
            -- Overwrite full partitions that contain rows updated since last run
            where
                timestamp_trunc(datetimeto_utc, day) in (
                    select distinct timestamp_trunc(datetimeto_utc, day)
                    from {{ ref("stg_openaq__measurements") }}
                    -- FinOps: Only scan the last 10 days of 'this' staging table
                    -- We can do a full refresh if we need to go further back
                    -- in case of downtimes, late updates...
                    where
                        datetimeto_utc  -- FinOps: only look at recent data (SOURCE) to find updates
                        >= timestamp_sub(current_timestamp(), interval 15 day)
                        and updated_at >= (
                            select coalesce(max(updated_at), "1900-01-01")
                            from {{ this }}
                            where
                                datetimeto_utc  -- FinOps: only look at recent data (DEST) to find updates
                                >= timestamp_sub(current_timestamp(), interval 10 day)
                        )
                -- where updated_at >= (select max(updated_at) from {{ this }})
                )
        {% endif %}
    ),

    sensors_with_locations as (
        select
            -- =========================
            -- SENSORS FIELDS
            sensors.id as sensor_id,
            sensors.location_id,
            sensors.sensor_name,
            -- sensors.parameter_id,
            -- sensors.parameter_name,
            -- sensors.parameter_units,
            sensors.parameter_displayname,
            -- sensors.ingested_at,
            -- sensors.updated_at,
            -- sensors.refreshed_at
            -- =========================
            -- LOCATIONS FIELDS
            -- locations.id,
            locations.location_name,
            -- locality,
            locations.timezone,
            locations.ismobile,
            locations.ismonitor,
            -- locations.distance,
            -- locations.bounds_min_lon,
            -- locations.bounds_min_lat,
            -- locations.bounds_max_lon,
            -- locations.bounds_max_lat,
            -- locations.country_id,
            -- locations.country_code,
            locations.country_name,
            -- locations.owner_id,
            locations.owner_name,
            -- locations.provider_id,
            locations.provider_name,
            locations.coordinates_latitude as location_latitude,
            locations.coordinates_longitude as location_longitude,
            locations.datetimefirst_utc,
            locations.datetimelast_utc
        -- datetimefirst_local,
        -- datetimelast_local,
        -- ingested_at,
        -- updated_at,
        -- refreshed_at,
        from {{ ref("stg_openaq__sensors") }} as sensors

        left join
            {{ ref("stg_openaq__locations") }} as locations
            on sensors.location_id = locations.id
    )

-- [FINAL QUERY OUTPUT] ======================================================
select
    sensors.location_id,
    measurements.sensor_id,
    measurements.value,
    sensors.sensor_name,

    measurements.parameter_id,
    measurements.parameter_name,
    measurements.parameter_units,
    sensors.parameter_displayname,

    measurements.datetimeto_utc,
    measurements.datetimeto_local,
    sensors.timezone,

    sensors.location_name,
    sensors.location_latitude,
    sensors.location_longitude,
    sensors.ismobile,
    sensors.ismonitor,
    sensors.datetimefirst_utc,
    sensors.datetimelast_utc,

    sensors.country_name,
    sensors.owner_name,
    sensors.provider_name,

    measurements.ingested_at,
    measurements.updated_at,
    measurements.refreshed_at

from new_measurements as measurements

left join
    sensors_with_locations as sensors on measurements.sensor_id = sensors.sensor_id
