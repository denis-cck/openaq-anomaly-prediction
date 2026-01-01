-- ===========================================================================
-- STG_OPENMETEO__HISTORICAL.SQL
-- ===========================================================================
-- models/staging/_openmeteo/stg_openmeteo__historical.sql
--
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "datetimeto_utc",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["location_id"],
    )
}}


-- [CTEs] ====================================================================
with
    new_or_updated_rows as (
        select *
        from {{ source("src_openmeteo", "src_openmeteo__historical") }}

        {% if is_incremental() %}
            -- Overwrite full partitions that contain rows updated since last run
            where
                timestamp_trunc(datetimeto_utc, day) in (
                    select distinct timestamp_trunc(datetimeto_utc, day)
                    from {{ source("src_openmeteo", "src_openmeteo__historical") }}
                    -- FinOps: Only scan the last 10 days of 'this' staging table
                    -- We can do a full refresh if we need to go further back
                    -- in case of downtimes, late updates...
                    where
                        datetimeto_utc  -- FinOps: only look at recent data (SOURCE) to find updates
                        >= timestamp_sub(current_timestamp(), interval 15 day)
                        and updated_at >= (
                            select
                                coalesce(
                                    max(updated_at), '1900-01-01'  -- if 
                                )
                            from {{ this }}
                            where
                                datetimeto_utc  -- FinOps: only look at recent data (DEST) to find updates
                                >= timestamp_sub(current_timestamp(), interval 15 day)
                        )
                -- where updated_at >= (select max(updated_at) from {{ this }})
                )
        {% endif %}
    ),

    -- If duplicates exist, keep only the most recent version of each measurement
    deduplicated_rows as (
        select *
        from new_or_updated_rows

        -- Native BigQuery optimization over dbt_utils.deduplicate
        qualify
            row_number() over (
                partition by location_id, datetimeto_utc order by updated_at desc
            )
            = 1
    )

-- [FINAL QUERY OUTPUT] ======================================================
select
    location_id,
    datetimeto_utc,
    datetimeto_local,
    weather_latitude,
    weather_longitude,
    timezone,
    elevation,
    temperature_2m,
    relative_humidity_2m,
    dew_point_2m,
    apparent_temperature,
    precipitation,
    rain,
    snowfall,
    snow_depth,
    shortwave_radiation,
    direct_radiation,
    diffuse_radiation,
    global_tilted_irradiance,
    direct_normal_irradiance,
    terrestrial_radiation,
    weather_code,
    pressure_msl,
    surface_pressure,
    cloud_cover,
    cloud_cover_low,
    cloud_cover_mid,
    cloud_cover_high,
    vapour_pressure_deficit,
    et0_fao_evapotranspiration,
    wind_speed_100m,
    wind_speed_10m,
    wind_direction_10m,
    wind_direction_100m,
    wind_gusts_10m,
    is_day,
    ingested_at,
    updated_at,
    refreshed_at

from deduplicated_rows
