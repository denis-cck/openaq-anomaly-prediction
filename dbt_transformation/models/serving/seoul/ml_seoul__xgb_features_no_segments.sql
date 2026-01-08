-- depends_on: {{ ref('int_seoul__measurements_pivoted') }}
-- ===========================================================================
-- ML_SEOUL__XGB_FEATURES_NO_SEGMENTS.SQL
-- ===========================================================================
-- models/serving/seoul/ml_seoul__xgb_features_no_segments.sql
--
-- This model replicates ml_seoul__xgb_features but WITHOUT segment filtering.
-- It uses RANGE-based windows (time-based) instead of ROWS-based windows
-- to correctly handle the sparse time spine with NULL pollutant values.
--
{{
    config(
        materialized="table",
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

    -- ===============================================================
    -- [BASE DATA]: Full time spine with all joined columns
    -- ---------------------------------------------------------------
    base_joined as (
        select
            pivoted.location_id,
            pivoted.datetimeto_utc,
            -- Convert timestamp to Unix seconds for RANGE-based windows
            unix_seconds(pivoted.datetimeto_utc) as datetimeto_unix,
            pivoted.datetimeto_local,
            pivoted.concurrent_sensors,

            pivoted.pm25_ugm3,
            pivoted.pm10_ugm3,
            pivoted.no2_ppm,
            pivoted.o3_ppm,
            pivoted.so2_ppm,
            pivoted.co_ppm,

            locations.ismonitor as oaq_ismonitor,
            locations.owner_id as oaq_owner_id,
            locations.provider_id as oaq_provider_id,
            locations.coordinates_latitude as oaq_latitude,
            locations.coordinates_longitude as oaq_longitude,

            openmeteo.elevation as om_elevation,
            openmeteo.temperature_2m as om_temperature_2m,
            openmeteo.relative_humidity_2m as om_relative_humidity_2m,
            openmeteo.dew_point_2m as om_dew_point_2m,
            openmeteo.apparent_temperature as om_apparent_temperature,
            openmeteo.precipitation as om_precipitation,
            openmeteo.rain as om_rain,
            openmeteo.snowfall as om_snowfall,
            openmeteo.snow_depth as om_snow_depth,

            openmeteo.shortwave_radiation as om_shortwave_radiation,
            openmeteo.direct_radiation as om_direct_radiation,
            openmeteo.diffuse_radiation as om_diffuse_radiation,
            openmeteo.global_tilted_irradiance as om_global_tilted_irradiance,
            openmeteo.direct_normal_irradiance as om_direct_normal_irradiance,
            openmeteo.terrestrial_radiation as om_terrestrial_radiation,

            openmeteo.weather_code as om_weather_code,
            openmeteo.pressure_msl as om_pressure_msl,
            openmeteo.surface_pressure as om_surface_pressure,
            openmeteo.cloud_cover as om_cloud_cover,
            openmeteo.cloud_cover_low as om_cloud_cover_low,
            openmeteo.cloud_cover_mid as om_cloud_cover_mid,
            openmeteo.cloud_cover_high as om_cloud_cover_high,
            openmeteo.vapour_pressure_deficit as om_vapour_pressure_deficit,
            openmeteo.et0_fao_evapotranspiration as om_et0_fao_evapotranspiration,

            openmeteo.wind_speed_100m as om_wind_speed_100m,
            openmeteo.wind_speed_10m as om_wind_speed_10m,
            openmeteo.wind_direction_10m as om_wind_direction_10m,
            openmeteo.wind_direction_100m as om_wind_direction_100m,
            openmeteo.wind_gusts_10m as om_wind_gusts_10m,

            openmeteo.is_day as om_is_day,

            -- Time helpers
            extract(hour from pivoted.datetimeto_local) as feat_time__hour,
            extract(day from pivoted.datetimeto_local) as _feat_time__day,
            mod(
                extract(dayofweek from pivoted.datetimeto_local) + 5, 7
            ) as feat_time__dow,
            extract(month from pivoted.datetimeto_local) - 1 as feat_time__month

        from {{ ref("int_seoul__measurements_pivoted") }} pivoted

        left join
            {{ ref("stg_openaq__locations") }} locations
            on pivoted.location_id = locations.id

        left join
            {{ ref("stg_openmeteo__historical") }} openmeteo
            on pivoted.location_id = openmeteo.location_id
            and pivoted.datetimeto_utc = openmeteo.datetimeto_utc
    ),

    -- ===============================================================
    -- [POLLUTANT FEATURES]: Using RANGE windows for time-based lookback
    --
    -- Key differences from ml_seoul__xgb_features:
    -- - Uses RANGE with unix seconds (time-based) instead of ROWS (row-based)
    -- - AVG/STDDEV/MAX naturally ignore NULLs
    -- - LAG needs LAST_VALUE with IGNORE NULLS for proper handling
    --
    -- Time constants in seconds:
    -- - 1 hour = 3600 seconds
    -- - 24 hours = 86400 seconds
    -- - 168 hours (1 week) = 604800 seconds
    -- - 192 hours = 691200 seconds
    -- - 720 hours (30 days) = 2592000 seconds
    -- ---------------------------------------------------------------
    add_pollutant_features as (
        select
            *,

            -- ===============================================================
            -- [EXPANDING MEAN] (LOCATION LIFETIME)
            -- AVG ignores NULLs, so this works correctly on sparse data
            -- ===============================================================
            avg(pm25_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_pm25_ugm3,
            avg(pm10_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_pm10_ugm3,
            avg(no2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_no2_ppm,
            avg(o3_ppm) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_o3_ppm,
            avg(so2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_so2_ppm,
            avg(co_ppm) over (
                partition by location_id
                order by datetimeto_unix
                rows between unbounded preceding and 1 preceding
            ) as feat_pollutant__expmean_co_ppm,

            -- ===============================================================
            -- [ROLLING MEAN] (30 DAYS = 720 hours = 2592000 seconds)
            -- Using RANGE to look back exactly 720 hours (not 720 rows)
            -- ===============================================================
            avg(pm25_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_pm25_ugm3,
            avg(pm10_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_pm10_ugm3,
            avg(no2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_no2_ppm,
            avg(o3_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_o3_ppm,
            avg(so2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_so2_ppm,
            avg(co_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 2592000 preceding and 3600 preceding
            ) as feat_pollutant__roll720_mean_co_ppm,

            -- ===============================================================
            -- [LAG FEATURES] (168 hours = 1 week = 604800 seconds)
            --
            -- In ml_seoul__xgb_features: LAG(x, 168) works because data is dense
            -- Here we need the value from exactly 168 hours ago (if it exists)
            -- Using a small window centered at 168 hours ago (604800 +/- 1800 seconds)
            -- ===============================================================
            last_value(pm25_ugm3 ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_pm25_ugm3,
            last_value(pm10_ugm3 ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_pm10_ugm3,
            last_value(no2_ppm ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_no2_ppm,
            last_value(o3_ppm ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_o3_ppm,
            last_value(so2_ppm ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_so2_ppm,
            last_value(co_ppm ignore nulls) over (
                partition by location_id
                order by datetimeto_unix
                range between 606600 preceding and 603000 preceding
            ) as feat_pollutant__lag168_co_ppm,

            -- ===============================================================
            -- [ROLLING STATS around LAG168] (24-hour window ending at 168h ago)
            -- Window: from 192 hours ago to 168 hours ago
            -- 192 hours = 691200 seconds, 168 hours = 604800 seconds
            -- ===============================================================
            -- MEAN
            avg(pm25_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_pm25_ugm3,
            avg(pm10_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_pm10_ugm3,
            avg(no2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_no2_ppm,
            avg(o3_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_o3_ppm,
            avg(so2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_so2_ppm,
            avg(co_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_mean_co_ppm,

            -- STDDEV
            stddev(pm25_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_pm25_ugm3,
            stddev(pm10_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_pm10_ugm3,
            stddev(no2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_no2_ppm,
            stddev(o3_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_o3_ppm,
            stddev(so2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_so2_ppm,
            stddev(co_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_stddev_co_ppm,

            -- MAX
            max(pm25_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_pm25_ugm3,
            max(pm10_ugm3) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_pm10_ugm3,
            max(no2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_no2_ppm,
            max(o3_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_o3_ppm,
            max(so2_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_so2_ppm,
            max(co_ppm) over (
                partition by location_id
                order by datetimeto_unix
                range between 691200 preceding and 604800 preceding
            ) as feat_pollutant__lag168_roll24_max_co_ppm

        from base_joined
    ),

    -- ===============================================================
    -- [POLLUTANT RATIOS]:
    -- ---------------------------------------------------------------
    add_pollutant_ratios as (
        select
            *,
            -- Fine Particle Ratio: High = Combustion/Smog, Low = Dust/Sand
            safe_divide(
                feat_pollutant__lag168_pm25_ugm3,
                (feat_pollutant__lag168_pm10_ugm3 + 0.1)
            ) as feat_pollutant__lag168_pm25_pm10_index,
            -- Traffic Intensity Proxy: NO2 usually spikes with CO during traffic
            safe_divide(
                feat_pollutant__lag168_no2_ppm, (feat_pollutant__lag168_co_ppm + 0.01)
            ) as feat_pollutant__lag168_no2_co_index
        from add_pollutant_features
    ),

    -- ===============================================================
    -- [TIME FEATURES]:
    -- ---------------------------------------------------------------
    add_time_features as (
        select
            * except (_feat_time__day, datetimeto_unix),
            if(
                feat_time__is_summer_break = 1
                or feat_time__is_winter_break = 1
                or feat_time__is_spring_break = 1,
                1,
                0
            ) as feat_time__is_school_break
        from
            (
                select
                    *,
                    -- fmt: off
                    -- FOURIER TERMS
                    sin(2 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k1,
                    sin(4 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k2,
                    cos(4 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k2,
                    sin(6 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k3,
                    cos(6 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k3,
                    sin(2 * 3.141592653589793 * feat_time__dow / 7) as feat_time__dow_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__dow / 7) as feat_time__dow_cos_k1,
                    sin(2 * 3.141592653589793 * feat_time__month / 12) as feat_time__month_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__month / 12) as feat_time__month_cos_k1,

                    -- DAILY/WEEKLY PATTERNS
                    if(feat_time__hour in (7, 8, 9, 17, 18, 19), 1, 0) as feat_time__is_rush_hour,
                    if(feat_time__dow in (5, 6), 1, 0) as feat_time__is_weekend,
                    if(
                        (feat_time__dow = 4 and feat_time__hour >= 21)
                        or (feat_time__dow = 5 and (feat_time__hour <= 2 or feat_time__hour >= 21))
                        or (feat_time__dow = 6 and feat_time__hour <= 2),
                    1, 0) as feat_time__is_party_night,

                    -- SEASONAL PATTERNS
                    if(feat_time__month in (2, 3, 4), 1, 0) as feat_time__is_yellow_dust_season,
                    if(feat_time__month in (6, 7), 1, 0) as feat_time__is_moonsoon_season,
                    if(feat_time__month in (11, 0, 1), 1, 0) as feat_time__is_winter_season,

                    -- NATIONAL HOLIDAYS
                    if(
                        (feat_time__month = 6 and _feat_time__day >= 21)
                        or (feat_time__month = 7 and _feat_time__day <= 21),
                    1, 0) as feat_time__is_summer_break,
                    if(
                        (feat_time__month = 11 and _feat_time__day >= 24) or (feat_time__month = 0),
                    1, 0) as feat_time__is_winter_break,
                    if(
                        (feat_time__month = 1 and _feat_time__day >= 15),
                    1, 0) as feat_time__is_spring_break
                    -- fmt: on
                from add_pollutant_ratios
            )
    ),

    -- ===============================================================
    -- [WEATHER FEATURES]:
    -- Weather data exists for all hours (complete), so we can use
    -- simple LAG for weather variables (unlike pollutants)
    -- ---------------------------------------------------------------
    add_weather_features as (
        select
            *,

            -- WIND DIRECTION TRIGONOMETRY
            sin(
                2 * 3.141592653589793 * om_wind_direction_10m / 360
            ) as feat_weather__wind_direction_10m_sin,
            cos(
                2 * 3.141592653589793 * om_wind_direction_10m / 360
            ) as feat_weather__wind_direction_10m_cos,

            -- fmt: off
            -- WEATHER INDEXES
            (om_precipitation * om_wind_speed_10m) as feat_weather__washout_index,
            safe_divide(om_relative_humidity_2m * om_pressure_msl, greatest(om_wind_speed_10m, 0.5)) as feat_weather__moisture_trap_index,
            safe_divide(om_pressure_msl, greatest(om_wind_speed_10m, 0.5)) as feat_weather__atmos_stability_index,
            safe_divide(1, (om_vapour_pressure_deficit + 0.1)) as feat_weather__moisture_saturation_index,
            (om_temperature_2m - om_dew_point_2m) as feat_weather__dew_point_depression,
            safe_divide(om_pressure_msl * om_shortwave_radiation, greatest(om_wind_speed_10m, 0.5)) as feat_weather__photolytic_flux_index,
            safe_divide(om_wind_gusts_10m, greatest(om_wind_speed_10m, 0.5)) as feat_weather__wind_burst_ratio_index_10m,

            -- CHEMISTRY-WEATHER INTERACTIONS
            (om_shortwave_radiation * feat_pollutant__lag168_no2_ppm) as feat_weather__o3_potential_lag168,
            (om_relative_humidity_2m * feat_pollutant__lag168_so2_ppm) as feat_weather__sulfate_potential_lag168,
            ((100 - om_temperature_2m) * om_relative_humidity_2m * feat_pollutant__lag168_no2_ppm) as feat_weather__nitrate_potential_lag168,

            -- LAG/DELTA FEATURES (weather is complete, so simple LAG works)
            lag(om_precipitation, 1) over (
                partition by location_id order by datetimeto_utc
            ) as feat_weather__lag1_precipitation,
            (om_pressure_msl - lag(om_pressure_msl, 1) over (
                partition by location_id order by datetimeto_utc
            )) as feat_weather__delta1_pressure_msl,
            (om_temperature_2m - lag(om_temperature_2m, 3) over (
                partition by location_id order by datetimeto_utc
            )) as feat_weather__delta3_temperature_2m
            -- fmt: on
        from add_time_features
    ),

    final_table as (
        select
            location_id,
            datetimeto_utc,
            datetimeto_local as oaq_datetimeto_local,
            concurrent_sensors,

            -- Raw pollutants (will be NULL where no measurement exists)
            pm25_ugm3,
            pm10_ugm3,
            no2_ppm,
            o3_ppm,
            so2_ppm,
            co_ppm,

            -- Location metadata
            oaq_ismonitor,
            oaq_owner_id,
            oaq_provider_id,
            oaq_latitude,
            oaq_longitude,

            -- Weather (raw)
            om_elevation,
            om_temperature_2m,
            om_relative_humidity_2m,
            om_dew_point_2m,
            om_precipitation,
            om_rain,
            om_snowfall,
            om_snow_depth,
            om_shortwave_radiation,
            om_direct_radiation,
            om_diffuse_radiation,
            om_weather_code,
            om_pressure_msl,
            om_cloud_cover,
            om_cloud_cover_low,
            om_cloud_cover_mid,
            om_cloud_cover_high,
            om_vapour_pressure_deficit,
            om_wind_speed_100m,
            om_wind_speed_10m,
            om_wind_direction_10m,
            om_wind_gusts_10m,
            om_is_day,

            -- Time features
            feat_time__hour,
            feat_time__dow,
            feat_time__month,
            feat_time__hour_sin_k1,
            feat_time__hour_cos_k1,
            feat_time__hour_sin_k2,
            feat_time__hour_cos_k2,
            feat_time__hour_sin_k3,
            feat_time__hour_cos_k3,
            feat_time__dow_sin_k1,
            feat_time__dow_cos_k1,
            feat_time__month_sin_k1,
            feat_time__month_cos_k1,
            feat_time__is_rush_hour,
            feat_time__is_weekend,
            feat_time__is_party_night,
            feat_time__is_yellow_dust_season,
            feat_time__is_moonsoon_season,
            feat_time__is_winter_season,
            feat_time__is_summer_break,
            feat_time__is_winter_break,
            feat_time__is_spring_break,
            feat_time__is_school_break,

            -- Pollutant features
            feat_pollutant__expmean_pm25_ugm3,
            feat_pollutant__expmean_pm10_ugm3,
            feat_pollutant__expmean_no2_ppm,
            feat_pollutant__expmean_o3_ppm,
            feat_pollutant__expmean_so2_ppm,
            feat_pollutant__expmean_co_ppm,
            feat_pollutant__roll720_mean_pm25_ugm3,
            feat_pollutant__roll720_mean_pm10_ugm3,
            feat_pollutant__roll720_mean_no2_ppm,
            feat_pollutant__roll720_mean_o3_ppm,
            feat_pollutant__roll720_mean_so2_ppm,
            feat_pollutant__roll720_mean_co_ppm,
            feat_pollutant__lag168_pm25_ugm3,
            feat_pollutant__lag168_pm10_ugm3,
            feat_pollutant__lag168_no2_ppm,
            feat_pollutant__lag168_o3_ppm,
            feat_pollutant__lag168_so2_ppm,
            feat_pollutant__lag168_co_ppm,
            feat_pollutant__lag168_roll24_mean_pm25_ugm3,
            feat_pollutant__lag168_roll24_mean_pm10_ugm3,
            feat_pollutant__lag168_roll24_mean_no2_ppm,
            feat_pollutant__lag168_roll24_mean_o3_ppm,
            feat_pollutant__lag168_roll24_mean_so2_ppm,
            feat_pollutant__lag168_roll24_mean_co_ppm,
            feat_pollutant__lag168_roll24_stddev_pm25_ugm3,
            feat_pollutant__lag168_roll24_stddev_pm10_ugm3,
            feat_pollutant__lag168_roll24_stddev_no2_ppm,
            feat_pollutant__lag168_roll24_stddev_o3_ppm,
            feat_pollutant__lag168_roll24_stddev_so2_ppm,
            feat_pollutant__lag168_roll24_stddev_co_ppm,
            feat_pollutant__lag168_roll24_max_pm25_ugm3,
            feat_pollutant__lag168_roll24_max_pm10_ugm3,
            feat_pollutant__lag168_roll24_max_no2_ppm,
            feat_pollutant__lag168_roll24_max_o3_ppm,
            feat_pollutant__lag168_roll24_max_so2_ppm,
            feat_pollutant__lag168_roll24_max_co_ppm,
            feat_pollutant__lag168_pm25_pm10_index,
            feat_pollutant__lag168_no2_co_index,

            -- Weather features
            feat_weather__wind_direction_10m_sin,
            feat_weather__wind_direction_10m_cos,
            feat_weather__washout_index,
            feat_weather__moisture_trap_index,
            feat_weather__atmos_stability_index,
            feat_weather__moisture_saturation_index,
            feat_weather__dew_point_depression,
            feat_weather__photolytic_flux_index,
            feat_weather__wind_burst_ratio_index_10m,
            feat_weather__o3_potential_lag168,
            feat_weather__sulfate_potential_lag168,
            feat_weather__nitrate_potential_lag168,
            feat_weather__lag1_precipitation,
            feat_weather__delta1_pressure_msl,
            feat_weather__delta3_temperature_2m

        from add_weather_features
    )

-- [FINAL QUERY OUTPUT] ======================================================
select *
from final_table
