-- depends_on: {{ ref('int_seoul__measurements_segments') }}
-- ===========================================================================
-- ML_SEOUL__XGB_FEATURES.SQL
-- ===========================================================================
-- models/serving/seoul/ml_seoul__xgb_features.sql
--
-- {{ config(materialized="view", cluster_by=["location_id"]) }}
{{
    config(
        materialized="table",
        partition_by={
            "field": "datetimeto_utc",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["location_id", "segment_tier"],
    )
}}  -- No incremental strategy to simplify shattering

-- [CTEs] ====================================================================
with

    -- ===============================================================
    -- [JOINS]: join all relevant tables to get exogenous features
    -- ---------------------------------------------------------------
    gold_segments_joined as (
        select
            segments.location_id,
            segments.datetimeto_utc,
            segments.concurrent_sensors,
            -- segments.hours_since_last,
            -- segments.is_new_segment,
            segments.segment_id,
            segments.segment_start,
            segments.segment_end,
            segments.segment_perfect_hours,
            segments.segment_total_hours,
            segments.segment_perfect_density,
            segments.segment_tier,

            segments.segments_num,
            segments.segments_total,

            segments.pm25_ugm3,
            segments.pm10_ugm3,
            segments.no2_ppm,
            segments.o3_ppm,
            segments.so2_ppm,
            segments.co_ppm,

            segments.datetimeto_local as oaq_datetimeto_local,
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
            extract(hour from segments.datetimeto_local) as feat_time__hour,
            extract(day from segments.datetimeto_local) as _feat_time__day,  -- just for calculation
            mod(
                extract(dayofweek from segments.datetimeto_local) + 5, 7
            ) as feat_time__dow,  -- monday=0
            extract(month from segments.datetimeto_local) - 1 as feat_time__month,  -- starts from 0

        from {{ ref("int_seoul__measurements_segments") }} segments

        left join
            {{ ref("stg_openaq__locations") }} locations
            on segments.location_id = locations.id

        left join
            {{ ref("stg_openmeteo__historical") }} openmeteo
            on segments.location_id = openmeteo.location_id
            and segments.datetimeto_utc = openmeteo.datetimeto_utc

    -- filter when consuming the table
    -- where segments.segment_tier = 'gold'
    ),

    -- ===============================================================
    -- [TIME FEATURES]:
    -- ---------------------------------------------------------------
    add_time_features as (
        select
            * except (_feat_time__day),
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
                    -- ---------------------------------------------------------------
                    -- FOURIER TERMS:
                    --
                    sin(2 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k1,
                    sin(4 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k2,
                    cos(4 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k2,
                    sin(6 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k3,
                    cos(6 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k3,
                    -- sin(8 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k4,
                    -- cos(8 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k4,
                    -- sin(10 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_sin_k5,
                    -- cos(10 * 3.141592653589793 * feat_time__hour / 24) as feat_time__hour_cos_k5,
                    sin(2 * 3.141592653589793 * feat_time__dow / 7) as feat_time__dow_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__dow / 7) as feat_time__dow_cos_k1,
                    sin(2 * 3.141592653589793 * feat_time__month / 12) as feat_time__month_sin_k1,
                    cos(2 * 3.141592653589793 * feat_time__month / 12) as feat_time__month_cos_k1,

                    -- ---------------------------------------------------------------
                    -- DAILY/WEEKLY PATTERNS:
                    --
                    if(feat_time__hour in (7, 8, 9, 17, 18, 19), 1, 0) as feat_time__is_rush_hour,
                    if(feat_time__dow in (5, 6), 1, 0) as feat_time__is_weekend,
                    if(
                        (feat_time__dow = 4 and feat_time__hour >= 21)  -- Friday 9pm+
                        or (feat_time__dow = 5 and (feat_time__hour <= 2 or feat_time__hour >= 21))  -- Saturday 2am-/9pm+
                        or (feat_time__dow = 6 and feat_time__hour <= 2),  -- Sunday 2am-
                    1, 0 ) as feat_time__is_party_night,
                    -- Specific "high emission + low dispersion" window in the evening, hard to predict
                    if(feat_time__hour in (20, 21, 22, 23, 0), 1, 0) as feat_time__is_evening_transition,

                    -- ---------------------------------------------------------------
                    -- SEASONAL PATTERNS (SOUTH KOREA):
                    --
                    if(feat_time__month in (2, 3, 4), 1, 0) as feat_time__is_yellow_dust_season,  -- Mar-Apr-May
                    if(feat_time__month in (6, 7), 1, 0) as feat_time__is_moonsoon_season,  -- Jun-Jul
                    if(feat_time__month in (11, 0, 1), 1, 0) as feat_time__is_winter_season,  -- Dec-Jan-Feb

                    -- ---------------------------------------------------------------
                    -- NATIONAL HOLIDAYS:
                    --
                    if(
                        (feat_time__month = 6 and _feat_time__day >= 21)
                        or (feat_time__month = 7 and _feat_time__day <= 21),
                    1, 0) as feat_time__is_summer_break,  -- Late Jul to Late Aug
                    if(
                        (feat_time__month = 11 and _feat_time__day >= 24) or (feat_time__month = 0),
                    1, 0) as feat_time__is_winter_break,  -- Late Dec to Jan
                    if(
                        (feat_time__month = 1 and _feat_time__day >= 15),
                    1, 0) as feat_time__is_spring_break,  -- Late Feb to March 2
                    -- fmt: on
                from gold_segments_joined
            )
    ),

    -- ===============================================================
    -- [POLLUTANTS FEATURES]:
    -- ---------------------------------------------------------------
    add_pollutant_features as (
        select *,
        -- ---------------------------------------------------------------
        -- -- [POLLUTION RATIOS] (CHEMICAL CHARACTERIZATION)
        -- --
        -- -- Fine Particle Ratio (dust): High = Combustion/Smog, Low = Dust/Sand
        -- safe_divide(
        -- feat_pollutant__lag168_pm25_ugm3,
        -- (feat_pollutant__lag168_pm10_ugm3 + 0.1)
        -- ) as feat_pollutant__lag168_pm25_pm10_index,
        -- -- Traffic Intensity Proxy: NO2 usually spikes with CO during traffic
        -- safe_divide(
        -- feat_pollutant__lag168_no2_ppm, (feat_pollutant__lag168_co_ppm + 0.01)
        -- ) as feat_pollutant__lag168_no2_co_index
        from
            (
                select
                    *,

                    -- ---------------------------------------------------------------
                    -- [EXPANDING MEAN] (LOCATION LIFETIME):
                    --
                    -- Beware of data leakage /!\ averages up to the preceding row
                    avg(pm25_ugm3) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_pm25_ugm3,
                    avg(pm10_ugm3) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_pm10_ugm3,
                    avg(no2_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_no2_ppm,
                    avg(o3_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_o3_ppm,
                    avg(so2_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_so2_ppm,
                    avg(co_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between unbounded preceding and 1 preceding
                    ) as feat_pollutant__expmean_co_ppm,

                    -- ---------------------------------------------------------------
                    -- [EXPANDING MEAN] (30 DAYS):
                    --
                    -- Beware of data leakage /!\ averages up to the preceding row
                    avg(pm25_ugm3) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_pm25_ugm3,
                    avg(pm10_ugm3) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_pm10_ugm3,
                    avg(no2_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_no2_ppm,
                    avg(o3_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_o3_ppm,
                    avg(so2_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_so2_ppm,
                    avg(co_ppm) over (
                        partition by location_id
                        order by datetimeto_utc
                        rows between 719 preceding and 1 preceding
                    ) as feat_pollutant__roll720_mean_co_ppm,

                -- ---------------------------------------------------------------
                -- -- [LAG FEATURES] (LONG TERM MEMORY):
                -- --
                -- -- Partition by segment_id to make sure we never cross gaps (lazy)
                -- lag(pm25_ugm3, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_pm25_ugm3,
                -- lag(pm10_ugm3, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_pm10_ugm3,
                -- lag(no2_ppm, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_no2_ppm,
                -- lag(o3_ppm, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_o3_ppm,
                -- lag(so2_ppm, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_so2_ppm,
                -- lag(co_ppm, 168) over (
                -- partition by segment_id order by datetimeto_utc
                -- ) as feat_pollutant__lag168_co_ppm,
                -- ---------------------------------------------------------------
                -- -- [ROLLING FEATURES] (LONG TERM MEMORY):
                -- --
                -- -- 24-hour rolling average of the lag168 value
                -- avg(pm25_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_pm25_ugm3,
                -- avg(pm10_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_pm10_ugm3,
                -- avg(no2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_no2_ppm,
                -- avg(o3_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_o3_ppm,
                -- avg(so2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_so2_ppm,
                -- avg(co_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_mean_co_ppm,
                -- -- 24-hour rolling stddev of the lag168 value
                -- stddev(pm25_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_pm25_ugm3,
                -- stddev(pm10_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_pm10_ugm3,
                -- stddev(no2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_no2_ppm,
                -- stddev(o3_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_o3_ppm,
                -- stddev(so2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_so2_ppm,
                -- stddev(co_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_stddev_co_ppm,
                -- -- 24-hour rolling max of the lag168 value
                -- max(pm25_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_pm25_ugm3,
                -- max(pm10_ugm3) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_pm10_ugm3,
                -- max(no2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_no2_ppm,
                -- max(o3_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_o3_ppm,
                -- max(so2_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_so2_ppm,
                -- max(co_ppm) over (
                -- partition by segment_id
                -- order by datetimeto_utc
                -- rows between 191 preceding and 168 preceding
                -- ) as feat_pollutant__lag168_roll24_max_co_ppm,
                from add_time_features
            )
    ),

    -- ===============================================================
    -- [WEATHER FEATURES]:
    -- ---------------------------------------------------------------
    add_weather_features as (
        select
            *,

            -- ---------------------------------------------------------------
            -- [TRIGONOMETRY]: Wind Direction (CIRCULAR)
            --
            -- sin(
            -- 2 * 3.141592653589793 * om_wind_direction_10m / 360
            -- ) as feat_weather__wind_direction_10m_sin,
            -- cos(
            -- 2 * 3.141592653589793 * om_wind_direction_10m / 360
            -- ) as feat_weather__wind_direction_10m_cos,
            -- Betther than normal sin/cos features: U/V components
            (
                om_wind_speed_10m
                * sin(2 * 3.141592653589793 * om_wind_direction_10m / 360)
            ) as feat_weather__wind_u_10m,
            (
                om_wind_speed_10m
                * cos(2 * 3.141592653589793 * om_wind_direction_10m / 360)
            ) as feat_weather__wind_v_10m,

            -- ---------------------------------------------------------------
            -- [NEW] VERTICAL STABILITY PROXIES
            --
            -- 1. WIND SHEAR: Difference between upper and lower wind.
            -- High Shear = The air above is moving, but ground air is stuck
            -- (Decoupled/Inversion).
            (
                om_wind_speed_100m - om_wind_speed_10m
            ) as feat_weather__wind_shear_100m_10m,

            -- Rapid cooling (negative large number) often triggers boundary layer
            -- collapse.
            (
                om_temperature_2m - lag(om_temperature_2m, 1) over (
                    partition by segment_id order by datetimeto_utc
                )
            ) as feat_weather__cooling_rate_1h,

            (
                (
                    om_temperature_2m - lag(om_temperature_2m, 1) over (
                        partition by segment_id order by datetimeto_utc
                    )
                )
                * safe_divide(1, greatest(om_wind_speed_10m, 0.5))
            ) as feat_weather__stagnation_cooling_index,

            -- fmt: off
            -- ---------------------------------------------------------------
            -- [WEATHER INDEXES]
            --
            -- DISPERSION: High wind and high rain (Inverse effect: High value = CLEAN air)
            (om_precipitation * om_wind_speed_10m) as feat_weather__washout_index,

            -- STAGNATION: High humidity/pressure trapped by low wind
            SAFE_DIVIDE(om_relative_humidity_2m * om_pressure_msl, GREATEST(om_wind_speed_10m, 0.5)) as feat_weather__moisture_trap_index,

            -- ATMOSPHERIC STABILITY: Boundary layer proxy
            SAFE_DIVIDE(om_pressure_msl, GREATEST(om_wind_speed_10m, 0.5)) as feat_weather__atmos_stability_index,

            -- SATURATION: Fog risk (Inverse VDP)
            SAFE_DIVIDE(1, (om_vapour_pressure_deficit + .1)) as feat_weather__moisture_saturation_index,

            -- DEW POINT DEPRESSION: Temperature minus dew point
            (om_temperature_2m - om_dew_point_2m) as feat_weather__dew_point_depression,

            -- PHOTOLYTIC FLUX: Ozone driver (High = Photochemical risk)
            SAFE_DIVIDE(om_pressure_msl * om_shortwave_radiation, GREATEST(om_wind_speed_10m, 0.5)) as feat_weather__photolytic_flux_index,

            -- TURBULENCE: Wind burst ratio (stagnant air)
            SAFE_DIVIDE(om_wind_gusts_10m, GREATEST(om_wind_speed_10m, 0.5)) as feat_weather__wind_burst_ratio_index_10m,

            -- WIND CHILL: Low temperature and high wind => Need to use an actual wind chill formula
            --
            -- ---------------------------------------------------------------
            -- [CHEMISTRY-WEATHER INTERACTIONS]
            -- --
            -- -- Ozone Potential (Lag168): High Sun + High NO2 History -> O3
            -- (om_shortwave_radiation * feat_pollutant__lag168_no2_ppm) as feat_weather__o3_potential_lag168,
            -- -- Sulfate Formation Potential: Humidity + SO2 History -> PM2.5
            -- -- High Humidity converts industrial SO2 gas into liquid/solid particles
            -- (om_relative_humidity_2m * feat_pollutant__lag168_so2_ppm) as feat_weather__sulfate_potential_lag168,
            -- -- Nitrate Formation Potential: Humidity + Cold + NO2 History -> PM2.5
            -- -- Nitrates form best when it is DAMP and COLD: Higher value = Higher risk of Nitrate PM formation
            -- -- If Temp is low (e.g. -5C), we want a HIGH number.
            -- ((100 - om_temperature_2m) * om_relative_humidity_2m * feat_pollutant__lag168_no2_ppm) as feat_weather__nitrate_potential_lag168,
            -- ---------------------------------------------------------------
            -- [LAG/DELTA FEATURES]
            --
            -- Lagged precipitation (1 hour lag)
            lag(om_precipitation, 1) over (
                partition by segment_id order by datetimeto_utc
            ) as feat_weather__lag1_precipitation,

            -- Delta pressure (current - 1 hour lag)
            (om_pressure_msl - lag(om_pressure_msl, 1) over (
                partition by segment_id order by datetimeto_utc
            )) as feat_weather__delta1_pressure_msl,

            -- Delta temperature (current - 3 hour lag)
            (om_temperature_2m - lag(om_temperature_2m, 3) over (
                partition by segment_id order by datetimeto_utc
            )) as feat_weather__delta3_temperature_2m,
            -- fmt: on
        from add_pollutant_features
    ),

    final_table as (select * from add_weather_features)

-- [FINAL QUERY OUTPUT] ======================================================
select
    * except (
        om_apparent_temperature,
        om_surface_pressure,
        om_global_tilted_irradiance,
        om_direct_normal_irradiance,
        om_terrestrial_radiation,
        om_et0_fao_evapotranspiration,

        --
        om_wind_direction_10m,
        om_wind_direction_100m
    -- om_wind_speed_100m, -- should i drop this too?
    -- om_rain,
    -- om_snowfall,
    )
from final_table
