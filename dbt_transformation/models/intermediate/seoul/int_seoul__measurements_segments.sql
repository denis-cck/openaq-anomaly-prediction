-- ===========================================================================
-- INT_SEOUL__MEASUREMENTS_SEGMENTS.SQL
-- ===========================================================================
-- models/intermediate/seoul/int_seoul__measurements_segments.sql
--
{{
    config(
        materialized="table",
        partition_by={
            "field": "datetimeto_utc",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["location_id", "segment_id"],
    )
}}  -- No incremental strategy to simplify shattering


-- [CTEs] ====================================================================
with
    pivoted as (select * from {{ ref("int_seoul__measurements_pivoted") }}),

    -- =======================================================================
    -- STRATEGY A: Time since last PERFECT hour (all sensors reporting)
    -- -----------------------------------------------------------------------
    --
    -- Identify hours where ALL 6 sensors are present as the baseline
    perfection_check as (
        select *, case when concurrent_sensors = 6 then 1 else 0 end as is_perfect_hour
        from pivoted
    ),

    -- Track the timestamp of the PREVIOUS perfect hour
    track_last_perfect as (
        select
            *,
            -- Identify the first ever perfect hour for each location
            min(case when is_perfect_hour = 1 then datetimeto_utc end) over (
                partition by location_id
            ) as starting_perfect_hour_timestamp,

            last_value(
                -- "IGNORE NULLS" finds the last time we saw a 1 (perfect hour)
                case when is_perfect_hour = 1 then datetimeto_utc end ignore nulls
            ) over (
                partition by location_id
                order by datetimeto_utc
                -- between the first and the n-1 row in the partition
                rows between unbounded preceding and 1 preceding
            ) as last_perfect_hour_timestamp
        from perfection_check
    ),
    calc_hours_since_last_perfect as (
        select
            *,
            datetime_diff(
                datetimeto_utc, last_perfect_hour_timestamp, hour
            ) as hours_since_last,
        from track_last_perfect
    ),

    -- Calculate the gap and identify shatter points:
    -- A) It's the first record ever
    -- B) The gap in between 2 perfect hours is > 4h since the LAST perfect hour
    shatter_logic as (
        select
            *,
            case
                -- First record
                when last_perfect_hour_timestamp is null
                then 1
                -- Gap > 4h since last perfect hour
                when is_perfect_hour = 1 and hours_since_last > 4
                then 1
                -- No gaps larger than 4h
                else 0
            end as is_new_segment
        from calc_hours_since_last_perfect
        where datetimeto_utc >= starting_perfect_hour_timestamp
    ),

    -- Generate segments from the identified shatter points
    -- We only assign segment IDs to perfect hours or the small gaps between them
    generate_ids as (
        select
            *,
            sum(is_new_segment) over (
                partition by location_id order by datetimeto_utc
            ) as segments_num
        from shatter_logic

        -- Filter: We only want to keep rows that are part of a valid segment
        -- This removes long gaps (>4h) from our final training table
        where is_perfect_hour = 1 or hours_since_last <= 4
    ),

    -- Identify the last perfect hour for every individual segment
    trimming_metadata as (
        select
            *,
            max(case when is_perfect_hour = 1 then datetimeto_utc end) over (
                partition by location_id, segments_num
            ) as last_perfect_hour_in_segment
        from generate_ids
    ),
    trimmed_data as (
        -- Physically remove the trailing imperfect rows
        select *
        from trimming_metadata
        where datetimeto_utc <= last_perfect_hour_in_segment
    ),

    -- =======================================================================
    -- STRATEGY B: Time since last VALID entry for each individual sensor
    -- -----------------------------------------------------------------------
    --
    -- -- Track the timestamp of the PREVIOUS valid entry for each pollutant
    -- pollutant_trackers as (
    -- select
    -- *,
    -- last_value(
    -- case when pm25_ugm3 is not null then datetimeto_utc end ignore
    -- nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_pm25,
    -- last_value(
    -- case when pm10_ugm3 is not null then datetimeto_utc end ignore
    -- nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_pm10,
    -- last_value(
    -- case when no2_ppm is not null then datetimeto_utc end ignore nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_no2,
    -- last_value(
    -- case when o3_ppm is not null then datetimeto_utc end ignore nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_o3,
    -- last_value(
    -- case when so2_ppm is not null then datetimeto_utc end ignore nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_so2,
    -- last_value(
    -- case when co_ppm is not null then datetimeto_utc end ignore nulls
    -- ) over (
    -- partition by location_id
    -- order by datetimeto_utc
    -- rows between unbounded preceding and 1 preceding
    -- ) as last_co
    -- from pivoted
    -- ),
    -- -- Calculate the maximum gap amongst ALL 6 pollutants at every row
    -- gap_analysis as (
    -- select
    -- *,
    -- greatest(
    -- coalesce(datetime_diff(datetimeto_utc, last_pm25, hour), 0),
    -- coalesce(datetime_diff(datetimeto_utc, last_pm10, hour), 0),
    -- coalesce(datetime_diff(datetimeto_utc, last_no2, hour), 0),
    -- coalesce(datetime_diff(datetimeto_utc, last_o3, hour), 0),
    -- coalesce(datetime_diff(datetimeto_utc, last_so2, hour), 0),
    -- coalesce(datetime_diff(datetimeto_utc, last_co, hour), 0)
    -- ) as max_individual_gap
    -- from pollutant_trackers
    -- ),
    -- -- Identify shatter points: shatter ONLY if 
    -- -- A) It's the first record ever
    -- -- B) The "weakest" pollutant has a gap > 3h
    -- shatter_logic as (
    -- select
    -- *,
    -- case
    -- -- First record if any of the last_pollutant is null
    -- when last_pm25 is null or max_individual_gap > 3 then 1 else 0
    -- end as is_new_segment
    -- from gap_analysis
    -- ),
    -- -- Generate segments from the identified shatter points
    -- -- We only assign segment IDs to perfect hours or the small gaps
    -- between them
    -- generate_ids as (
    -- select
    -- *,
    -- max_individual_gap as hours_since_last,
    -- sum(is_new_segment) over (
    -- partition by location_id order by datetimeto_utc
    -- ) as segments_num
    -- from shatter_logic
    -- -- Filter: We only want to keep rows that are part of a valid "bridge"
    -- -- This removes long "Dead Zones" (>3h) from our final training table
    -- where max_individual_gap <= 3 or last_pm25 is null
    -- ),
    --
    -- =======================================================================
    -- POST SHATTERING
    -- -----------------------------------------------------------------------
    --
    -- Create aggregated segment columns
    aggregate_segments as (
        select
            location_id,
            datetimeto_utc,
            hours_since_last,
            concurrent_sensors,
            is_new_segment,

            pm25_ugm3,
            pm10_ugm3,
            no2_ppm,
            o3_ppm,
            so2_ppm,
            co_ppm,

            concat(location_id, '_S', segments_num) as segment_id,
            min(datetimeto_utc) over (
                partition by location_id, segments_num
            ) as segment_start,
            max(datetimeto_utc) over (
                partition by location_id, segments_num
            ) as segment_end,
            segments_num,
            count(distinct segments_num) over (
                partition by location_id
            ) as segments_total,
            count(case when is_perfect_hour = 1 then 1 end) over (
                partition by location_id, segments_num
            ) as segment_perfect_hours
        from trimmed_data
    ),

    -- Create aggregated segment columns (twice)
    aggregate_segments_twice as (
        select
            *,
            -- At minimum we have 2h segments, so we always add 1 to the diff
            datetime_diff(segment_end, segment_start, hour) + 1 as segment_total_hours,
        from aggregate_segments
    ),

    -- FINAL TABLE: compute final segment metrics
    final_segments as (
        select
            *,
            safe_divide(
                segment_perfect_hours, segment_total_hours
            ) as segment_perfect_density,
            case
                when segment_total_hours >= 24 * 14
                then 'gold'
                when segment_total_hours >= 24 * 7
                then 'silver'
                else 'bronze'
            end as segment_tier
        from aggregate_segments_twice
    )

-- [FINAL QUERY OUTPUT] ======================================================
select
    location_id,
    datetimeto_utc,
    hours_since_last,
    concurrent_sensors,
    is_new_segment,

    pm25_ugm3,
    pm10_ugm3,
    no2_ppm,
    o3_ppm,
    so2_ppm,
    co_ppm,

    segment_id,
    segment_start,
    segment_end,
    segment_perfect_hours,
    segment_total_hours,
    segment_perfect_density,
    segment_tier,

    segments_num,
    segments_total

from final_segments
