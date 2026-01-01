-- ===========================================================================
-- STG_OPENAQ__MEASUREMENTS.SQL
-- ===========================================================================
-- models/staging/_openaq/stg_openaq__measurements.sql
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
        cluster_by=["sensor_id", "datetimeto_utc"],
    )
}}


-- [CTEs] ====================================================================
with
    new_or_updated_rows as (
        select *
        from {{ source("src_openaq", "src_openaq__measurements") }}

        {% if is_incremental() %}
            -- Overwrite full partitions that contain rows updated since last run
            where
                timestamp_trunc(`period_datetimeTo_utc`, day) in (
                    select distinct timestamp_trunc(`period_datetimeTo_utc`, day)
                    from {{ source("src_openaq", "src_openaq__measurements") }}
                    -- FinOps: Only scan the last 10 days of 'this' staging table
                    -- We can do a full refresh if we need to go further back
                    -- in case of downtimes, late updates...
                    where
                        period_datetimeto_utc  -- FinOps: only look at recent data (SOURCE) to find updates
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
                partition by sensor_id, `period_datetimeTo_utc` order by updated_at desc
            )
            = 1
    )

-- [FINAL QUERY OUTPUT] ======================================================
select
    sensor_id,
    value,
    -- `flagInfo_hasFlags`,
    parameter_id,
    parameter_name,
    parameter_units,
    -- `parameter_displayName`,
    -- period_label,
    -- period_interval,
    -- `period_datetimeFrom_utc` as datetimefrom_utc,
    -- `period_datetimeFrom_local` as datetimefrom_local,
    `period_datetimeTo_utc` as datetimeto_utc,
    `period_datetimeTo_local` as datetimeto_local,
    -- summary_min,
    -- summary_q02,
    -- summary_q25,
    -- summary_median,
    -- summary_q75,
    -- summary_q98,
    -- summary_max,
    -- summary_avg,
    -- summary_sd,
    -- `coverage_expectedCount`,
    -- `coverage_expectedInterval`,
    -- `coverage_observedCount`,
    -- `coverage_observedInterval`,
    -- `coverage_percentComplete`,
    -- `coverage_percentCoverage`,
    -- `coverage_datetimeFrom_utc`,
    -- `coverage_datetimeFrom_local`,
    -- `coverage_datetimeTo_utc`,
    -- `coverage_datetimeTo_local`,
    ingested_at,
    updated_at,
    refreshed_at

from deduplicated_rows
