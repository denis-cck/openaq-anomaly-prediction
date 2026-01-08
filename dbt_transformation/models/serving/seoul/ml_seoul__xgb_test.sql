-- ===========================================================================
-- ML_SEOUL__XGB_TEST.SQL
-- ===========================================================================
-- models/serving/seoul/ml_seoul__xgb_test.sql
--
{{ config(materialized="view") }}


-- [CTEs] ====================================================================
with

    only_test_locations as (
        select *
        from {{ ref("ml_seoul__xgb_features") }}
        where
            -- HARD-CODED TEST LOCATIONS
            location_id in (
                '2622837',
                '2623116',
                '2623504',
                '2623643',
                '2622918',
                '2623277',
                '2623465',
                '2623532',
                '2622689',
                '2622825',
                '2623557',
                '2623394',
                '2623718',
                '2623617'
            )
    )

-- [FINAL QUERY OUTPUT] ======================================================
select *
from only_test_locations
