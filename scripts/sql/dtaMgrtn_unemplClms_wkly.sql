-- 
-- 2020-05-19 (Tue.) Haresh Bhatia
--
-- This is DML script is to migrate the next incremental data bulk for
-- unemployment data to CRE_UNEMPLOYMENT_CLMS table from the corresponding
-- staging tables.
-- 
-- This script CONSIDERS the STaGing tables having already been updated with
-- fresh data load.
-- 
-- A. for CRE_UNEMPLOYMENT_CLMS
-- -- -------------------------
--  1. DELETE the CRE_UNEMPLOYMENT_CLMS records with PERIOD_END_DATE on or after
--     the 'LAST_SUCCESSFUL' run date.
--  2. INSERT the records from the staging tables with PERIOD_END_DATE that is
--      on or after the 'LAST_SUCCESSFUL' run date.
--
--==================================================================================
--

-- A1. DELETE the CRE_UNEMPLOYMENT_CLMS records with PERIOD_END_DATE on or after the
--     'LAST_SUCCESSFUL' run date.
DELETE
  FROM  cre_unemployment_clms
 WHERE  period_end_date >= (SELECT  lst_success_dt - buffer_cnt
                              FROM  cre_last_success_run_dt
                             WHERE  run_cd = 'WKLY_ALL'
                           )
;

-- A2. INSERT the records from the staging tables with PERIOD_END_DATE that is on or
--     after the 'LAST_SUCCESSFUL' run date.
WITH lst_sccss_run AS
(SELECT  (lst_success_dt - buffer_cnt)  incr_data_ref_dt     -- This should return only one record ....
   FROM  cre_last_success_run_dt
  WHERE  run_cd = 'WKLY_ALL'                                 -- ... given this condition.
)
INSERT INTO cre_unemployment_clms
(data_source,
 period_end_date,   -- This is week ending Sat. date for MO
 geo_id,
 state_nm,
 county_nm,
 claims_cnt         -- unemployment claims count.
 -- the last 2 columns of created and last-update TSPs default to currentTSP.
)
(SELECT 'STG_MO_UNEMP_CLMS'       data_source,
         u.week_ending_sat_dt,
         g.geo_id,
         g.state_nm,
         g.county_nm,
         u.claims_cnt
   FROM  stg_mo_unemployment_clms   u,
         lkup_vu_county_geoid       g,
         lst_sccss_run              sr
  WHERE  u.county              =  g.county_nm
    AND  g.state_nm            = 'Missouri'
    AND  u.week_ending_sat_dt >= sr.incr_data_ref_dt
)
;

---------------------------------------------------------------------------

