-- 
-- 2020-05-20 (Wed.) Keenan Berry
--
-- This DML script is to migrate the 2019 data bulk for unemployment
-- statistics data to CRE_BLS_UNEMPLOYMENT_DATA table from the corresponding
-- staging table(s) - stg_bls_unemployment_data_2019
--
-- This script CONSIDERS the STaGing tables having already been updated with
-- fresh data load.
-- 
-- A. for CRE_BLS_UNEMPLOYMENT_DATA
-- -- -----------------------------
-- 1. DELETE the CRE_BLS_UNEMPLOYMENT_DATA records with MONTH_LAST_DATE on or
--    after the 'LAST_SUCCESSFUL' run date.
-- 2. INSERT the records from the staging table(s) with MONTH_LAST_DATE that is
--    on or after the 'LAST_SUCCESSFUL' run date.
--
--==================================================================================
--

-- A1. DELETE the CRE_BLS_UNEMPLOYMENT_DATA records with MONTH_LAST_DATE on or
--     after the 'LAST_SUCCESSFUL' run date.
DELETE
  FROM  cre_bls_unemployment_data
 WHERE  month_last_date >= (SELECT  lst_success_dt - buffer_cnt
                              FROM  cre_last_success_run_dt
                             WHERE  run_cd = 'MNTHLY_ALL'
                           )
;


-- A2. INSERT the records from the staging table(s) with MONTH_LAST_DATE that is
--     on or after the 'LAST_SUCCESSFUL' run date.
--
WITH lst_sccss_run AS
(SELECT  (lst_success_dt - buffer_cnt)  incr_data_ref_dt     -- This should return only one record ....
   FROM  cre_last_success_run_dt
  WHERE  run_cd = 'MNTHLY_ALL'                               -- ... given this condition.
)
INSERT INTO cre_bls_unemployment_data
(state_fips_cd,
 county_fips_cd,
 month_last_date,
 state_nm,
 county_nm,
 labor_force,
 employed,
 unemployed,
 unemployed_rate
 -- the last 2 columns of created and last-update TSPs default to currentTSP.
)
(SELECT  u.state_fips_cd,
         u.county_fips_cd,
         u.month_last_date,
         g.state_nm,
         g.county_nm,
         u.labor_force,
         u.employed,
         u.unemployed,
         u.unemployed_rate
   FROM  stg_bls_unemployment_data_2019  u,
         lkup_areas_of_intr_geo_scope    g,
         lst_sccss_run                   sr
  WHERE  u.geo_id           = g.geo_id
    AND  u.month_last_date >= sr.incr_data_ref_dt
)


---------------------------------------------------------------------------

