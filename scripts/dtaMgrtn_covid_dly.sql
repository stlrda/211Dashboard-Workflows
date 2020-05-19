-- 
-- 2020-05-07 (Thu.) Haresh Bhatia
--
-- This is DML script is to migrate the next incremental data bulk from staging tables
-- to the core tables.
-- 
-- This script CONSIDERS the STaGing tables having already been updated with
-- fresh data load.
-- 
-- A. for CRE_COVID_DATA
-- -- ------------------
--  1. DELETE the CRE_COVID_DATA records with REPORT_DATE on or after the
--     'LAST_SUCCESSFUL' run date.
--  2. INSERT the records from the staging tables with REPORT_DATE that is on or
--     after the 'LAST_SUCCESSFUL' run date.
--
-- ---------------------------------------------------------------------
-- 2020-05-15 (Fri.) Haresh Bhatia.
--
-- Chanded the reference day for incremental data to be a day prior to the 
-- LST_CUCESS_DT. 
-- 
-- This is to circumvent the issues with missing a day's worth of data due to
-- the corresponding daily DAG running mid-night. [This is not a perfect 
-- solution, but a solution until a better resolution is found / decided.]
--
-- ---------------------------------------------------------------------
-- 2020-05-19 (Tue.) Haresh Bhatia.
--
-- Chanded the reference day for incremental data to use the new BUFFER_CNT for
-- offsetting the LAST_SUCCESS_DT so as to avoid missing any data records.
--
--==================================================================================
--
--

-- A1. DELETE the CRE_COVID_DATA records with REPORT_DATE on or after the
--     'LAST_SUCCESSFUL' run date.
DELETE
  FROM  cre_covid_data
 WHERE  report_date >= (SELECT  lst_success_dt - buffer_cnt
                          FROM  cre_last_success_run_dt
                         WHERE  run_cd = 'DLY_ALL'
                       )
;

-- A2. INSERT the records from the staging tables with REPORT_DATE that is on or
--     after the 'LAST_SUCCESSFUL' run date.
WITH lst_sccss_run AS
--(SELECT  lst_success_dt   incr_data_ref_dt      -- Change done on 05/15/2020
(SELECT  (lst_success_dt - buffer_cnt)  incr_data_ref_dt   -- Change done on 05/15/2020
   FROM  cre_last_success_run_dt
  WHERE  run_cd = 'DLY_ALL'
)
INSERT INTO cre_covid_data
(data_source,
 report_date,
 state_nm,
 county_nm,
 geo_id,
 zip_cd,
 cases,
 case_rate,
 new_cases,
 case_avg,
 death,
 mortality_rate,
 new_deaths,
 death_avg,
 case_fatality_rate  -- the last 2 columns of created and last-update TSPs default to currentTSP.
)
(SELECT 'ALL_COUNTY'       data_source,
         ca.report_date,
         ca.state_nm,
         ca.county        county_nm,
         ca.geo_id,
         ''               zip_cd,
         ca.cases,
         ca.case_rate,
         ca.new_cases,
         ca.case_avg,
         ca.death,
         ca.mortality_rate,
         ca.new_deaths,
         ca.death_avg,
         ca.case_fatality_rate
   FROM  stg_covid_dly_viz_cnty_all   ca,
         lst_sccss_run                  sr
  WHERE  ca.report_date >= sr.incr_data_ref_dt
 UNION ALL
 SELECT 'STL_COUNTY'    data_source,
         cn.report_dt   report_date,
         cn.state_nm,
         cn.county      county_nm,
         cn.geo_id,
         cn.zip         zip_cd,
         cn.cases,
         cn.case_rate,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL
   FROM  stg_covid_zip_stl_county   cn,
         lst_sccss_run              sr
  WHERE  cn.report_dt >= sr.incr_data_ref_dt
 UNION ALL
 SELECT 'STL_CITY'      data_source,
         ct.report_dt   report_date,
         ct.state_nm,
         ct.county      county_nm,
         ct.geo_id,
         ct.zip         zip_cd,
         ct.cases,
         ct.case_rate,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL
   FROM  stg_covid_zip_stl_city   ct,
         lst_sccss_run            sr
  WHERE  ct.report_dt >= sr.incr_data_ref_dt
)
;

---------------------------------------------------------------------------

