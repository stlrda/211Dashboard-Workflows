-- 
-- 2020-04-28 (Tue.) Haresh Bhatia (HB)
--
-- This is DDL to create table STG_COVID_DLY_VIZ_CNTY_ALL.
-- 
--  ------------------------------------------------------------------------------
-- 1. The file that loads this data is located at (HB's laptop) path 
--    "C:\aData\clntsPrjcts\RgnlDtAlnce\dtaRsrcs\gitHubDta\covidDlyViz_cntyAll_pipDlm.csv"
--   [This file was adapted from file "county_full.csv" that was downloaded from 
--    "https://github.com/slu-openGIS/covid_daily_viz/tree/master/data/county". The file
--    was further modified by replacing comma with "pipe" delimiter.]
--  ------------------------------------------------------------------------------
-- 1.a. The earlier data load by HB was a test. The actual data load would happen
--      from S3 bucket file-path "s3://uw211dashboard-workbucket/covid_county_full.csv"
--
-- 2. This file was downloaded by Keenan Berry from ...
--
-- 3. The first row of this (data) file has column names
--
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_covid_dly_viz_cnty_all
(report_date         DATE,
 geo_id              VARCHAR(10),
 county              VARCHAR(30),
 state_nm            VARCHAR(30),
 cases               INTEGER,
 case_rate           NUMERIC(12,6),
 new_cases           INTEGER,
 case_avg            NUMERIC(12,6),
 death               INTEGER,
 mortality_rate      NUMERIC(12,6),
 new_deaths          INTEGER,
 death_avg           NUMERIC(12,6),
 case_fatality_rate  NUMERIC(12,6),
 PRIMARY KEY (report_date, geo_id, county, state_nm)
);

COMMENT ON TABLE uw211dashboard.public.stg_covid_dly_viz_cnty_all IS
'Table contains the covid case data (like cases, mortality, and corresponding rates and averages). The raw data for this is in file "s3://uw211dashboard-workbucket/covid_county_full.csv" that was fetched from ... .

The RATEs (CASE_ and MORTALITY_) are per 1000; case_FATALITY_RATE is fetalities per case (in other words, it is proportion of fatalities). The AVeraGes (CASE_ and DEATH_) are over the past week.
'
;


