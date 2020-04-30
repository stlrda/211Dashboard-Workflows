-- 
-- 2020-04-28 (Tue.) Haresh Bhatia
--
-- This is DDL to create table STG_COVID_DLY_VIZ_CNTY_ALL.
-- 
-- 1. The file that loads this data is located at (HB's laptop) path 
--    "C:\aData\clntsPrjcts\RgnlDtAlnce\dtaRsrcs\gitHubDta\covidDlyViz_cntyAll_pipDlm.csv"
--   [This file was adapted from file "county_full.csv" that was downloaded from 
--    "https://github.com/slu-openGIS/covid_daily_viz/tree/master/data/county". The file
--    was further modified by replacing comma with "pipe" delimiter.]
-- 
-- 2. The first row of this (data) file has column names
--
CREATE TABLE 
          IF NOT EXISTS  public.stg_covid_dly_viz_cnty_all
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

COMMENT ON TABLE stg_covid_dly_viz_cnty_all IS
'Table contains the raw data from file ''county_full.csv'' at ''https://github.com/slu-openGIS/covid_daily_viz/tree/master/data/county''.'
;


