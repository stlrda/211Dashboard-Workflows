-- 
-- 2020-04-29 (Wed.) Haresh Bhatia
--
-- This is DDL to create Following staging tables for the Covid updates on
-- STL City and County
-- 
-- A. Table STG_COVID_ZIP_STL_CITY
-- 
-- 1. The file that loads data was taken from the AWS S3 bucket with the path 
--    "s3://uw211dashboard-workbucket/covid_zip_stl_city.csv" 
--
-- 2. This file was downloaded by Keenan Berry from 
--    "https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_city.csv"
--    The data was generated by Chris Prener (SLU Proffesor & member of STL RDA)
-- 
-- 3. The first row of this (data) file has column names
--    Null values are indicated by "NaN"
--    File delimiter: ","
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_covid_zip_stl_city
(report_dt           DATE,
 zip                 VARCHAR(10),
 geo_id              VARCHAR(10),
 county              VARCHAR(30),
 state_nm            VARCHAR(30),
-- last_update         TIMESTAMP WITH TIME ZONE,
 cases               INTEGER,
 case_rate           NUMERIC(12,6),
 PRIMARY KEY (report_dt, zip, county, state_nm)
);

COMMENT ON TABLE uw211dashboard.public.stg_covid_zip_stl_city IS
'This table contains the covid case count (and corresponding rate) data, for STL city, from file "s3://uw211dashboard-workbucket/covid_zip_stl_city.csv" that was fetched from "https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_city.csv".'
;

-------------------------

-- B. Table STG_COVID_ZIP_STL_COUNTY
-- 
-- 1. The file that loads data was taken from the AWS S3 bucket with the path 
--    "s3://uw211dashboard-workbucket/covid_zip_stl_county.csv" 
--
-- 2. This file was downloaded by Keenan Berry from
--    "https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_county.csv"
--    The data was generated by Chris Prener (SLU Proffesor & member of STL RDA)
-- 
-- 3. The first row of this (data) file has column names
--    Null values are indicated by "NaN"
--    File delimiter: ","
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_covid_zip_stl_county
(report_dt           DATE,
 zip                 VARCHAR(10),
 geo_id              VARCHAR(10),
 county              VARCHAR(30),
 state_nm            VARCHAR(30),
-- last_update         TIMESTAMP WITH TIME ZONE,
 cases               INTEGER,
 case_rate           NUMERIC(12,6),
 PRIMARY KEY (report_dt, zip, county, state_nm)
);

COMMENT ON TABLE uw211dashboard.public.stg_covid_zip_stl_county IS
'This table contains the covid case count (and corresponding rate) data, for STL county, from file "s3://uw211dashboard-workbucket/covid_zip_stl_county.csv" that was fetched from "https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_county.csv".'
;


