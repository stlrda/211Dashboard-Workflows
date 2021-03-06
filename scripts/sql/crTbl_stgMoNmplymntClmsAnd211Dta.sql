-- 
-- 2020-04-30 (Thu.) Haresh Bhatia
--
-- This is DDL to create Following staging tables for MO
-- A. Uemployment Claims Data 
--    and
-- B. 211 Data
-- 
-- A. Table STG_MO_UNEMPLOYMENT_CLMS
-- 
-- 1. This table gest data from the file in AWS S3 bucket with the path 
--    "s3://uw211dashboard-workbucket/mo_unemployment_claims.csv"
--
-- 2. This file was downloaded by Keenan Berry from "https://data.mo.gov/api/views/qet9-8yam/rows.csv?accessType=DOWNLOAD"
--    This data can also be accessed via API call via data.mo.gov:
--      - API Endpoint: "https://data.mo.gov/resource/qet9-8yam.json"
--      - Dataset Name: "qet9-8yam"
-- 
-- 3. The first row of this (data) file has column names
--    File delimiter: ","
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_mo_unemployment_clms
(week_ending_sat_dt     DATE,
 county                 VARCHAR(30),
 claims_cnt             INTEGER,
 PRIMARY KEY (week_ending_sat_dt, county)
);

COMMENT ON TABLE uw211dashboard.public.stg_mo_unemployment_clms IS
'This table contains unemployment data from file "s3://uw211dashboard-workbucket/mo_unemployment_claims.csv" that was fetched from "https://data.mo.gov/resource/qet9-8yam.json".'
;

-------------------------

-- B. Table STG_MO_211_DATA
-- 
-- 1. This table gest data from the file in AWS S3 bucket with the path 
--    "s3://uw211dashboard-workbucket/..."
--    [INitially only sample file "sample_211_mo_data_20200330_20200403.csv" 
--     was used that shall be finalized later.]
--
-- 2. This file was downloaded by Keenan Berry from ...
--    The sample file was provided to Keenan Berry by Paul Sorenson with STL RDA
-- 
-- 3. The first row of this (data) file has column names
--    File delimiter: ","
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_mo_211_data
(counts          INTEGER,
 category        VARCHAR(100),
 sub_category    VARCHAR(100),
 call_dt         DATE,
 census_cd       VARCHAR(30),
 PRIMARY KEY (call_dt, census_cd, category, sub_category)
);

COMMENT ON TABLE uw211dashboard.public.stg_mo_211_data IS
'This table contains 211 data from file "s3://uw211dashboard-workbucket/..." that was fetched from ... .';


