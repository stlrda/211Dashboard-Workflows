-- 
-- 2020-05-19 (Thu.) Haresh Bhatia
--
-- This is DDL to create 
-- A. STG_BLS_UNEMPLOYMENT_DATA
--    and
-- B. STG_BLS_UNEMPLOYMENT_2019
-- 
-- A. Table STG_BLS_UNEMPLOYMENT_DATA 
--  1. This table gets data from the file in AWS S3 bucket with the path 
--     "s3://uw211dashboard-workbucket/unemployment_stats_current.csv"
--  2. The data in that file was downloaded by Keenan Berry from Bureau of Labor
--     Satistics (BLS) web-page ...
--  3. The first row of this (data) file has column names
--
-- ==========================================================================================
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_bls_unemployment_data_curr
(state_fips_cd     VARCHAR(2),
 county_fips_cd    VARCHAR(3),
 geo_id            VARCHAR(10),  -- This may be redundant (as State and County FIPS together make GEO_ID) - but since data file has it, we load it.
 month_last_date   DATE,
 labor_force       INTEGER,
 employed          INTEGER,
 unemployed        INTEGER,
 unemployed_rate   NUMERIC(6,3),
 PRIMARY KEY (state_fips_cd, county_fips_cd, month_last_date)
);

COMMENT ON TABLE uw211dashboard.public.stg_bls_unemployment_data_curr IS
'This table contains unemployment data collected from Bureau of Labor Statistics (BLS) by Keenan Berry. These data were downloaded into the S3 file at path "s3://uw211dashboard-workbucket/unemployment_stats_current.csv".'
;

-------------------------------------------------------------------------------------------

-- B. STG_BLS_UNEMPLOYMENT_2019
--  1. This table gets data from the file in AWS S3 bucket with the path 
--     "s3://uw211dashboard-workbucket/unemployment_stats_2019.csv"
--  2. The data in that file was downloaded by Keenan Berry (for 2019 unemp.)
--     from Bureau of Labor Satistics (BLS) web-page ...
--  3. The first row of this (data) file has column names
--
-------------------------
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.stg_bls_unemployment_data_2019
(state_fips_cd     VARCHAR(2),
 county_fips_cd    VARCHAR(3),
 geo_id            VARCHAR(10),  -- This may be redundant (as State and County FIPS together make GEO_ID) - but since data file has it, we load it.
 month_last_date   DATE,
 labor_force       INTEGER,
 employed          INTEGER,
 unemployed        INTEGER,
 unemployed_rate   NUMERIC(6,3),
 PRIMARY KEY (state_fips_cd, county_fips_cd, month_last_date)
);

COMMENT ON TABLE uw211dashboard.public.stg_bls_unemployment_data_2019 IS
'This table contains unemployment data collected from Bureau of Labor Statistics (BLS) - for year 2019 - by Keenan Berry. These data were downloaded into the S3 file at path "s3://uw211dashboard-workbucket/unemployment_stats_2019.csv".'
;


