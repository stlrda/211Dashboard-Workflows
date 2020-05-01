-- 
-- 2020-04-30 (Thu.) Haresh Bhatia
--
-- This is DDL to create table CRE_STL_RGNL_FNDNG_CLND
-- 
-- 1. This table contains data from the S3 file 
--    "s3://uw211dashboard-workbucket/stl_regional_funding_cleaned.csv"
-- 
-- 2. This file was downloaded by Keenan Berry from ... .
-- 
-- 3. The first row of this file data has column-names.
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_stl_rgnl_fndng_clnd
(county                  VARCHAR(30),
 state_nm                VARCHAR(30),
 funder_nm               VARCHAR(500),
 funder_addr             VARCHAR(1000),
 website                 VARCHAR(1000),   -- Prob. this is funder website.
 funding_type            VARCHAR(100),
 philanthropy_type       VARCHAR(50),
 recipient_org_nm        VARCHAR(500),
 recipient_main_addr     VARCHAR(1000),  -- The data file had heading "Main Addr", presumably it is of 'recipient'.
 recipient_org_website   VARCHAR(1000),
 award_amount            NUMERIC(15,2),   -- Presumably this is the 'Grant' amount.
 united_way_impact_area  VARCHAR(500),
 united_way_topic        VARCHAR(500),
 data_source             VARCHAR(200),
 description             VARCHAR(10000),
 zip_cd                  VARCHAR(10)
);


COMMENT ON TABLE uw211dashboard.public.cre_stl_rgnl_fndng_clnd IS
'Table contains the funding data for St. Louis region from from the S3 file "s3://uw211dashboard-workbucket/stl_regional_funding_cleaned.csv".'
;


