-- 
-- 2020-06-11 (Thu.) Haresh Bhatia
--
-- This is DDL to create table CRE_STL_RGNL_FNDNG_CLND
-- 
-- 1. This table contains data from the S3 file 
--    "s3://uw211dashboard-workbucket/funding_data_final_public_pipedlm.csv"
-- 
-- 2. This file was revised and cleaned up by Paul Sorenson (from the older
--    version of this data file named "stl_regional_funding_cleaned.csv").
--    [The earlier version loaded in the DB table 
-- 
-- 3. The first row of this file data has column-names.
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_stl_rgnl_fndng_clnd
(funding_id         INTEGER,
 funder_name        VARCHAR(500),
 funder_website     VARCHAR(1000),
 funder_address     VARCHAR(1000),
 funder_county      VARCHAR(30),
 funder_state       VARCHAR(30),
 funder_zip         VARCHAR(10),
 funding_type       VARCHAR(50),
 philanthropy_type  VARCHAR(50),
 funder_ntee        VARCHAR(30),
 recipient_name     VARCHAR(500),
 recipient_website  VARCHAR(1000),
 recipient_address  VARCHAR(1000),
 recipient_county   VARCHAR(30),
 recipient_state    VARCHAR(30),
 recipient_zip      VARCHAR(10),
 uw_impact_area     VARCHAR(200),
 uw_topic           VARCHAR(200),
 recipient_program  VARCHAR(200),
 recipient_ntee     VARCHAR(30),
 recipient_airs     VARCHAR(2000),
 award_amount       NUMERIC(15,2),
 unrestricted_ind   NUMERIC(1),    -- Apparently this is just 0 or 1       
 data_source        VARCHAR(30),
 notes              VARCHAR(2000),
 created_tsp        TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp    TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (funding_id)
);


COMMENT ON TABLE uw211dashboard.public.cre_stl_rgnl_fndng_clnd IS
'Table contains the funding data for St. Louis region from from the S3 file "s3://uw211dashboard-workbucket/funding_data_final_public_pipedlm.csv".

The data file was revised and cleaned up by Paul Sorenson (from the older version of this data file named "stl_regional_funding_cleaned.csv".
'
;


