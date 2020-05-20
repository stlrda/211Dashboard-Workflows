-- 
-- 2020-05-11 (Mon.) Haresh Bhatia
--
-- This is DDL to create table LKUP_ZIP_TRACT_GEOID
-- 
-- 1. This table contains data from the S3 file 
--    "s3://uw211dashboard-workbucket/zip_tract_geoid.csv"
-- 
-- 2. These data were collected by Keenan Berry from ... .
--    [Keenan also formatted it to be pipe (|) delimited.
-- 
-- 3. The first row of this file data has column-names.
--
-- 4. 
-- -----------------------------------------------------------------------------
-- 2020-05-19 (Tue.) Haresh Bhatia
--
-- This change is to create a view LKUP_VU_COUNTY_GEOID (on LKUP_ZIP_TRACT_GEOID)
-- that lists distinct counties along with corresponding GEO_ID.
--
-- ==========================================================================================================
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_zip_tract_geoid
(zip_cd                  VARCHAR(10),
 tract_cd                VARCHAR(30),   -- Census Tract
 geo_id                  VARCHAR(10),
 county_nm               VARCHAR(30),
 state_nm                VARCHAR(30),
 PRIMARY KEY (zip_cd, tract_cd)
);


COMMENT ON TABLE uw211dashboard.public.lkup_zip_tract_geoid IS
'This table contains the lookup data for geographic regions pertaining to Zip, Census-Tract, Geo-ID, County, and State.

This table as loaded from the S3 file "s3://uw211dashboard-workbucket/zip_tract_geoid.csv".'
;

-- -----------------------------------------------------------------------------
-- 2020-05-19
-- 
-- Creating view LKUP_VU_COUNTY_GEOID
--
CREATE OR REPLACE VIEW uw211dashboard.public.lkup_vu_county_geoid
AS
(SELECT  geo_id,
         county_nm,
         state_nm,
         COUNT(*)    county_zip_cd_cnt
   FROM  uw211dashboard.public.lkup_zip_tract_geoid
  GROUP  BY geo_id,
            county_nm,
            state_nm
);

COMMENT ON VIEW uw211dashboard.public.lkup_vu_county_geoid IS
'This view is used for look-up on GEO_ID for a given COUNTY_NM or to get COUNTY_NM for a given GEO_ID.
This view is based on LKUP_ZIP_TRACT_GEOID table. For more details check comments on that table.'
;


