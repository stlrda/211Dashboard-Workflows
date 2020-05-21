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
-- -----------------------------------------------------------------------------
-- 2020-05-20 (Wed.) Haresh Bhatia
--
-- 1. The table LKUP_ZIP_TRACT_GEOID was ALTERedto add two new
--    columns - viz. AREA_OF_INTR_FLG_CORE and AREA_OF_INTR_FLG_METRO - to indicate
--    the territories that span the St. Louis areas of interest spanning "Core" and
--    "Metro" areas (as per the definition of those terms in the RDA document sent 
--    initially as attachment to the meeting invite on Apr. 23, 2020 (Thu.)
-- 2. This is another exception to include DML for updating of these two new
--    attributes. These attributes are updated using data from table
--    LKUP_AREAS_OF_INTR_GEO_SCOPE
-- 3. After UPDATing LKUP_ZIP_TRACT_GEOID records for 'areas of interest' flags,
--    The data was exported and corresponding file in S3 (at the file path
--    "s3://uw211dashboard-workbucket/zip_tract_geoid.csv") was replaced with the
--    new data (that now has data for the two new flags).
--  !!!!!!
--  ANOTHER EXCEPTION!
-- 4. The table was then DROPped and recreated - and the new data file was 
--    into the table.
-- 5. View LKUP_VU_COUNTY_GEOID was also modified to include the new 'Areas of intrest'
--    flags.
--
-- ==========================================================================================================
-- 2020-05-20 (Wed.)
-- 4. The table was then DROPped and recreated - and the new data file was 
--    into the table.
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_zip_tract_geoid
(zip_cd                  VARCHAR(10),
 tract_cd                VARCHAR(30),   -- Census Tract
 geo_id                  VARCHAR(10),
 county_nm               VARCHAR(30),
 state_nm                VARCHAR(30),
-- 2020-05-20 (Wed.)  -- see notes above.
 area_of_intr_flg_core   VARCHAR(1),  -- 2020-05-20 modificatin (see details in header)
 area_of_intr_flg_metro  VARCHAR(1),  -- 2020-05-20 modificatin (see details in header)
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
-- -----------------------------------
-- 5. View LKUP_VU_COUNTY_GEOID was also RECREATED to include the new 'Areas of intrest'
--    flags.
CREATE OR REPLACE VIEW uw211dashboard.public.lkup_vu_county_geoid
AS
(SELECT  geo_id,
         county_nm,
         state_nm,
         area_of_intr_flg_core,     -- 2020-05-20 modificatin (see details in header)
         area_of_intr_flg_metro,    -- 2020-05-20 modificatin (see details in header)
         COUNT(*)    county_zip_cd_cnt
   FROM  uw211dashboard.public.lkup_zip_tract_geoid
  GROUP  BY geo_id,
            county_nm,
            state_nm,
            area_of_intr_flg_core,   -- 2020-05-20 modificatin (see details in header)
            area_of_intr_flg_metro   -- 2020-05-20 modificatin (see details in header)
);

COMMENT ON VIEW uw211dashboard.public.lkup_vu_county_geoid IS
'This view is used for look-up on GEO_ID for a given COUNTY_NM or to get COUNTY_NM for a given GEO_ID.
This view is based on LKUP_ZIP_TRACT_GEOID table. For more details check comments on that table.'
;

/*   The following section was executed once and no longer requierd - see the detailed
--   comments in header section of this script above under date 2020-05-20 (Wed.)
-- -----------------------------------------------------------------------------
-- 2020-05-20 (Wed.) Haresh Bhatia
--
-- 1. The definition of table LKUP_ZIP_TRACT_GEOID was modified to add two new
--    columns - viz. AREA_OF_INTR_FLG_CORE and AREA_OF_INTR_FLG_METRO ...

ALTER TABLE  uw211dashboard.public.lkup_zip_tract_geoid
  ADD COLUMN area_of_intr_flg_core    VARCHAR(1),
  ADD COLUMN area_of_intr_flg_metro   VARCHAR(1)
;

-- 2. updating of these two new attributes...
--    These attributes are updated using data from table LKUP_AREAS_OF_INTR_GEO_SCOPE
--
--   2a. First AREA_OF_INTR_FLG_CORE ...
UPDATE  lkup_zip_tract_geoid    g
   SET  area_of_intr_flg_core = 'Y'
 WHERE  EXISTS (SELECT  1
                  FROM  LKUP_AREAS_OF_INTR_GEO_SCOPE  i
                 WHERE  i.geo_id           = g.geo_id
                   AND  i.area_of_interest = 'Core Region'
               )
;

--   2b. Next AREA_OF_INTR_FLG_METRO ...
UPDATE  lkup_zip_tract_geoid    g
   SET  area_of_intr_flg_metro = 'Y'
 WHERE  EXISTS (SELECT  1
                  FROM  LKUP_AREAS_OF_INTR_GEO_SCOPE  i
                 WHERE  i.geo_id           = g.geo_id
                   AND  i.area_of_interest = 'Metro Area'
               )
;

-- 2020-05-20 (Wed.) Haresh Bhatia
-- 3. After UPDATing LKUP_ZIP_TRACT_GEOID records for 'areas of interest' flags,
--    The data was exported and corresponding file in S3 (at the file path
--    "s3://uw211dashboard-workbucket/zip_tract_geoid.csv") was replaced with the
--    new data (that now has data for the two new flags).
*/