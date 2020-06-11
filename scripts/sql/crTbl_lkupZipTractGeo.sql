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
-- -----------------------------------------------------------------------------
-- 2020-06-04 (Thu.) Haresh Bhatia
--
-- 0. Creation of the following
--      a. table LKUP_ZIP_COUNTY_MPG_GTWY_RGNL and 
--      b. view  LKUP_VU_ZIP_COUNTY_TRANS_GTWY_RGNL
--    
-- 1. The ZIP-TRACT-GEOID data (the table LKUP_ZIP_TRACT_GEOID and corresponding
--    view LKUP_VU_COUNTY_GEOID) do not provide proper translation of a given
--    ZIP-CODE to the (a) county that it is part of. 
-- 2. One reason of not being able to translate a given ZIP-CODE to a single
--    county is that a given ZIP-CODE may span multiple counties.
-- 3. HUDS website (https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data)
--    provides the proportion of addresses (Residential, Business, or otherwise)
--    in a given zip-code that come under jurisdiction of different counties.
-- 4. This dataset was downloaded by Keenan Berry (to the filepath
--    s3://uw211dashboard-workbucket/zip_county_mapping_gtwy_rgnl.csv and it
--    was then loaded it in the table.
-- 5. When the distribution of proportions of ZIP-CODEs across counties was
--    studied (Haresh and Keenan studied violin plots - using R - of the 
--    distribution), it appeared that a good majority (almost 2/3) of the ZIP-
--    CODEs had more than 70% of their area in a single county.
-- 6. Given the many-to-many relationship between ZIP-CODE and COUNTY (given 
--    ZIP-CODE spanning multiple counties and a single county having mulitple
--    ZIP-CODEs), and that a huge majority of ZIP-CODEs had more than 70% presence
--    in a single county, an approximation was achived by representing a given
--    ZIP-CODE by the COUNTY that has max-proportion of the ZIP-CODE for RESidential
--    addresses [i.e., MAX(res_ratio) over all COUNTies (or GEO_IDs) for each
--    ZIP-CODE.]
-- 7. A View was defined using the details described above to translate given
--    ZIP-CODE to corresponding COUNTY (or GEO_ID).
--
-- -----------------------------------------------------------------------------
-- 2020-06-08 (Mon.) Haresh Bhatia
--
-- 0. Creation of the following
--      a. table LKUP_COUNTY_ZIP_MPG_GTWY_RGNL and
--      b. table LKUP_TRACT_ZIP_MPG_GTWY_RGNL
--    
-- 1. These tables are quite similar to LKUP_ZIP_COUNTY_MPG_GTWY_RGNL - do look at
--    the header notes / comments for that table above and note the switching of
--    the terms COUNTY and ZIP in the table names (and also TRACT vs. ZIP).
-- 2. The main difference here is that the RES_..., BUS_..., OTH_..., and TOT_...
--    RATIOs reflect the proportion of corresponding total COUNTY / TRACT numbers
--    covered the respective ZIP-CODEs.
-- 3. HUDS website (https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data)
--    provides the proportion of addresses (Residential, Business, or otherwise)
--    in a given COUNTY / TRACT that belong to respective zip codes.
-- 4. These datasets were downloaded by Keenan Berry (to the respective filepaths
--    s3://uw211dashboard-workbucket/county_zip_mapping_gtwy_rgnl.csv and 
--    s3://uw211dashboard-workbucket/tract_zip_mapping_gtwy_rgnl.csv. 
--    These were then loaded in the respective tables.
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
---------------------------------------------------------------------------------

-- 2020-06-04 (Thu.) Haresh Bhatia
--
-- 0. Creation of table LKUP_ZIP_COUNTY_MPG_GTWY_RGNL
-- [Check for the detailed notes in the header-section above.]
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_zip_county_mpg_gtwy_rgnl
(zip_cd           VARCHAR(10),
 geo_id           VARCHAR(10),   -- This is representative of COUNTY
 res_ratio        NUMERIC(5,3),  -- Proportion of RESidential addresses with the ZIP-CODE in the COUNTY (represented by the GEO_ID).
 bus_ratio        NUMERIC(5,3),  -- Proportion of BUSiness    addresses with the ZIP-CODE in the COUNTY (represented by the GEO_ID).
 oth_ratio        NUMERIC(5,3),  -- Proportion of OTHer       addresses with the ZIP-CODE in the COUNTY (represented by the GEO_ID).
 tot_ratio        NUMERIC(5,3),  -- Proportion of TOTal       addresses with the ZIP-CODE in the COUNTY (represented by the GEO_ID).
 created_tsp      TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp  TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (zip_cd, geo_id)
);

COMMENT ON TABLE uw211dashboard.public.lkup_zip_county_mpg_gtwy_rgnl IS
'This table contains the lookup data on "crosswalk", and corresponding proportion, of ZIP-CODEs across various counties. This data was collected from HUDS web-page at "https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data"

The HUDS webpage stated abobve provides the proportion of addresses (Residential, Business, or otherwise) in a given zip-code that come under jurisdiction of different counties. 

Initially, this look-up table contains data for ZIP-CODEs in IL and MO states only.

This table as loaded from the S3 file "s3://uw211dashboard-workbucket/zip_county_mapping_gtwy_rgnl.csv".
'
;

-- 2020-06-04 (Thu.) Haresh Bhatia
--
-- 0. Creation of view  LKUP_VU_ZIP_COUNTY_TRANS_GTWY_RGNL
-- [Check for the detailed notes in the header-section above.]
--
CREATE OR REPLACE VIEW uw211dashboard.public.lkup_vu_zip_county_trans_gtwy_rgnl
AS
(SELECT  r2.zip_cd,
         r2.geo_id,
         ge.county_nm,
         ge.state_nm,
         r2.res_ratio   rep_res_ratio,   -- Representative RESidential_RATIO.
         r2.bus_ratio   rep_bus_ratio,   -- Representative BUSiness_RATIO.
         r2.oth_ratio   rep_oth_ratio,   -- Representative OTHer_RATIO.
         r2.tot_ratio   rep_tot_ratio    -- Representative TOTal_RATIO.
   FROM (SELECT  zip_cd,
                 max(res_ratio) max_res,
                 count(*)       cnty_cnt    -- dummy attribute
           FROM  lkup_zip_county_mpg_gtwy_rgnl
          GROUP  BY zip_cd
        )                                r1,
         lkup_zip_county_mpg_gtwy_rgnl   r2,
         lkup_vu_county_geoid            ge   -- to get County and State names.
  WHERE  r2.zip_cd    = r1.zip_cd
    AND  r2.res_ratio = r1.max_res
    AND  r2.geo_id    = ge.geo_id
);

COMMENT ON VIEW uw211dashboard.public.lkup_vu_zip_county_trans_gtwy_rgnl IS
'This view is used for look-up on COUNTY-NAME for a given ZIP-CODE.
This view is based on LKUP_ZIP_COUNTY_MPG_GTWY_RGNL table (do check comments on that table).

One reason of not being able to translate a given ZIP-CODE to a single county is that a given ZIP-CODE may span multiple counties.

When the distribution of proportions of ZIP-CODEs across counties was studied (Haresh and Keenan studied violin plots - using R - of the distribution), it appeared that a good majority (almost 2/3) of the ZIP-CODEs had more than 70% of their area in a single county.

Given the many-to-many relationship between ZIP-CODE and COUNTY (given ZIP-CODE spanning multiple counties and a single county having mulitple ZIP-CODEs), and that a huge majority of ZIP-CODEs had more than 70% presence in a single county, an approximation was achived by representing a given ZIP-CODE by the COUNTY that has max-proportion of the ZIP-CODE for RESidential addresses [i.e., MAX(res_ratio) over all COUNTies (or GEO_IDs).]

This View was defined using the details described above to translate given ZIP-CODE to corresponding COUNTY (or GEO_ID). 

IMPORTANT NOTE:
It is possible that, for some ZIP-CODEs there may be mulitple records for distinct counties with the same MAX value for RES_RATIO. However, that was not the case for the IL-MO data we currently have. So the view query has been maintained in its simpler format. If such a condition arises in future, the view query shall be modified appropriately.
'
;
-- -----------------------------------------------------------------------------
-- 2020-06-08 (Mon.) Haresh Bhatia
--
-- 0. Creation table LKUP_COUNTY_ZIP_MPG_GTWY_RGNL
-- [Check for the detailed notes in the header-section above.]
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_county_zip_mpg_gtwy_rgnl
(geo_id           VARCHAR(10),   -- This is representative of COUNTY
 zip_cd           VARCHAR(10),
 res_ratio        NUMERIC(5,3),  -- Proportion of all RESidential addresses within the COUNTY (GEO_ID) that are in the ZIP-CODE.
 bus_ratio        NUMERIC(5,3),  -- Proportion of all BUSiness    addresses within the COUNTY (GEO_ID) that are in the ZIP-CODE.
 oth_ratio        NUMERIC(5,3),  -- Proportion of all OTHer       addresses within the COUNTY (GEO_ID) that are in the ZIP-CODE.
 tot_ratio        NUMERIC(5,3),  -- Proportion of all             addresses within the COUNTY (GEO_ID) that are in the ZIP-CODE.
 created_tsp      TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp  TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (geo_id, zip_cd)
);

COMMENT ON TABLE uw211dashboard.public.lkup_county_zip_mpg_gtwy_rgnl IS
'This table contains the lookup data on "crosswalk", and corresponding proportion, of each COUNTY across ZIP-CODEs within its limits. This data was collected from HUDS web-page at "https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data"

The HUDS webpage stated abobve provides the proportion of addresses (Residential, Business, or otherwise) in a given COUNTY that come under respective zip-codes. 

Initially, this look-up table contains data for COUNTies in IL and MO states only.

This table as loaded from the S3 file "s3://uw211dashboard-workbucket/county_zip_mapping_gtwy_rgnl.csv".
'
;

-- ---------------

-- 2020-06-08 (Mon.) Haresh Bhatia
--
-- 0. Creation table LKUP_TRACT_ZIP_MPG_GTWY_RGNL
-- [Check for the detailed notes in the header-section above.]
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_tract_zip_mpg_gtwy_rgnl
(tract_cd         VARCHAR(30),   -- This is representative of COUNTY
 zip_cd           VARCHAR(10),
 res_ratio        NUMERIC(5,3),  -- Proportion of all RESidential addresses within the TRACT that are in the ZIP-CODE.
 bus_ratio        NUMERIC(5,3),  -- Proportion of all BUSiness    addresses within the TRACT that are in the ZIP-CODE.
 oth_ratio        NUMERIC(5,3),  -- Proportion of all OTHer       addresses within the TRACT that are in the ZIP-CODE.
 tot_ratio        NUMERIC(5,3),  -- Proportion of all             addresses within the TRACT that are in the ZIP-CODE.
 created_tsp      TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp  TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (tract_cd, zip_cd)
);

COMMENT ON TABLE uw211dashboard.public.lkup_tract_zip_mpg_gtwy_rgnl IS
'This table contains the lookup data on "crosswalk", and corresponding proportion, of each TRACT across ZIP-CODEs within its limits. This data was collected from HUDS web-page at "https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data"

The HUDS webpage stated abobve provides the proportion of addresses (Residential, Business, or otherwise) in a given TRACT that come under respective zip-codes. 

Initially, this look-up table contains data for TRACTs in IL and MO states only.

This table as loaded from the S3 file "s3://uw211dashboard-workbucket/tract_zip_mapping_gtwy_rgnl.csv".
'
;

---------------------------------------------------------------------------------

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

