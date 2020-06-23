-- 
-- 2020-04-28 (Tue.) Haresh Bhatia (HB)
--
-- This is DDL to create a few lookup tables 
--
-- ---------------------------------------------------------------------------------------
-- 2020-05-18 (Mon.) 
-- 1. Table LKUP_COUNTY_ZIP_CITY was later deemed redundent and dropped.
--
-- 2. The name of the column COUNTY was changed to COUNTY_NM to stadardize the 
--    naming convention.
--
--=======================================================================================================
-- A. LKUP_COUNTY_ZIP_CITY.
-- 
-- 1. This table contains the lookup for County along with underling zip-codes
--    and cities.
-- 2. The data for this table comes from the S3 bucket file-path
--    "s3://uw211dashboard-workbucket/mo_county_zip_city.csv�"
-- 3. This file was downloaded by Keenan Berry from ...
--
-- 4. The first row of this (data) file has column names
--  
--
-- CREATE TABLE 
--           IF NOT EXISTS  uw211dashboard.public.lkup_county_zip_city
-- (county         VARCHAR(30),
--  zip_code       VARCHAR(10),
--  city           VARCHAR(30)
-- );

-- COMMENT ON TABLE stg_covid_dly_viz_cnty_all IS
-- 'Table contains the raw data from file that was downloaded from "https://data.mo.gov/Geography/Missouri-Zip-Codes-by-County-City/im7g-fucq".'
-- ;

-- -- ---------------------------
-- -- 2020-05-18 (Mon.) 
-- -- 1. Table LKUP_COUNTY_ZIP_CITY was later deemed redundent and dropped.
-- DROP TABLE uw211dashboard.public.lkup_county_zip_city
-- ;

-- ----------------------------------------------------------------
-- B. LKUP_AREAS_OF_INTR_GEO_SCOPE.
-- 
-- EDIT: KB edited this table 6/22/2020
-- The creation of this file has been added to the update DAG.
-- The file structure slighly changed to resemble the zip_tract_geoid table.
-- This table is used to filter the data in the final views of this database.
--

-- 1. This DDL creates table LKUP_AREAS_OF_INTR_GEO_SCOPE.
-- 2. The data for this was manually created using the following
--    a. The document sent by Maureen Morgan (of Daugherty) attached to meeting
--       (on Thu. 04/23/2020 at 10am) - sectoin "Geographic Scope" on page 1
--    b. Map at "https://helpingpeople.org/who-we-are/our-reach/" - showing
--       St. Louis bi-state Metro area.
-- 3. The data for this was created manually (HB) - using the resources listed above.
--    a. The list for Missouri counties (116 of those) was created from the table
--       STG_COVID_DLY_VIZ_CNTY_ALL (by listing distinct counties for Missouri).
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_areas_of_intr_geo_scope
(geo_id                  VARCHAR(10),
 county_nm               VARCHAR(30),
 state_nm                VARCHAR(30),
-- 2020-05-20 (Wed.)  -- see notes above.
 area_of_intr_flg_metro   VARCHAR(1),
 area_of_intr_flg_core  VARCHAR(1),
 PRIMARY KEY (geo_id)
);

COMMENT ON TABLE uw211dashboard.public.lkup_areas_of_intr_geo_scope IS
'Table contains the raw data for the areas of interest. The areas of interest are those listed in the RDA document titled "St. Louis Region / Missouri Community Impact Dashboard" under the section "Geographic Scope". This table shall be used to aggregate data based on the 3 areas of interest - viz. Core Region, Metro Area, and MO-state. as stated in the document.

The data for this table was originally manually created using information from the metro area map provided by the RDA at "https://helpingpeople.org/who-we-are/our-reach/" (for metro area counties) and table that has daily covid counts (got distinct county names for Missouri State).

Now the "areas_of_interest.json" file holds this information. This file is used to update this table every time the update dag is triggered.

Specifically, we use all county geo_ids in the census files and then use the "areas_of_interest.json" file to filter, selecting only the areas that we are concerned with for this project.

This table is used to filter the core views in this database. The filtered views will only contain counties listed in the "areas_of_interest.json" file. Presently, this includes all Missouri counties and the 16 metro counties of STL (which includes 8 (?) counties in IL.'
;

-- ---------------------------
-- 2020-05-18 (Mon.) 
-- 1. ... (see the file header above)
--
-- 2. The name of the column COUNTY was changed to COUNTY_NM to stadardize the 
--    naming convention.

-- ALTER  TABLE   uw211dashboard.public.lkup_areas_of_intr_geo_scope
-- RENAME COLUMN  county  TO  county_nm
-- ;

