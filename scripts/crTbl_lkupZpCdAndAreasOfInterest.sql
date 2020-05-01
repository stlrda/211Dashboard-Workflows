-- 
-- 2020-04-28 (Tue.) Haresh Bhatia (HB)
--
-- This is DDL to create a few lookup tables 
--
-- A. LKUP_COUNTY_ZIP_CITY.
-- 
-- 1. This table contains the lookup for County along with underling zip-codes
--    and cities.
-- 2. The data for this table comes from the S3 bucket file-path
--    "s3://uw211dashboard-workbucket/mo_county_zip_city.csvø"
-- 3. This file was downloaded by Keenan Berry from ...
--
-- 4. The first row of this (data) file has column names
--  
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_county_zip_city
(county         VARCHAR(30),
 zip_code       VARCHAR(10),
 city           VARCHAR(30)
);

COMMENT ON TABLE stg_covid_dly_viz_cnty_all IS
'Table contains the raw data from file that was downloaded from "https://data.mo.gov/Geography/Missouri-Zip-Codes-by-County-City/im7g-fucq".'
;

-- B. LKUP_COUNTY_ZIP_CITY.
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
(area_of_interest   VARCHAR(30),
 state_nm           VARCHAR(30),
 county             VARCHAR(30)
);

COMMENT ON TABLE stg_covid_dly_viz_cnty_all IS
'Table contains the raw data for the areas of interest. The areas of interest are those listed in the RDA document titled "St. Louis Region / Missouri Community Impact Dashboard" under the section "Geographic Scope". This table shall be used to aggregate data based on the 3 areas of interest - viz. Core Region, Metro Area, and MO-state. as stated in the document.

The data for this table was manually created using information from the metro area map provided by the RDA at "https://helpingpeople.org/who-we-are/our-reach/" (for metro area counties) and table that has daily covid counts (got distinct county names for Missouri State).'
;


