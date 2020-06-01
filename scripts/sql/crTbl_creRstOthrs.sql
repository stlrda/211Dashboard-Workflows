-- 
-- 2020-05-05 (Tue.) Haresh Bhatia
--
-- This is DDL to create Rest of the core tables.
-- These core-tables pertain to the factual data for covid-19 and 211-help-line 
--
-------------------------------------------------------------------
-- 2020-05-11 (Mon.) Haresh Bhatia.
--
-- 1. Included additional attribute DATA_SOURCE, in table CRE_COVID_DATA (see below)
-- 2. The values of the attribute shall indicate the staging table the data are 
--    collected from. [See the table comments for more details.]
-- 
-------------------------------------------------------------------
-- 2020-05-19 (Tue.) Haresh Bhatia.
--
-- 1. Additional core table CRE_UNEMPLOYMENT_CLMS created
-- 2. Modified table CRE_LAST_SUCCESS_RUN_DT to include attribute BUFFER_CNT
--    that represents a margin of safty (around the SUCCESSFUL cut-off date for
--    incremental data load.
-- 3. Included additional records (to start with) in the INSERT statement for
--    table CRE_LAST_SUCCESS_RUN_DT
--
-------------------------------------------------------------------
-- 2020-05-20 (Wed.) Haresh Bhatia.
--
-- Additional core table CRE_BLS_UNEMPLOYMENT_DATA created
--
--==================================================================================
-- A. Table CRE_LAST_SUCCESS_RUN_DT
--
-- 1. Table CRE_LAST_SUCCESS_RUN_DT contains the last successful run date that
--    is referenced by periodic data load processes. Every next data load cycle
--    would refer to the date corresponding to the RUN_CD that indicates the  
--    type of intended data-feed. All the records after that reference date are
--    dropped from the "core" data tables relevant to the RUN_CD, and 
--    corresponding records form the staging tables are copied - with 
--    appropriate transformation, if needed - into the core data tables.
--    
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_last_success_run_dt
(run_cd           VARCHAR(30) NOT NULL,   -- This refers to the run cycle code (e.g., DLY_ALL for all daily feeds)
 lst_success_dt   DATE        NOT NULL,
 buffer_cnt       INTEGER     NOT NULL DEFAULT 0,   -- (2020-05-19) This is the constant buffer to account for safety (an extra period of data load)
 PRIMARY KEY (run_cd)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_last_success_run_dt IS
'This table contains the last successful run date that is referenced by is referenced by periodic data load processes. Every next data load cycle would refer to the date corresponding to the RUN_CD that indicates the type of intended data-feed. All the records after that reference date are dropped from the "core" data tables relevant to the RUN_CD, and corresponding records form the staging tables are copied - with appropriate transformation, if needed - into the core data tables.'
;

-- Following data shall be preloaded in this table.
-- This is the only table that this is done for in this DDL.

INSERT INTO uw211dashboard.public.cre_last_success_run_dt
VALUES ('DLY_ALL'   , '1900-01-01', 0),
       ('WKLY_ALL'  , '1900-01-01', 0),   -- added 2020-05-19 
       ('MNTHLY_ALL', '1900-01-01', 0)    -- added 2020-05-19 
;

--==================================================================================

--
-- B. Table CRE_COVID_DATA
--
-- 1. Table CRE_COVID_DATA contains data for the covid related attributes (cases,
--    mortality, and respective rates)
-- 2. The data for this table was consolidated from the following staging
--    tables (only daily feeds).
--
--    a. STG_COVID_DLY_VIZ_CNTY_ALL (this has the MO & IL case details by county)
--    b. STG_COVID_ZIP_STL_CounTY   (this has the STL county case details for
--                                   STL CounTY, by ZIP codes)
--    c. STG_COVID_ZIP_STL_CiTY     (this has the STL county case details for
--                                   STL CiTY, by ZIP codes)
-- 
-- 3. The data in STG_COVID_ZIP_STL_CounTY and ..._CiTY are (at least for present)
--    limited only to CASE COUNT and RATE. [These data may get expanded to other 
--    attributes covered in STG_COVID_DLY_VIZ_CNTY_ALL.]
-- 
-- 4. This table gets daily data feeds from the corresponding source (staging)
--    tables.
-- 
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_covid_data
(data_source         VARCHAR(15),
 report_date         DATE,
 state_nm            VARCHAR(30),
 county_nm           VARCHAR(30),
 geo_id              VARCHAR(10),
 zip_cd              VARCHAR(10),
 cases               INTEGER,
 case_rate           NUMERIC(12,6),
 new_cases           INTEGER,
 case_avg            NUMERIC(12,6),
 death               INTEGER,
 mortality_rate      NUMERIC(12,6),
 new_deaths          INTEGER,
 death_avg           NUMERIC(12,6),
 case_fatality_rate  NUMERIC(12,6),
 created_tsp         TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp     TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (data_source, report_date, state_nm, county_nm, geo_id, zip_cd)
);


COMMENT ON TABLE uw211dashboard.public.cre_covid_data IS
'Table CRE_COVID_DATA contains data for the covid related attributes (cases, mortality, and respective rates).

The data for this table was consolidated from the staging tables - STG_COVID_DLY_VIZ_CNTY_ALL, STG_COVID_ZIP_STL_CounTY, and STG_COVID_ZIP_STL_CiTY; as indicated by the respective values of "ALL_COUNTY", "STL_COUNTY", and "STL_CITY" in the attribute DATA_SOURCE. [The attribute DATA_SOURCE is part of the PK.] 

The data in STG_COVID_ZIP_STL_CounTY and ..._CiTY are (at least for present) limited only to CASE COUNT and RATE. [These data may get expanded to other attributes covered in STG_COVID_DLY_VIZ_CNTY_ALL.

These tables get daily data feeds from the source (staging) tables.'
;

------------------------------------------------------------------------------------
-- 2020-05-19 (Tue.) Haresh Bhatia
--
-- C. Table CRE_UNEMPLOYMENT_CLMS
--
-- 1. Table CRE_UNEMPLOYMENT_CLMS contains the unemployment claims data
-- 2. The data for this table was consolidated from the following staging
--    tables (usually weekly feeds).
--
--    a. STG_MO_UNEMPLOYMENT_CLMS (this has weekly MO claims details by county
--       for week ending Sat. dates)
-- 
-- 3. Given that unemployment data are by county, these data records are
--    associated with corresponding GEO_ID (from LKUP_ZIP_TRACT_GEOID).
-- 
-- 4. This table gets daily data feeds from the corresponding source (staging)
--    tables.
-- 
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_unemployment_clms
(data_source         VARCHAR(20),  -- e.g., STG_MO_UNEMP_CLMS - for MO
 period_end_date     DATE,         -- This is week ending Sat. date for MO
 geo_id              VARCHAR(10),
 state_nm            VARCHAR(30),
 county_nm           VARCHAR(30),
 claims_cnt          INTEGER,      -- unemployment claims count.
 created_tsp         TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp     TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (data_source, period_end_date, geo_id)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_unemployment_clms IS
'Table CRE_UNEMPLOYMENT_CLMS contains the unemployment claims data for MO and IL. 

At the time of creation of this core table, only MO-UNEMPLOYMENT data was available. [If the IL unemployment data structure does not conform to this table, it may be modified later.]

Given that the unemployment data (at least for MO) is on weekly basis, this core table is scheduled to get weekly incremental feeds from the corresponding staging table source(s).]
'
;

------------------------------------------------------------------------------------
-- 2020-05-20 (Wed.) Haresh Bhatia
--
-- D. Table CRE_BLS_UNEMPLOYMENT_DATA
--
-- 1. Table CRE_BLS_UNEMPLOYMENT_DATA contains the unemployment statistics
-- 2. The data for this table is taken from staging table STG_BLS_UNEMPLOYMENT_DATA_CURR,
--    which contains monthly unemployment data, by county, as reported by Bureau
--    Labor Statistics (BLS).
-- 3. Clarifying background:
--     a. The BLS current unemployment data download is for 14 months. The BLS
--        website also maintains yearly files.
--     b. To get all relevant months for 2019, the corresponding annual data was
--        also downloaded in a separate file
--     c. After loading the 2019 data (for each of the 12 months), only 2020
--        data (by month) was loaded from 'Current' file.
-- 4. The STATE_FIPS_CD (2-char) and COUNTY_FIPS_CD (3-char) combined (total 5-char)
--    makes the GEO_ID. 
--     a. Given that GEO_ID can be derived by combining STATE_... and COUNTY_FIPS_CD
--        GEO_ID was not ported from the staging file.
--     b. This table was, however, maintained in a bit de-normalized form, and
--        includes STATE_NM and COUNTY_NM for the corresponding GEO_ID.
-- 5. This table is expected to be refreshed every month.
-- 
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_bls_unemployment_data
(state_fips_cd     VARCHAR(2),
 county_fips_cd    VARCHAR(3),
 month_last_date   DATE,
 state_nm          VARCHAR(30),
 county_nm         VARCHAR(30),
 labor_force       INTEGER,
 employed          INTEGER,
 unemployed        INTEGER,
 unemployed_rate   NUMERIC(6,3),
 created_tsp       TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp   TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (state_fips_cd, county_fips_cd, month_last_date)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_bls_unemployment_data IS
'This table contains the unemployment statistics (for MO and IL).

The data are taken from the staging table STG_BLS_UNEMPLOYMENT_DATA_CURR, which contains monthly unemployment data, by county, as reported on Bureau of Labor Statistics (BLS).

The STATE_FIPS_CD and COUNTY_FIPS_CD, 2- and 3- characters respectively, make the GEO_ID (5-chars). This table is denormalized to include respective STATE_NM and COUNTY_NM as well.

This core table data is expected to get incremental feeds monthly.
'
;

------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
-- 2020-05-29 (Fri.) Haresh Bhatia
--
-- E. Set of Tables for 211 Data.
-- E1. Table LKUP_211_CATEGORY
--    [This happens to be a deviation to include definition for two lookup tables
--     (one each, for CATEGORY and SUB_CATEGORY) in this script that is meant
--     mainly for creating 'CORE...' tables. But these are closely related to,
--     and meant only for, the 211 data.
--
--    1. This table contains the distinct 211 CATEGORY defintions for 211 data.
--    2. While there may be a standardized list of CATEGORies, it is not yet 
--       available at the time of creation of these data objects. Therefore, until
--       a standardized list is provided (by the RDA team) the data for CATEGORies
--       is derived from the raw 211 calls data file that feeds into the
--       corresponding staging table.
-- 

CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_211_category
(two11_category_id   SERIAL       NOT NULL,
 two11_category_nm   VARCHAR(100) NOT NULL,
 created_tsp       TIMESTAMPTZ    NOT NULL DEFAULT now(),
 last_update_tsp   TIMESTAMPTZ    NOT NULL DEFAULT now(),
 PRIMARY KEY (two11_category_id)
)
;

COMMENT ON TABLE uw211dashboard.public.lkup_211_category IS
'This table contains the definition of 211-categories for given 211 data.

While there may be a standardized list of CATEGORies, it is not yet available at the time of creation of this table. Therefore, until a standardized list is provided (by the RDA team) the data for CATEGORies is derived from the raw 211 calls data file that feeds into the corresponding staging table.
'
;

---------------------------------------------
-- E2. Table LKUP_211_SUB_CATEGORY
--    [This happens to be a deviation to include definition for two lookup tables
--     (one each, for CATEGORY and SUB_CATEGORY) in this script that is meant
--     mainly for creating 'CORE...' tables. But these are closely related to,
--     and meant only for, the 211 data.
--
--    1. This table contains the distinct 211 SUB-CATEGORY defintions for 211 data.
--    2. While there may be a standardized list of SUB-CATEGORies, for respective
--       CATEGORies, it is not yet available at the time of creation of these
--       data objects. Therefore, until a standardized list is provided (by the
--       RDA team) the data for SUB-CATEGORies is derived from the raw 211 calls data
--       file that feeds into the corresponding staging table.
-- 

CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.lkup_211_sub_category
(two11_category_id       INTEGER      NOT NULL REFERENCES uw211dashboard.public.lkup_211_category (two11_category_id),
 two11_sub_category_id   SERIAL       NOT NULL,
 two11_sub_category_nm   VARCHAR(100) NOT NULL,
 created_tsp             TIMESTAMPTZ  NOT NULL DEFAULT now(),
 last_update_tsp         TIMESTAMPTZ  NOT NULL DEFAULT now(),
 PRIMARY KEY (two11_category_id, two11_sub_category_id)
)
;

COMMENT ON TABLE uw211dashboard.public.lkup_211_category IS
'This table contains the definition of 211-sub-categories for given 211 data.

While there may be a standardized list of SUB-CATEGORies, for respective CATEGORies, it is not yet available at the time of creation of this table. Therefore, until a standardized list is provided (by the RDA team) the data for SUB-CATEGORies is derived from the raw 211 calls data file that feeds into the corresponding staging table.
'
;

---------------------------------------------
-- E3. Table CRE_211_DATA
--
--  1. This table contains the normalized version of the 211 data incrementally
--     trasferred from the corresponding staging table.
--  2. The CATEGORY and SUB-CATEGORY IDs are derived by matching the respective 
--     names, from the staging data, with the corresponding LKUP tables (as
--     defined above).
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_211_data
(call_date              DATE,
 state_nm               VARCHAR(30),
 county_nm              VARCHAR(30),
 zip_cd                 VARCHAR(10),
 two11_category_id      INTEGER      REFERENCES uw211dashboard.public.lkup_211_category     (two11_category_id),
 two11_sub_category_id  INTEGER,
 call_counts            INTEGER,
 created_tsp            TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp        TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (call_date, zip_cd, two11_category_id, two11_sub_category_id),
 FOREIGN KEY (two11_category_id, two11_sub_category_id)
  REFERENCES uw211dashboard.public.lkup_211_sub_category (two11_category_id, two11_sub_category_id)
);


COMMENT ON TABLE uw211dashboard.public.cre_211_data IS
'Table CRE_211_DATA contains data for the 211 calls.
The CATEGORY and SUB-CATEGORY IDs are derived by matching the respective names, from the staging data, with the corresponding LKUP tables
'
;

---------------------------------------------
-- E4. View CRE_VU_211_DATA
--
--  1. This view contains the denormalized version of the 211 data (and is based
--     on CRE_211_DATA table.
--  2. The purpose of this denormalized view (esp. after creating the normalized
--     version from the staging data table) is to facilitate an effortless data
--     fetch by UI/UX tool (mainly Tableau) - while keeping the DB DB structure
--     flexible and optimized.
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_211_data
AS
(SELECT  t1.call_date,
         t1.state_nm,
         t1.county_nm,
         t1.zip_cd,
         c1.two11_category_nm,
         s1.two11_sub_category_nm,
         t1.call_counts
   FROM  cre_211_data           t1,
         lkup_211_category      c1,
         lkup_211_sub_category  s1
  WHERE  t1.two11_category_id     = c1.two11_category_id
    AND  t1.two11_category_id     = s1.two11_category_id
    AND  t1.two11_sub_category_id = s1.two11_sub_category_id
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_211_data IS
'Table CRE_VU_211_DATA contains data for the 211 calls.
The CATEGORY and SUB-CATEGORY names are derived by matching the respective IDs with the corresponding LKUP tables.
'

