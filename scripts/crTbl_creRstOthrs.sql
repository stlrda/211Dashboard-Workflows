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
-- 1. Additional core table CRE_UNEMPLOYMENT_CLMS
-- 2. Modified table CRE_LAST_SUCCESS_RUN_DT to include attribute BUFFER_CNT
--    that represents a margin of safty (around the SUCCESSFUL cut-off date for
--    incremental data load.
-- 3. Included additional records (to start with) in the INSERT statement for
--    table CRE_LAST_SUCCESS_RUN_DT
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
       ('WKLY_ALL'  , '1900-01-01', 0),   -- 2020-05-19 
       ('MNTHLY_ALL', '1900-01-01', 0)    -- 2020-05-19 
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
-- 1. Table CRE_UNEMPLOYMENT_CLMS contains data for the unemployment claims data
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
'Table CRE_UNEMPLOYMENT_CLMS gets data for the unemployment claims data for MO and IL. 

At the time of creation of this core table, only MO-UNEMPLOYMENT data was available. [If the IL unemployment data structure does not conform to this table, it may be modified later.]

Given that the unemployment data (at least for MO) is on weekly basis, this core table is scheduled to get weekly incremental feeds from the corresponding staging table source(s).]
'
;



/* This is INCOMPLETE still...
-------------------------------------------------------------------------------
--
-- D. Table CRE_MO_211_DATA
--
-- 1. Table CRE_MO_211_DATA contains data for the 211 call-support related
--    attributes along with CALL_DT (call category, sub-category, census_cd, etc.)
-- 
-- 2. The data for this table were gathered from the corresponding staging table
--    STG_MO_211_DATA.
-- 
-- 3. This table gets daily data feeds from the corresponding source (staging)
--    tables.
-- 
-- 
--
CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_211_data
(call_date         DATE,
 state_nm          VARCHAR(30),
 county_nm         VARCHAR(30),
 zip_cd            VARCHAR(10),
 category          VARCHAR(200),
 sub_category      VARCHAR(200),
 call_counts       INTEGER,
 created_tsp       TIMESTAMPTZ     NOT NULL DEFAULT now(),
 last_update_tsp   TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (call_date, state_nm, county_nm, zip_cd)
);


COMMENT ON TABLE uw211dashboard.public.cre_mo_211_data IS
'Table CRE_MO_211_DATA contains data for the 211 calls.'
;
*/

