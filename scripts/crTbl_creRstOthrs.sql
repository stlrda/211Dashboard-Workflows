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
 PRIMARY KEY (run_cd)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_last_success_run_dt IS
'This table contains the last successful run date that is referenced by is referenced by periodic data load processes. Every next data load cycle would refer to the date corresponding to the RUN_CD that indicates the type of intended data-feed. All the records after that reference date are dropped from the "core" data tables relevant to the RUN_CD, and corresponding records form the staging tables are copied - with appropriate transformation, if needed - into the core data tables.'
;

-- Following data shall be preloaded in this table.
-- This is the only table that this is done for in this DDL.

INSERT INTO uw211dashboard.public.cre_last_success_run_dt
VALUES ('DLY_ALL', '1900-01-01')
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
-- 4. These tables get daily data feeds from the source (staging) tables.
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
 PRIMARY KEY (data_source, report_date, state_nm, county_nm, geo_id)
);


COMMENT ON TABLE uw211dashboard.public.cre_covid_data IS
'Table CRE_COVID_DATA contains data for the covid related attributes (cases, mortality, and respective rates).

The data for this table was consolidated from the staging tables - STG_COVID_DLY_VIZ_CNTY_ALL, STG_COVID_ZIP_STL_CounTY, and STG_COVID_ZIP_STL_CiTY; as indicated by the respective values of "ALL_COUNTY", "STL_COUNTY", and "STL_CITY" in the attribute DATA_SOURCE. [The attribute DATA_SOURCE is part of the PK.] 

The data in STG_COVID_ZIP_STL_CounTY and ..._CiTY are (at least for present) limited only to CASE COUNT and RATE. [These data may get expanded to other attributes covered in STG_COVID_DLY_VIZ_CNTY_ALL.

These tables get daily data feeds from the source (staging) tables.'
;


