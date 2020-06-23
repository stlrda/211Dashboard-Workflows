--
-- 2020-06-22 (Mon.) Keenan Berry
--
-- Create VIEWs for remaining core tables, filtered by "areas of interest"
-- Also, create VIEWs for county to zip code mappings for both unemployment data types (tables E and F)
--
-- This DDL creates the following VIEWs.
--
-- A. View CRE_VU_BLS_UNEMPLOYMENT_DATA.
-- B. View CRE_VU_UNEMPLOYMENT_CLMS.
-- C. View CRE_VU_COVID_COUNTY. 
-- D. View CRE_VU_COVID_ZIP. 
-- E. View CRE_VU_BLS_UNEMPLOYMENT_MAP_2_ZIP.
-- F. View CRE_VU_UNEMPLOYMENT_CLMS_MAP_2_ZIP.
--
-- For more details on census the underlying tables, please check out the
-- comments on the existing tables.
--
--==================================================================================================
--
-- A. View CRE_VU_BLS_UNEMPLOYMENT_DATA.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_bls_unemployment_data
AS
(SELECT  CONCAT(state_fips_cd, county_fips_cd) as geo_id,
         month_last_date,
         state_nm,
         county_nm,
         labor_force,
         employed,
         unemployed,
         unemployed_rate
   FROM  cre_bls_unemployment_data
   WHERE CONCAT(state_fips_cd, county_fips_cd) in (SELECT DISTINCT geo_id FROM lkup_areas_of_intr_geo_scope)
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_bls_unemployment_data IS
'This view shows BLS unemployment statistics filtered to include only those counties in the lkup_areas_of_intr_geo_scope lookup table.'
;

--==================================================================================================
--
-- B. View CRE_VU_UNEMPLOYMENT_CLMS.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_unemployment_clms
AS
(SELECT  period_end_date,
         geo_id,
         state_nm,
         county_nm,
         claims_cnt
   FROM  cre_unemployment_clms
   WHERE geo_id in (SELECT DISTINCT geo_id FROM lkup_areas_of_intr_geo_scope)
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_unemployment_clms IS
'This view shows unemployment claims filtered to include only those counties in the lkup_areas_of_intr_geo_scope lookup table.
 Although all counties in this core table belong to Missouri (and are included in the areas of interest), this view is included
 for future cases where areas of interest or claims data might change.'
;

--==================================================================================================
--
-- C. View CRE_VU_COVID_COUNTY.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_covid_county
AS
(SELECT  report_date,
         state_nm,
         county_nm,
         geo_id,
         cases,
         case_rate,
         new_cases,
         case_avg,
         death,
         mortality_rate,
         new_deaths,
         death_avg,
         case_fatality_rate
   FROM  cre_covid_data
   WHERE geo_id in (SELECT DISTINCT geo_id FROM lkup_areas_of_intr_geo_scope)
     AND data_source = 'ALL_COUNTY'
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_covid_county IS
'This view shows county level covid data for all counties in our areas of interest table.'
;

--==================================================================================================
--
-- D. View CRE_VU_COVID_ZIP.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_covid_zip
AS
(SELECT  report_date,
         state_nm,
         county_nm,
         geo_id,
         zip_cd,
         cases,
         case_rate
   FROM  cre_covid_data
   WHERE data_source = 'STL_COUNTY' OR data_source = 'STL_CITY'
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_covid_zip IS
'This view shows zip code level data for STL_COUNTY and STL_CITY data sources.'
;


--==================================================================================================
--
-- E. View CRE_VU_BLS_UNEMPLOYMENT_MAP_2_ZIP.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_bls_unemployment_map_2_zip
AS
(SELECT  zip_cd,
         month_last_date,
         SUM(labor_force)        AS  labor_force,
         SUM(employed)           AS  employed,
         SUM(unemployed)         AS  unemployed,
         SUM(unemployed_rate)    AS  unemployed_rate            -- NOT-DERIVABLE, therefore NULL
   FROM (SELECT  ct.geo_id,
                 ct.month_last_date,
                 zo.zip_cd,
                 zo.res_ratio,
                 zo.res_ratio * ct.labor_force   AS  labor_force,
                 zo.res_ratio * ct.employed      AS  employed,
                 zo.res_ratio * ct.unemployed    AS  unemployed,
                 CAST(NULL  AS NUMERIC)          AS  unemployed_rate                              -- NOT-DERIVABLE, therefore NULL
           FROM  cre_vu_bls_unemployment_data           ct,
                 lkup_county_zip_mpg_gtwy_rgnl          zo
          WHERE  ct.geo_id = zo.geo_id
        )     zt
  GROUP  BY zip_cd, month_last_date
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_bls_unemployment_map_2_zip IS
'This view translates monthly unemployment stats by county to zip-code level.
 Warning - The method used for translation is a rough estimate.'
--
--
--==================================================================================================
--
-- E. View CRE_VU_UNEMPLOYMENT_CLMS_MAP_2_ZIP.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_unemployment_clms_map_2_zip
AS
(SELECT  zip_cd,
         period_end_date,
         SUM(claims_cnt)        AS  claims_cnt
   FROM (SELECT  ct.geo_id,
                 ct.period_end_date,
                 zo.zip_cd,
                 zo.res_ratio,
                 zo.res_ratio * ct.claims_cnt   AS  claims_cnt
           FROM  cre_vu_unemployment_clms            ct,
                 lkup_county_zip_mpg_gtwy_rgnl       zo
          WHERE  ct.geo_id = zo.geo_id
        )     zt
  GROUP  BY zip_cd, period_end_date
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_unemployment_clms_map_2_zip IS
'This view translates weekly unemployment claims counts by county to zip-code level.
 Warning - The method used for translation is a very rough estimate.'