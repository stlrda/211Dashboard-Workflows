--
-- 2020-05-14 (Thu.) Haresh Bhatia
--
-- To streamline the usage and data processing, a view each - by TRACT_CD
-- and GEO_ID (the latter being synonyms with county in a give state) - shall
-- be created on (the latest) censuss table to use appropriate version (year)
-- of census data.
--
-- This DDL creates the VIEWs for (latest) census data as indicated above.
--
-- B. View CRE_VU_CENSUS_DATA_BY_TRACT_CURR.

/* For now this is not done due to the lack of aggregation clarity on 
   some of the data attributes.
--  and
-- C. View CRE_VU_CENSUS_DATA_BY_GEO_ID_CURR.
*/
-- ---------------------------------------------------------------------
--
-- B. View CRE_VU_CENSUS_DATA_BY_TRACT_CURR.
--
-- 1. This view fetches (CURRent / latest) census data by CENSUS_TRACT.
-- 2. All the data processing scripts shall use this view, unless a specific year
--    data is required that is other than the table-instance this view is pointing
--    to (in which case, correponding table may be used).
-- 3. At present this view points to table for 2018 census data. 
--    It may be changed later to provide the required (latest available) census
--    data.
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_data_by_tract_curr
AS
(SELECT  tract_cd,
         geo_id,
         popl_age_65plus,
         popl_civln_wth_dsblty,
         popl_wth_no_hgh_schl,
         unempl_rate,
         popl_non_hispanic,
         popl_hispanic,
         popl_wth_no_hlth_insur,
         popl_res_owned,
         popl_res_rented,
         GINI_indx,
         popl_wth_no_internet,
         median_hh_income,
         per_capita_income,
         popl_wth_knwn_pvrty_status,
         popl_below_pvrty_lvl,
         popl_white,
         popl_afr_amer,
         popl_amer_Indian_alaskn,
         popl_asian,
         popl_hwaian_pcfc_isl,
         popl_othr,
         popl_wth_2plus_races,
         totl_popl,
         popl_usng_public_trans_2work,
         MUA_score,
         tract_area_in_sq_mi,
         SVI_score
   FROM  cre_census_data_by_tract_yr2018
)
;


COMMENT ON VIEW uw211dashboard.public.cre_vu_census_data_by_tract_curr IS
'This view is intended to point to the current (required or latest) census data table and fetches census data by CENSUS_TRACT.

All the dollar figures are for the year that the underlying table indicates and are inflation adjusted for the year indicated in the underlying census-data table.

GINI - Measures extent of economic inequality
MUA - Medically Underserved Area
SVI - Sensus Vulnerability Index'
;

/* For now this is not done due to the lack of aggregation clarity on 
   some of the data attributes.
-- ---------------------------------------------------------------------
-- B. View CRE_VU_CENSUS_DATA_BY_GEO_ID_CURR.
--
-- 1. This view fetches (CURRent / latest) census data by CENSUS_TRACT.
-- 2. All the data processing scripts shall use this view, unless a specific year
--    data is required that is other than the table-instance this view is pointing
--    to (in which case, correponding table may be used).
-- 3. At present this view points to table for 2018 census data. 
--    It may be changed later to provide the required (latest available) census
--    data.
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_data_by_geo_id_curr
AS
(SELECT  --tract_cd,
         geo_id,
         SUM(popl_age_65plus,
         SUM(popl_civln_wth_dsblty,
         SUM(popl_wth_no_hgh_schl,
         unempl_rate,
         SUM(popl_non_hispanic,
         SUM(popl_hispanic,
         SUM(popl_wth_no_hlth_insur,
         SUM(popl_res_owned,
         SUM(popl_res_rented,
         GINI_indx,
         SUM(popl_wth_no_internet,
         median_hh_income,
         per_capita_income,
         SUM(popl_wth_knwn_pvrty_status,
         SUM(popl_below_pvrty_lvl,
         SUM(popl_white,
         SUM(popl_afr_amer,
         SUM(popl_amer_Indian_alaskn,
         SUM(popl_asian,
         SUM(popl_hwaian_pcfc_isl,
         SUM(popl_othr,
         SUM(popl_wth_2plus_races,
         SUM(totl_popl,
         SUM(popl_usng_public_trans_2work,
         MUA_score,
         SUM(tract_area_in_sq_mi,
         SVI_score
   FROM  cre_census_data_by_tract_yr2018
)
;


COMMENT ON VIEW uw211dashboard.public.cre_vu_census_data_by_geo_id_curr IS
'This view is intended to point to the current (required or latest) census data table and fetches census data by GEO_ID (which is equivalent to county for a given state).

All the dollar figures are for the year that the underlying table indicates and are inflation adjusted for the year indicated in the underlying census-data table.

GINI - Measures extent of economic inequality
MUA - Medically Underserved Area
SVI - Sensus Vulnerability Index'
;
*/
