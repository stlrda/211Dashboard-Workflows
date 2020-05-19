--
-- 2020-05-13 (Wed.) Haresh Bhatia
--
-- This DDL creates the table for (2018) census data.
--
-- A. Table CRE_CENSUS_DATA_BY_TRACT_YR2018.
--
-- 1. As there may be periodic (annual) releases of census data, and this being
--    for 2018, the table name reflects that.
-- 2. All $ figures are for the year indicated in the name and inflation
--    adjusted for that year.
-- 3. To streamline the usage and data processing, a view each - by TRACT_CD
--    and GEO_ID (the latter being synonyms with county in a give state) - shall
--    be created on (the latest) censuss table to use appropriate version (year)
--    of census data.
--
--====================================================================================

CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_census_data_by_tract_yr2018
(tract_cd                      VARCHAR(30),
 geo_id                        VARCHAR(10),
 popl_age_65plus               INTEGER,
 popl_civln_wth_dsblty         INTEGER,
 popl_wth_no_hgh_schl          INTEGER,
 unempl_rate                   NUMERIC(6,3),
 popl_non_hispanic             INTEGER,
 popl_hispanic                 INTEGER,
 popl_wth_no_hlth_insur        INTEGER,
 popl_res_owned                INTEGER,
 popl_res_rented               INTEGER,
 GINI_indx                     NUMERIC(10,6),
 popl_wth_no_internet          INTEGER,
 median_hh_income              NUMERIC(15,3),
 per_capita_income             NUMERIC(15,3),
 popl_wth_knwn_pvrty_status    INTEGER,
 popl_below_pvrty_lvl          INTEGER,
 popl_white                    INTEGER,
 popl_afr_amer                 INTEGER,
 popl_amer_Indian_alaskn       INTEGER,
 popl_asian                    INTEGER,
 popl_hwaian_pcfc_isl          INTEGER,
 popl_othr                     INTEGER,
 popl_wth_2plus_races          INTEGER,
 totl_popl                     INTEGER,
 popl_usng_public_trans_2work  INTEGER,
 MUA_score                     NUMERIC(10,2),
 tract_area_in_sq_mi           NUMERIC(10,6),
 SVI_score                     NUMERIC(10,6),
 PRIMARY KEY (tract_cd)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_census_data_by_tract_yr2018 IS
'This table contains the 2018 census data attributes relevant to the STL Regional Data Alliance (STL-RDA).

All the dollar figures are for the year indicated in the table-name and are inflation adjusted for that year.

GINI - Measures extent of economic inequality
MUA - Medically Underserved Area
SVI - Sensus Vulnerability Index'
;
--
/* The view definition was transferred to a separate DDL file
----------------------------------------------------------------------------

-- B. View CRE_VU_CENSUS_DATA_BY_TRACT_CURR.
--  and
-- C. View CRE_VU_CENSUS_DATA_BY_GEO_ID_CURR.
-- 
--   These are created in a separate script.
--
-- 1. To streamline the usage and data processing, a view is created on the 
--    'CURRENT' (latest year available) census data table.
-- 2. All the data processing shall use this view, unless a specific year
--    data is required (in which case, specific table may be used).
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
'This view is intended to point to the current (required or latest) census data table.

All the dollar figures are for the year that the underlying table indicates and are inflation adjusted for that year.

GINI - Measures extent of economic inequality
MUA - Medically Underserved Area
SVI - Sensus Vulnerability Index'
;
*/

