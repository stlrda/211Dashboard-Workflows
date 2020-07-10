--
-- 2020-06-10 (Wed.) Haresh Bhatia
--
-- This DDL creates the tables for (2018) census data.
-- A. Table CRE_CENSUS_DATA_BY_TRACT_YR2018.
-- B. Table CRE_CENSUS_DATA_BY_COUNTY_YR2018.
--
--====================================================================================
-- A. Table CRE_CENSUS_DATA_BY_TRACT_YR2018.
--
-- 1. As there may be periodic (annual) releases of census data, and this being
--    for 2018, the table name reflects that.
--    1.a. Separate tables for census data are created for TRACT and COUNTY
--         (COUNTY is identified by GEO_ID).
-- 2. All $ figures are for the year indicated in the name and inflation
--    adjusted for that year.
-- 3. To streamline the usage and data processing, a view each - by TRACT_CD
--    and GEO_ID (the latter being synonyms with county in a give state) - shall
--    be created on (the latest) censuss table to use appropriate version (year)
--    of census data.
-- 4. These data were collected by Keenan from the census webpage into respective
--    files for COUNTY and TRACT (as indicated in the file-name) in the following
--    S3 file-paths.
--      COUNTY - s3://uw211dashboard-workbucket/census_data_by_county.csv
--      TRACT  - s3://uw211dashboard-workbucket/census_data_by_tract.csv
--
----------------------------------------------------------------------------------------------------

CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_census_data_by_tract_yr2018
(tract_cd                                VARCHAR(30),
 est_pop_age_65pl                        INTEGER,
 est_pop_wth_dsablty                     INTEGER,
 est_pop_age_25pl                        INTEGER,
 est_pop_age_25pl_hgh_schl_orls          INTEGER,
 est_pop_age_16pl                        INTEGER,
 est_pop_age_16pl_in_lbr_frce_prop       NUMERIC(6,3),
 est_pop_age_16pl_empld_prop             NUMERIC(6,3),
 est_pop_age_16pl_unempl_rt              NUMERIC(6,3),
 est_pop_wthout_hlth_insr                INTEGER,
 est_pop_not_hisp_latino                 INTEGER,
 est_pop_hisp_latino                     INTEGER,
 est_tot_hh                              INTEGER,
 est_tot_hh_own_res                      INTEGER,
 est_tot_hh_rent_res                     INTEGER,
 est_GINI_ndx                            NUMERIC(10,6),
 est_pop_no_internet_access              INTEGER,
 est_pop_commute_2_wrk                   INTEGER,
 est_pop_publ_trans_2_wrk                INTEGER,
 est_mdn_hh_ncome_ttm_2018nfl_adj        NUMERIC(15,3),
 est_mdn_hh_ncome_ttm_abov_belo_ind      VARCHAR(10),   -- pertains to "est_mdn_hh_ncome_ttm_2018nfl_adj" and Indicates amounts beyond '+' (Above) / '-' (Below)....
 est_mdn_percap_ncome_ttm_2018nfl_adj    NUMERIC(15,3),
 est_pop_wth_knwn_pvrty_stts             INTEGER,
 est_pop_undr_pvrty_wth_knwn_pvrty_stts  INTEGER,
 est_pop_white                           INTEGER,
 est_pop_blk                             INTEGER,
 est_pop_am_ind                          INTEGER,
 est_pop_asian                           INTEGER,
 est_pop_hwaiian                         INTEGER,
 est_pop_othr                            INTEGER,
 est_pop_2pl_race                        INTEGER,
 est_tot_pop                             INTEGER,
 imu_score                               NUMERIC(10,2),
 rpl_themes_svi_ndx                      NUMERIC(10,6),
 area_sq_mi                              NUMERIC(10,6),
 --created_tsp                             TIMESTAMPTZ     NOT NULL DEFAULT now(), -- Removed for DAG Testing
 --last_update_tsp                         TIMESTAMPTZ     NOT NULL DEFAULT now(), -- Removed for DAG Testing
 PRIMARY KEY (tract_cd)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_census_data_by_tract_yr2018 IS
'This table contains the 2018 census data attributes relevant to the STL Regional Data Alliance (STL-RDA).

All the dollar figures are for the year indicated in the table-name and are inflation adjusted for that year.

To streamline the usage and data processing, a view each - by TRACT_CD and GEO_ID (the latter being synonyms with COUNTY in a give state) - shall be created on (the latest) censuss table to use appropriate version (year) of census data.

These data were collected by Keenan from the census webpage into respective files for COUNTY and TRACT (as indicated in the file-name) in the following S3 file-paths.
    COUNTY - s3://uw211dashboard-workbucket/census_data_by_county.csv
    TRACT  - s3://uw211dashboard-workbucket/census_data_by_tract.csv

The description of the attributes corresponding to the respective columns names are listed below.

tract_cd ................................. TRACT
est_pop_age_65pl ......................... Estimate!!Total!!Total population!!SELECTED AGE CATEGORIES!!65 years and over
est_pop_wth_dsablty ...................... Estimate Total Population With a disability
est_pop_age_25pl ......................... Estimate Total Population 25 years and over
est_pop_age_25pl_hgh_schl_orls ........... Estimate Total Pop 25 and over with high school equivalency or less
est_pop_age_16pl ......................... Estimate!!Total!!Population 16 years and over
est_pop_age_16pl_in_lbr_frce_prop ........ Estimate!!Labor Force Participation Rate!!Population 16 years and over
est_pop_age_16pl_empld_prop .............. Estimate!!Employment/Population Ratio!!Population 16 years and over
est_pop_age_16pl_unempl_rt ............... Estimate!!Unemployment rate!!Population 16 years and over
est_pop_wthout_hlth_insr ................. Estime Total Population No health insurance coverage
est_pop_not_hisp_latino .................. Estimate!!Total!!Not Hispanic or Latino
est_pop_hisp_latino ...................... Estimate!!Total!!Hispanic or Latino
est_tot_hh ............................... Estimate Total Households
est_tot_hh_own_res ....................... Estimate!!Total!!Owner occupied
est_tot_hh_rent_res ...................... Estimate!!Total!!Renter occupied
est_GINI_ndx ............................. Estimate!!Gini Index (measures the extent of economic inqueality)
est_pop_no_internet_access ............... Estimate!!Total!!No Internet access
est_pop_commute_2_wrk .................... Estimate Total Population Commute to work
est_pop_publ_trans_2_wrk ................. Estimate!!Total!!Public transportation (excluding taxicab)
est_mdn_hh_ncome_ttm_2018nfl_adj ......... Estimate!!Median household income in the past 12 months (in 2018 inflation-adjusted dollars)
est_mdn_hh_ncome_ttm_abov_belo_ind........ Values "+" / "-" ... indicating amounts beyond Estimate!!Median household income in the past 12 months (in 2018 inflation-adjusted dollars)
est_mdn_percap_ncome_ttm_2018nfl_adj ..... Estimate!!Per capita income in the past 12 months (in 2018 inflation-adjusted dollars)
est_pop_wth_knwn_pvrty_stts .............. Estimate!!Total!!Population for whom poverty status is determined
est_pop_undr_pvrty_wth_knwn_pvrty_stts ... Estimate!!Below poverty level!!Population for whom poverty status is determined
est_pop_white ............................ Estimate!!Total!!White alone
est_pop_blk .............................. Estimate!!Total!!Black or African American alone
est_pop_am_ind ........................... Estimate!!Total!!American Indian and Alaska Native alone
est_pop_asian ............................ Estimate!!Total!!Asian alone
est_pop_hwaiian .......................... Estimate!!Total!!Native Hawaiian and Other Pacific Islander alone
est_pop_othr ............................. Estimate!!Total!!Some other race alone
est_pop_2pl_race ......................... Estimate!!Total!!Two or more races
est_tot_pop .............................. Estimate Total Population
imu_score ................................ IMU Score (score indicator for Medically Underserved Area)
rpl_themes_svi_ndx ....................... RPL_THEMES (Social Vulnerability Index)
area_sq_mi ............................... Area in square miles
'
;

----------------------------------------------------------------------------

-- B. Table CRE_CENSUS_DATA_BY_COUNTY_YR2018.
--
-- 1. As there may be periodic (annual) releases of census data, and this being
--    for 2018, the table name reflects that.
--    1.a. Separate tables for census data are created for TRACT and COUNTY
--         (COUNTY is identified by GEO_ID).
-- 2. All $ figures are for the year indicated in the name and inflation
--    adjusted for that year.
-- 3. To streamline the usage and data processing, a view each - by TRACT_CD
--    and GEO_ID (the latter being synonyms with county in a give state) - shall
--    be created on (the latest) censuss table to use appropriate version (year)
--    of census data.
-- 4. These data were collected by Keenan from the census webpage into respective
--    files for COUNTY and TRACT (as indicated in the file-name) in the following
--    S3 file-paths.
--      COUNTY - s3://uw211dashboard-workbucket/census_data_by_county.csv
--      TRACT  - s3://uw211dashboard-workbucket/census_data_by_tract.csv
--
----------------------------------------------------------------------------------------------------

CREATE TABLE 
          IF NOT EXISTS  uw211dashboard.public.cre_census_data_by_county_yr2018
(geo_id                                  VARCHAR(10),
 est_pop_age_65pl                        INTEGER,
 est_pop_wth_dsablty                     INTEGER,
 est_pop_age_25pl                        INTEGER,
 est_pop_age_25pl_hgh_schl_orls          INTEGER,
 est_pop_age_16pl                        INTEGER,
 est_pop_age_16pl_in_lbr_frce_prop       NUMERIC(6,3),
 est_pop_age_16pl_empld_prop             NUMERIC(6,3),
 est_pop_age_16pl_unempl_rt              NUMERIC(6,3),
 est_pop_wthout_hlth_insr                INTEGER,
 est_pop_not_hisp_latino                 INTEGER,
 est_pop_hisp_latino                     INTEGER,
 est_tot_hh                              INTEGER,
 est_tot_hh_own_res                      INTEGER,
 est_tot_hh_rent_res                     INTEGER,
 est_GINI_ndx                            NUMERIC(10,6),
 est_pop_no_internet_access              INTEGER,
 est_pop_commute_2_wrk                   INTEGER,
 est_pop_publ_trans_2_wrk                INTEGER,
 est_mdn_hh_ncome_ttm_2018nfl_adj        NUMERIC(15,3),
 est_mdn_hh_ncome_ttm_abov_belo_ind      VARCHAR(10),   -- Indicates Above / Below....
 est_mdn_percap_ncome_ttm_2018nfl_adj    NUMERIC(15,3),
 est_pop_wth_knwn_pvrty_stts             INTEGER,
 est_pop_undr_pvrty_wth_knwn_pvrty_stts  INTEGER,
 est_pop_white                           INTEGER,
 est_pop_blk                             INTEGER,
 est_pop_am_ind                          INTEGER,
 est_pop_asian                           INTEGER,
 est_pop_hwaiian                         INTEGER,
 est_pop_othr                            INTEGER,
 est_pop_2pl_race                        INTEGER,
 est_tot_pop                             INTEGER,
 imu_score                               NUMERIC(10,2),
 rpl_themes_svi_ndx                      NUMERIC(10,6),
 area_sq_mi                              NUMERIC(10,6),
 --created_tsp                             TIMESTAMPTZ     NOT NULL DEFAULT now(),
 --last_update_tsp                         TIMESTAMPTZ     NOT NULL DEFAULT now(),
 PRIMARY KEY (geo_id)
)
;

COMMENT ON TABLE uw211dashboard.public.cre_census_data_by_county_yr2018 IS
'This table contains the 2018 census data attributes relevant to the STL Regional Data Alliance (STL-RDA).

All the dollar figures are for the year indicated in the table-name and are inflation adjusted for that year.

To streamline the usage and data processing, a view each - by TRACT_CD and GEO_ID (the latter being synonyms with COUNTY in a give state) - shall be created on (the latest) censuss table to use appropriate version (year) of census data.

These data were collected by Keenan from the census webpage into respective files for COUNTY and TRACT (as indicated in the file-name) in the following S3 file-paths.
    COUNTY - s3://uw211dashboard-workbucket/census_data_by_county.csv
    TRACT  - s3://uw211dashboard-workbucket/census_data_by_tract.csv

The description of the attributes corresponding to the respective columns names are listed below.

geo_id ................................... GEO-ID (representing corresponding COUNTY).
est_pop_age_65pl ......................... Estimate!!Total!!Total population!!SELECTED AGE CATEGORIES!!65 years and over
est_pop_wth_dsablty ...................... Estimate Total Population With a disability
est_pop_age_25pl ......................... Estimate Total Population 25 years and over
est_pop_age_25pl_hgh_schl_orls ........... Estimate Total Pop 25 and over with high school equivalency or less
est_pop_age_16pl ......................... Estimate!!Total!!Population 16 years and over
est_pop_age_16pl_in_lbr_frce_prop ........ Estimate!!Labor Force Participation Rate!!Population 16 years and over
est_pop_age_16pl_empld_prop .............. Estimate!!Employment/Population Ratio!!Population 16 years and over
est_pop_age_16pl_unempl_rt ............... Estimate!!Unemployment rate!!Population 16 years and over
est_pop_wthout_hlth_insr ................. Estime Total Population No health insurance coverage
est_pop_not_hisp_latino .................. Estimate!!Total!!Not Hispanic or Latino
est_pop_hisp_latino ...................... Estimate!!Total!!Hispanic or Latino
est_tot_hh ............................... Estimate Total Households
est_tot_hh_own_res ....................... Estimate!!Total!!Owner occupied
est_tot_hh_rent_res ...................... Estimate!!Total!!Renter occupied
est_GINI_ndx ............................. Estimate!!Gini Index (measures the extent of economic inqueality)
est_pop_no_internet_access ............... Estimate!!Total!!No Internet access
est_pop_commute_2_wrk .................... Estimate Total Population Commute to work
est_pop_publ_trans_2_wrk ................. Estimate!!Total!!Public transportation (excluding taxicab)
est_mdn_hh_ncome_ttm_2018nfl_adj ......... Estimate!!Median household income in the past 12 months (in 2018 inflation-adjusted dollars)
est_mdn_hh_ncome_ttm_abov_belo_ind........ Values "+" / "-" ... indicating amounts beyond Estimate!!Median household income in the past 12 months (in 2018 inflation-adjusted dollars)
est_mdn_percap_ncome_ttm_2018nfl_adj ..... Estimate!!Per capita income in the past 12 months (in 2018 inflation-adjusted dollars)
est_pop_wth_knwn_pvrty_stts .............. Estimate!!Total!!Population for whom poverty status is determined
est_pop_undr_pvrty_wth_knwn_pvrty_stts ... Estimate!!Below poverty level!!Population for whom poverty status is determined
est_pop_white ............................ Estimate!!Total!!White alone
est_pop_blk .............................. Estimate!!Total!!Black or African American alone
est_pop_am_ind ........................... Estimate!!Total!!American Indian and Alaska Native alone
est_pop_asian ............................ Estimate!!Total!!Asian alone
est_pop_hwaiian .......................... Estimate!!Total!!Native Hawaiian and Other Pacific Islander alone
est_pop_othr ............................. Estimate!!Total!!Some other race alone
est_pop_2pl_race ......................... Estimate!!Total!!Two or more races
est_tot_pop .............................. Estimate Total Population
imu_score ................................ IMU Score (score indicator for Medically Underserved Area)
rpl_themes_svi_ndx ....................... RPL_THEMES (Social Vulnerability Index)
area_sq_mi ............................... Area in square miles
'
;

----------------------------------------------------------------------------

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

