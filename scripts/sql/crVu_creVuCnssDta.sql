--
-- 2020-05-14 (Thu.) Haresh Bhatia
--
-- To streamline the usage and data processing, a view each - by TRACT_CD
-- and GEO_ID (the latter being synonyms with county in a give state) - are
-- be created on (the latest) censuss table to use appropriate version (year)
-- of census data.
--
-- This DDL creates the VIEWs for (latest) census data as indicated above.
--
-- A. View CRE_VU_CENSUS_DATA_BY_TRACT_CURR.
-- B. View CRE_VU_CENSUS_DATA_BY_COUNTY_CURR.
--
-- For more details on census data and underlying tables, please check out the
-- comments on the underlying tables.
--
--==================================================================================================
--
-- A. View CRE_VU_CENSUS_DATA_BY_TRACT_CURR.
--
-- 1. This view fetches (CURRent / latest) census data by CENSUS_TRACT.
-- 2. All the data processing scripts shall use this view, unless a specific year
--    data that is other than the table-instance this view is pointing
--    to is required (in which case, correponding table may be used).
-- 3. At present this view points to table for 2018 census data. 
--    It may be changed later to provide the required (latest available) census
--    data.
-- 4. Details of the individual column / attributes may be found in Table comments
--    of the underlying table(s).
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_data_by_tract_curr
AS
(SELECT  tract_cd,
         est_pop_age_65pl,
         est_pop_wth_dsablty,
         est_pop_age_25pl,
         est_pop_age_25pl_hgh_schl_orls,
         est_pop_age_16pl,
         est_pop_age_16pl_in_lbr_frce_prop,
         est_pop_age_16pl_empld_prop,
         est_pop_age_16pl_unempl_rt,
         est_pop_wthout_hlth_insr,
         est_pop_not_hisp_latino,
         est_pop_hisp_latino,
         est_tot_hh,
         est_tot_hh_own_res,
         est_tot_hh_rent_res,
         est_GINI_ndx,
         est_pop_no_internet_access,
         est_pop_commute_2_wrk,
         est_pop_publ_trans_2_wrk,
         est_mdn_hh_ncome_ttm_2018nfl_adj,
         est_mdn_hh_ncome_ttm_abov_belo_ind,   -- pertains to "est_mdn_hh_ncome_ttm_2018nfl_adj" and Indicates amounts beyond '+' (Above) / '-' (Below)....
         est_mdn_percap_ncome_ttm_2018nfl_adj,
         est_pop_wth_knwn_pvrty_stts,
         est_pop_undr_pvrty_wth_knwn_pvrty_stts,
         est_pop_white,
         est_pop_blk,
         est_pop_am_ind,
         est_pop_asian,
         est_pop_hwaiian,
         est_pop_othr,
         est_pop_2pl_race,
         est_tot_pop,
         imu_score,
         rpl_themes_svi_ndx,
         area_sq_mi,
         created_tsp,
         last_update_tsp
   FROM  cre_census_data_by_tract_yr2018
)
;


COMMENT ON VIEW uw211dashboard.public.cre_vu_census_data_by_tract_curr IS
'This view is intended to point to the current (required or latest) census data table and fetches census data by CENSUS_TRACT.

All the dollar figures are for the year that the underlying table indicates and are inflation adjusted for the year indicated in the underlying census-data table.

All the data processing scripts shall use this view, unless a specific year data that is other than the table-instance this view is pointing to is required (in which case, correponding table may be used).

At present this view points to table for 2018 census data.  It may be changed later to provide the required (latest available) census data.

Details of the individual column / attributes may be found in Table comments of the underlying table(s).
'
;

-- ---------------------------------------------------------------------
--
-- B. View CRE_VU_CENSUS_DATA_BY_COUNTY_CURR.
--
-- 1. This view fetches (CURRent / latest) census data by COUNTY (GEO_ID).
-- 2. All the data processing scripts shall use this view, unless a specific year
--    data that is other than the table-instance this view is pointing
--    to is required (in which case, correponding table may be used).
-- 3. At present this view points to table for 2018 census data. 
--    It may be changed later to provide the required (latest available) census
--    data.
-- 4. Details of the individual column / attributes may be found in Table comments
--    of the underlying table(s).
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_data_by_county_curr
AS
(SELECT  geo_id,                               -- Synonymous to COUNTY (First two characters of this code point to state, and the remaining 3 to a county within).
         est_pop_age_65pl,
         est_pop_wth_dsablty,
         est_pop_age_25pl,
         est_pop_age_25pl_hgh_schl_orls,
         est_pop_age_16pl,
         est_pop_age_16pl_in_lbr_frce_prop,
         est_pop_age_16pl_empld_prop,
         est_pop_age_16pl_unempl_rt,
         est_pop_wthout_hlth_insr,
         est_pop_not_hisp_latino,
         est_pop_hisp_latino,
         est_tot_hh,
         est_tot_hh_own_res,
         est_tot_hh_rent_res,
         est_GINI_ndx,
         est_pop_no_internet_access,
         est_pop_commute_2_wrk,
         est_pop_publ_trans_2_wrk,
         est_mdn_hh_ncome_ttm_2018nfl_adj,
         est_mdn_hh_ncome_ttm_abov_belo_ind,   -- pertains to "est_mdn_hh_ncome_ttm_2018nfl_adj" and Indicates amounts beyond '+' (Above) / '-' (Below)....
         est_mdn_percap_ncome_ttm_2018nfl_adj,
         est_pop_wth_knwn_pvrty_stts,
         est_pop_undr_pvrty_wth_knwn_pvrty_stts,
         est_pop_white,
         est_pop_blk,
         est_pop_am_ind,
         est_pop_asian,
         est_pop_hwaiian,
         est_pop_othr,
         est_pop_2pl_race,
         est_tot_pop,
         imu_score,
         rpl_themes_svi_ndx,
         area_sq_mi,
         created_tsp,
         last_update_tsp
   FROM  cre_census_data_by_county_yr2018
)
;


COMMENT ON VIEW uw211dashboard.public.cre_vu_census_data_by_county_curr IS
'This view is intended to point to the current (required or latest) census data table and fetches census data by COUNTY. [GEO_ID is synonymous to county. The first two characters of GEO_ID identify a state in the US, and the following 3 characters identify a county within that state.]

All the dollar figures are for the year that the underlying table indicates and are inflation adjusted for the year indicated in the underlying census-data table.

All the data processing scripts shall use this view, unless a specific year data that is other than the table-instance this view is pointing to is required (in which case, correponding table may be used).

At present this view points to table for 2018 census data.  It may be changed later to provide the required (latest available) census data.

Details of the individual column / attributes may be found in Table comments of the underlying table(s).
'
;


