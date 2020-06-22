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
-- C. View CRE_VU_CENSUS_TRACT_2_ZIP_CURR. 
-- D. View CRE_VU_CENSUS_COUNTY_2_ZIP_CURR. 
--
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
   WHERE LEFT(tract_cd, 5) in (SELECT DISTINCT geo_id FROM lkup_areas_of_intr_geo_scope)
   -- KB added where statement to filter for "areas of interest" - 6/22/2020
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
   WHERE geo_id in (SELECT DISTINCT geo_id FROM lkup_areas_of_intr_geo_scope)
   -- KB added where statement to filter for "areas of interest" - 6/22/2020
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

-- ---------------------------------------------------------------------
--
-- C. View CRE_VU_CENSUS_TRACT_2_ZIP_CURR. 
--
-- 1. This view translates census data to zip-level from CRE_VU_CENSUS_DATA_BY_TRACT_CURR
--    by using the proprtion of res. addresses in respective ZIP_CDs as indicated
--    in LKUP_TRACT_ZIP_MPG_GTWY_RGNL.
-- 2. Check the comments on these objects for more details.
-- 3. Notes of caution:
--      a. All these translations (derived values) of census numbers are putative.
--         These derived values have no accuracy or level of error associated
--         with them.
--      b. Not all derivation would make sense (e.g., those of median values, 
--         rates, and proportions). The corresponding derived attributes are
--         made NULLs (see comments of the view)
-- 4. The data processing tools would use this view, unless specific year data,
--    other than table-instance this view is pointing to, is required (in which
--    case the this view-query may be appropriately modified).
-- 5. At present this view points to the TRACT-level census data for 2018.
--
--
CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_tract_2_zip_curr
AS
(SELECT  zip_cd,
         SUM(est_pop_age_65pl)                        AS  est_pop_age_65pl,
         SUM(est_pop_wth_dsablty)                     AS  est_pop_wth_dsablty,
         SUM(est_pop_age_25pl)                        AS  est_pop_age_25pl,
         SUM(est_pop_age_25pl_hgh_schl_orls)          AS  est_pop_age_25pl_hgh_schl_orls,
         SUM(est_pop_age_16pl)                        AS  est_pop_age_16pl,
         SUM(est_pop_age_16pl_in_lbr_frce_prop)       AS  est_pop_age_16pl_in_lbr_frce_prop,
         SUM(est_pop_age_16pl_empld_prop)             AS  est_pop_age_16pl_empld_prop,            -- NOT-DERIVABLE, therefore NULL
         SUM(est_pop_age_16pl_unempl_rt)              AS  est_pop_age_16pl_unempl_rt,             -- NOT-DERIVABLE, therefore NULL
         SUM(est_pop_wthout_hlth_insr)                AS  est_pop_wthout_hlth_insr,
         SUM(est_pop_not_hisp_latino)                 AS  est_pop_not_hisp_latino,
         SUM(est_pop_hisp_latino)                     AS  est_pop_hisp_latino,
         SUM(est_tot_hh)                              AS  est_tot_hh,
         SUM(est_tot_hh_own_res)                      AS  est_tot_hh_own_res,
         SUM(est_tot_hh_rent_res)                     AS  est_tot_hh_rent_res,
         SUM(est_GINI_ndx)                            AS  est_GINI_ndx,                           -- NOT-DERIVABLE, therefore NULL
         SUM(est_pop_no_internet_access)              AS  est_pop_no_internet_access,
         SUM(est_pop_commute_2_wrk)                   AS  est_pop_commute_2_wrk,
         SUM(est_pop_publ_trans_2_wrk)                AS  est_pop_publ_trans_2_wrk,
         SUM(est_mdn_hh_ncome_ttm_2018nfl_adj)        AS  est_mdn_hh_ncome_ttm_2018nfl_adj,       -- NOT-DERIVABLE, therefore NULL
         SUM(est_mdn_hh_ncome_ttm_abov_belo_ind)      AS  est_mdn_hh_ncome_ttm_abov_belo_ind,     -- NOT-DERIVABLE, therefore NULL
         SUM(est_mdn_percap_ncome_ttm_2018nfl_adj)    AS  est_mdn_percap_ncome_ttm_2018nfl_adj,   -- NOT-DERIVABLE, therefore NULL
         SUM(est_pop_wth_knwn_pvrty_stts)             AS  est_pop_wth_knwn_pvrty_stts,
         SUM(est_pop_undr_pvrty_wth_knwn_pvrty_stts)  AS  est_pop_undr_pvrty_wth_knwn_pvrty_stts,
         SUM(est_pop_white)                           AS  est_pop_white,
         SUM(est_pop_blk)                             AS  est_pop_blk,
         SUM(est_pop_am_ind)                          AS  est_pop_am_ind,
         SUM(est_pop_asian)                           AS  est_pop_asian,
         SUM(est_pop_hwaiian)                         AS  est_pop_hwaiian,
         SUM(est_pop_othr)                            AS  est_pop_othr,
         SUM(est_pop_2pl_race)                        AS  est_pop_2pl_race,
         SUM(est_tot_pop)                             AS  est_tot_pop,
         SUM(imu_score)                               AS  imu_score,                              -- NOT-DERIVABLE, therefore NULL
         SUM(rpl_themes_svi_ndx)                      AS  rpl_themes_svi_ndx,                     -- NOT-DERIVABLE, therefore NULL
         SUM(area_sq_mi)                              AS  area_sq_mi
   FROM (SELECT  ct.tract_cd,
                 zo.zip_cd,
                 zo.res_ratio,
                 zo.res_ratio * ct.est_pop_age_65pl                         AS  est_pop_age_65pl,
                 zo.res_ratio * ct.est_pop_wth_dsablty                      AS  est_pop_wth_dsablty,
                 zo.res_ratio * ct.est_pop_age_25pl                         AS  est_pop_age_25pl,
                 zo.res_ratio * ct.est_pop_age_25pl_hgh_schl_orls           AS  est_pop_age_25pl_hgh_schl_orls,
                 zo.res_ratio * ct.est_pop_age_16pl                         AS  est_pop_age_16pl,
                 zo.res_ratio * ct.est_pop_age_16pl_in_lbr_frce_prop        AS  est_pop_age_16pl_in_lbr_frce_prop,
                 CAST(NULL  AS NUMERIC)                                     AS  est_pop_age_16pl_empld_prop,            -- NOT-DERIVABLE, therefore NULL
                 CAST(NULL  AS NUMERIC)                                     AS  est_pop_age_16pl_unempl_rt,             -- NOT-DERIVABLE, therefore NULL
                 zo.res_ratio * ct.est_pop_wthout_hlth_insr                 AS  est_pop_wthout_hlth_insr,
                 zo.res_ratio * ct.est_pop_not_hisp_latino                  AS  est_pop_not_hisp_latino,
                 zo.res_ratio * ct.est_pop_hisp_latino                      AS  est_pop_hisp_latino,
                 zo.res_ratio * ct.est_tot_hh                               AS  est_tot_hh,
                 zo.res_ratio * ct.est_tot_hh_own_res                       AS  est_tot_hh_own_res,
                 zo.res_ratio * ct.est_tot_hh_rent_res                      AS  est_tot_hh_rent_res,
                 CAST(NULL  AS NUMERIC)                                     AS  est_GINI_ndx,                           -- NOT-DERIVABLE, therefore NULL
                 zo.res_ratio * ct.est_pop_no_internet_access               AS  est_pop_no_internet_access,
                 zo.res_ratio * ct.est_pop_commute_2_wrk                    AS  est_pop_commute_2_wrk,
                 zo.res_ratio * ct.est_pop_publ_trans_2_wrk                 AS  est_pop_publ_trans_2_wrk,
                 CAST(NULL  AS NUMERIC)                                     AS  est_mdn_hh_ncome_ttm_2018nfl_adj,       -- NOT-DERIVABLE, therefore NULL
                 CAST(NULL  AS NUMERIC)                                     AS  est_mdn_hh_ncome_ttm_abov_belo_ind,     -- NOT-DERIVABLE, therefore NULL
                 CAST(NULL  AS NUMERIC)                                     AS  est_mdn_percap_ncome_ttm_2018nfl_adj,   -- NOT-DERIVABLE, therefore NULL
                 zo.res_ratio * ct.est_pop_wth_knwn_pvrty_stts              AS  est_pop_wth_knwn_pvrty_stts,
                 zo.res_ratio * ct.est_pop_undr_pvrty_wth_knwn_pvrty_stts   AS  est_pop_undr_pvrty_wth_knwn_pvrty_stts,
                 zo.res_ratio * ct.est_pop_white                            AS  est_pop_white,
                 zo.res_ratio * ct.est_pop_blk                              AS  est_pop_blk,
                 zo.res_ratio * ct.est_pop_am_ind                           AS  est_pop_am_ind,
                 zo.res_ratio * ct.est_pop_asian                            AS  est_pop_asian,
                 zo.res_ratio * ct.est_pop_hwaiian                          AS  est_pop_hwaiian,
                 zo.res_ratio * ct.est_pop_othr                             AS  est_pop_othr,
                 zo.res_ratio * ct.est_pop_2pl_race                         AS  est_pop_2pl_race,
                 zo.res_ratio * ct.est_tot_pop                              AS  est_tot_pop,
                 CAST(NULL  AS NUMERIC)                                     AS  imu_score,                              -- NOT-DERIVABLE, therefore NULL
                 CAST(NULL  AS NUMERIC)                                     AS  rpl_themes_svi_ndx,                     -- NOT-DERIVABLE, therefore NULL
                 zo.res_ratio * ct.area_sq_mi                               AS  area_sq_mi
           FROM  cre_vu_census_data_by_tract_curr      ct,
                 lkup_tract_zip_mpg_gtwy_rgnl          zo
          WHERE  ct.tract_cd = zo.tract_cd
        )     zt
  GROUP  BY zip_cd
)
;

COMMENT ON VIEW uw211dashboard.public.cre_vu_census_tract_2_zip_curr IS
'This view translates census data to zip-level from CRE_VU_CENSUS_DATA_BY_TRACT_CURR by using the proportion of res. addresses in respective ZIP_CDs as indicated by LKUP_TRACT_ZIP_MPG_GTWY_RGNL (Check the comments on these objects for more details).

Notes of caution:
 a. All these translations (derived values) of census numbers are putative. These derived values have no accuracy or level of error associated with them.
 b. Not all derivation would make sense (e.g., those of median values, rates, and proportions). The corresponding derived attributes for the following, are therefore NULL.
         est_pop_age_16pl_empld_prop             -- NOT-DERIVABLE, therefore NULL
         est_pop_age_16pl_unempl_rt              -- NOT-DERIVABLE, therefore NULL
         est_GINI_ndx                            -- NOT-DERIVABLE, therefore NULL
         est_mdn_hh_ncome_ttm_2018nfl_adj        -- NOT-DERIVABLE, therefore NULL
         est_mdn_hh_ncome_ttm_abov_belo_ind      -- NOT-DERIVABLE, therefore NULL
         est_mdn_percap_ncome_ttm_2018nfl_adj    -- NOT-DERIVABLE, therefore NULL
         imu_score                               -- NOT-DERIVABLE, therefore NULL
         rpl_themes_svi_ndx                      -- NOT-DERIVABLE, therefore NULL

The data processing tools would use this view, unless specific year data, other than table-instance this view is pointing to, is required (in which case the this view-query may be appropriately modified). [At present this view points to the TRACT-level census data for 2018.]
'
;

-- ---------------------------------------------------------------------
-- EDIT: KB (6/22/2020) - This table is not needed as the tract --> zip translation is more accurate
--
-- D. View CRE_VU_CENSUS_COUNTY_2_ZIP_CURR. 
--
-- 1. This view translates census data to zip-level from CRE_VU_CENSUS_DATA_BY_COUNTY_CURR
--    by using the proprtion of res. addresses in respective ZIP_CDs as indicated
--    in LKUP_COUNTY_ZIP_MPG_GTWY_RGNL.
-- 2. Check the comments on these objects for more details.
-- 3. Notes of caution:
--      a. All these translations (derived values) of census numbers are putative.
--         These derived values have no accuracy or level of error associated
--         with them.
--      b. Not all derivation would make sense (e.g., those of median values, 
--         rates, and proportions). The corresponding derived attributes are
--         made NULLs (see comments of the view)
-- 4. The data processing tools would use this view, unless specific year data,
--    other than table-instance this view is pointing to, is required (in which
--    case the this view-query may be appropriately modified).
-- 5. At present this view points to the COUNTY-level census data for 2018.
--
--
-- CREATE OR REPLACE VIEW uw211dashboard.public.cre_vu_census_county_2_zip_curr
-- AS
-- (SELECT  zip_cd,
--          SUM(est_pop_age_65pl)                        AS  est_pop_age_65pl,
--          SUM(est_pop_wth_dsablty)                     AS  est_pop_wth_dsablty,
--          SUM(est_pop_age_25pl)                        AS  est_pop_age_25pl,
--          SUM(est_pop_age_25pl_hgh_schl_orls)          AS  est_pop_age_25pl_hgh_schl_orls,
--          SUM(est_pop_age_16pl)                        AS  est_pop_age_16pl,
--          SUM(est_pop_age_16pl_in_lbr_frce_prop)       AS  est_pop_age_16pl_in_lbr_frce_prop,
--          SUM(est_pop_age_16pl_empld_prop)             AS  est_pop_age_16pl_empld_prop,            -- NOT-DERIVABLE, therefore NULL
--          SUM(est_pop_age_16pl_unempl_rt)              AS  est_pop_age_16pl_unempl_rt,             -- NOT-DERIVABLE, therefore NULL
--          SUM(est_pop_wthout_hlth_insr)                AS  est_pop_wthout_hlth_insr,
--          SUM(est_pop_not_hisp_latino)                 AS  est_pop_not_hisp_latino,
--          SUM(est_pop_hisp_latino)                     AS  est_pop_hisp_latino,
--          SUM(est_tot_hh)                              AS  est_tot_hh,
--          SUM(est_tot_hh_own_res)                      AS  est_tot_hh_own_res,
--          SUM(est_tot_hh_rent_res)                     AS  est_tot_hh_rent_res,
--          SUM(est_GINI_ndx)                            AS  est_GINI_ndx,                           -- NOT-DERIVABLE, therefore NULL
--          SUM(est_pop_no_internet_access)              AS  est_pop_no_internet_access,
--          SUM(est_pop_commute_2_wrk)                   AS  est_pop_commute_2_wrk,
--          SUM(est_pop_publ_trans_2_wrk)                AS  est_pop_publ_trans_2_wrk,
--          SUM(est_mdn_hh_ncome_ttm_2018nfl_adj)        AS  est_mdn_hh_ncome_ttm_2018nfl_adj,       -- NOT-DERIVABLE, therefore NULL
--          SUM(est_mdn_hh_ncome_ttm_abov_belo_ind)      AS  est_mdn_hh_ncome_ttm_abov_belo_ind,     -- NOT-DERIVABLE, therefore NULL
--          SUM(est_mdn_percap_ncome_ttm_2018nfl_adj)    AS  est_mdn_percap_ncome_ttm_2018nfl_adj,   -- NOT-DERIVABLE, therefore NULL
--          SUM(est_pop_wth_knwn_pvrty_stts)             AS  est_pop_wth_knwn_pvrty_stts,
--          SUM(est_pop_undr_pvrty_wth_knwn_pvrty_stts)  AS  est_pop_undr_pvrty_wth_knwn_pvrty_stts,
--          SUM(est_pop_white)                           AS  est_pop_white,
--          SUM(est_pop_blk)                             AS  est_pop_blk,
--          SUM(est_pop_am_ind)                          AS  est_pop_am_ind,
--          SUM(est_pop_asian)                           AS  est_pop_asian,
--          SUM(est_pop_hwaiian)                         AS  est_pop_hwaiian,
--          SUM(est_pop_othr)                            AS  est_pop_othr,
--          SUM(est_pop_2pl_race)                        AS  est_pop_2pl_race,
--          SUM(est_tot_pop)                             AS  est_tot_pop,
--          SUM(imu_score)                               AS  imu_score,                              -- NOT-DERIVABLE, therefore NULL
--          SUM(rpl_themes_svi_ndx)                      AS  rpl_themes_svi_ndx,                     -- NOT-DERIVABLE, therefore NULL
--          SUM(area_sq_mi)                              AS  area_sq_mi
--    FROM (SELECT  ct.geo_id,
--                  zo.zip_cd,
--                  zo.res_ratio,
--                  zo.res_ratio * ct.est_pop_age_65pl                         AS  est_pop_age_65pl,
--                  zo.res_ratio * ct.est_pop_wth_dsablty                      AS  est_pop_wth_dsablty,
--                  zo.res_ratio * ct.est_pop_age_25pl                         AS  est_pop_age_25pl,
--                  zo.res_ratio * ct.est_pop_age_25pl_hgh_schl_orls           AS  est_pop_age_25pl_hgh_schl_orls,
--                  zo.res_ratio * ct.est_pop_age_16pl                         AS  est_pop_age_16pl,
--                  zo.res_ratio * ct.est_pop_age_16pl_in_lbr_frce_prop        AS  est_pop_age_16pl_in_lbr_frce_prop,
--                  CAST(NULL  AS NUMERIC)                                     AS  est_pop_age_16pl_empld_prop,            -- NOT-DERIVABLE, therefore NULL
--                  CAST(NULL  AS NUMERIC)                                     AS  est_pop_age_16pl_unempl_rt,             -- NOT-DERIVABLE, therefore NULL
--                  zo.res_ratio * ct.est_pop_wthout_hlth_insr                 AS  est_pop_wthout_hlth_insr,
--                  zo.res_ratio * ct.est_pop_not_hisp_latino                  AS  est_pop_not_hisp_latino,
--                  zo.res_ratio * ct.est_pop_hisp_latino                      AS  est_pop_hisp_latino,
--                  zo.res_ratio * ct.est_tot_hh                               AS  est_tot_hh,
--                  zo.res_ratio * ct.est_tot_hh_own_res                       AS  est_tot_hh_own_res,
--                  zo.res_ratio * ct.est_tot_hh_rent_res                      AS  est_tot_hh_rent_res,
--                  CAST(NULL  AS NUMERIC)                                     AS  est_GINI_ndx,                           -- NOT-DERIVABLE, therefore NULL
--                  zo.res_ratio * ct.est_pop_no_internet_access               AS  est_pop_no_internet_access,
--                  zo.res_ratio * ct.est_pop_commute_2_wrk                    AS  est_pop_commute_2_wrk,
--                  zo.res_ratio * ct.est_pop_publ_trans_2_wrk                 AS  est_pop_publ_trans_2_wrk,
--                  CAST(NULL  AS NUMERIC)                                     AS  est_mdn_hh_ncome_ttm_2018nfl_adj,       -- NOT-DERIVABLE, therefore NULL
--                  CAST(NULL  AS NUMERIC)                                     AS  est_mdn_hh_ncome_ttm_abov_belo_ind,     -- NOT-DERIVABLE, therefore NULL
--                  CAST(NULL  AS NUMERIC)                                     AS  est_mdn_percap_ncome_ttm_2018nfl_adj,   -- NOT-DERIVABLE, therefore NULL
--                  zo.res_ratio * ct.est_pop_wth_knwn_pvrty_stts              AS  est_pop_wth_knwn_pvrty_stts,
--                  zo.res_ratio * ct.est_pop_undr_pvrty_wth_knwn_pvrty_stts   AS  est_pop_undr_pvrty_wth_knwn_pvrty_stts,
--                  zo.res_ratio * ct.est_pop_white                            AS  est_pop_white,
--                  zo.res_ratio * ct.est_pop_blk                              AS  est_pop_blk,
--                  zo.res_ratio * ct.est_pop_am_ind                           AS  est_pop_am_ind,
--                  zo.res_ratio * ct.est_pop_asian                            AS  est_pop_asian,
--                  zo.res_ratio * ct.est_pop_hwaiian                          AS  est_pop_hwaiian,
--                  zo.res_ratio * ct.est_pop_othr                             AS  est_pop_othr,
--                  zo.res_ratio * ct.est_pop_2pl_race                         AS  est_pop_2pl_race,
--                  zo.res_ratio * ct.est_tot_pop                              AS  est_tot_pop,
--                  CAST(NULL  AS NUMERIC)                                     AS  imu_score,                              -- NOT-DERIVABLE, therefore NULL
--                  CAST(NULL  AS NUMERIC)                                     AS  rpl_themes_svi_ndx,                     -- NOT-DERIVABLE, therefore NULL
--                  zo.res_ratio * ct.area_sq_mi                               AS  area_sq_mi
--            FROM  cre_vu_census_data_by_county_curr      ct,
--                  lkup_county_zip_mpg_gtwy_rgnl          zo
--           WHERE  ct.geo_id = zo.geo_id
--         )     zt
--   GROUP  BY zip_cd
-- )
-- ;

-- COMMENT ON VIEW uw211dashboard.public.cre_vu_census_county_2_zip_curr IS
-- 'This view translates census data to zip-level from CRE_VU_CENSUS_DATA_BY_COUNTY_CURR by using the proportion of res. addresses in respective ZIP_CDs as indicated by LKUP_COUNTY_ZIP_MPG_GTWY_RGNL (Check the comments on these objects for more details).

-- Notes of caution:
--  a. All these translations (derived values) of census numbers are putative. These derived values have no accuracy or level of error associated with them.
--  b. Not all derivation would make sense (e.g., those of median values, rates, and proportions). The corresponding derived attributes for the following, are therefore NULL.
--          est_pop_age_16pl_empld_prop             -- NOT-DERIVABLE, therefore NULL
--          est_pop_age_16pl_unempl_rt              -- NOT-DERIVABLE, therefore NULL
--          est_GINI_ndx                            -- NOT-DERIVABLE, therefore NULL
--          est_mdn_hh_ncome_ttm_2018nfl_adj        -- NOT-DERIVABLE, therefore NULL
--          est_mdn_hh_ncome_ttm_abov_belo_ind      -- NOT-DERIVABLE, therefore NULL
--          est_mdn_percap_ncome_ttm_2018nfl_adj    -- NOT-DERIVABLE, therefore NULL
--          imu_score                               -- NOT-DERIVABLE, therefore NULL
--          rpl_themes_svi_ndx                      -- NOT-DERIVABLE, therefore NULL

-- The data processing tools would use this view, unless specific year data, other than table-instance this view is pointing to, is required (in which case the this view-query may be appropriately modified). [At present this view points to the COUNTY-level census data for 2018.]
-- '
-- ;
