-- 
-- 2020-05-07 (Thu.) Haresh Bhatia
--
-- This DDL script is to TRUNCATE all the staging tables that are to be
-- refreshed for daily run.
--==================================================================================

TRUNCATE TABLE stg_covid_dly_viz_cnty_all
;

TRUNCATE TABLE stg_covid_zip_stl_city
;

TRUNCATE TABLE stg_covid_zip_stl_county
;

TRUNCATE TABLE stg_mo_211_data
;
