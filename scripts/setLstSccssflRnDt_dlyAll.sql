-- 
-- 2020-05-11 (Mon.) Haresh Bhatia
--
-- This is DML script is to update the LST_SUCCESS_DT, in the CRE_LAST_SUCCESS_RUN_DT
-- table, and executed after all the 'Daily DAGs' are executed successfully.
-- 
-- X. UPDATE the CRE_LAST_SUCCESS_RUN_DT
-- -- ----------------------------------
--  1. For given run-cycle, update the corresponding record, in table 
--     CRE_LAST_SUCCESS_RUN_DT, with the CURRENT_DATE.
--
-------------------------------------------------------------------------------------
--
-- 2020-05-19 (Tue.) Haresh Bhatia
--
-- Changing the logic of setting the LST_SUCCESS_DT of table CRE_LAST_SUCCESS_RUN_DT
-- to max. of all the dates of objects that are part of the daily DAG.
--
-- Philosophy behind this setting:
--   As per one discussion it was decided to have SQL scripts that are granular
--   (separate for different objects / tasks). However, that may result in several 
--   SQL scripts (DDL and DMLs) for daily-DAG tasks. Which requires the updating
--   LST_SUCCESS_DT of table CRE_LAST_SUCCESS_RUN_DT after all those are run (in 
--   a separate SQL script - which is this).
-- Deciding on the actual logic:
--   To accommodate that, the LST_SUCCESS_DT is set to max. of all the corresponding
--   dates from the staging data that are part of the daily-DAG.
--
--==================================================================================
--
---------------------------------------------------------------------------
-- X1. For given run-cycle, update the corresponding record, in table 
--     CRE_LAST_SUCCESS_RUN_DT, with the CURRENT_DATE.
UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = CURRENT_DATE
 WHERE  run_cd = 'DLY_ALL'
;

-------------------------------------------------------------------------------------
--
-- 2020-05-19 (Tue.) Haresh Bhatia
-- Newer logic of setting LST_SUCCESS_DT in CRE_LAST_SUCCESS_RUN_DT

UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = (SELECT  MIN(mx_rp_dt)
                            FROM (SELECT  MAX(report_date)  mx_rp_dt  FROM stg_covid_dly_viz_cnty_all
                                  UNION ALL
                                  SELECT  MAX(report_dt  )  mx_rp_dt  FROM stg_covid_zip_stl_city
                                  UNION ALL
                                  SELECT  MAX(report_dt  )  mx_rp_dt  FROM stg_covid_zip_stl_county
                                 )    c
                         )
 WHERE  run_cd = 'DLY_ALL'
;

