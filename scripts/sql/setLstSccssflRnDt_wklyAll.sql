-- 
-- 2020-05-20 (Wed.) Haresh Bhatia
--
-- This is DML script is to update the LST_SUCCESS_DT, in the CRE_LAST_SUCCESS_RUN_DT
-- table, and executed after all the 'Weekly DAGs' are executed successfully.
-- 
-- X. UPDATE the CRE_LAST_SUCCESS_RUN_DT
-- -- ----------------------------------
--  1. For given run-cycle, update the corresponding record, in table 
--     CRE_LAST_SUCCESS_RUN_DT, to the earliest of the max. of all the dates of
--     objects that are part of the Weekly DAG.
--
-- Philosophy behind this setting:
--   As per one discussion it was decided to have SQL scripts that are granular
--   (separate for different objects / tasks). However, that may result in several 
--   SQL scripts (DDL and DMLs) for Weekly-DAG tasks. Which requires the updating
--   LST_SUCCESS_DT of table CRE_LAST_SUCCESS_RUN_DT after all those are run (in 
--   a separate SQL script - which is this).
-- Deciding on the actual logic:
--   To accommodate that, the LST_SUCCESS_DT is set to the earliest of the max.
--    of all the corresponding dates from the staging data that are part of the
--    Weekly-DAG.
--
--==================================================================================
--

UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = (SELECT  MIN(mx_prd_end_dt)
                            FROM (SELECT  MAX(week_ending_sat_dt)  mx_prd_end_dt  FROM stg_mo_unemployment_clms
--                                UNION ALL
                                 )    c
                         )
 WHERE  run_cd = 'WKLY_ALL'
;

