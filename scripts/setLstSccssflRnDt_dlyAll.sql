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
--==================================================================================
--
---------------------------------------------------------------------------
-- X1. For given run-cycle, update the corresponding record, in table 
--     CRE_LAST_SUCCESS_RUN_DT, with the CURRENT_DATE.
UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = CURRENT_DATE
 WHERE  run_cd = 'DLY_ALL'
;


