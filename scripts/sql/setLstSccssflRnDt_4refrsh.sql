-- 
-- 2020-06-23 (Tues.) Keenan Berry
--
-- This is DML script is to update the LST_SUCCESS_DT, in the CRE_LAST_SUCCESS_RUN_DT
-- table. This script is executed in the refresh DAG. After this script runs, the "natural"
-- data loads for daily and weekly DAGs should be completely refreshed to include "past"
-- data that may have changed.
--
-- Weekly unemployment claims data
UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = '2019-01-01'
 WHERE  run_cd = 'WKLY_ALL'
;

-- Daily COVID-19 data
UPDATE  cre_last_success_run_dt
   SET  lst_success_dt = '2019-01-01'
 WHERE  run_cd = 'DLY_ALL'
;