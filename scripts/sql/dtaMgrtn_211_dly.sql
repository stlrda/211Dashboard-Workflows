-- 
-- 2020-05-29 (Fri.) Haresh Bhatia
--
-- This is DML script is to migrate the next incremental data bulk from staging tables
-- to the core tables for 211 data.
-- 
-- This script CONSIDERS the STaGing tables having already been updated with
-- fresh data load.
-- 
-- B. for CRE_211_DATA
-- -- ------------------
--  1. DELETE the CRE_COVID_DATA records with REPORT_DATE on or after the
--     'LAST_SUCCESSFUL' run date.
--  2. INSERT records for CATEGORies and (corresponding) SUB-CATEGORies
--     corersponding to respective new names, if any, in the refreshed staging
--     data. 
--  3. INSERT the records from the staging tables with CALL_DATE that is on or
--     after the 'LAST_SUCCESSFUL' run date. [While migrating data records from
--     211 staging table to the core table, the CATEGORY and SUB-CATEGORY names
--     are translated into corresponding IDs in an effort to normalize this core
--     table.]
--
--
--==================================================================================
--
--
-- B1. DELETE the CRE_211_DATA records with CALL_DATE on or after the
--     'LAST_SUCCESSFUL' run date.
DELETE
  FROM  cre_211_data
 WHERE  report_date >= (SELECT  lst_success_dt - buffer_cnt
                          FROM  cre_last_success_run_dt
                         WHERE  run_cd = 'DLY_ALL'
                       )
;

-- B2-1. INSERT the records for any new CATEGORies and 
INSERT INTO lkup_211_category
(two11_category_nm)
(SELECT  DISTINCT  category  new_categ
   FROM  stg_mo_211_data    s
  WHERE  NOT EXISTS (SELECT  1 FROM lkup_211_category  c
                      WHERE  c.two11_category_nm = s.category
                    )
)
;

-- B2-2. INSERT the records for any new SUB-CATEGORies (corresponding to CATEGORies).
INSERT INTO lkup_211_sub_category
(two11_category_id, two11_sub_category_nm)
(SELECT  DISTINCT  c.two11_category_id, sub_category  new_sub_categ
   FROM  stg_mo_211_data    s,
         lkup_211_category  c
  WHERE  c.two11_category_nm = s.category
    AND  NOT EXISTS (SELECT  1 FROM lkup_211_sub_category  b
                      WHERE  b.two11_category_id     = c.two11_category_id
                        AND  b.two11_sub_category_nm = s.sub_category
                    )
)
;
-- ----------

-- B3. INSERT the records from the staging tables with REPORT_DATE that is on or
--     after the 'LAST_SUCCESSFUL' run date.
WITH lst_sccss_run AS
(SELECT  (lst_success_dt - buffer_cnt)  incr_data_ref_dt 
   FROM  cre_last_success_run_dt
  WHERE  run_cd = 'DLY_ALL'
)
INSERT INTO cre_211_data
(call_date,
-- state_nm,
-- county_nm,
 zip_cd,
 two11_category_id,
 two11_sub_category_id,
 call_counts
)
SELECT  t1.call_dt,
        t1.zip_cd,
        c1.two11_category_id,
        s1.two11_sub_category_id,
        t1.counts
  FROM  stg_mo_211_data        t1,
        lkup_211_category      c1,
        lkup_211_sub_category  s1,
        lst_sccss_run          sr
 WHERE  t1.category           = c1.two11_category_nm
   AND  t1.sub_category       = s1.two11_sub_category_nm
   AND  c1.two11_category_id  = s1.two11_category_id
   AND  t1.call_dt           >= sr.incr_data_ref_dt
;


