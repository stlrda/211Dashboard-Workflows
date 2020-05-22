--
-- 2020-05-22 (Fri.) Keenan Berry
--
-- This creates the function for reporting active COVID cases given new cases.
--
-- A. Function calc_active_cases.
--
-- 1. Arguments:
--      current_day     DATE
--      geoid           TEXT
--      source_type     TEXT
-- 2. Returns:
--      sum(new_cases)  INTEGER
--
--====================================================================================

CREATE OR REPLACE FUNCTION calc_active_cases(current_day DATE, geoid TEXT, source_type TEXT) RETURNS bigint AS $$
	select sum(new_cases)
	from cre_covid_data
	where report_date between current_day-17 and current_day  -- calculation may change 
		and geo_id = geoid
		and data_source = source_type
	;
$$ LANGUAGE SQL;