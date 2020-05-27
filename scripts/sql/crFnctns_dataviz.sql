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

-- FUNCTION: public.calc_active_cases(date, text, text, integer)

-- DROP FUNCTION public.calc_active_cases(date, text, text, integer);

CREATE OR REPLACE FUNCTION public.calc_active_cases(
	current_day date,
	geoid text,
	source_type text,
	time_period integer)
    RETURNS bigint
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
AS $BODY$
	select sum(new_cases)
	from cre_covid_data
	where report_date between current_day-time_period and current_day
		and geo_id = geoid
		and data_source = source_type
	;
$BODY$;

ALTER FUNCTION public.calc_active_cases(date, text, text, integer)
    OWNER TO airflow_user;