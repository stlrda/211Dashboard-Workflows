-- 
-- 2020-05-21 (Thurs.) Keenan Berry
--
-- This script is used to grant read privileges to the dataviz user.
-- Read access allowed for all public tables.
-- "dataviz" user is user for Tableau dashboard development.
--==================================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA public TO dataviz;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO dataviz;