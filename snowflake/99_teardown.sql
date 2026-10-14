-- =============================================================================
-- 99_teardown.sql
-- Removes everything this project created in Snowflake. Run as ACCOUNTADMIN.
-- This deletes all loaded data. The raw files in S3 are not touched, so the
-- warehouse can be rebuilt from them with `make backfill`.
-- =============================================================================

use role accountadmin;

drop database if exists AIR_QUALITY;
drop warehouse if exists ELT_WH;
drop resource monitor if exists ELT_MONITOR;
drop storage integration if exists S3_AIR_QUALITY;
drop user if exists ELT_SERVICE;
drop role if exists ELT_ROLE;
drop role if exists REPORTER;
