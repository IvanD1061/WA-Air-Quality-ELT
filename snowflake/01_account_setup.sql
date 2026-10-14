-- =============================================================================
-- 01_account_setup.sql
-- Run once in a Snowsight worksheet, top to bottom, as ACCOUNTADMIN.
-- Replace the three <PLACEHOLDERS> first. docs/setup.md walks through each one.
--
-- COST: nothing here uses credits by itself. The warehouse is created
-- suspended and only bills while a query is running (60 second minimum each
-- time it wakes up).
-- =============================================================================

use role accountadmin;

-- 1. Compute -----------------------------------------------------------------
-- X-Small is the cheapest size and far more than this data needs.
-- auto_suspend = 60 is the setting that protects your trial credits: the
-- warehouse turns itself off after one idle minute.
create warehouse if not exists ELT_WH
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'Air quality ELT: loads, dbt, dashboard';

-- 2. Cost guardrail ----------------------------------------------------------
-- A hard stop in case something loops. A normal month of this pipeline uses a
-- small fraction of this quota.
create or replace resource monitor ELT_MONITOR
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 100 percent do suspend;

alter warehouse ELT_WH set resource_monitor = ELT_MONITOR;

-- 3. Database and schemas ----------------------------------------------------
-- RAW     : JSON exactly as it arrived (written by the loader)
-- STAGING : cleaned, typed views (written by dbt)
-- MARTS   : the star schema (written by dbt, read by the dashboard)
create database if not exists AIR_QUALITY;
create schema if not exists AIR_QUALITY.RAW;
create schema if not exists AIR_QUALITY.STAGING;
create schema if not exists AIR_QUALITY.MARTS;

-- 4. Roles: least privilege --------------------------------------------------
-- ELT_ROLE can write everywhere in this database. REPORTER can only read the
-- finished marts, so a dashboard bug or leaked dashboard credential cannot
-- touch raw data.
create role if not exists ELT_ROLE;
create role if not exists REPORTER;

-- Custom roles roll up to SYSADMIN so an admin can always manage what they own.
grant role ELT_ROLE to role SYSADMIN;
grant role REPORTER to role SYSADMIN;

grant usage on warehouse ELT_WH to role ELT_ROLE;
grant usage on warehouse ELT_WH to role REPORTER;

grant usage, create schema on database AIR_QUALITY to role ELT_ROLE;
grant all on schema AIR_QUALITY.RAW to role ELT_ROLE;
grant all on schema AIR_QUALITY.STAGING to role ELT_ROLE;
grant all on schema AIR_QUALITY.MARTS to role ELT_ROLE;

grant usage on database AIR_QUALITY to role REPORTER;
grant usage on schema AIR_QUALITY.MARTS to role REPORTER;
-- "future" grants cover tables dbt has not created yet, and tables it
-- re-creates on later runs.
grant select on future tables in schema AIR_QUALITY.MARTS to role REPORTER;
grant select on future views in schema AIR_QUALITY.MARTS to role REPORTER;

-- 5. Service user ------------------------------------------------------------
-- TYPE = SERVICE means "this is a program, not a person": it cannot use a
-- password at all and must sign in with the key pair made by `make keys`.
-- Paste the public key as one line, without the BEGIN/END lines.
create user if not exists ELT_SERVICE
    type = service
    default_role = ELT_ROLE
    default_warehouse = ELT_WH
    default_namespace = AIR_QUALITY.RAW
    rsa_public_key = '<PASTE_PUBLIC_KEY_HERE>'
    comment = 'Runs the air quality pipeline and dashboard';

grant role ELT_ROLE to user ELT_SERVICE;
grant role REPORTER to user ELT_SERVICE;

-- Let yourself use both roles in worksheets too.
set me = current_user();
grant role ELT_ROLE to user identifier($me);
grant role REPORTER to user identifier($me);

-- 6. Access to S3 ------------------------------------------------------------
-- A storage integration lets Snowflake read the bucket by assuming an IAM
-- role, so no AWS access key is ever stored inside Snowflake.
create storage integration if not exists S3_AIR_QUALITY
    type = external_stage
    storage_provider = 'S3'
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-air-quality-read'
    storage_allowed_locations = ('s3://<S3_BUCKET>/raw/openaq/');

grant usage on integration S3_AIR_QUALITY to role ELT_ROLE;

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this output
-- into the IAM role's trust policy (docs/setup.md, step 3d).
desc integration S3_AIR_QUALITY;
