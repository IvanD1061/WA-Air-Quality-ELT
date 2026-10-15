-- =============================================================================
-- 02_raw_objects.sql
-- Run after 01_account_setup.sql AND after the IAM trust policy is updated.
-- Replace <S3_BUCKET> first.
--
-- COST: the last statement lists files, which wakes the warehouse for a few
-- seconds (billed as 60 seconds of X-Small, well under one cent of credit).
-- =============================================================================

use role ELT_ROLE;
use warehouse ELT_WH;
use schema AIR_QUALITY.RAW;

-- One JSON document per line, gzip-compressed.
create file format if not exists NDJSON_GZ
    type = json
    compression = gzip
    strip_outer_array = false;

-- A stage is a named pointer at the S3 folder the extractor writes to.
create stage if not exists OPENAQ_STAGE
    url = 's3://<S3_BUCKET>/raw/openaq/'
    storage_integration = S3_AIR_QUALITY
    file_format = NDJSON_GZ;

-- Raw tables. Each row is one JSON record, stored whole in a VARIANT column,
-- plus four columns that say where the row came from. Nothing is parsed here:
-- if the API adds, renames or drops a field tomorrow, the load still works and
-- the fix is a one-line change in a dbt model.
create table if not exists LOCATIONS (
    raw_json          variant        not null,
    partition_date    date           not null,  -- snapshot_date from the file path
    source_file       string         not null,
    source_row_number number         not null,
    loaded_at         timestamp_ltz  not null
);

create table if not exists MEASUREMENTS (
    raw_json          variant        not null,
    partition_date    date           not null,  -- measurement_date from the file path
    source_file       string         not null,
    source_row_number number         not null,
    loaded_at         timestamp_ltz  not null
);

-- Smoke test. An empty result is fine before the first extract. An error here
-- means the storage integration or IAM trust policy is not right yet.
list @OPENAQ_STAGE;
