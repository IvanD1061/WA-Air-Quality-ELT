# Design decisions

Why each piece is the way it is, what it was chosen over, and the mistake it
avoids. Read this before an interview: every heading is a question someone can
ask, and every answer points at code you can open.

## The shape of the pipeline

### Why ELT instead of ETL?

**ETL** transforms data before loading it. **ELT** loads it raw and transforms
it inside the warehouse. This project does ELT for three reasons.

1. **Raw data is kept.** The cleaning rules here (what counts as a bad
   reading, how AQI is computed) are judgment calls that will change. With raw
   JSON in S3 and in `RAW`, a rule change is a SQL edit and a re-run. With ETL,
   anything the transform threw away is gone.
2. **The warehouse is the right place to compute.** Snowflake scales compute
   separately from storage, so there is no need for a second processing engine
   just to reshape data.
3. **Transformations become SQL in version control**, with tests, lineage and
   documentation, instead of logic buried in a Python script.

ETL is still the better choice when data must not land raw (personal data
that has to be masked first) or when the destination cannot do the computing.

> **Beginner mistake:** "cleaning up a little" in the extract script. The
> moment `extract.py` drops a null or renames a field, the raw zone is no
> longer raw. See `tests/test_extract.py::test_bad_values_are_landed_as_is_not_cleaned`.

### Why land in S3 first instead of loading straight into Snowflake?

- **Decoupling.** If Snowflake is down or the trial ends, extraction still
  works and nothing is lost. The warehouse can be rebuilt from the bucket.
- **Replay.** Any day can be re-loaded without calling the API again, which
  matters because the API is rate-limited and old data can change or vanish.
- **Openness.** Files in S3 can be read by Athena, Spark or DuckDB. Data that
  only exists inside one warehouse is locked to that vendor.

### Why gzipped NDJSON and not Parquet?

NDJSON is one JSON document per line. It keeps the API's nested structure
exactly as received, needs no schema decided in advance, and Snowflake loads
each line into a `VARIANT` natively. Gzip shrinks it several-fold.

Parquet is columnar, typed and much faster to query, and it would be the right
choice for a *processed* layer in the lake or for files queried in place.
Writing Parquet at the raw layer would force type and schema decisions into
the extract step, which is the opposite of landing data untouched.

Short version: **JSON for the raw zone because fidelity matters most there;
Parquet for analytics layers because scan speed matters most there.**

### Why wrap each record in an envelope?

The hourly endpoint returns measurements without the sensor id (it is only in
the URL). Each record is therefore stored as
`{sensor_id, location_id, extracted_at, request_path, data: <untouched record>}`.
The payload stays byte-for-byte what the API sent, and the lineage needed to
use it travels with it.

### Why Hive-style paths (`measurement_date=2026-10-06/`)?

The path is deterministic for a given day, so a re-run overwrites instead of
adding a second copy. The `name=value` convention is understood by Athena,
Spark, DuckDB and Snowflake external tables as a partition column.

## Reliability

### What makes the pipeline idempotent?

Idempotent means running a step twice leaves the same result as running it
once. Three layers each guarantee it:

1. **Extract** writes to the same S3 key for the same day, and only uploads
   after every sensor has been fetched.
2. **Load** deletes a day's rows and copies that day's file inside one
   transaction (`pipeline/load.py`). If the copy fails, the delete is rolled
   back. If there is no file, the delete is rolled back too, so a missing file
   never wipes good data.
3. **dbt** de-duplicates on `(sensor_id, measured_at_utc)` in staging and
   merges into the fact table on a hash of that same key.

CI proves it: `scripts/dbt_emulator.py` re-loads a day with one changed value
and asserts the row count is unchanged and the new value replaced the old.

> **Beginner mistake:** a load that appends. It looks fine until the first
> retry, and then every number in the dashboard is doubled.

### Why `force = true` on COPY?

Snowflake remembers which file names it has loaded and skips them. That is
helpful for append-only files and wrong here, where the same file name is
deliberately rewritten with fresher data. `force = true` turns the skip off,
and the delete-then-copy transaction is what prevents duplicates instead.

### How is late-arriving data handled?

Sensors report late, so yesterday's data is often incomplete on the first
pull. Each daily run re-extracts the last two UTC days
(`PIPELINE_LOOKBACK_DAYS`). Because every layer replaces rather than appends,
re-pulling a day costs a few API calls and nothing else. Data later than that
window needs a manual `make backfill`, which is a documented limitation.

### Why does the DAG take its dates from the run, not from the clock?

`resolve_window` computes the window from the run's logical date. A run that
failed on Tuesday and is retried on Friday still processes Tuesday's days. If
the code called `datetime.now()`, a retry would silently process different
data than the original attempt.

### What happens when the API misbehaves?

`pipeline/openaq_client.py`:

- throttles itself under the published limits before the server has to refuse;
- on `429`, waits as long as the `x-ratelimit-reset` header says;
- on `5xx` and timeouts, retries with exponential backoff and jitter;
- on `401`, `403`, `404` and `422`, fails immediately, because retrying a
  request that is wrong only burns the rate limit.

A few sensors failing is tolerated and reported. More than 5% failing aborts
the run **before** anything is uploaded, so S3 never holds a half-complete day.

## The warehouse

### Why load JSON into a VARIANT column instead of real columns?

Schema-on-read. If the API adds, renames or removes a field tomorrow, the
load still succeeds and the fix is one line in a staging model. With typed raw
tables, a new field breaks the load at 5 a.m. and data is lost until someone
fixes it.

### Why a star schema?

A star schema is one fact table of measurements surrounded by dimension
tables of descriptive context.

- **Simple queries.** "Average PM2.5 by station type by month" is one fact
  table and two joins, always in the same shape. BI tools are built for it.
- **Clear grain.** `fct_measurements_hourly` is one row per sensor per hour.
  Stating the grain is what prevents double counting.
- **Fast.** Facts are narrow and numeric; filtering happens on small dimensions.

Versus **one big table**: simpler to query but every attribute change rewrites
history, and nothing stops a join from fanning out. Versus **third normal
form**: no redundancy, but analysts need many joins for every question.

### Why is `sensor_id` on the fact and not a dimension?

It is a *degenerate dimension*. In OpenAQ a sensor is exactly one instrument
measuring one pollutant at one location, so its only attributes are a location
and a parameter, and both already have dimensions. A `dim_sensor` would just
point at those two tables, turning the star into a snowflake for no gain.

### Why surrogate keys when OpenAQ already has ids?

`location_key` is a hash of `location_id`. Surrogate keys keep the warehouse
independent of the source's numbering (ids can be reused or a second source
added), and they are the hook for tracking history later: with SCD type 2, one
`location_id` has several rows and only a surrogate key can tell them apart.
Hashes rather than sequences because a hash is the same on every run and in
every environment, so builds are repeatable and need no lookups.

### Why keep bad readings instead of deleting them?

A reading of `-999` is still a true fact about what the sensor sent, and "how
often does this station send junk" is a real question. Every row gets a
`quality_status`; downstream models average only `is_valid` rows. Small
negatives are kept as valid because monitors legitimately report slightly
below zero from instrument noise; the limits live in
`dbt/seeds/parameter_limits.csv`, not in code.

### Why is the fact table incremental?

It grows forever while each day only touches a sliver of it. The model
processes rows loaded since the previous run and merges them on the key.
Honest caveat: at a few thousand rows a day a full rebuild would be fine. It
is incremental because the pattern matters at real volume and because
re-loaded days need to update in place. `--full-refresh` rebuilds from scratch.

### Why are staging models views and marts tables?

Staging views cost no storage and always reflect the latest raw rows. Marts
are tables because the dashboard queries them repeatedly, and computing once
per run is cheaper than once per chart.

### Why is the date key based on local time?

"Tuesday's air quality" means Tuesday where the station is. Files are
partitioned by UTC day because that is unambiguous for ingestion; analysis
uses the station's local calendar. One consequence, handled in the daily
model and its tests: a local day has 25 hours when clocks go back.

## Testing

### What do the dbt tests protect?

- **Structure** (fail the run): unique and not-null keys, every fact row
  matches a row in each dimension.
- **Logic** (fail the run): unit tests for the quality rules and the AQI
  calculation, plus a reconciliation between the hourly and daily tables.
- **Observations about messy real data** (warn only): coordinate ranges,
  coverage percentages, data recency. A warning is visible without blocking
  the day's load over one odd station.

`dbt build` runs each model's tests right after building it, so a failing
model stops everything downstream from being built on bad data.

### Why does CI use an emulator instead of Snowflake?

CI must run on every push, on forks, and after the trial account is gone,
without secrets. `fakesnow` speaks Snowflake's protocol and translates the SQL
to DuckDB locally. The trade-off is stated plainly in the README: it proves
the SQL runs and the tests pass on known data, not that Snowflake agrees on
every edge case. Two small workarounds and one strategy override (delete+insert
instead of merge) are documented at the top of `scripts/dbt_emulator.py`.

## Orchestration and operations

### Why Airflow, and why does the DAG only run shell commands?

Airflow provides scheduling, ordering, retries, run history and alerting. The
DAG deliberately contains no business logic: each task runs a command
(`python -m pipeline.extract`, `dbt build`) that works identically from a
laptop or CI. That keeps the pipeline testable without Airflow.

The commands run in a **separate virtual environment** inside the image,
because Airflow and dbt pin many of the same libraries to different versions.

> **Beginner mistake:** `pip install dbt-snowflake` into Airflow's own
> environment. It often works until the next rebuild.

### Why does `dbt_build` retry once when the other tasks retry three times?

Network calls fail for transient reasons and deserve retries with backoff. A
failed data test fails the same way every time, so retrying only spends
warehouse credits.

### Why LocalExecutor?

One machine, one DAG, five tasks. Celery or Kubernetes executors add a broker
and workers that solve a scaling problem this project does not have.

### Why key-pair authentication and a storage integration?

Snowflake blocks password sign-in for service users, so programs authenticate
with an RSA key pair. The storage integration lets Snowflake read S3 by
assuming an IAM role, so no AWS key is stored in Snowflake at all.

### Why two Snowflake roles?

`ELT_ROLE` can write. `REPORTER` can only read `MARTS`. A bug or leaked
credential in the dashboard cannot touch raw data. (In production the
dashboard would also get its own user and key.)

### How is cost controlled?

X-Small warehouse, 60-second auto-suspend, a resource monitor that suspends
the warehouse at a monthly credit quota, and a dashboard that caches results
so idle browser tabs run no queries.

### Why Streamlit and not Power BI?

Power BI Desktop runs only on Windows and cannot live in the Docker setup, so
a reviewer could not start it with one command. Streamlit is a container like
everything else. The marts are a plain star schema, so connecting Power BI to
them later is straightforward.

## What would change at 1,000 times the volume

- Extract in parallel within the rate limit, or switch to OpenAQ's bulk
  archive on S3 instead of the API.
- A Parquet "processed" layer in the lake, and Snowpipe or external tables
  instead of a COPY issued by the orchestrator.
- Incremental staging models and a clustering key on the fact table.
- dbt snapshots for dimension history.
- Airflow on a managed service with a remote executor, and dbt run through a
  package such as Cosmos so each model is its own retryable task.
