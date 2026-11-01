# Setup guide

Start to finish, in the order that wastes the least of your Snowflake trial.
Every step says up front whether it can cost money.

**The short version of the cost picture**

| Service | What you use | Cost |
|---|---|---|
| OpenAQ | API key | Free. 60 requests a minute, 2,000 an hour. |
| AWS S3 | A few MB of JSON | Covered by AWS free credits on a new account. Check your own account's plan before you start. |
| Snowflake | 30-day trial | Free trial credits, no card needed. **The 30 days start when you sign up**, so do steps 1 to 3 first. After the trial it needs a card to keep running. |
| Docker Desktop, dbt Core, Airflow, Streamlit | Local | Free for personal use. |
| GitHub Actions | CI | Free on public repositories. |

Nothing in this project turns on a paid feature by itself. The one thing that
spends Snowflake credits is the warehouse running, and it suspends itself
after 60 idle seconds.

---

## 1. Local setup (free, no accounts)

You need Git, Docker Desktop, `make`, and Python 3.12 or newer. On Windows,
do everything inside WSL2. Give Docker at least 4 GB of memory (Docker
Desktop, Settings, Resources); Airflow will not start reliably with less.

```bash
git clone <your repo url> && cd wa-air-quality-elt
make init      # creates .env from .env.example
make venv      # installs the dev tools into .venv
make check     # lint, unit tests, and a full dbt build on a local emulator
```

If `make check` passes, your machine is ready and you have seen every dbt
model build and every test run without spending anything.

> **Common mistake:** committing `.env`. It is already in `.gitignore`. Run
> `git status` before your first commit and make sure it is not listed.

## 2. OpenAQ key and a dry run (free)

1. Register at <https://explore.openaq.org/register> and copy your API key.
2. Put it in `.env` as `OPENAQ_API_KEY=...`.
3. Pull one day to a local folder instead of S3:

```bash
RAW_LOCAL_DIR=/tmp/openaq_raw .venv/bin/python -m pipeline.extract --end 2026-10-06
```

(Load `.env` into your shell first, for example `set -a; source .env; set +a`.)

The last log line starts with `EXTRACT_SUMMARY` and tells you how many
locations and sensors were found, how many requests were made, and how long
it took. Open one of the files and read a few records:

```bash
find /tmp/openaq_raw -name '*.gz' | head
gunzip -c /tmp/openaq_raw/raw/openaq/measurements/*/measurements.jsonl.gz | head -3
```

This is the moment to get to know your data. Look for null values, negative
values, stations with no name, and how many sensors answered.

> **If the run is slow:** one request per sensor at 60 a minute means a few
> hundred sensors take several minutes. To go faster while you learn, set
> `OPENAQ_MONITORS_ONLY=true` or `OPENAQ_MAX_SENSORS=50` in `.env`.

> **Common mistake:** pulling the whole country "to have more data". More rows
> do not make the project better and will hit the hourly rate limit.

## 3. AWS (uses free credits; a few cents of storage at most)

Pick one region and use it for both S3 and Snowflake, for example
`us-west-2` (Oregon). Data moving between regions is slower and can be billed.

**3a. Create the bucket.** S3 console, Create bucket, a globally unique name,
your region, and leave "Block all public access" on. Put the name in `.env`
as `S3_BUCKET`.

**3b. Create a user for the extractor.** IAM, Users, Create user (no console
access). Attach this inline policy, then create an access key for it and put
the two values in `.env` as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/raw/openaq/*"
    }
  ]
}
```

This user can write files under one prefix and do nothing else. That is the
principle of least privilege, and it is why a leaked key here would be an
annoyance rather than a disaster.

**3c. Create a role for Snowflake.** IAM, Roles, Create role, "AWS account"
as the trusted entity (choose "This account" for now; you replace it in step
4d). Name it `snowflake-air-quality-read` and attach this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/raw/openaq/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET",
      "Condition": {"StringLike": {"s3:prefix": ["raw/openaq/*"]}}
    }
  ]
}
```

Note the role's ARN and your 12-digit AWS account id.

**3d. Test the upload.** Remove `RAW_LOCAL_DIR` from your shell and run the
extract again for one day. The files should now appear in the bucket.

> **Common mistake:** using your root account's access key, or giving the user
> `AmazonS3FullAccess`. Both work and both are the wrong answer in an interview.

## 4. Snowflake (starts the 30-day trial clock)

**4a. Sign up** at <https://signup.snowflake.com>. Choose AWS and the same
region as your bucket. Standard edition is enough.

**4b. Make the key pair.** Snowflake no longer lets programs sign in with a
password, so the pipeline uses an RSA key pair.

```bash
make keys
```

This writes the private key to `secrets/` (git-ignored) and prints the public
key as one line.

**4c. Run the account setup.** Open `snowflake/01_account_setup.sql`, replace
`<PASTE_PUBLIC_KEY_HERE>`, `<AWS_ACCOUNT_ID>` and `<S3_BUCKET>`, paste it into
a Snowsight worksheet and run all of it. The last statement,
`desc integration`, prints two values you need next:
`STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.

**4d. Let Snowflake assume the role.** Back in AWS, open the
`snowflake-air-quality-read` role, Trust relationships, Edit, and replace the
policy with:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "STORAGE_AWS_IAM_USER_ARN from step 4c"},
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {"sts:ExternalId": "STORAGE_AWS_EXTERNAL_ID from step 4c"}
      }
    }
  ]
}
```

**4e. Create the stage and raw tables.** Replace `<S3_BUCKET>` in
`snowflake/02_raw_objects.sql` and run it. The final `list @OPENAQ_STAGE`
should show the files from step 3d. An error there means the trust policy or
the bucket name is off.

**4f. Fill in `.env`.** `SNOWFLAKE_ACCOUNT` is your account identifier in the
form `orgname-accountname` (Snowsight, account menu at the bottom left, "View
account details").

> **Common mistakes:** leaving a warehouse without auto-suspend (the setup
> script sets 60 seconds), doing everything as `ACCOUNTADMIN` (the pipeline
> uses `ELT_ROLE`, the dashboard uses `REPORTER`), and re-creating the storage
> integration after editing the trust policy, which changes the external id
> and breaks the trust again.

## 5. First load (seconds to minutes of X-Small warehouse time)

```bash
make up                                          # builds the images, starts Airflow and the dashboard
make backfill START=2026-09-08 END=2026-10-06    # extract, load, dbt build
make stats
```

The first `make up` downloads the Airflow base image (over 1 GB) and takes
several minutes. Later builds reuse it and take seconds.

A backfill makes one request per sensor for the whole range, so a month costs
about the same number of API calls as a day.

Check the result in a worksheet:

```sql
select count(*) from AIR_QUALITY.RAW.MEASUREMENTS;
select quality_status, count(*) from AIR_QUALITY.MARTS.FCT_MEASUREMENTS_HOURLY group by 1;
select * from AIR_QUALITY.MARTS.FCT_AIR_QUALITY_DAILY limit 20;
```

> **This is the first time the code meets real Snowflake and real data.** If a
> dbt test fails, read which one. A failing `not_null` or range test on real
> data is the pipeline doing its job, and deciding what to do about it (fix the
> model, loosen the test to a warning, or widen a limit in
> `dbt/seeds/parameter_limits.csv`) is exactly the work worth describing in an
> interview.

## 6. Turn on the schedule

Open <http://localhost:8080> (user `airflow`, password `airflow`), then:

```bash
make unpause    # the DAG now runs every day at 12:00 UTC
make trigger    # or start a run now
```

Airflow only runs while your containers are up. For a week of scheduled runs,
leave Docker running or start it once a day; a missed day is filled by the
next run's two-day window or by `make backfill`.

For failure alerts, create a Slack incoming webhook (free) and set
`ALERT_WEBHOOK_URL` in `.env`, then `make up` again. To see one fire, break
something on purpose: put a wrong value in `SNOWFLAKE_USER` and trigger a run.

## 7. Dashboard

<http://localhost:8501>. It reads `MARTS` with the `REPORTER` role and caches
results for 15 minutes, so leaving the tab open does not keep the warehouse
awake.

## 8. CI

Push to a public GitHub repository. The workflow in
`.github/workflows/ci.yml` runs without any secrets. Add the status badge to
the README once the first run is green.

## 9. Before the trial ends

Around day 25, while everything still runs:

1. Save the output of `make stats`.
2. Screenshot the Airflow grid with a week of green runs, the DAG graph, and
   the dashboard. Put them in `docs/images/` and link them from the README.
3. Record a one-minute screen capture of the dashboard if you want a demo link.

Then either add a payment method (a daily run on X-Small is a small fraction
of a credit per day, but check current pricing first) or run
`snowflake/99_teardown.sql`. The raw files stay in S3, and the CI checks keep
passing without Snowflake.

To remove the AWS side: empty and delete the bucket, delete the IAM user's
access key, then the user and the role.
