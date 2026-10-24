"""Daily air quality ELT.

    resolve_window -> extract -> load -> dbt_source_freshness -> dbt_build

Airflow's job here is narrow on purpose: decide *when* and *in what order*,
retry what is worth retrying, and shout when something breaks. The work itself
lives in plain command-line programs (`python -m pipeline.extract`, `dbt
build`) that run exactly the same way from a laptop or CI. That keeps the
pipeline testable without Airflow and keeps Airflow's own Python environment
free of dbt and Snowflake dependencies.

To re-run specific days, trigger the DAG with `start_date` and `end_date`.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import date, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task

log = logging.getLogger(__name__)

PROJECT_DIR = os.environ.get("ELT_PROJECT_DIR", "/opt/project")
PYTHON = os.environ.get("ELT_PYTHON", "/opt/elt-venv/bin/python")
DBT = os.environ.get("ELT_DBT", "/opt/elt-venv/bin/dbt")

# Each run re-pulls the last N full UTC days, not just yesterday. Sensors
# report late, so yesterday's file is often incomplete on the first pull.
# Because every step replaces a day rather than appending to it, pulling a day
# twice costs a few API calls and nothing else.
LOOKBACK_DAYS = int(os.environ.get("PIPELINE_LOOKBACK_DAYS", "2"))
RETRIES = int(os.environ.get("PIPELINE_RETRIES", "3"))


def notify_failure(context) -> None:
    """Runs once a task has used up its retries.

    Always logs. Also posts to a chat webhook when ALERT_WEBHOOK_URL is set
    (Slack incoming webhooks work as is; for Discord, add /slack to the URL).
    """
    ti = context["ti"]
    message = (
        ":red_circle: Air quality pipeline failed\n"
        f"Task: {ti.dag_id}.{ti.task_id} (attempt {ti.try_number})\n"
        f"Run: {context.get('run_id')}\n"
        f"Error: {str(context.get('exception'))[:500]}"
    )
    # The exception from a shell command only says "exit code 1"; the real
    # reason is in the task log, so link straight to it when Airflow can.
    log_url = getattr(ti, "log_url", None)
    if log_url:
        message += f"\nLog: {log_url}"
    log.error(message)

    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception:
        # A broken alert channel must never hide the original failure.
        log.exception("Could not deliver the failure alert")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Network calls fail for boring reasons. Wait 5, 10, 20 minutes and try again.
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}

# Dates reach the commands as environment variables rather than being pasted
# into the command text, so nothing typed into the trigger form is ever
# interpreted by the shell.
WINDOW_ENV = {
    "WINDOW_START": "{{ ti.xcom_pull(task_ids='resolve_window')['start'] }}",
    "WINDOW_END": "{{ ti.xcom_pull(task_ids='resolve_window')['end'] }}",
    "SNAPSHOT_DATE": "{{ ti.xcom_pull(task_ids='resolve_window')['snapshot'] }}",
}

with DAG(
    dag_id="air_quality_daily",
    description="OpenAQ -> S3 -> Snowflake -> dbt star schema",
    # 12:00 UTC is early morning in Washington, hours after the UTC day closes.
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Backfills go through `make backfill`, which fetches a whole date range in
    # one request per sensor. Catchup would spend a full run per missed day.
    catchup=False,
    # Two runs loading the same day at once could interleave their
    # delete-then-copy steps.
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="First UTC day to process. Leave empty for the normal daily window.",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Last UTC day to process, inclusive. Leave empty for yesterday.",
        ),
    },
    tags=["elt", "openaq", "snowflake", "dbt"],
    doc_md=__doc__,
) as dag:

    @task(retries=0)
    def resolve_window(**context) -> dict[str, str]:
        """Work out which days this run covers, once, for every later task.

        The window comes from the run's own date, never from the clock. A run
        that failed on Tuesday and is re-run on Friday still processes
        Tuesday's days, which is what makes a re-run safe.
        """
        dag_run = context["dag_run"]
        params = context["params"]
        # A manually triggered run can have no logical date; fall back to the
        # moment it was asked to run.
        anchor = pendulum.instance(dag_run.logical_date or dag_run.run_after).in_timezone("UTC")

        end = (
            date.fromisoformat(params["end_date"])
            if params.get("end_date")
            else anchor.date() - timedelta(days=1)
        )
        start = (
            date.fromisoformat(params["start_date"])
            if params.get("start_date")
            else end - timedelta(days=LOOKBACK_DAYS - 1)
        )
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")

        # The location snapshot is filed under the day it is actually taken.
        # extract and load both read this value, so they always agree.
        window = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "snapshot": pendulum.now("UTC").date().isoformat(),
        }
        log.info("Window for this run: %s", window)
        return window

    extract = BashOperator(
        task_id="extract",
        bash_command=(
            f'"{PYTHON}" -m pipeline.extract '
            '--start "$WINDOW_START" --end "$WINDOW_END" --snapshot-date "$SNAPSHOT_DATE"'
        ),
        env=WINDOW_ENV,
        append_env=True,
        cwd=PROJECT_DIR,
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            f'"{PYTHON}" -m pipeline.load '
            '--start "$WINDOW_START" --end "$WINDOW_END" --snapshot-date "$SNAPSHOT_DATE"'
        ),
        env=WINDOW_ENV,
        append_env=True,
        cwd=PROJECT_DIR,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f'"{DBT}" source freshness --target prod',
        cwd=f"{PROJECT_DIR}/dbt",
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # `build` runs seeds, models and tests in dependency order, so a model
        # whose tests fail stops everything downstream of it from being built
        # on bad data.
        bash_command=f'"{DBT}" build --target prod',
        cwd=f"{PROJECT_DIR}/dbt",
        append_env=True,
        # A failed data test fails the same way on the next attempt. One retry
        # covers a dropped connection; more would only burn warehouse time.
        retries=min(RETRIES, 1),
    )

    resolve_window() >> extract >> load >> dbt_source_freshness >> dbt_build
