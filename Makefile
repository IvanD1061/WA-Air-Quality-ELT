# Shortcuts for everything you do in this project. Run `make` to list them.
# Needs make, Docker and (for the local targets) Python 3.12+.
# On Windows, run these inside WSL2, which Docker Desktop already uses.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Dates default to yesterday (UTC). Override: make backfill START=2026-09-01 END=2026-09-30
END      ?= $(shell date -u -d yesterday +%F 2>/dev/null || date -u -v-1d +%F)
START    ?= $(END)
SNAPSHOT ?= $(shell date -u +%F)

VENV := .venv
PY   := $(VENV)/bin/python
# One-off command inside the pipeline image, with .env loaded. The command is
# single-quoted so $$ELT_PYTHON is filled in inside the container, not here.
RUN  := docker compose run --rm pipeline

.PHONY: help init keys venv lint format test dbt-ci check up down ps logs unpause trigger runs \
        extract load dbt-build dbt-freshness backfill stats

help: ## List the available commands
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-15s %s\n", $$1, $$2}'

# --- First-time setup ---------------------------------------------------------

init: ## Create .env and the local folders (safe to re-run)
	@mkdir -p logs secrets
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		if [ "$$(uname)" = "Linux" ]; then sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env; fi; \
		echo "Created .env. Fill in the blanks before running anything else."; \
	else \
		echo ".env already exists, left untouched."; \
	fi

keys: ## Generate the Snowflake key pair and print the public key to paste
	@mkdir -p secrets
	@if [ -f secrets/snowflake_rsa_key.p8 ]; then \
		echo "secrets/snowflake_rsa_key.p8 already exists, not overwriting."; \
	else \
		openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -nocrypt -out secrets/snowflake_rsa_key.p8; \
		chmod 600 secrets/snowflake_rsa_key.p8; \
		openssl rsa -in secrets/snowflake_rsa_key.p8 -pubout -out secrets/snowflake_rsa_key.pub 2>/dev/null; \
	fi
	@echo "Paste this single line as rsa_public_key in snowflake/01_account_setup.sql:"
	@echo
	@grep -v -- "-----" secrets/snowflake_rsa_key.pub | tr -d '\n'; echo
	@echo

venv: ## Create .venv with the dev tools (tests, lint, emulator)
	python3 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -r requirements-dev.txt
	cd dbt && ../$(VENV)/bin/dbt deps

# --- Checks that need no cloud account ------------------------------------------

lint: ## Lint and format-check the Python code
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

format: ## Auto-format the Python code
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

test: ## Run the Python unit tests
	$(PY) -m pytest

dbt-ci: ## Build and test every dbt model against the local Snowflake emulator
	$(PY) scripts/dbt_emulator.py

check: lint test dbt-ci ## Everything CI runs, in one go

# --- The stack ----------------------------------------------------------------

up: ## Build the images and start Airflow (localhost:8080) and the dashboard (localhost:8501)
	docker compose up -d --build
	@echo "Airflow:   http://localhost:8080  (user and password: airflow)"
	@echo "Dashboard: http://localhost:8501"

down: ## Stop everything (Airflow's run history is kept)
	docker compose down

ps: ## Show container status
	docker compose ps

logs: ## Follow the scheduler logs
	docker compose logs -f airflow-scheduler

unpause: ## Turn the daily schedule on
	docker compose exec airflow-scheduler airflow dags unpause air_quality_daily

trigger: ## Start a run right now
	docker compose exec airflow-scheduler airflow dags trigger air_quality_daily

runs: ## List past runs with their start and end times
	docker compose exec airflow-scheduler airflow dags list-runs air_quality_daily

# --- Pipeline steps by hand (same code Airflow runs) ----------------------------
# COST: load, dbt-build, dbt-freshness, backfill and stats wake the Snowflake
# warehouse. Each is seconds to a few minutes of X-Small compute.

extract: ## Pull START..END from OpenAQ into S3
	$(RUN) '$$ELT_PYTHON -m pipeline.extract --start $(START) --end $(END) --snapshot-date $(SNAPSHOT)'

load: ## Copy START..END from S3 into Snowflake RAW
	$(RUN) '$$ELT_PYTHON -m pipeline.load --start $(START) --end $(END) --snapshot-date $(SNAPSHOT)'

dbt-build: ## Run and test all dbt models against Snowflake
	$(RUN) 'cd dbt && $$ELT_DBT build --target prod'

dbt-freshness: ## Check that RAW was loaded recently
	$(RUN) 'cd dbt && $$ELT_DBT source freshness --target prod'

backfill: extract load dbt-build ## Extract, load and transform START..END in one go

stats: ## Print the numbers for your README and resume
	$(RUN) '$$ELT_PYTHON scripts/pipeline_stats.py'
