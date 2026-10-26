# One image for every Airflow component (API server, scheduler, DAG processor)
# and for one-off pipeline commands.
#
# Airflow itself is left exactly as the official image ships it. The pipeline
# code and dbt go into a separate virtual environment, because Airflow and dbt
# both pin many of the same libraries to different versions and installing
# them side by side is a well-known way to break one or the other.

FROM apache/airflow:3.3.2-python3.12

USER root
RUN mkdir -p /opt/elt-venv /opt/project \
    && chown -R airflow:0 /opt/elt-venv /opt/project
USER airflow

# Dependencies first: this layer is only rebuilt when requirements.txt
# changes, so editing pipeline code does not reinstall everything.
COPY --chown=airflow:0 requirements.txt /opt/project/requirements.txt
RUN python -m venv /opt/elt-venv \
    && /opt/elt-venv/bin/pip install --no-cache-dir -r /opt/project/requirements.txt

# dbt project, with its packages downloaded at build time so a run never
# depends on the package hub being reachable.
COPY --chown=airflow:0 dbt /opt/project/dbt
RUN cd /opt/project/dbt && /opt/elt-venv/bin/dbt deps

COPY --chown=airflow:0 pipeline /opt/project/pipeline
COPY --chown=airflow:0 scripts /opt/project/scripts

# Airflow containers may run as your own user id with group 0 (see AIRFLOW_UID
# in .env), and dbt writes its target/ and logs/ folders next to the project.
RUN chmod -R g=u /opt/project

ENV ELT_PROJECT_DIR=/opt/project \
    ELT_PYTHON=/opt/elt-venv/bin/python \
    ELT_DBT=/opt/elt-venv/bin/dbt \
    DBT_PROFILES_DIR=/opt/project/dbt
