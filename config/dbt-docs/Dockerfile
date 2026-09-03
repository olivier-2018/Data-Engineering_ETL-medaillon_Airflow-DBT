# dbt docs generate + serve, one container. No separate dbt-only image
# exists elsewhere in this project (dbt currently only runs inside
# airflow-scheduler, via config/airflow/Dockerfile) - this is deliberately
# its own minimal image rather than reusing that one, since it needs none
# of Airflow's own dependencies, just the dbt CLI + a Postgres connection.
FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-core==1.8.8 dbt-postgres==1.8.2

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Runs as a non-root uid (default 1000, matching a typical single-user host
# dev uid - override with --build-arg DBT_DOCS_UID/GID if yours differs, e.g.
# `id -u`/`id -g`) instead of root, so this container doesn't leave root-owned
# files on the ./dbt bind mount. Not sufficient on its own for cross-container
# writes though: airflow-scheduler's own dbt calls (gold_dbt_dag.py) run as a
# different uid (50000) - entrypoint.sh's and gold_dbt_dag.py's `umask 000`
# are what keep dbt/{logs,dbt_packages,target} writable by both regardless of
# which one touches a given file first.
ARG DBT_DOCS_UID=1000
ARG DBT_DOCS_GID=1000
RUN groupadd -g ${DBT_DOCS_GID} dbtdocs \
    && useradd -u ${DBT_DOCS_UID} -g ${DBT_DOCS_GID} -m -s /usr/sbin/nologin dbtdocs
USER dbtdocs

WORKDIR /opt/dbt
ENTRYPOINT ["/entrypoint.sh"]
