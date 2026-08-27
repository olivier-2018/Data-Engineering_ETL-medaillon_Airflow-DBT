# dbt docs generate + serve, one container. No separate dbt-only image
# exists elsewhere in this project (dbt currently only runs inside
# airflow-scheduler, via config/airflow/Dockerfile) - this is deliberately
# its own minimal image rather than reusing that one, since it needs none
# of Airflow's own dependencies, just the dbt CLI + a Postgres connection.
FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-core==1.8.8 dbt-postgres==1.8.2

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /opt/dbt
ENTRYPOINT ["/entrypoint.sh"]
