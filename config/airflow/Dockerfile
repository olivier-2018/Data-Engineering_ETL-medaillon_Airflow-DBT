# Airflow 3, shared by airflow-init/api-server/scheduler/dag-processor.
# LocalExecutor means the scheduler executes task code directly (no
# separate Celery worker), so this image needs the Spark client
# (spark-submit) + dbt installed, not just Airflow itself.
FROM apache/airflow:3.0.3-python3.11

# apache-airflow-providers-standard/postgres/http/common-sql already ship
# pre-installed with this base image (verified: 1.4.1/6.2.1/5.3.2/1.27.3 on
# 3.0.3-python3.11) - an earlier draft of this file explicitly pinned older
# versions of these (e.g. providers-standard==0.3.0), which broke the
# build: apache-airflow-core==3.0.3 requires providers-standard>=0.4.0, so
# pinning 0.3.0 made pip's resolver silently DOWNGRADE apache-airflow
# itself to 2.11.2 to satisfy the (too-old) pin, rather than failing loudly.
# Confirmed by actually running the build and inspecting `pip show
# apache-airflow` inside the container. Fix: only pin packages NOT already
# bundled (apache-spark provider, psycopg2, dbt) - let the base image's own
# already-compatible provider versions stand.
RUN pip install --no-cache-dir \
        apache-airflow-providers-apache-spark==5.0.0 \
        psycopg2-binary==2.9.9 \
        dbt-core==1.8.8 \
        dbt-postgres==1.8.2

USER root

# Client-only Spark distribution (same version as the cluster, 3.5.0) so
# SparkSubmitOperator has a real spark-submit binary to shell out to - a JRE
# is required for spark-submit itself even though no Spark daemon runs here.
ARG SPARK_VERSION=3.5.0
ARG SPARK_DIST=spark-${SPARK_VERSION}-bin-hadoop3
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
    && curl -fsSL -o /tmp/${SPARK_DIST}.tgz \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz" \
    && tar -xzf /tmp/${SPARK_DIST}.tgz -C /opt \
    && mv /opt/${SPARK_DIST} /opt/spark \
    && rm /tmp/${SPARK_DIST}.tgz \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Same Kafka connector jars as config/spark/Dockerfile, checksum-verified
# against Maven Central - without these, every batch SparkSubmitOperator job
# that reads Kafka (bronze_ingestion_dag) fails at runtime with
# "Failed to find data source: kafka" (confirmed by testing): this client-only
# Spark distribution ships with none of the Kafka connector on its classpath,
# unlike the cluster's own image.
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    set -eu; \
    fetch_jar() { \
        group_path="$1"; artifact="$2"; version="$3"; \
        base_url="https://repo1.maven.org/maven2/${group_path}/${artifact}/${version}/${artifact}-${version}.jar"; \
        dest="/opt/spark/jars/${artifact}-${version}.jar"; \
        curl -fsSL -o "$dest" "$base_url"; \
        curl -fsSL -o "${dest}.sha1" "${base_url}.sha1"; \
        echo "$(cat "${dest}.sha1")  $dest" | sha1sum -c -; \
        rm "${dest}.sha1"; \
    }; \
    fetch_jar org/apache/spark spark-sql-kafka-0-10_2.12 3.5.0; \
    fetch_jar org/apache/spark spark-token-provider-kafka-0-10_2.12 3.5.0; \
    fetch_jar org/apache/kafka kafka-clients 3.4.1; \
    fetch_jar org/apache/commons commons-pool2 2.11.1; \
    apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# The Spark cluster (workers, built from config/spark/Dockerfile FROM
# apache/spark:3.5.0, Ubuntu Focal) only has Python 3.8 available. This base
# Airflow image is Python 3.11. PySpark requires the driver and every executor
# to share the same major.minor Python version - client-mode batch jobs here
# run their driver inside this container, so a bare mismatch fails every job
# with PYSPARK's PYTHON_VERSION_MISMATCH (confirmed by testing). `uv` (already
# bundled by the Airflow base image) fetches a checksum-verified prebuilt
# CPython 3.8 and creates an isolated venv for it - this does NOT touch
# Airflow's own Python 3.11 runtime or its installed packages; it's used only
# via PYSPARK_DRIVER_PYTHON below, which affects nothing except how
# spark-submit launches its local driver process. PYSPARK_PYTHON is
# deliberately left unset so executors keep using each worker's own native
# python3 (already 3.8, already correct) - pointing it at this venv's path
# would break executors, since that path doesn't exist on the worker containers.
RUN uv python install 3.8 \
    && uv venv --python 3.8 /home/airflow/.pyspark38-venv \
    && uv pip install --python /home/airflow/.pyspark38-venv/bin/python3.8 \
        psycopg2-binary==2.9.9 pyyaml==6.0.2
ENV PYSPARK_DRIVER_PYTHON=/home/airflow/.pyspark38-venv/bin/python3.8
