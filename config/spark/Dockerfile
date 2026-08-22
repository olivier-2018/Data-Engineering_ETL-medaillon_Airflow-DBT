# Spark with Kafka connector baked in for PySpark jobs (Postgres access is via
# psycopg2, not JDBC - see the pip install below)
FROM apache/spark:3.5.0

USER root

# zip is needed by scripts/submit-spark-jobs.sh to package spark-structured-streaming-jobs/shared_utils
# for distribution to executors via --py-files
RUN apt-get update && apt-get install -y \
    curl \
    zip \
    && rm -rf /var/lib/apt/lists/*

# psycopg2-binary lets ingest jobs issue raw SQL (INSERT ... ON CONFLICT) for
# idempotent writes - Spark's built-in JDBC writer has no upsert mode. Using
# the -binary package (bundles libpq) avoids needing libpq-dev/gcc at build time.
# Pinned (unlike an unpinned install) for the same build reproducibility the
# jar-fetching below already gets from checksum verification.
# pyyaml is here so restock_check.py can read data_generators/config.yaml's
# restock threshold/target directly - the same single source of truth the
# generator uses, rather than duplicating those two numbers into .env too.
RUN pip3 install psycopg2-binary==2.9.9 pyyaml==6.0.2

# Bake in the Kafka connector (structured streaming) so spark-submit never needs
# network access to Maven Central at job-submit time.
# Each jar is verified against its official Maven Central sha1 checksum, failing the
# build on any mismatch.
RUN set -eu; \
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
    chown spark:spark /opt/spark/jars/*.jar

# curl was only needed to fetch the jars above - drop it so it doesn't linger
# in the runtime image as unnecessary attack surface. zip stays (needed at
# runtime by scripts/submit-spark-jobs.sh to package shared_utils for --py-files).
RUN apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

USER spark

WORKDIR /opt/spark
