# One-shot Kafka topic-provisioning image (kafka-init service). Separate,
# tiny image rather than reusing data_generators' own build: this only ever
# runs create_topics.py once per stack startup and exits, so it needs none
# of the generator's Postgres/business-logic dependencies - just
# confluent-kafka's AdminClient and PyYAML.
#
# python:3.12-slim (Debian/glibc), not -alpine: confluent-kafka only ships
# prebuilt manylinux (glibc) wheels, not musllinux - confirmed by testing,
# Alpine fails needing a full gcc/librdkafka-dev build toolchain. Same
# reasoning already documented in data_generators/Dockerfile.
FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka==2.15.0 pyyaml==6.0.2

WORKDIR /app
COPY create_topics.py topics_config.yml ./

ENTRYPOINT ["python3", "create_topics.py"]
