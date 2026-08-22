# TimescaleDB (hypertables for the event-log domains) + PostGIS (geography
# columns for truck/weather locations).
#
# Uses timescale/timescaledb-ha:pg15 (Ubuntu-based), which bundles PostGIS
# already correctly matched to its own Postgres 15 build. A leaner
# timescale/timescaledb:pg15 (Alpine) + `apk add postgis` was tried first
# and confirmed broken: Alpine's `postgis` package always tracks whatever
# Postgres major is currently default in that Alpine release, never
# matching Timescale's pinned older major - CREATE EXTENSION postgis fails
# with "extension not available" regardless of which pg1X tag is used.
#
# -ha also bundles pgvector, Patroni, and other HA tooling we don't use -
# verified this costs nothing at runtime: only a plain `postgres` process
# runs by default (no Patroni process at all unless separately configured),
# measured idle RAM ~117MB (barely more than plain postgres:15-alpine), and
# pgvector shows as available-but-not-installed in pg_available_extensions
# until something actually calls CREATE EXTENSION vector (which nothing
# here does - only timescaledb + postgis are created, in init.sql).
FROM timescale/timescaledb-ha:pg15
