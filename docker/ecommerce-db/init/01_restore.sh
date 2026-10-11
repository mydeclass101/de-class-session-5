#!/bin/bash
# Runs once, on the first start (empty volume): restore `ecommerce` and create the class user.
# Sourced by the postgres entrypoint, so avoid `set -e` / top-level `exit 0`.

restore_ecommerce() {
    echo ">>> restoring ecommerce from /data/ecommerce_pg.dump (takes a few minutes, please wait) ..."
    pg_restore -U "$POSTGRES_USER" -d ecommerce --no-owner --no-acl --exit-on-error -j 2 /data/ecommerce_pg.dump || return 1
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ecommerce <<'SQL' || return 1
-- student: read the shop data (like a real source system) + do anything inside schema lab
CREATE ROLE student LOGIN PASSWORD 'student123';
GRANT CONNECT ON DATABASE ecommerce TO student;
GRANT USAGE ON SCHEMA public TO student;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO student;
CREATE SCHEMA lab AUTHORIZATION student;
ALTER ROLE student SET search_path = public, lab;
ANALYZE;
SQL
    echo ">>> ecommerce restored"
}

if ! restore_ecommerce; then
    # postgres will not re-run init scripts on this volume: keep the container unhealthy (see healthcheck)
    touch /var/lib/postgresql/data/COURSE_DATA_MISSING
    echo "!!! Could not load the course data. Fix data/ecommerce_pg.dump, then run:"
    echo "!!!     docker compose down -v"
    echo "!!!     docker compose up -d"
fi
