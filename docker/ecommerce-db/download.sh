#!/bin/sh
# ecommerce-data service: make sure /data/ecommerce_pg.dump exists and is not corrupted.
# Downloads it on the first start (from the GitHub Release of this repo); later starts finish in a second.
DUMP=/data/ecommerce_pg.dump
cd /data || exit 1
check() { tr -d '\r' < ecommerce_pg.dump.sha256 | sha256sum -c - >/dev/null 2>&1; }

if [ -s "$DUMP" ] && check; then
    echo ">>> data file OK"
    exit 0
fi
echo ">>> downloading $DATA_URL (about 100 MB) ..."
curl -fL --retry 3 --progress-bar -o "$DUMP.part" "$DATA_URL" && mv "$DUMP.part" "$DUMP" || {
    echo "!!! download failed: download ecommerce_pg.dump yourself from the Releases page, put it in data/, then run docker compose up -d again"
    exit 1
}
check || { echo "!!! checksum mismatch: delete data/ecommerce_pg.dump and run docker compose up -d again"; exit 1; }
echo ">>> data file OK"
