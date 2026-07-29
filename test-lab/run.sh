#!/usr/bin/env bash
# Mounts /srv/in through slowfs at /mnt/in, then runs whatever it was given.
# LATENCY (ms, per seek) and RATE (MB/s, shared) shape it; 0 means unlimited.
set -euo pipefail

LATENCY=${LATENCY:-0}
RATE=${RATE:-0}

mkdir -p /mnt/in /srv/out
python3 /usr/local/bin/slowfs.py /srv/in /mnt/in "$LATENCY" "$RATE" &
FS=$!
trap 'fusermount -u /mnt/in 2>/dev/null || true; kill $FS 2>/dev/null || true' EXIT

for _ in $(seq 50); do
    mountpoint -q /mnt/in && break
    sleep 0.1
done
mountpoint -q /mnt/in || { echo "slowfs failed to mount" >&2; exit 1; }

echo "lab: /mnt/in at ${LATENCY}ms per seek, ${RATE} MB/s shared (0 = unlimited)" >&2
exec "$@"
