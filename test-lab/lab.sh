#!/usr/bin/env bash
# Run a command inside the lab, with fixtures/ mounted at /mnt/in and output at /mnt/out.
#
#   test-lab/lab.sh                                   # a shell
#   LATENCY=5 RATE=340 test-lab/lab.sh \
#       sas7bdat convert /mnt/in/big.sas7bdat --out-dir /mnt/out --io-backend buffered
#   LATENCY=5 RATE=340 test-lab/lab.sh python3 /usr/local/bin/probe.py /mnt/in/big.sas7bdat
#
# IN=<dir> picks a different source. --build rebuilds the image first.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
image=sas7bdat-lab
IN=${IN:-$root/fixtures}
OUT=${OUT:-$root/target/lab-out}

if [ "${1:-}" = "--build" ]; then
    shift
    docker build -f "$root/test-lab/Dockerfile" -t "$image" "$root"
    [ $# -eq 0 ] && exit 0
fi
docker image inspect "$image" >/dev/null 2>&1 \
    || docker build -f "$root/test-lab/Dockerfile" -t "$image" "$root"

mkdir -p "$OUT"
# Interactive only when there is a terminal to be interactive with.
tty=()
[ -t 0 ] && tty=(-it)
# FUSE needs the device and the mount capability; nothing else here is privileged.
exec docker run --rm "${tty[@]}" \
    --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor=unconfined \
    -e "LATENCY=${LATENCY:-0}" -e "RATE=${RATE:-0}" \
    -v "$IN:/srv/in:ro" -v "$OUT:/srv/out" \
    "$image" "${@:-bash}"
