#!/usr/bin/env bash
# Interleaved A/B benchmark for reflexdb-server binaries.
#
# Sequential "all of A, then all of B" measurement lets machine drift bias the
# comparison. This alternates A,B,A,B... across rounds so background load hits
# both sides equally, then feeds the combined history to compare.py.
#
#   benchmarks/ab.sh BIN_A LABEL_A BIN_B LABEL_B OUT.jsonl [ROUNDS]
#
# Env knobs (all optional):
#   AB_PORT      server port both sides take turns on   (default 7410)
#   AB_SECONDS   seconds per run.py sample              (default 3)
#   AB_RUNS      run.py --runs per side per round       (default 2)
#   AB_THRESHOLD compare.py regression threshold %      (default 10)
#   AB_EXTRA     extra args forwarded to run.py         (e.g. "--workloads hot-key")
set -euo pipefail

if [ $# -lt 5 ]; then
    echo "usage: $0 BIN_A LABEL_A BIN_B LABEL_B OUT.jsonl [ROUNDS]" >&2
    exit 2
fi
BIN_A=$1; LABEL_A=$2; BIN_B=$3; LABEL_B=$4; OUT=$5; ROUNDS=${6:-4}
PORT=${AB_PORT:-7410}
SECS=${AB_SECONDS:-3}
RUNS=${AB_RUNS:-2}
THRESH=${AB_THRESHOLD:-10}
DIR=$(cd "$(dirname "$0")" && pwd)

for round in $(seq 1 "$ROUNDS"); do
    for side in A B; do
        if [ "$side" = A ]; then bin=$BIN_A; label=$LABEL_A; else bin=$BIN_B; label=$LABEL_B; fi
        dir=$(mktemp -d)
        "$bin" --dir "$dir" --port "$PORT" >/dev/null 2>&1 &
        pid=$!
        sleep 1.5
        # shellcheck disable=SC2086
        python3 "$DIR/run.py" --port "$PORT" --runs "$RUNS" --seconds "$SECS" \
            --output "$OUT" --label "$label" ${AB_EXTRA:-}
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        rm -rf "$dir"
        echo "round $round $side done" >&2
    done
done

python3 "$DIR/compare.py" "$OUT" --base "$LABEL_A" --head "$LABEL_B" --threshold "$THRESH"
