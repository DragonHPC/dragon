#!/usr/bin/env bash
# run_ext_attach_srun_demo.sh — End-to-end demo of dragon_ext_attach()
# interoperability at scale using Slurm srun.
#
# This script:
#   1. Builds ext_attach_client if needed.
#   2. Launches `dragon -a ext_attach_publisher.py --srun` in the background.
#   3. Waits for publisher readiness sentinel in this directory.
#   4. Runs ext_attach_client via `srun --ntasks-per-node=128` across the
#      full current Slurm allocation.
#   5. Waits for publisher to consume all replies and exit.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck disable=SC1091
set +u
cd "$REPO_ROOT"
source devtools/setup
set -u

cd "$SCRIPT_DIR"

echo "[run_ext_attach_srun_demo] Building ext_attach_client..."
make >/tmp/ext_attach_srun_demo_build.log 2>&1
if [ ! -x "$SCRIPT_DIR/ext_attach_client" ]; then
    echo "[run_ext_attach_srun_demo] ERROR: build failed; see /tmp/ext_attach_srun_demo_build.log"
    cat /tmp/ext_attach_srun_demo_build.log
    exit 1
fi

if [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "[run_ext_attach_srun_demo] ERROR: no active Slurm allocation detected (SLURM_JOB_ID not set)."
    echo "[run_ext_attach_srun_demo] Run this script inside an allocation (e.g., salloc/sbatch)."
    exit 1
fi

READY_FILE="${SCRIPT_DIR}/ext_attach_publisher_ready"

rm -f "$READY_FILE"

if [ -z "${DRAGON_ATTACH_TOKEN:-}" ]; then
    DRAGON_ATTACH_TOKEN="ext-attach-srun-demo-$(date +%s)-$$"
fi
export DRAGON_ATTACH_TOKEN

#dhosts --wlm slurm

echo "[run_ext_attach_srun_demo] Launching 'dragon -a ext_attach_publisher.py --srun' in background..."
dragon --wlm=drun -a ext_attach_publisher.py --srun &
SERVER_PID=$!

echo "[run_ext_attach_srun_demo] Waiting for publisher readiness sentinel ${READY_FILE} (up to 120 s)..."
for i in $(seq 1 120); do
    kill -0 "$SERVER_PID" 2>/dev/null || {
        echo "[run_ext_attach_srun_demo] ERROR: ext_attach_publisher.py exited early"
        wait "$SERVER_PID"
        exit 1
    }
    if [ -f "$READY_FILE" ]; then
        break
    fi
    sleep 1
done

if [ ! -f "$READY_FILE" ]; then
    echo "[run_ext_attach_srun_demo] ERROR: ${READY_FILE} never appeared"
    kill "$SERVER_PID" 2>/dev/null
    exit 1
fi

echo "[run_ext_attach_srun_demo] Publisher ready. Launching ext_attach_client with srun..."
SRUN_ARGS=(--ntasks-per-node=128)
#SRUN_ARGS=(--ntasks-per-node=8)
if [ -n "${SLURM_NNODES:-}" ]; then
    SRUN_ARGS+=(--nodes="${SLURM_NNODES}")
    #SRUN_ARGS+=(--nodes=1)
fi

srun "${SRUN_ARGS[@]}" "$SCRIPT_DIR/ext_attach_client" --srun
CLIENT_RC=$?

echo "[run_ext_attach_srun_demo] srun/ext_attach_client exited with code ${CLIENT_RC}."

if [ "$CLIENT_RC" -ne 0 ]; then
    echo "[run_ext_attach_srun_demo] ERROR: srun failed; terminating publisher to avoid hanging queue2.get()."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    exit 1
fi

echo "[run_ext_attach_srun_demo] Waiting for ext_attach_publisher.py to exit..."
wait "$SERVER_PID"
SERVER_RC=$?

echo "[run_ext_attach_srun_demo] ext_attach_publisher.py exited with code ${SERVER_RC}."

if [ "$SERVER_RC" -ne 0 ]; then
    echo "[run_ext_attach_srun_demo] FAILED (client_rc=${CLIENT_RC}, server_rc=${SERVER_RC})"
    exit 1
fi

echo "[run_ext_attach_srun_demo] Done. All srun ranks completed successfully."
exit 0
