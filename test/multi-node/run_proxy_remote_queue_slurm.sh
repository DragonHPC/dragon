#!/usr/bin/env bash
set -euo pipefail

client_single_node=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --client-single-node)
            client_single_node=1
            shift
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--client-single-node]" >&2
            exit 2
            ;;
    esac
done

CALLER_CWD="$(pwd -P)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export DRAGON_LA_LOG_DIR="$CALLER_CWD"

# The harness runs two independent Dragon runtimes against one another. The
# server runtime owns the channels and processes created on the remote side;
# the client runtime publishes the proxy operations and verifies the replies.
# Keeping the runtimes independent is important: it exercises the channels-mesh
# contract (runtime UID plus host ID) instead of only testing ordinary
# same-runtime off-node channels.
#
# There are two client layouts on purpose. --client-single-node keeps the
# client-side launcher out of the test so failures in proxy routing are easier
# to distinguish from a second multi-node startup problem. The default layout
# splits the allocation into disjoint server and client node sets and validates
# the full multi-node-to-multi-node path. Separate transport and overlay ports
# prevent the two runtimes from accidentally sharing endpoint state.

PYTHON_BIN="${PYTHON_BIN:-python}"

mapfile -t ALLOCATION_HOSTS < <(scontrol show hostnames "$SLURM_NODELIST")

ALLOCATION_HOSTS_CSV="$(IFS=,; echo "${ALLOCATION_HOSTS[*]}")"
WORK_DIR="${WORK_DIR:-$REPO_ROOT/test/multi-node/.proxy_remote_queue}"
RUNTIME_NAME="${RUNTIME_NAME:-proxy-remote-queue-runtime}"
PUBLISH_DIR="$WORK_DIR"
EXIT_PATH="$WORK_DIR/exit_remote_runtime"
SERVER_LOG="$CALLER_CWD/remote_runtime.log"
CLIENT_LOG="$CALLER_CWD/local_client.log"
CLIENT_TIMEOUT_SEC="${CLIENT_TIMEOUT_SEC:-300}"
SERVER_WAIT_SEC="${SERVER_WAIT_SEC:-120}"
SERVER_TRANSPORT_PORT="${SERVER_TRANSPORT_PORT:-7575}"
SERVER_OVERLAY_PORT="${SERVER_OVERLAY_PORT:-6565}"
SERVER_FRONTEND_PORT="${SERVER_FRONTEND_PORT:-6566}"
CLIENT_TRANSPORT_PORT="${CLIENT_TRANSPORT_PORT:-7675}"
CLIENT_OVERLAY_PORT="${CLIENT_OVERLAY_PORT:-6665}"
CLIENT_FRONTEND_PORT="${CLIENT_FRONTEND_PORT:-6666}"
SERVER_NET_CONF="$WORK_DIR/server-network-config.json"
CLIENT_NET_CONF="$WORK_DIR/client-network-config.json"

# Which client driver to run against the published remote runtime. Defaults to
# the object suite; the relay matrix target overrides this via the environment.
SERVER_SCRIPT="$REPO_ROOT/test/multi-node/proxy_remote_queue_server.py"
CLIENT_SCRIPT="${CLIENT_SCRIPT:-$REPO_ROOT/test/multi-node/proxy_remote_queue_client.py}"
if [[ "$CLIENT_SCRIPT" != /* ]]; then
    CLIENT_SCRIPT="$REPO_ROOT/$CLIENT_SCRIPT"
fi

# Success marker the client must print on the happy path. Defaults to the object
# suite's marker; the relay matrix target overrides this via the environment.
CLIENT_SUCCESS_MARKER="${CLIENT_SUCCESS_MARKER:-PROXY_REMOTE_QUEUE_TEST_OK}"

SERVER_HOSTS=("${ALLOCATION_HOSTS[@]}")
CLIENT_HOSTS=()

if (( client_single_node )); then
    if (( ${#ALLOCATION_HOSTS[@]} < 2 )); then
        echo "The proxy remote queue single-node client mode requires at least 2 allocated hosts" >&2
        exit 1
    fi
else
    if (( ${#ALLOCATION_HOSTS[@]} < 4 )); then
        echo "The proxy remote queue two-multinode mode requires at least 4 allocated hosts" >&2
        exit 1
    fi

    server_host_count=$(( ${#ALLOCATION_HOSTS[@]} / 2 ))
    client_host_count=$(( ${#ALLOCATION_HOSTS[@]} - server_host_count ))

    if (( server_host_count < 2 || client_host_count < 2 )); then
        echo "Unable to split allocation into two disjoint multi-node runtimes: hosts=$ALLOCATION_HOSTS_CSV" >&2
        exit 1
    fi

    SERVER_HOSTS=("${ALLOCATION_HOSTS[@]:0:server_host_count}")
    CLIENT_HOSTS=("${ALLOCATION_HOSTS[@]:server_host_count:client_host_count}")
fi

SERVER_HOSTS_CSV="$(IFS=,; echo "${SERVER_HOSTS[*]}")"
CLIENT_HOSTS_CSV="$(IFS=,; echo "${CLIENT_HOSTS[*]}")"

write_subset_network_config() {
    local output_path=$1
    local overlay_port=$2
    local hosts_csv=$3

    # A Slurm allocation describes all nodes together, but each runtime needs a
    # self-consistent view in which exactly one of its nodes is primary. Build a
    # subset config instead of letting the two runtimes see one another's
    # launcher nodes.
    "$PYTHON_BIN" - "$output_path" "$overlay_port" "$hosts_csv" <<'PY'
import json
import sys

from dragon.launcher.network_config import NetworkConfig
from dragon.launcher.wlm import WLM

output_path, overlay_port, hosts_csv = sys.argv[1:]
hosts = [host for host in hosts_csv.split(",") if host]

if not hosts:
    raise RuntimeError("expected at least one host when writing subset network config")

network_config = NetworkConfig.from_wlm(workload_manager=WLM.SLURM, port=int(overlay_port))
full_config = network_config.get_network_config()
nodes_by_host = {}

for node in full_config.values():
    host_name = node.host_name or node.name
    nodes_by_host[host_name] = node

subset = {}
for idx, host in enumerate(hosts):
    if host not in nodes_by_host:
        raise RuntimeError(f"host {host!r} not found in Slurm network config")

    sdict = nodes_by_host[host].get_sdict()
    sdict["is_primary"] = idx == 0
    subset[str(idx)] = sdict

with open(output_path, "w", encoding="utf-8") as out_file:
    json.dump(subset, out_file, ensure_ascii=True, indent=2)
PY
}

mkdir -p "$WORK_DIR"
# The descriptor and exit file are shared control-plane sentinels. Remove both
# before launch so a prior interrupted allocation cannot make a new runtime
# appear ready or already shut down.
rm -f "$EXIT_PATH" "$PUBLISH_DIR/$RUNTIME_NAME" "$SERVER_LOG" "$CLIENT_LOG" "$SERVER_NET_CONF" "$CLIENT_NET_CONF"

echo "Slurm allocation job=$SLURM_JOB_ID nodes=${#ALLOCATION_HOSTS[@]} hosts=$ALLOCATION_HOSTS_CSV"

if (( ! client_single_node )); then
    echo "Generating subset network configs for server hosts=$SERVER_HOSTS_CSV and client hosts=$CLIENT_HOSTS_CSV"
    write_subset_network_config "$SERVER_NET_CONF" "$SERVER_OVERLAY_PORT" "$SERVER_HOSTS_CSV"
    write_subset_network_config "$CLIENT_NET_CONF" "$CLIENT_OVERLAY_PORT" "$CLIENT_HOSTS_CSV"
fi

if (( client_single_node )); then
    # The server remains a multi-node runtime even in this mode; only the
    # client is forced to single-node. That isolates proxy/mesh behavior from
    # client-side HSTA startup while still giving the remote side real nodes.
    echo "Starting remote runtime across all allocated nodes"
    dragon -o hsta \
        -p "$SERVER_TRANSPORT_PORT" \
        --overlay-port "$SERVER_OVERLAY_PORT" \
        --frontend-port "$SERVER_FRONTEND_PORT" \
        "$SERVER_SCRIPT" \
        --runtime-name "$RUNTIME_NAME" \
        --publish-dir "$PUBLISH_DIR" \
        --exit-path "$EXIT_PATH" \
        >"$SERVER_LOG" 2>&1 &
else
    # In the full case, both runtimes get their own node subset and their own
    # network configuration. This tests direct cross-runtime routing and
    # prevents a local gateway or overlay connection from masking a mesh bug.
    echo "Starting server multi-node runtime on hosts=$SERVER_HOSTS_CSV"
    dragon -m -N "${#SERVER_HOSTS[@]}" -o hsta \
        -p "$SERVER_TRANSPORT_PORT" \
        --overlay-port "$SERVER_OVERLAY_PORT" \
        --frontend-port "$SERVER_FRONTEND_PORT" \
        --network-config "$SERVER_NET_CONF" \
        "$SERVER_SCRIPT" \
        --runtime-name "$RUNTIME_NAME" \
        --publish-dir "$PUBLISH_DIR" \
        --exit-path "$EXIT_PATH" \
        >"$SERVER_LOG" 2>/dev/null &
fi
SERVER_PID=$!

stop_child() {
    local child_pid=$1

    if ps -p "$child_pid" >/dev/null 2>&1; then
        kill "$child_pid" >/dev/null 2>&1 || true
        # A launcher can remain alive after its Dragon child stops responding.
        # Give it a short grace period, then force-kill it so a failed test
        # cannot leave the Slurm allocation waiting on an unbounded shell wait.
        for _ in $(seq 1 10); do
            if ! ps -p "$child_pid" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if ps -p "$child_pid" >/dev/null 2>&1; then
            kill -KILL "$child_pid" >/dev/null 2>&1 || true
        fi
    fi

    wait "$child_pid" >/dev/null 2>&1 || true
}

cleanup() {
    stop_child "$SERVER_PID"
    if [[ -n "${CLIENT_PID:-}" ]]; then
        stop_child "$CLIENT_PID"
    fi
}
trap cleanup EXIT

echo "Waiting for runtime publish file: $PUBLISH_DIR/$RUNTIME_NAME"
for _ in $(seq 1 180); do
    if [[ -s "$PUBLISH_DIR/$RUNTIME_NAME" ]]; then
        break
    fi
    # The publish directory is commonly on a parallel filesystem. Refreshing
    # the directory entry avoids waiting for a stale negative lookup after the
    # remote process has already written the descriptor.
    ls -la "$PUBLISH_DIR" >/dev/null 2>&1 || true
    if ! ps -p "$SERVER_PID" >/dev/null 2>&1; then
        echo "Remote runtime exited before publishing runtime descriptor" >&2
        cat "$SERVER_LOG"
        exit 1
    fi
    sleep 1
done

if [[ ! -s "$PUBLISH_DIR/$RUNTIME_NAME" ]]; then
    echo "Remote runtime descriptor was not published" >&2
    cat "$SERVER_LOG"
    exit 1
fi

if (( client_single_node )); then
    # The client starts only after the descriptor is visible. It can therefore
    # attach to an already-running remote runtime instead of racing its
    # bootstrap, which keeps failures attributable to proxy operations.
    echo "Starting local single-node runtime with -s"
    dragon -s "$CLIENT_SCRIPT" \
        --runtime-name "$RUNTIME_NAME" \
        --publish-dir "$PUBLISH_DIR" \
        --remote-working-dir "$WORK_DIR" \
        --exit-path "$EXIT_PATH" \
        --allocation-hosts "$ALLOCATION_HOSTS_CSV" \
        --expected-remote-hosts "$SERVER_HOSTS_CSV" \
        >"$CLIENT_LOG" 2>&1 &
else
    # This launch is intentionally multi-node: it is the second half of the
    # two-runtime mesh test, not just another proxy API invocation.
    echo "Starting client multi-node runtime on hosts=$CLIENT_HOSTS_CSV"
    dragon -m -N "${#CLIENT_HOSTS[@]}" -o hsta \
        -p "$CLIENT_TRANSPORT_PORT" \
        --overlay-port "$CLIENT_OVERLAY_PORT" \
        --frontend-port "$CLIENT_FRONTEND_PORT" \
        --network-config "$CLIENT_NET_CONF" \
        "$CLIENT_SCRIPT" \
        --runtime-name "$RUNTIME_NAME" \
        --publish-dir "$PUBLISH_DIR" \
        --remote-working-dir "$WORK_DIR" \
        --exit-path "$EXIT_PATH" \
        --allocation-hosts "$ALLOCATION_HOSTS_CSV" \
        --expected-remote-hosts "$SERVER_HOSTS_CSV" \
        >"$CLIENT_LOG" 2>/dev/null &
fi
CLIENT_PID=$!

for _ in $(seq 1 "$CLIENT_TIMEOUT_SEC"); do
    if ! ps -p "$CLIENT_PID" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ps -p "$CLIENT_PID" >/dev/null 2>&1; then
    echo "Local client timed out after ${CLIENT_TIMEOUT_SEC}s" >&2
    stop_child "$CLIENT_PID"
    echo "Remote runtime log:" >&2
    cat "$SERVER_LOG"
    echo "Local client log:" >&2
    cat "$CLIENT_LOG"
    exit 1
fi

CLIENT_RC=0
if wait "$CLIENT_PID"; then
    CLIENT_RC=0
else
    CLIENT_RC=$?
fi

if [[ "$CLIENT_RC" -ne 0 ]]; then
    echo "Local client exited with code ${CLIENT_RC}" >&2
    echo "Remote runtime log:" >&2
    cat "$SERVER_LOG"
    echo "Local client log:" >&2
    cat "$CLIENT_LOG"
    exit "$CLIENT_RC"
fi

if ! grep -q "$CLIENT_SUCCESS_MARKER" "$CLIENT_LOG"; then
    echo "Local client did not report success marker ($CLIENT_SUCCESS_MARKER)" >&2
    echo "Remote runtime log:" >&2
    cat "$SERVER_LOG"
    echo "Local client log:" >&2
    cat "$CLIENT_LOG"
    exit 1
fi

# The client signals the remote runtime through EXIT_PATH. Allow the server a
# bounded teardown window, then terminate it so a failed cleanup cannot hold a
# Slurm allocation indefinitely.
for _ in $(seq 1 "$SERVER_WAIT_SEC"); do
    if ! ps -p "$SERVER_PID" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ps -p "$SERVER_PID" >/dev/null 2>&1; then
    echo "Remote runtime did not exit within ${SERVER_WAIT_SEC}s after client completion" >&2
    stop_child "$SERVER_PID"
fi

trap - EXIT

echo "Remote runtime log:"
cat "$SERVER_LOG"
echo "Local client log:"
cat "$CLIENT_LOG"

echo "PROXY_REMOTE_QUEUE_SLURM_OK"
