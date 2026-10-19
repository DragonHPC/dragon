#!/usr/bin/env bash
# run_interactive_demo.sh — Interactive demo of the Dragon co-located attach() capability.
#
# Run from any working directory; the script cd's to its own directory first.
#
# Steps:
#   1. Launch the Dragon single-node runtime head (_runtime_head.py) in the
#      background.  The head runs a loopback TCP control server and publishes
#      both the standard attach_info.txt AND the combined ctrl-info file.
#   2. Wait for BOTH the standard attach_info.txt (so dragon.attach works)
#      AND the ctrl-info file (so the client can read the shutdown coordinates)
#      to appear, up to 60 s, bailing early if the head process dies.
#   3. Pause so the USER can manually open ANOTHER shell, activate the env,
#      and run `python client.py` (which defaults to --attach).  The client
#      runs the demo workload and shuts the runtime down itself via the
#      control-token TCP sequence (DragonRuntime.stop()).
#   4. Wait for the runtime head to exit (it exits once the token arrives,
#      or until the user aborts with Ctrl-C).
set -uo pipefail

# Resolve absolute paths so activation works regardless of CWD at invocation.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Return to the example directory so client.py / scroll.py are found.
cd "$SCRIPT_DIR"

UID_NUM="$(id -u)"
RT_DIR="${DRAGON_ATTACH_DIR:-$(python3 -c 'from tempfile import gettempdir; print(gettempdir())')/dragon_${UID_NUM}}"
ATTACH_INFO="${RT_DIR}/attach_info.txt"
CTRL_FILE="${RT_DIR}/attach_ctrl.json"

rm -f "$CTRL_FILE"
export DRAGON_ATTACH_INFO_PATH="$CTRL_FILE"

# Resolve the path to the internal runtime head script via Python so we don't
# hardcode a relative path that may break if the layout changes.

dragon -a dragon-waiting-runtime-head &
SERVER_PID=$!

echo "[interactive] Runtime is up."
echo "[interactive] In ANOTHER shell, activate the env and run:  python client.py --attach"
echo "[interactive] The client will attach, run the demo, and shut the runtime down via the control event."
echo "[interactive] Waiting for the runtime to shut down (Ctrl-C to abort)..."
wait "$SERVER_PID"
echo "[interactive] Runtime has shut down. Done."
exit 0
