"""Internal head process for :py:func:`dragon.runtime.start_runtime`.

Executed inside a ``dragon -s`` session as the managed head process.  Retrieves
attach-info from the Local Services key-value store, starts a loopback TCP
control server, prints one sentinel JSON line to stdout for the parent process,
then blocks until a valid shutdown token arrives — at which point it exits,
triggering Dragon teardown.

.. warning::
    This is an internal module.  Do not import or invoke it directly.
"""

import json
import os
import sys
import time
import base64

import dragon.infrastructure.facts as dfacts
import dragon.globalservices.api_setup as das
import dragon.infrastructure.messages as dmsg
from dragon.native.event import Event
from dragon.workflows.runtime import RUNTIME_INFO_PREFIX

#: Maximum seconds to wait for the attach-info KVS key to appear.
_KVS_TIMEOUT = 30.0

#: Per-call timeout / retry interval when polling the KVS key.
_KVS_RETRY_INTERVAL = 1.0


def _get_attach_info_from_kvs(timeout: float = _KVS_TIMEOUT) -> dict:
    """Retrieve attach-info from the Local Services key-value store.

    Uses :py:func:`dragon.utils.get_local_kv` to fetch the well-known
    ``ATTACH_INFO_KVS_KEY`` entry that Local Services pre-populates at
    startup. Retries on :py:exc:`KeyError` until the deadline expires, to
    tolerate any startup race.

    :param timeout: Maximum number of seconds to wait for the key to appear.
    :type timeout: float
    :returns: The parsed attach-info dictionary.
    :rtype: dict
    :raises RuntimeError: If the key does not appear within ``timeout`` seconds.
    """
    from dragon.utils import get_local_kv

    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                f"attach-info not found in Local Services KVS "
                f"(key={dfacts.ATTACH_INFO_KVS_KEY!r}) after {timeout}s. "
                f"Is the Dragon runtime starting correctly?"
            )
        try:
            value = get_local_kv(
                dfacts.ATTACH_INFO_KVS_KEY,
                timeout=min(remaining, _KVS_RETRY_INTERVAL),
            )
            return json.loads(value)
        except KeyError:
            time.sleep(min(_KVS_RETRY_INTERVAL, max(remaining, 0)))


def main() -> None:
    """Entry point for the Dragon runtime head process.

    Retrieves attach-info from the Local Services KVS, starts a loopback TCP
    control server, prints the sentinel JSON line, then waits for a valid
    shutdown token.
    """
    attach_info = _get_attach_info_from_kvs()

    ctrl_event = Event()  # Create a new Dragon event for control signaling

    # Serialize the event state and broadcast it to every LS node via GS so
    # that any attached process (e.g. DragonRuntime.stop()) can retrieve and
    # set it through the KVS without needing a direct reference.
    sdesc, cuid = ctrl_event.__getstate__()
    req_msg = dmsg.GSRegisterCtrlEvent(
        tag=das.next_tag(),
        ctrl_event_sdesc=base64.b64encode(sdesc).decode("ascii"),
        ctrl_event_cuid=cuid,
    )
    das.gs_request(req_msg, expecting_response=False)

    # Print the sentinel line — the parent reads this from the subprocess stdout.
    sys.stdout.write(RUNTIME_INFO_PREFIX + json.dumps(attach_info) + "\n")
    sys.stdout.flush()

    ctrl_event.wait()


if __name__ == "__main__":
    main()
