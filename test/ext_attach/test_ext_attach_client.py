"""Standalone Python test client for external attach/detach behavior.

Usage: test_ext_attach_client.py <mode>
  attach_detach  - attach with a correct token, print p_uid, detach cleanly
  bad_token      - attach with a wrong token; expect auth failure
  double_detach  - attach, detach, detach again; expect the 2nd to fail
  reattach       - attach, detach, attach again; both must succeed
  stale_runtime  - point DRAGON_ATTACH_DIR at a dir with no attach_info.txt
"""

import sys
import time

import dragon.workflows.runtime as runtime


def mode_attach_detach():
    rt = None
    try:
        rt = runtime.attach()
        puid = getattr(rt, "_puid", None)
        if puid is None:
            print("attach_detach: missing p_uid on attached runtime", file=sys.stderr)
            return 1
        print(f"p_uid={puid}")
        sys.stdout.flush()
        time.sleep(0.2)
        rt.detach()
        return 0
    except Exception as ex:
        print(f"attach_detach: attach/detach failed: {ex}", file=sys.stderr)
        return 1


def mode_bad_token():
    rt = None
    try:
        rt = runtime.attach(auth_token="definitely-the-wrong-token")
    except PermissionError:
        return 0
    except Exception as ex:
        print(f"bad_token: expected auth failure, got: {ex}", file=sys.stderr)
        return 1

    try:
        rt.detach()
    except Exception:
        pass
    print("bad_token: expected attach to fail, but attach succeeded", file=sys.stderr)
    return 1


def mode_double_detach():
    try:
        rt = runtime.attach()
    except Exception as ex:
        print(f"double_detach: attach failed: {ex}", file=sys.stderr)
        return 1

    try:
        rt.detach()
    except Exception as ex:
        print(f"double_detach: first detach failed: {ex}", file=sys.stderr)
        return 1

    try:
        rt.detach()
    except RuntimeError:
        return 0
    except Exception as ex:
        print(f"double_detach: unexpected error on 2nd detach: {ex}", file=sys.stderr)
        return 1

    print("double_detach: expected second detach to fail", file=sys.stderr)
    return 1


def mode_reattach():
    try:
        rt1 = runtime.attach()
        rt1.detach()

        rt2 = runtime.attach()
        rt2.detach()
        return 0
    except Exception as ex:
        print(f"reattach: failed: {ex}", file=sys.stderr)
        return 1


def mode_stale_runtime():
    rt = None
    try:
        rt = runtime.attach()
    except RuntimeError:
        return 0
    except Exception as ex:
        print(f"stale_runtime: expected RuntimeError, got: {ex}", file=sys.stderr)
        return 1

    try:
        rt.detach()
    except Exception:
        pass
    print("stale_runtime: expected attach to fail for missing attach_info", file=sys.stderr)
    return 1


def main(argv):
    if len(argv) != 2:
        print(f"usage: {argv[0]} <mode>", file=sys.stderr)
        return 1

    mode = argv[1]

    if mode == "attach_detach":
        return mode_attach_detach()
    if mode == "bad_token":
        return mode_bad_token()
    if mode == "double_detach":
        return mode_double_detach()
    if mode == "reattach":
        return mode_reattach()
    if mode == "stale_runtime":
        return mode_stale_runtime()

    print(f"unknown mode: {mode}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
