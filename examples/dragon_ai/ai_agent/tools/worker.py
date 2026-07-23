"""Monte Carlo worker — launched as a Dragon process via ProcessGroup.

Must be module-level so ProcessTemplate can pickle it.
"""


def monte_carlo_worker(
    n_samples: int,
    seed: int,
    serialized_ddict: bytes,
) -> None:
    """Run a single Monte Carlo π estimation and write results to DDict.

    Progress protocol (DDict keys):
      - ``experiment:sample:{n_samples}:status``  → "pending" | "running" | "done" | "error"
      - ``experiment:sample:{n_samples}:result``  → JSON-encoded result dict
      - ``experiment:sample:{n_samples}:hostname`` → hostname where this worker ran

    :param n_samples: Number of random (x, y) points to sample.
    :param seed: Random seed for reproducibility.
    :param serialized_ddict: Serialized DDict handle (from ``ddict.serialize()``).
    """
    import json as _json
    import math as _math
    import socket as _socket
    import time as _time

    import numpy as _np
    from dragon.data.ddict import DDict as _DDict

    hostname = _socket.gethostname()
    prefix = f"experiment:sample:{n_samples}"

    # Attach to the shared progress DDict
    ddict = _DDict.attach(serialized_ddict)

    try:
        # Mark running
        ddict[f"{prefix}:status"] = "running"
        ddict[f"{prefix}:hostname"] = hostname
        print(
            f"[worker] n_samples={n_samples:,} seed={seed} "
            f"started on {hostname}",
            flush=True,
        )

        # --- Monte Carlo π estimation ---
        rng = _np.random.default_rng(seed=seed)
        start = _time.perf_counter()
        pts = rng.random((n_samples, 2))
        inside = int(_np.sum(pts[:, 0] ** 2 + pts[:, 1] ** 2 <= 1.0))
        elapsed = _time.perf_counter() - start
        pi_est = 4.0 * inside / n_samples

        result = {
            "n_samples": n_samples,
            "seed": seed,
            "pi_estimate": round(pi_est, 8),
            "absolute_error": round(abs(pi_est - _math.pi), 8),
            "wall_time_s": round(elapsed, 4),
            "hostname": hostname,
        }

        # Write result and mark done
        ddict[f"{prefix}:result"] = _json.dumps(result)
        ddict[f"{prefix}:status"] = "done"
        print(
            f"[worker] n_samples={n_samples:,} DONE → "
            f"pi={result['pi_estimate']}, error={result['absolute_error']}, "
            f"time={result['wall_time_s']}s on {hostname}",
            flush=True,
        )

    except Exception as exc:
        try:
            ddict[f"{prefix}:status"] = "error"
            ddict[f"{prefix}:result"] = _json.dumps({"error": str(exc)})
        except Exception as write_exc:
            print(
                f"[worker] n_samples={n_samples:,} WARNING: could not write "
                f"error status to DDict: {write_exc}",
                flush=True,
            )
        print(f"[worker] n_samples={n_samples:,} ERROR: {exc}", flush=True)
        raise
