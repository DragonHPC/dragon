"""Analyzer tool — convergence analysis (no HITL)."""

import numpy as np


def analyze_convergence(results: list) -> dict:
    """Analyze convergence behaviour across multiple simulation runs.

    Fits a log-log line to (n_samples, absolute_error) and computes
    convergence rate, R², and a quality assessment.

    :param results: List of dicts with ``n_samples`` and ``absolute_error``.
    :returns: Dict with slope, r_squared, convergence_rate, quality_grade.
    """
    if not results or len(results) < 2:
        return {
            "error": "Need at least 2 data points for convergence analysis.",
            "slope": None,
            "r_squared": None,
        }

    ns = np.array([r["n_samples"] for r in results], dtype=float)
    errors = np.array([r["absolute_error"] for r in results], dtype=float)
    errors = np.maximum(errors, 1e-12)

    log_n = np.log10(ns)
    log_e = np.log10(errors)

    slope, intercept = np.polyfit(log_n, log_e, 1)

    predicted_log_e = slope * log_n + intercept
    ss_res = float(np.sum((log_e - predicted_log_e) ** 2))
    ss_tot = float(np.sum((log_e - log_e.mean()) ** 2))
    r2 = 1.0 - ss_res / (ss_tot + 1e-12)

    predicted_100m = 10 ** (slope * np.log10(100_000_000) + intercept)

    slope_delta = abs(slope - (-0.5))
    if slope_delta < 0.05:
        grade = "A — excellent match to theory"
    elif slope_delta < 0.15:
        grade = "B — good match to theory"
    elif slope_delta < 0.3:
        grade = "C — moderate deviation"
    else:
        grade = "D — significant deviation from 1/√n"

    best_idx = int(np.argmin(errors))
    worst_idx = int(np.argmax(errors))

    result = {
        "slope": round(float(slope), 4),
        "intercept": round(float(intercept), 4),
        "r_squared": round(r2, 4),
        "convergence_rate": f"error ∝ n^{slope:.3f}",
        "theoretical_rate": "error ∝ n^-0.500",
        "quality_grade": grade,
        "predicted_error_at_100M": round(float(predicted_100m), 10),
        "best_result": {
            "n_samples": int(ns[best_idx]),
            "error": float(errors[best_idx]),
        },
        "worst_result": {
            "n_samples": int(ns[worst_idx]),
            "error": float(errors[worst_idx]),
        },
        "interpretation": (
            f"Empirical convergence slope = {slope:.3f} (theory: −0.500). "
            f"R² = {r2:.4f}. {grade}. "
            f"Predicted absolute error at 100M samples: {predicted_100m:.2e}."
        ),
    }
    print(
        f"[analyzer] analyze_convergence -> slope={result['slope']}, "
        f"R2={result['r_squared']}, grade={grade}",
        flush=True,
    )
    return result
