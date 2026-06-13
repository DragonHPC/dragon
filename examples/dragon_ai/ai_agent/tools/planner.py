"""Planner tool — propose_experiment with NON-BINARY HITL."""


def propose_experiment(
    description: str,
    sample_sizes: list,
    convergence_target: float,
    methodology: str,
) -> dict:
    """Submit an experiment proposal for human review.

    :param description: Short description of the experiment purpose.
    :param sample_sizes: List of sample sizes to test (e.g. [1000, 10000]).
    :param convergence_target: Desired absolute error threshold (e.g. 0.001).
    :param methodology: Description of the methodology.
    :returns: Dict echoing the approved experiment plan.
    """
    plan = {
        "status": "approved",
        "description": description,
        "sample_sizes": sample_sizes,
        "convergence_target": convergence_target,
        "methodology": methodology,
        "num_runs": len(sample_sizes),
    }
    print(
        f"[planner] propose_experiment -> approved "
        f"({len(sample_sizes)} runs, target={convergence_target})",
        flush=True,
    )
    return plan
