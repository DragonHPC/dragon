"""What to run next, and when to stop. Pure functions over small dicts.

Nothing here touches Dragon or the stores. `next_dials` needs the promoted
model only to score candidates; the caller fetches it.
"""

import random

import settings


def random_dials(count, bounds, seed):
    rng = random.Random(seed)
    return [[rng.uniform(lo, hi) for lo, hi in bounds] for _ in range(count)]


def clamp_region(region, bounds):
    return [[max(box_lo, lo), min(box_hi, hi)]
            for (lo, hi), (box_lo, box_hi) in zip(region, bounds)]


def next_dials(model, bounds, region, budget_left, seed):
    """Random until a model exists, then where the members disagree most."""
    count = min(settings.RUNS_PER_WAVE, budget_left)
    if model is None or count < 2:
        return random_dials(count, bounds, seed)
    from science.surrogate import predict

    pool = random_dials(settings.CANDIDATE_POOL, region or bounds, seed)
    _, disagreement = predict(model, pool)
    ranked = sorted(zip(pool, disagreement), key=lambda item: item[1], reverse=True)
    # One free point every wave, so the search never parks in one corner.
    return [dials for dials, _ in ranked[:count - 1]] + random_dials(1, bounds, seed + 1)


def target_met(state):
    """Judged on the promoted model's own frozen counts, not on later arrivals."""
    model = state["model_info"]
    return (model is not None
            and model["validate_error"] <= state["brief"]["target_fraction"]
            and model["n_total"] >= settings.MIN_POINTS_TO_STOP)


def launch_allowed(state):
    """Budget, wave limit and target: all three gate the next launch.

    This is next_step's first question. True means "propose another wave",
    even if earlier waves are still computing.
    """
    return (state["sims_run"] < state["brief"]["sim_budget"]
            and state["wave"] < settings.MAX_WAVES and not target_met(state))


def training_due(state, count, last_fit):
    """Once launching has stopped, a single new label is worth a final fit."""
    minimum = settings.MIN_NEW_POINTS if launch_allowed(state) else 1
    return not target_met(state) and count >= 3 and count - last_fit >= minimum


def fit_counts(n):
    from science.surrogate import split_counts
    train, validate = split_counts(n)
    return f"n_train={train} n_validate={validate} n_total={n}"
