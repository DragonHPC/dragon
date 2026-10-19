"""Bounded settings and text-action schemas for the allocation-local LLM."""

import settings

BRIEF_SYSTEM = (
    "Return bounded miniAMR campaign settings as JSON. Bounds must be inside the "
    f"given domain, sim_budget between 1 and {settings.DEFAULT_BUDGET}, "
    "target_fraction greater than 0 and at most 1."
)
AXIS = {"type": "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 2}
REGION = {"type": "array", "items": AXIS, "minItems": 2, "maxItems": 2}
BRIEF_SCHEMA = {
    "type": "object", "properties": {"bounds": REGION,
        "sim_budget": {"type": "integer", "minimum": 1, "maximum": 100},
        "target_fraction": {"type": "number", "exclusiveMinimum": 0, "maximum": 1}},
    "required": ["bounds", "sim_budget", "target_fraction"], "additionalProperties": False,
}
ANALYZE_SYSTEM = (
    "Choose the region for the next miniAMR wave, using evidence you observed. Return one JSON "
    "action per turn. Which tool you call, and in what order, is your decision: pick the next "
    "action from what the previous observation told you.\n"
    "The goal is coverage, not optimisation. A surrogate model is learning block count as a "
    "function of the two dials, so steer toward dials where measured data is thin or where the "
    "block count changes fastest between neighbouring runs. A high or low block count is not "
    "better or worse by itself; a run is only bad if it was invalid.\n"
    "Every action carries a `thought`: one or two sentences saying what the last observation "
    "told you and why this is the right next step. Write it before choosing the action.\n"
    "Tools: list_runs() names the finished runs and whether each one was valid. "
    "get_run_summary(run_id) reads one run's small result, including the dials that produced it. "
    "get_training_summary() reports how many points exist and the dials at the smallest and "
    "largest block counts measured so far.\n"
    "At most four turns and three tool actions; the last turn must finish. Tool observations "
    "arrive as user messages. Finish with action=finish, "
    "region=[[speed_low,speed_high],[size_low,size_high]], and evidence containing a short "
    "explanation grounded in an observation you actually made. "
    "You cannot change safety rules, cancel tasks, launch solvers, or select paths or hosts."
)

THOUGHT = {"type": "string", "minLength": 1, "maxLength": 400}


def _action(name, **properties):
    return {"type": "object",
            "properties": {"action": {"const": name}, "thought": THOUGHT, **properties},
            "required": ["action", "thought", *properties], "additionalProperties": False}


# Named separately: the last turn is decoded against this one alone.
FINISH_SCHEMA = _action(
    "finish", region=REGION,
    evidence={"type": "string", "minLength": 1, "maxLength": 300})

ACTION_SCHEMA = {
    "oneOf": [
        _action("list_runs"),
        _action("get_run_summary", run_id={"type": "string"}),
        _action("get_training_summary"),
        FINISH_SCHEMA,
    ],
}
