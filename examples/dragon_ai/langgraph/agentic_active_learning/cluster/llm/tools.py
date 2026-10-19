"""Bounded text actions for the thinker's analyze step.

The model picks a tool each turn, so the order of work here is not fixed: what
it calls next depends on what the last observation said. Every tool is
read-only, takes (ddict, campaign, allowed, **arguments), and returns a small
dict that is handed straight back to the model.
"""

import json

import settings
from execution.clients import UnsafeTermination
from llm import prompts
from science.sampling import clamp_region

MAX_TURNS = 4    # LLM replies per analyze call
MAX_CALLS = 3    # successful tool actions allowed inside those turns
SUMMARY_KEYS = ("run", "timestep", "valid", "reason", "complete", "blocks",
                "interior_finite", "dials")


def _final_summary(ddict, campaign, run_id):
    summary = ddict.get(f"desc/{campaign}/{run_id}")
    if not isinstance(summary, dict) or summary.get("run") != run_id:
        raise ValueError("missing or mismatched run summary")
    return summary


def list_runs(ddict, campaign, allowed):
    """Names and pass/fail only, so the model can choose what to open next."""
    rows = []
    for run_id in allowed:
        summary = ddict.get(f"desc/{campaign}/{run_id}")
        if isinstance(summary, dict) and summary.get("run") == run_id:
            rows.append({"run": run_id, "valid": bool(summary.get("valid")),
                         "blocks": summary.get("blocks")})
    if not rows:
        raise ValueError("no run summaries available")
    return {"runs": rows}


def get_run_summary(ddict, campaign, allowed, run_id):
    """One finished run in detail, including the dials that produced it."""
    if run_id not in allowed:
        raise ValueError("run is not allowed")
    summary = _final_summary(ddict, campaign, run_id)
    result = {key: summary[key] for key in SUMMARY_KEYS if key in summary}
    json.dumps(result, allow_nan=False)
    return result


def get_training_summary(ddict, campaign, allowed):
    """Where the campaign already has data, and where it is thin."""
    pairs = ddict.get(settings.K_TRAIN) or []
    if not pairs:
        raise ValueError("no training points yet")
    lowest = min(pairs, key=lambda pair: pair["quantity"])
    highest = max(pairs, key=lambda pair: pair["quantity"])
    result = {"n_points": len(pairs),
              "blocks_min": lowest["quantity"], "dials_at_min": lowest["dials"],
              "blocks_max": highest["quantity"], "dials_at_max": highest["dials"]}
    json.dumps(result, allow_nan=False)
    return result


# The model picks the name; this table is the only thing it can reach.
TOOLS = {"list_runs": list_runs,
         "get_run_summary": get_run_summary,
         "get_training_summary": get_training_summary}


async def analyze(llm, ddict, campaign, bounds, allowed):
    """One ReAct loop: the model writes a thought, picks a tool, Python runs it, the result comes back.

    Each turn the model returns one JSON action with a `thought` field: what
    the last observation told it and why it picks this action. If the action
    names a tool, the tool runs against the campaign DDict and its small
    result is appended as the next user message. If it names `finish`, the
    loop ends. Which tool, and in what order, is the model's call; nothing
    here fixes the sequence. Every turn prints thought, act, and observe.

    Three read-only tools: list_runs (ids and pass/fail), get_run_summary (one
    run with the dials that made it), get_training_summary (where data is
    dense and thin). Budgets are enforced in code, not in the prompt:
    MAX_TURNS replies, MAX_CALLS successful tool actions, and once either is
    spent the reply is decoded against the finish schema alone.

    Finishing requires at least one successful observation, a region inside
    the bounds, and a short evidence string. A rejected action is fed back as
    the next observation so the model can correct itself. Any exhaustion or
    malformed reply falls back to an empty region, and the campaign samples by
    ensemble disagreement alone.

    Only `steer_region` reaches the graph. The model cannot launch, cancel, or
    touch anything but these three reads.
    """
    messages = [{"role": "system", "content": prompts.ANALYZE_SYSTEM},
                {"role": "user", "content": json.dumps({"bounds": bounds,
                 "allowed_runs": allowed}, allow_nan=False)}]
    calls = 0
    seen = set()
    result = {"steer_region": [], "analysis_evidence": "unsteered ensemble fallback"}
    for turn in range(MAX_TURNS):
        must_finish = turn == MAX_TURNS - 1 or calls >= MAX_CALLS
        label = f"[analyze] turn {turn + 1}/{MAX_TURNS}"
        try:
            # Out of budget: decode against finish alone rather than ask for it.
            schema = prompts.FINISH_SCHEMA if must_finish else prompts.ACTION_SCHEMA
            # Do not cancel chat: a late response could outlive its response-queue lease.
            # Dragon: llm.chat goes to the vLLM the launcher started; no API key, no data leaves the cluster.
            text = await llm.chat(messages=messages, json_schema=schema)
            if not isinstance(text, str) or len(text) > 4096:
                raise ValueError("invalid action text")
            reply = json.loads(text)
            if not isinstance(reply, dict):
                raise ValueError("expected an action object")
            action = reply.get("action")
            thought = reply.get("thought")
            if not isinstance(thought, str) or not thought.strip():
                raise ValueError("every action needs a thought")
            print(f"{label}  thought: {thought.strip()}", flush=True)
            messages.append({"role": "assistant", "content": text})
            if action == "finish":
                evidence = reply.get("evidence")
                if (set(reply) != {"action", "thought", "region", "evidence"} or not calls or
                        not settings.valid_region(reply.get("region"), bounds) or
                        not isinstance(evidence, str) or not 1 <= len(evidence.strip()) <= 300):
                    raise ValueError("finish needs valid bounds, evidence, and a successful observation")
                result = {"steer_region": clamp_region(reply["region"], bounds),
                          "analysis_evidence": evidence.strip()}
                break
            if action not in TOOLS:
                raise ValueError("action not allowed")
            if must_finish:
                raise ValueError("tool budget exhausted; finish required")
            arguments = {key: value for key, value in reply.items() if key not in ("action", "thought")}
            request = json.dumps({"action": action, **arguments}, sort_keys=True)
            if request in seen:  # the whole call, not just the run id; a new thought does not make it new
                raise ValueError("duplicate tool request")
            seen.add(request)
            observation = TOOLS[action](ddict, campaign, allowed, **arguments)  # the dispatch: name to function
            calls += 1
            messages.append({"role": "user", "content": json.dumps(
                {"tool": action, "observation": observation}, allow_nan=False)})
            print(f"{label}  act:     {action}({arguments})\n"
                  f"{label}  observe: {observation}", flush=True)
        except UnsafeTermination:
            raise
        except Exception as exc:
            error = str(exc)[:240]
            print(f"{label}  observe: action rejected or failed: {error}", flush=True)
            messages.append({"role": "user", "content": json.dumps({"error": error})})
    print(f"[analyze] decision: steer_region={result['steer_region'] or 'none (ensemble disagreement alone)'}; "
          f"evidence: {result['analysis_evidence']}", flush=True)
    return result