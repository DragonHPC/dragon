"""DDict key templates for the Dragon AI Agent framework.

All keys are str.format() templates, formatted at runtime with task_id,
agent_id, dispatch_id, etc.  Centralised here so that key naming conventions
are defined in exactly one place.
"""

# ---------------------------------------------------------------------------
# Per-invocation keys — include dispatch_id so concurrent dispatches of the
# same (task_id, agent_id) pair (e.g. retries, parallel pipelines) never
# collide in DDict.
# ---------------------------------------------------------------------------
RESULT_KEY       = "{task_id}:{agent_id}:{dispatch_id}:result"
STATUS_KEY       = "{task_id}:{agent_id}:{dispatch_id}:status"

# Stable lookup key — written by the dispatcher so the orchestrator can
# retrieve dispatch_id after task.run() to construct the correct RESULT_KEY.
# Safe to reuse: task_id is unique per orchestrator.run() call, so each
# (task_id, agent_id) pair is dispatched exactly once per pipeline run.
DISPATCH_ID_KEY  = "{task_id}:{agent_id}:dispatch_id"

# Per-run keys — shared across all agents in one pipeline run; no dispatch_id.
USER_INPUT_KEY   = "{task_id}:user_input"

# Ordered list of {agent_id, answer} dicts appended by each agent on completion.
# Deprecated for writes — agents now write to GLOBAL_STATE_ENTRY_KEY instead.
# This key is still seeded by the orchestrator (with the user prompt) and read
# during result collection for backward compatibility.
GLOBAL_STATE_KEY = "{task_id}:global_state"

# Per-agent global state entry — one key per agent, written atomically when
# the agent finishes.  Avoids the read-modify-write race on GLOBAL_STATE_KEY
# that occurs when parallel DAG branches complete simultaneously.
GLOBAL_STATE_ENTRY_KEY = "{task_id}:{agent_id}:global_entry"

# ---------------------------------------------------------------------------
# Per-event keys — each LLM call, tool call, and HITL decision is written
# to its own DDict key the moment it happens, giving real-time visibility.
# A companion :count key tracks how many events exist so readers (e.g. the
# TCP bridge) know when to stop reading without prefix-scan support.
# ---------------------------------------------------------------------------
LLM_EVENT_KEY         = "{task_id}:{agent_id}:{dispatch_id}:llm:{index}"
LLM_EVENT_COUNT_KEY   = "{task_id}:{agent_id}:{dispatch_id}:llm:count"
TOOL_EVENT_KEY        = "{task_id}:{agent_id}:{dispatch_id}:tool:{index}"
TOOL_EVENT_COUNT_KEY  = "{task_id}:{agent_id}:{dispatch_id}:tool:count"
HITL_EVENT_KEY        = "{task_id}:{agent_id}:{dispatch_id}:hitl:{index}"
HITL_EVENT_COUNT_KEY  = "{task_id}:{agent_id}:{dispatch_id}:hitl:count"
MEMORY_EVENT_KEY       = "{task_id}:{agent_id}:{dispatch_id}:memory:{index}"
MEMORY_EVENT_COUNT_KEY = "{task_id}:{agent_id}:{dispatch_id}:memory:count"

# Internal cursor key used by the TCP bridge to track per-event read position
EVENT_CURSOR_KEY      = "{agent_id}:{dispatch_id}:{event_type}"

# ---------------------------------------------------------------------------
# Tracing keys
# ---------------------------------------------------------------------------
# Ordered list of span dicts for a single agent invocation
TRACE_KEY            = "{task_id}:{agent_id}:{dispatch_id}:trace"
# Per-pipeline trace metadata (start/end time, name)
TRACE_META_KEY       = "{trace_id}:trace:meta"
# Per-agent slot written by the dispatcher so the TCP bridge
# can discover trace keys without a shared list (avoids fan-out race).
# Value = the formatted TRACE_KEY string for this agent/dispatch.
TRACE_INDEX_SLOT_KEY = "{task_id}:trace:slot:{agent_id}"

# ---------------------------------------------------------------------------
# HITL (Human-in-the-Loop) keys — scoped per dispatch to support concurrent
# approval requests from different agents / dispatches.
# ---------------------------------------------------------------------------
HITL_REQUEST_KEY  = "{task_id}:{agent_id}:{dispatch_id}:hitl:request"
HITL_RESPONSE_KEY = "{task_id}:{agent_id}:{dispatch_id}:hitl:response"
# Per-run key (no dispatch_id) — serialized Dragon Queue handle for the
# HITL notification channel.  Written once by the orchestrator.
HITL_QUEUE_KEY    = "hitl:queue"
