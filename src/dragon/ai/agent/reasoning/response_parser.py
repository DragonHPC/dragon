"""JSON parse / repair for structured-output LLM responses.

Extracts the response-parsing and final-answer logic that was previously
inlined in :class:`ToolDispatcher.chat`.  All functions are pure (no DDict,
no tracing side-effects) so they are straightforward to unit-test.
"""

from __future__ import annotations

import json
import re
from typing import Literal, List, Union

from pydantic import BaseModel, Field

from ..utils.errors import AgentLoopError
from ..utils.logging import get_agent_logger

log = get_agent_logger("response_parser")


# ---------------------------------------------------------------------------
# Structured-output response schema
# ---------------------------------------------------------------------------

class ToolCall(BaseModel):
    name: str
    args: dict | None = None


class ToolRequest(BaseModel):
    type: Literal["tool_request"] = "tool_request"
    tool_calls: List[ToolCall]


class FinalResponse(BaseModel):
    type: Literal["final_answer"] = "final_answer"
    content: str = Field(description="The final natural language answer to the user.")


class ResponseModel(BaseModel):
    """Top-level union that forces the LLM to choose between a tool request
    and a final answer on every turn."""
    response: Union[ToolRequest, FinalResponse]


# ---------------------------------------------------------------------------
# Parse / repair helpers
# ---------------------------------------------------------------------------

def parse_llm_response(
    output_text: str, iteration: int,
) -> tuple[ResponseModel, str | None]:
    """Parse structured-output JSON, repairing truncation when possible.

    Returns ``(parsed_model, truncated_prefix)``.  When the parse is
    clean, *truncated_prefix* is ``None``.  When the JSON was truncated
    mid-stream, the method extracts any partial ``content`` value so the
    caller can seed a continuation call.

    Raises :class:`AgentLoopError` if a ``tool_request`` was truncated
    (partial tool-call args cannot be safely recovered).
    """
    try:
        data = ResponseModel.model_validate_json(output_text)
        return data, None
    except Exception:
        pass

    # Extract the "type" field with a regex — it always appears early
    # in the JSON and is almost never the part that gets truncated.
    m = re.search(r'"type"\s*:\s*"([^"]+)"', output_text)
    if m and m.group(1) == "tool_request":
        log.error(
            "LLM produced truncated tool_request JSON (iteration %d): %r",
            iteration, output_text[:300],
        )
        raise AgentLoopError(
            f"LLM produced truncated tool_request JSON that could "
            f"not be parsed (iteration {iteration}). Raw output: "
            f"{output_text[:300]!r}"
        ) from None

    # Treat as final_answer. Extract any partial content the model already
    # wrote so the second call can continue from it.
    content_m = re.search(r'"content"\s*:\s*"(.*)', output_text, re.DOTALL)
    truncated_prefix = content_m.group(1) if content_m else None
    # A whitespace-only prefix has no continuation value — discard it.
    if truncated_prefix is not None and not truncated_prefix.strip():
        truncated_prefix = None
    log.warning(
        "Response iteration %d: JSON truncated (type=%s). "
        "Falling through to final-answer generation.",
        iteration, "final_answer" if not m else m.group(1),
    )
    data = ResponseModel(response=FinalResponse(type="final_answer", content=""))
    return data, truncated_prefix


async def handle_final_answer(
    data: ResponseModel,
    truncated_prefix: str | None,
    copy_prompts: list[dict],
    llm_engine,
) -> str:
    """Produce the final answer text from a ``final_answer`` response.

    * **Clean parse** — return ``data.response.content`` directly.
    * **Truncated parse** — make a second unconstrained LLM call to
      continue from *truncated_prefix*.
    """
    if truncated_prefix is not None:
        second_call_prompts = list(copy_prompts)
        second_call_prompts.append({
            "role": "assistant",
            "content": truncated_prefix,
        })
        final_text = await llm_engine.chat(
            second_call_prompts,
            tools=None,
            json_schema=None,
            continue_final_message=True,
        )
        final_text = truncated_prefix + final_text
        return unwrap_final_answer_json(final_text)

    return data.response.content


def unwrap_final_answer_json(text: str) -> str:
    """Extract the ``content`` field if *text* is a structured-output JSON echo.

    The second (unconstrained) LLM call occasionally repeats the
    ``{"response": {"type": "final_answer", "content": "..."}}``
    format instead of producing free-form text.  When that happens,
    return just the ``content`` value.  Otherwise return *text*
    unchanged.
    """
    stripped = text.strip()
    if not stripped.startswith("{"):
        return text
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            resp = parsed.get("response", parsed)
            if isinstance(resp, dict) and resp.get("type") == "final_answer":
                return resp.get("content", text)
    except (json.JSONDecodeError, TypeError, KeyError):
        pass
    return text
