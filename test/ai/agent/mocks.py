"""Reusable mock objects for Dragon agent framework tests.

Mocks non-Dragon dependencies (LLM engines, telemetry) so that agent
logic can be tested on the Dragon runtime.  Dragon primitives (Queue,
DDict, Event) are used directly — never mocked.
"""

import json
from typing import Any, Optional


# ---------------------------------------------------------------------------
# MockLLMEngine — drop-in for DragonQueueLLMProxy
# ---------------------------------------------------------------------------

class MockLLMEngine:
    """Deterministic async LLM engine for tests.

    Returns pre-configured responses in order.  Tracks all calls for
    assertion in tests.

    Usage::

        engine = MockLLMEngine(responses=["Hello", "World"])
        result = await engine.chat([...])  # returns "Hello"
        result = await engine.chat([...])  # returns "World"
    """

    def __init__(self, responses: Optional[list] = None) -> None:
        self._responses = list(responses or [])
        self._call_count = 0
        self.calls: list = []

    async def chat(
        self,
        prompts: list[dict],
        tools: Any = None,
        json_schema: Any = None,
        continue_final_message: bool = False,
    ) -> str:
        self.calls.append({
            "prompts": prompts,
            "tools": tools,
            "json_schema": json_schema,
            "continue_final_message": continue_final_message,
        })
        if self._call_count < len(self._responses):
            resp = self._responses[self._call_count]
        else:
            resp = ""
        self._call_count += 1
        return resp


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def create_final_answer_json(content: str) -> str:
    """Build a well-formed ``final_answer`` JSON string."""
    return json.dumps({"response": {"type": "final_answer", "content": content}})


def create_tool_request_json(tool_calls: list[dict]) -> str:
    """Build a well-formed ``tool_request`` JSON string."""
    return json.dumps({"response": {"type": "tool_request", "tool_calls": tool_calls}})
