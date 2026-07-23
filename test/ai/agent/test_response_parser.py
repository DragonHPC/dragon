"""Tests for the LLM response parser — pure logic, no mocks needed."""

import dragon
import multiprocessing as mp


import json

from unittest import TestCase, IsolatedAsyncioTestCase, main

from dragon.ai.agent.reasoning.response_parser import (
    ResponseModel,
    ToolCall,
    ToolRequest,
    FinalResponse,
    parse_llm_response,
    handle_final_answer,
    unwrap_final_answer_json,
)
from dragon.ai.agent.utils.errors import AgentLoopError


# ========================================================================
# parse_llm_response
# ========================================================================

class TestParseLLMResponse(TestCase):
    """Verify parse_llm_response handles clean, truncated, and garbage LLM outputs."""

    def _make_final_answer_json(self, content: str) -> str:
        return json.dumps({
            "response": {"type": "final_answer", "content": content}
        })

    def _make_tool_request_json(self, tool_calls: list) -> str:
        return json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": tool_calls,
            }
        })

    def test_clean_final_answer(self):
        """Well-formed final_answer JSON is parsed into a FinalResponse."""
        raw = self._make_final_answer_json("42 is the answer")
        data, trunc = parse_llm_response(raw, iteration=1)
        self.assertEqual(data.response.type, "final_answer")
        self.assertEqual(data.response.content, "42 is the answer")
        self.assertIsNone(trunc)

    def test_clean_tool_request(self):
        """Well-formed tool_request JSON is parsed with tool call list."""
        raw = self._make_tool_request_json([
            {"name": "search", "args": {"query": "python"}},
        ])
        data, trunc = parse_llm_response(raw, iteration=1)
        self.assertEqual(data.response.type, "tool_request")
        self.assertEqual(len(data.response.tool_calls), 1)
        self.assertEqual(data.response.tool_calls[0].name, "search")
        self.assertIsNone(trunc)

    def test_truncated_final_answer_extracts_prefix(self):
        """Truncated final_answer recovers the partial content string."""
        truncated = '{"response": {"type": "final_answer", "content": "This is a par'
        data, trunc = parse_llm_response(truncated, iteration=2)
        self.assertEqual(data.response.type, "final_answer")
        self.assertEqual(trunc, "This is a par")

    def test_truncated_final_answer_whitespace_only_discarded(self):
        """Truncated final_answer with only whitespace content returns None prefix."""
        truncated = '{"response": {"type": "final_answer", "content": "   '
        data, trunc = parse_llm_response(truncated, iteration=2)
        self.assertIsNone(trunc)

    def test_truncated_tool_request_raises(self):
        """Truncated tool_request cannot be recovered and raises AgentLoopError."""
        truncated = '{"response": {"type": "tool_request", "tool_calls": [{"name":'
        with self.assertRaisesRegex(AgentLoopError, "truncated tool_request"):
            parse_llm_response(truncated, iteration=3)

    def test_garbage_input_falls_through_to_final_answer(self):
        """Non-JSON input is treated as an empty final_answer (graceful degradation)."""
        data, trunc = parse_llm_response("totally not json", iteration=1)
        self.assertEqual(data.response.type, "final_answer")
        self.assertEqual(data.response.content, "")

    def test_multiple_tool_calls(self):
        """Multiple tool calls in a single response are all parsed."""
        raw = self._make_tool_request_json([
            {"name": "search", "args": {"q": "a"}},
            {"name": "calculate", "args": {"expr": "1+1"}},
        ])
        data, _ = parse_llm_response(raw, iteration=1)
        self.assertEqual(len(data.response.tool_calls), 2)


# ========================================================================
# unwrap_final_answer_json
# ========================================================================

class TestUnwrapFinalAnswerJson(TestCase):
    """Verify unwrap_final_answer_json extracts content from echoed JSON."""

    def test_plain_text_passthrough(self):
        """Plain text without JSON structure passes through unchanged."""
        self.assertEqual(unwrap_final_answer_json("Hello world"), "Hello world")

    def test_json_final_answer_extracted(self):
        """JSON with final_answer type has its content extracted."""
        text = json.dumps({
            "response": {"type": "final_answer", "content": "The answer is 42."}
        })
        self.assertEqual(unwrap_final_answer_json(text), "The answer is 42.")

    def test_malformed_json_passthrough(self):
        """Malformed JSON passes through as-is (no crash)."""
        text = '{"broken json'
        self.assertEqual(unwrap_final_answer_json(text), text)

    def test_json_without_type_passthrough(self):
        """Valid JSON that isn't a final_answer passes through."""
        text = json.dumps({"key": "value"})
        self.assertEqual(unwrap_final_answer_json(text), text)

    def test_leading_whitespace_handled(self):
        """Leading whitespace before JSON is tolerated."""
        text = '  {"response": {"type": "final_answer", "content": "hi"}}'
        self.assertEqual(unwrap_final_answer_json(text), "hi")


# ========================================================================
# handle_final_answer
# ========================================================================

class TestHandleFinalAnswer(IsolatedAsyncioTestCase):
    """Verify handle_final_answer with clean, truncated, and echoed-JSON cases."""

    async def test_clean_parse_returns_content(self):
        """Clean parse returns the content string directly."""
        data = ResponseModel(response=FinalResponse(content="Direct answer"))
        result = await handle_final_answer(data, None, [], None)
        self.assertEqual(result, "Direct answer")

    async def test_truncated_calls_llm_and_concatenates(self):
        """Truncated prefix triggers a continuation LLM call and concatenates."""
        data = ResponseModel(response=FinalResponse(content=""))

        class FakeLLM:
            async def chat(self, prompts, tools=None, json_schema=None,
                           continue_final_message=False):
                return " continued text"

        result = await handle_final_answer(
            data, "prefix", [{"role": "user", "content": "task"}], FakeLLM()
        )
        self.assertEqual(result, "prefix continued text")

    async def test_truncated_with_json_echo_unwrapped(self):
        """When the continuation call echoes structured JSON, unwrap it."""
        json_echo = json.dumps({
            "response": {"type": "final_answer", "content": "real answer"}
        })

        class FakeLLM:
            async def chat(self, prompts, **kw):
                return json_echo

        result = await handle_final_answer(
            ResponseModel(response=FinalResponse(content="")),
            "",  # empty prefix triggers continuation
            [{"role": "user", "content": "task"}],
            FakeLLM(),
        )
        # The prefix "" + json_echo is unwrapped
        self.assertIn("real answer", result)


# ========================================================================
# Pydantic model schemas
# ========================================================================

class TestResponseModels(TestCase):
    """Verify Pydantic model schemas for ToolCall, ToolRequest, FinalResponse."""

    def test_tool_call_model(self):
        """ToolCall stores name and args dict."""
        tc = ToolCall(name="search", args={"q": "test"})
        self.assertEqual(tc.name, "search")
        self.assertEqual(tc.args, {"q": "test"})

    def test_tool_request_model(self):
        """ToolRequest has type='tool_request' and a list of ToolCalls."""
        tr = ToolRequest(tool_calls=[ToolCall(name="a", args={})])
        self.assertEqual(tr.type, "tool_request")

    def test_final_response_model(self):
        """FinalResponse has type='final_answer' and content string."""
        fr = FinalResponse(content="done")
        self.assertEqual(fr.type, "final_answer")

    def test_response_model_json_schema_available(self):
        """ResponseModel exposes a JSON schema (used for structured output)."""
        schema = ResponseModel.model_json_schema()
        self.assertIn("properties", schema)


# ========================================================================
# parse_llm_response — additional edge cases
# ========================================================================

class TestParseLLMResponseEdgeCases(TestCase):
    """Verify parse_llm_response handles various edge cases."""

    def test_empty_string_input(self):
        """Empty string input treated as final_answer with empty content."""
        data, trunc = parse_llm_response("", iteration=0)
        self.assertEqual(data.response.type, "final_answer")
        self.assertEqual(data.response.content, "")

    def test_whitespace_only_input(self):
        """Whitespace-only input treated as final_answer with empty content."""
        data, trunc = parse_llm_response("   \n\t   ", iteration=0)
        self.assertEqual(data.response.type, "final_answer")

    def test_tool_call_with_no_args(self):
        """Tool call with args=None is handled gracefully."""
        raw = json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [{"name": "list_files", "args": None}],
            }
        })
        data, _ = parse_llm_response(raw, iteration=1)
        self.assertEqual(data.response.tool_calls[0].name, "list_files")

    def test_tool_call_with_empty_args(self):
        """Tool call with empty args dict is valid."""
        raw = json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [{"name": "get_time", "args": {}}],
            }
        })
        data, _ = parse_llm_response(raw, iteration=1)
        self.assertEqual(data.response.tool_calls[0].args, {})

    def test_final_answer_with_special_characters(self):
        """Final answer with special characters (newlines, quotes) parsed correctly."""
        content = 'Line1\nLine2\t"quoted"\nLine3'
        raw = json.dumps({
            "response": {"type": "final_answer", "content": content}
        })
        data, _ = parse_llm_response(raw, iteration=0)
        self.assertEqual(data.response.content, content)

    def test_final_answer_with_unicode(self):
        """Final answer with unicode characters parsed correctly."""
        content = "Résumé of François — 日本語テスト"
        raw = json.dumps({
            "response": {"type": "final_answer", "content": content}
        })
        data, _ = parse_llm_response(raw, iteration=0)
        self.assertEqual(data.response.content, content)


# ========================================================================
# ToolCall model edge cases
# ========================================================================

class TestToolCallEdgeCases(TestCase):
    """Verify ToolCall model validation edge cases."""

    def test_tool_call_none_args_defaults(self):
        """ToolCall with args=None stores None."""
        tc = ToolCall(name="tool", args=None)
        self.assertIsNone(tc.args)

    def test_tool_call_complex_args(self):
        """ToolCall with nested dict args stores correctly."""
        args = {"filter": {"status": "active"}, "limit": 10, "tags": ["a", "b"]}
        tc = ToolCall(name="search", args=args)
        self.assertEqual(tc.args["filter"]["status"], "active")
        self.assertEqual(tc.args["tags"], ["a", "b"])


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
