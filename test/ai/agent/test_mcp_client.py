"""Tests for MCPServerClient — connection, tool listing, and dispatch."""

import dragon
import multiprocessing as mp

from unittest import IsolatedAsyncioTestCase, TestCase, main
from unittest.mock import AsyncMock, MagicMock, patch

from dragon.ai.agent.tools.mcp_tool import MCPServerClient


class TestMCPServerClientInit(TestCase):
    """Verify MCPServerClient default field values."""

    def test_defaults(self):
        """Alias is stored; session starts as None; schemas and scoped_names empty."""
        client = MCPServerClient(alias="jupyter")
        self.assertEqual(client.alias, "jupyter")
        self.assertEqual(client.max_retries, 3)
        self.assertEqual(client.retry_delay, 0.5)
        self.assertEqual(client.timeout, 5.0)
        self.assertIsNone(client._mcp_session)
        self.assertEqual(client.tools_schemas, [])
        self.assertEqual(client.scoped_names, set())


class TestMCPServerClientStart(IsolatedAsyncioTestCase):
    """Verify start() connects, lists tools, and builds scoped schemas."""

    async def test_successful_connection(self):
        """Successful connection populates session, schemas, and scoped_names."""
        client = MCPServerClient(alias="jupyter", max_retries=1)

        mock_tool = MagicMock()
        mock_tool.name = "create_notebook"
        mock_tool.description = "Create a Jupyter notebook"
        mock_tool.inputSchema = {"type": "object", "properties": {}}

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.list_tools = AsyncMock(return_value=[mock_tool])

        # Mock the Client context manager
        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("dragon.ai.agent.tools.mcp_tool.AsyncExitStack") as mock_stack_cls:
            mock_stack = AsyncMock()
            mock_stack.enter_async_context = AsyncMock(return_value=mock_session)
            mock_stack.aclose = AsyncMock()
            mock_stack_cls.return_value = mock_stack

            with patch("fastmcp.Client", return_value=mock_client_ctx):
                await client.start("http://mcp:8000/mcp", "token123")

        self.assertIs(client._mcp_session, mock_session)
        self.assertEqual(len(client.tools_schemas), 1)
        self.assertIn("jupyter__create_notebook", client.scoped_names)
        schema = client.tools_schemas[0]
        self.assertEqual(schema["function"]["name"], "jupyter__create_notebook")

    async def test_idempotent_start(self):
        """Calling start() when already connected is a no-op."""
        client = MCPServerClient(alias="test")
        client._mcp_session = MagicMock()  # pretend connected
        await client.start("http://x", "token")
        # Should not have created a new session

    async def test_all_retries_exhausted(self):
        """ConnectionError raised when all retry attempts fail."""
        client = MCPServerClient(alias="fail", max_retries=2, retry_delay=0.01)

        with patch("dragon.ai.agent.tools.mcp_tool.AsyncExitStack") as mock_stack_cls:
            mock_stack = AsyncMock()
            mock_stack.enter_async_context = AsyncMock(
                side_effect=ConnectionError("refused")
            )
            mock_stack.aclose = AsyncMock()
            mock_stack_cls.return_value = mock_stack

            with patch("fastmcp.Client"):
                with self.assertRaisesRegex(ConnectionError, "failed to connect"):
                    await client.start("http://bad", "token")


class TestMCPServerClientCallTool(IsolatedAsyncioTestCase):
    """Verify call_tool dispatches to the correct MCP tool."""

    async def test_call_tool_success(self):
        """Structured content from MCP response is returned."""
        client = MCPServerClient(alias="jupyter")
        client._mcp_session = AsyncMock()
        client._scoped_names = {"jupyter__create_notebook"}

        mock_response = MagicMock()
        mock_response.structured_content = {"notebook_id": "123"}
        client._mcp_session.call_tool = AsyncMock(return_value=mock_response)

        result = await client.call_tool("jupyter__create_notebook", {"name": "demo"})
        self.assertEqual(result, {"notebook_id": "123"})
        client._mcp_session.call_tool.assert_called_once_with(
            "create_notebook", {"name": "demo"}
        )

    async def test_call_tool_falls_back_to_text(self):
        """When structured_content is None, text content is returned."""
        client = MCPServerClient(alias="fs")
        client._mcp_session = AsyncMock()
        client._scoped_names = {"fs__read_file"}

        mock_content = MagicMock()
        mock_content.text = "file contents"
        mock_response = MagicMock()
        mock_response.structured_content = None
        mock_response.content = [mock_content]
        client._mcp_session.call_tool = AsyncMock(return_value=mock_response)

        result = await client.call_tool("fs__read_file", {"path": "/etc/hosts"})
        self.assertEqual(result, "file contents")

    async def test_call_tool_not_connected_raises(self):
        """RuntimeError raised when session is not connected."""
        client = MCPServerClient(alias="x")
        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await client.call_tool("x__tool", {})

    async def test_call_tool_wrong_alias_raises(self):
        """ValueError raised when tool name does not match the client's alias."""
        client = MCPServerClient(alias="jupyter")
        client._mcp_session = AsyncMock()
        client._scoped_names = {"jupyter__create_notebook"}
        with self.assertRaisesRegex(ValueError, "does not belong"):
            await client.call_tool("other__tool", {})


class TestMCPServerClientClose(IsolatedAsyncioTestCase):
    """Verify close() cleans up session, schemas, and scoped_names."""

    async def test_close_cleans_up(self):
        """After close, session is None, schemas and scoped_names are cleared."""
        client = MCPServerClient(alias="test")
        client._mcp_exit_stack = AsyncMock()
        client._mcp_session = MagicMock()
        client._tools_schemas = [{"type": "function"}]
        client._scoped_names = {"test__tool"}

        await client.close()
        self.assertIsNone(client._mcp_session)
        self.assertEqual(client._tools_schemas, [])
        self.assertEqual(client._scoped_names, set())

    async def test_close_idempotent(self):
        """Calling close() multiple times does not raise."""
        client = MCPServerClient(alias="test")
        await client.close()  # no error when not connected
        await client.close()  # still no error


# ========================================================================
# MCPServerClient — edge cases and properties
# ========================================================================

class TestMCPServerClientEdgeCases(IsolatedAsyncioTestCase):
    """Verify MCPServerClient edge cases and property accessors."""

    async def test_custom_retry_parameters(self):
        """Custom max_retries, retry_delay, and timeout are stored."""
        client = MCPServerClient(
            alias="custom", max_retries=5, retry_delay=1.0, timeout=10.0
        )
        self.assertEqual(client.max_retries, 5)
        self.assertEqual(client.retry_delay, 1.0)
        self.assertEqual(client.timeout, 10.0)

    async def test_call_tool_multiple_text_content_joined(self):
        """Multiple text content items are joined with newlines."""
        client = MCPServerClient(alias="fs")
        client._mcp_session = AsyncMock()
        client._scoped_names = {"fs__list"}

        mock_c1 = MagicMock()
        mock_c1.text = "line1"
        mock_c2 = MagicMock()
        mock_c2.text = "line2"
        mock_response = MagicMock()
        mock_response.structured_content = None
        mock_response.content = [mock_c1, mock_c2]
        client._mcp_session.call_tool = AsyncMock(return_value=mock_response)

        result = await client.call_tool("fs__list", {})
        self.assertEqual(result, "line1\nline2")

    async def test_call_tool_content_without_text_skipped(self):
        """Content items without .text attribute are skipped."""
        client = MCPServerClient(alias="fs")
        client._mcp_session = AsyncMock()
        client._scoped_names = {"fs__list"}

        mock_text = MagicMock()
        mock_text.text = "real text"
        mock_image = MagicMock(spec=[])  # no .text attribute
        mock_response = MagicMock()
        mock_response.structured_content = None
        mock_response.content = [mock_text, mock_image]
        client._mcp_session.call_tool = AsyncMock(return_value=mock_response)

        result = await client.call_tool("fs__list", {})
        self.assertEqual(result, "real text")

    async def test_repr(self):
        """__repr__ includes alias and sorted tool names."""
        client = MCPServerClient(alias="jupyter")
        client._scoped_names = {"jupyter__b", "jupyter__a"}
        r = repr(client)
        self.assertIn("jupyter", r)
        self.assertIn("jupyter__a", r)
        self.assertIn("jupyter__b", r)

    async def test_start_cleanup_on_retry_failure(self):
        """Exit stack is cleaned up between retry attempts."""
        client = MCPServerClient(alias="retry", max_retries=2, retry_delay=0.01)
        cleanup_calls = []

        with patch("dragon.ai.agent.tools.mcp_tool.AsyncExitStack") as mock_stack_cls:
            mock_stack = AsyncMock()
            mock_stack.enter_async_context = AsyncMock(
                side_effect=RuntimeError("connection failed")
            )

            async def track_cleanup():
                cleanup_calls.append(1)

            mock_stack.aclose = track_cleanup
            mock_stack_cls.return_value = mock_stack

            with patch("fastmcp.Client"):
                with self.assertRaises(ConnectionError):
                    await client.start("http://bad", "token")

        # Cleanup should have been called for each failed attempt
        self.assertEqual(len(cleanup_calls), 2)

    async def test_tools_schemas_property(self):
        """tools_schemas property returns the internal list."""
        client = MCPServerClient(alias="test")
        schemas = [{"type": "function", "function": {"name": "test__tool"}}]
        client._tools_schemas = schemas
        self.assertIs(client.tools_schemas, schemas)

    async def test_scoped_names_property(self):
        """scoped_names property returns the internal set."""
        client = MCPServerClient(alias="test")
        names = {"test__a", "test__b"}
        client._scoped_names = names
        self.assertIs(client.scoped_names, names)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
