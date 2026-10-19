"""MCP server client — manages one persistent MCP server connection."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any

from ..utils.logging import get_agent_logger

log = get_agent_logger("mcp_tool")


class MCPServerClient:
    """Client that owns one persistent connection to a single MCP server.

    Each instance represents **one server**.  Tools from that server are
    exposed to the LLM with names scoped as ``{alias}__{tool_name}``
    (double-underscore separator) so that multiple servers can be connected
    simultaneously without name collisions.

    Parameters
    ----------
    alias:
        Short, unique label for this server (e.g. ``"jupyter"``,
        ``"filesystem"``).  Used as the routing prefix in tool names.
    max_retries:
        Number of connection attempts before raising ``ConnectionError``.
    retry_delay:
        Seconds to wait between retry attempts.
    timeout:
        Per-attempt connection timeout in seconds.

    Example
    -------
    ::

        client = MCPServerClient(alias="jupyter")
        await client.start("http://mcp-server/mcp", token)
        result = await client.call_tool("jupyter__create_notebook", {"name": "demo"})
        await client.close()
    """

    def __init__(
        self,
        alias: str,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        timeout: float = 5.0,
    ) -> None:
        self.alias = alias
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

        self._mcp_exit_stack: AsyncExitStack | None = None
        self._mcp_session = None
        self._tools_schemas: list[dict[str, Any]] = []
        self._scoped_names: set[str] = set()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def start(self, url: str, token: str) -> None:
        """Connect to the MCP server and cache its tool schemas.

        :param url: URL of the MCP server.
        :param token: Authentication token.
        :raises ConnectionError: If all retry attempts fail.
        """
        if self._mcp_session is not None:
            return  # already connected

        from fastmcp import Client

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._mcp_exit_stack = AsyncExitStack()
                self._mcp_session = await self._mcp_exit_stack.enter_async_context(
                    Client(url, auth=token, timeout=self.timeout)
                )
                await self._mcp_session.initialize()

                # Fetch and cache tool definitions for vLLM, scoped by alias
                mcp_tools = await self._mcp_session.list_tools()
                self._tools_schemas = [
                    {
                        "type": "function",
                        "function": {
                            "name": f"{self.alias}__{t.name}",
                            "description": t.description,
                            "parameters": t.inputSchema,
                        },
                    }
                    for t in mcp_tools
                ]
                self._scoped_names = {
                    f"{self.alias}__{t.name}" for t in mcp_tools
                }
                log.debug(
                    f"MCPServerClient '{self.alias}' connected to {url} "
                    f"with {len(self._tools_schemas)} tools."
                )
                return
            except Exception as e:
                last_error = e
                log.debug(f"[{self.alias}] Attempt {attempt} failed: {e}")

                # Clean up failed connection
                if self._mcp_exit_stack:
                    try:
                        await self._mcp_exit_stack.aclose()
                    except Exception as cleanup_exc:
                        log.warning(
                            "MCP exit stack cleanup failed for '%s' during retry: %s",
                            self.alias, cleanup_exc,
                        )
                    self._mcp_exit_stack = None
                    self._mcp_session = None

                if attempt < self.max_retries:
                    log.debug(f"[{self.alias}] Retrying in {self.retry_delay}s...")
                    await asyncio.sleep(self.retry_delay)

        log.error(
            "MCPServerClient '%s': failed to connect after %d attempts: %s",
            self.alias, self.max_retries, last_error,
            exc_info=last_error,
        )
        raise ConnectionError(
            f"MCPServerClient '{self.alias}': failed to connect after "
            f"{self.max_retries} attempts."
        ) from last_error

    async def close(self) -> None:
        """Close the MCP session and release all resources."""
        if self._mcp_exit_stack:
            await self._mcp_exit_stack.aclose()
            self._mcp_exit_stack = None
            self._mcp_session = None
            self._tools_schemas = []
            self._scoped_names = set()
            log.debug(f"MCPServerClient '{self.alias}' closed.")

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    async def call_tool(self, scoped_name: str, args: dict) -> Any:
        """Call a tool on this server by its scoped name.

        :param scoped_name: Tool name in ``{alias}__{tool_name}`` format.
        :param args: Arguments to pass to the tool.
        :returns: ``structured_content`` from the MCP response if available,
            otherwise the plain-text content joined into a single string.
        :raises RuntimeError: If the session is not started.
        :raises ValueError: If the scoped name does not belong to this client.
        """
        if self._mcp_session is None:
            raise RuntimeError(
                f"MCPServerClient '{self.alias}' is not connected. "
                "Call start() first."
            )
        if scoped_name not in self._scoped_names:
            raise ValueError(
                f"Tool '{scoped_name}' does not belong to alias '{self.alias}'."
            )

        # Strip alias prefix to get the actual MCP tool name
        actual_name = scoped_name[len(self.alias) + 2:]  # len("alias__")
        response = await self._mcp_session.call_tool(actual_name, args)
        # Prefer structured JSON content; fall back to joined plain text so
        # the LLM always receives something readable regardless of server type.
        if response.structured_content is not None:
            return response.structured_content
        return "\n".join(
            c.text for c in response.content if hasattr(c, "text")
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def tools_schemas(self) -> list[dict[str, Any]]:
        """vLLM-compatible tool schema list for this server's tools."""
        return self._tools_schemas

    @property
    def scoped_names(self) -> set[str]:
        """Set of scoped tool names (``{alias}__{tool_name}``) for this server."""
        return self._scoped_names

    def __repr__(self) -> str:
        return (
            f"MCPServerClient(alias={self.alias!r}, "
            f"tools={sorted(self._scoped_names)})"
        )

