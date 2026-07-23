"""Tool registry — holds registered tools and produces schemas for LLM context."""

from __future__ import annotations

from typing import Any, Callable

from ..tools.base import BaseTool


class ToolRegistry:
    """Registry of :class:`BaseTool` instances available to an agent.

    Usage::

        registry = ToolRegistry()
        registry.register(MyTool())
        tool = registry.get("my_tool")
        schemas = registry.list_tools()   # OpenAI-compatible function schemas
    """

    def __init__(self) -> None:
        self._tools: dict[str, BaseTool] = {}

    # -- mutation ------------------------------------------------------------

    def register(self, tool: BaseTool | Callable) -> None:
        """Register a tool.

        *tool* can be a :class:`BaseTool` instance **or** a plain callable
        (sync or async).  Callables are automatically wrapped in
        :class:`~dragon.ai.agent.tools.function_tool.FunctionTool`.

        Overwrites any existing tool with the same name.
        """
        if callable(tool) and not isinstance(tool, BaseTool):
            from ..tools.function_tool import FunctionTool
            tool = FunctionTool(tool)
        self._tools[tool.name] = tool

    def unregister(self, name: str) -> None:
        """Remove a tool by name.  No-op if not present."""
        self._tools.pop(name, None)

    # -- lookup --------------------------------------------------------------

    def get(self, name: str) -> BaseTool:
        """Return the tool registered under *name*.

        Raises
        ------
        KeyError
            If no tool with that name is registered.
        """
        if name not in self._tools:
            raise KeyError(f"Tool '{name}' is not registered.")
        return self._tools[name]

    def has(self, name: str) -> bool:
        """Return ``True`` if a tool with *name* is registered."""
        return name in self._tools

    # -- schema generation ---------------------------------------------------

    def list_tools(self) -> list[dict[str, Any]]:
        """Return a list of OpenAI-compatible tool/function schemas.

        These schemas are injected into the LLM system prompt so the model
        can decide which tools to call.
        """
        return [tool.to_schema() for tool in self._tools.values()]

    # -- introspection -------------------------------------------------------

    def tool_names(self) -> list[str]:
        """Return sorted list of registered tool names."""
        return sorted(self._tools.keys())

    def __len__(self) -> int:
        return len(self._tools)

    def __repr__(self) -> str:
        return f"ToolRegistry(tools={self.tool_names()})"

    # -- decorator shortcut --------------------------------------------------

    def tool(self, fn: Callable) -> "FunctionTool":
        """Decorator that wraps *fn* as a :class:`FunctionTool` and registers it.

        Works with both sync and async functions::

            registry = ToolRegistry()

            @registry.tool
            def calculate_magic_addition(a: int, b: int) -> int:
                \"\"\"Add two numbers with a magic twist.\"\"\"
                return a + b + 5

            @registry.tool
            async def fetch_data(url: str) -> dict:
                \"\"\"Fetch data from a URL.\"\"\"
                return await do_fetch(url)

        :returns: The created :class:`FunctionTool` instance.
        """
        from ..tools.function_tool import FunctionTool
        tool_instance = FunctionTool(fn)
        self.register(tool_instance)
        return tool_instance
