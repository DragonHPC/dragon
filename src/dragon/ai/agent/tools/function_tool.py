"""FunctionTool — wrap any plain Python callable as a BaseTool automatically."""

from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

from .base import BaseTool
from ..utils.logging import get_agent_logger

_log = get_agent_logger("function_tool")


# Mapping from Python built-in types to JSON Schema type strings
_PY_TO_JSON_TYPE: dict[type, str] = {
    int: "integer",
    str: "string",
    float: "number",
    bool: "boolean",
    list: "array",
    dict: "object",
}


def _py_type_to_json(py_type: Any) -> str:
    """Convert a Python type annotation to a JSON Schema type string."""
    return _PY_TO_JSON_TYPE.get(py_type, "string")


class FunctionTool(BaseTool):
    """Wrap any plain Python callable as a :class:`BaseTool`.

    The tool name, description, and parameter schema are derived
    automatically from the function's ``__name__``, ``__doc__``, and
    type annotations — no manual schema writing required.

    Parameter descriptions are extracted from the docstring when written
    in ``:param name: description`` format (Sphinx-style).

    Parameters
    ----------
    fn:
        The callable to wrap.
    name:
        Override the tool name (defaults to ``fn.__name__``).
    description:
        Override the description (defaults to the first line of
        ``fn.__doc__``, or the function name if no docstring).

    Example
    -------
    ::

        def calculate_magic_addition(a: int, b: int) -> int:
            \"\"\"Add two numbers with a magic twist.\"\"\"
            return a + b + 5

        registry = ToolRegistry()
        registry.register(calculate_magic_addition)

    Or using the decorator shortcut::

        @registry.tool
        def calculate_magic_addition(a: int, b: int) -> int:
            \"\"\"Add two numbers with a magic twist.\"\"\"
            return a + b + 5

    Async functions work identically::

        @registry.tool
        async def fetch_data(url: str) -> dict:
            \"\"\"Fetch data from a URL.\"\"\"
            return await do_fetch(url)
    """

    def __init__(
        self,
        fn: Callable,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        self._fn = fn
        self.name = name or fn.__name__
        # Use first non-empty line of docstring as description
        doc = (fn.__doc__ or "").strip()
        self.description = description or (doc.splitlines()[0] if doc else fn.__name__)
        # Parse :param name: description lines from the docstring
        self._param_descriptions = self._parse_param_descriptions(doc)

        # If the wrapped callable is async, override run() with an async
        # version so that inspect.iscoroutinefunction(tool.run) returns True
        # and the dispatcher will ``await`` it.
        if inspect.iscoroutinefunction(fn):
            async def _async_run(input: dict[str, Any]) -> Any:
                return await self._fn(**input)
            self.run = _async_run  # type: ignore[assignment]

    @staticmethod
    def _parse_param_descriptions(doc: str) -> dict[str, str]:
        """Extract parameter descriptions from Sphinx-style ``:param:`` lines."""
        import re
        descriptions: dict[str, str] = {}
        for m in re.finditer(r":param\s+(\w+)\s*:\s*(.+)", doc):
            descriptions[m.group(1)] = m.group(2).strip()
        return descriptions

    def run(self, input: dict[str, Any]) -> Any:
        """Call the wrapped function with ``input`` as keyword arguments."""
        return self._fn(**input)

    def to_schema(self) -> dict[str, Any]:
        """Build an OpenAI-compatible schema from type hints and signature."""
        try:
            # Returns {"param_name": <type>, ...} with string annotations
            # resolved to actual type objects.
            hints = get_type_hints(self._fn)
        except Exception as exc:
            _log.debug("get_type_hints failed for %s: %s", self._fn, exc)
            hints = {}

        # inspect.signature() returns the function's parameter list with
        # names, default values, and kind (positional, keyword, etc.).
        # We iterate over sig.parameters to discover each parameter and
        # check param.default to determine if it's required (no default)
        # or optional (has a default value).
        sig = inspect.signature(self._fn)
        properties: dict[str, Any] = {}
        required: list[str] = []

        for param_name, param in sig.parameters.items():
            if param_name == "return":
                continue

            py_type = hints.get(param_name)
            json_type = _py_type_to_json(py_type) if py_type is not None else "string"

            prop: dict[str, Any] = {"type": json_type}

            # Add description from docstring if available
            if param_name in self._param_descriptions:
                prop["description"] = self._param_descriptions[param_name]

            properties[param_name] = prop

            # Required if there is no default value
            if param.default is inspect.Parameter.empty:
                required.append(param_name)

        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            },
        }

    def __repr__(self) -> str:
        return f"FunctionTool(name={self.name!r}, fn={self._fn.__name__!r})"
