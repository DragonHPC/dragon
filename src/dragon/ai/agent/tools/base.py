"""Abstract base class for tools that agents can invoke inline."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseTool(ABC):
    """Base class for all tools available to Dragon agents.

    Subclasses must set ``name`` and ``description`` as class-level attributes
    (or properties) and implement :meth:`run`.

    Example
    -------
    ::

        class WebSearch(BaseTool):
            name = "web_search"
            description = "Search the web and return top results."

            def run(self, input: dict) -> dict:
                query = input["query"]
                ...
                return {"results": [...]}
    """

    name: str
    description: str

    @abstractmethod
    def run(self, input: dict[str, Any]) -> dict[str, Any]:
        """Execute the tool with the given *input* and return a result dict."""

    def to_schema(self) -> dict[str, Any]:
        """Return an OpenAI-compatible tool/function schema.

        Override this method to provide detailed parameter schemas.  The
        default implementation returns a minimal schema derived from
        ``name`` and ``description``.
        """
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": {},
                },
            },
        }
