"""Shared ANSI terminal colour utilities.

Used by both the trace viewer (``observability/``) and the HITL approval
client (``hitl/``) so the colour palette and helpers live in one place.
"""

from __future__ import annotations

import re

__all__ = ["Color", "colorize", "ANSI_RE", "visible_len", "strip_ansi"]


class Color:
    """ANSI escape codes for terminal colouring."""

    RESET    = "\033[0m"
    GREEN    = "\033[32m"
    YELLOW   = "\033[33m"
    RED      = "\033[31m"
    BOLD_RED = "\033[1;31m"
    CYAN     = "\033[36m"
    DIM      = "\033[2m"
    BOLD     = "\033[1m"

    @classmethod
    def disable(cls) -> None:
        """Replace all codes with empty strings (e.g. when piping)."""
        for attr in ("RESET", "GREEN", "YELLOW", "RED", "BOLD_RED",
                      "CYAN", "DIM", "BOLD"):
            setattr(cls, attr, "")


def colorize(text: str, color: str) -> str:
    """Wrap *text* in an ANSI colour sequence."""
    return f"{color}{text}{Color.RESET}"


# Pre-compiled regex to strip ANSI escape sequences.
ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def visible_len(text: str) -> int:
    """Return the visible width of *text* (stripping ANSI escapes)."""
    return len(ANSI_RE.sub("", text))


def strip_ansi(text: str) -> str:
    """Remove all ANSI escape sequences from *text*."""
    return ANSI_RE.sub("", text)
