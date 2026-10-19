"""dragon.ai.langgraph — Dragon HPC backend for LangGraph.

Allows existing LangGraph multi-agent graphs to run on a supercomputer
cluster with zero changes to graph definitions or node functions.

Public API
----------
DragonExecutor     — launches Dragon AgentHost processes and exposes node callables.
get_llm            — per-host accessor for a Dragon Inference LLM proxy, for use
                     inside agent node functions.
"""

from .executor import DragonExecutor
from .llm import get_llm

__all__ = [
    "DragonExecutor",
    "get_llm",
]
