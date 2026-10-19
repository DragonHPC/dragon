"""Agent-side: request approval and wait for a human operator's decision.

This module provides the async approval request mechanism used by
:class:`~dragon.ai.agent.reasoning.tool_dispatcher.ToolDispatcher` to pause
before executing a tool call and wait for a human operator's decision.

See the package docstring in ``__init__.py`` for the full architecture.
"""

from __future__ import annotations

import asyncio
from typing import Any

from dragon.native.queue import Queue

from ..utils.logging import get_agent_logger
from ..config import (
    HITL_REQUEST_KEY,
    HITL_RESPONSE_KEY,
    STATUS_KEY,
    TaskStatus,
)
from ..ddict import DDictAccessor
from .models import HumanApprovalRequest, HumanApprovalResponse

_log = get_agent_logger("hitl_approval")


async def request_human_approval(
    ddict: Any,
    hitl_queue: Any,
    task_id: str,
    agent_id: str,
    dispatch_id: str,
    tool_name: str,
    tool_args: dict[str, Any],
    context: str = "",
) -> HumanApprovalResponse:
    """Pause the current coroutine until a human operator approves or rejects.

    This function:

    1. Creates a per-request response :class:`~dragon.native.queue.Queue`.
    2. Writes the request payload to DDict (for in-runtime logging).
    3. Updates the agent's status to ``WAITING``.
    4. Puts ``(request, response_queue)`` on the shared ``hitl_queue``.
    5. ``await``s ``response_queue.get()`` (suspends the coroutine).
    6. Writes the operator's response to DDict (for in-runtime logging).
    7. Restores status to ``PROCESSING`` and cleans up.

    The HITL client only interacts via Queues — it never touches DDict.
    This is required because DDict/FLI are incompatible with the Dragon
    Runtime Proxy (see module docstring).

    Parameters
    ----------
    ddict:
        Shared Dragon Distributed Dictionary for the current run.
    hitl_queue:
        Dragon Queue used to send ``(request, response_queue)`` pairs to
        the HITL client.
    task_id:
        Current pipeline run identifier.
    agent_id:
        Identifier of the agent requesting approval.
    dispatch_id:
        Identifier of the specific dispatch (supports concurrent dispatches).
    tool_name:
        Name of the tool the LLM wants to call.
    tool_args:
        Arguments the LLM wants to pass to the tool.
    context:
        Human-readable explanation of why the tool is being called.

    Returns
    -------
    HumanApprovalResponse
        The operator's decision (approved/rejected + optional reason).
    """
    accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)
    fmt = dict(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
    request_key  = HITL_REQUEST_KEY.format(**fmt)
    response_key = HITL_RESPONSE_KEY.format(**fmt)
    status_key   = STATUS_KEY.format(**fmt)

    # 1. Create a per-request response Queue — single-slot, small block size
    #    to minimise GS channel create/destroy overhead.  A HumanApprovalResponse
    #    is a tiny dataclass, so 2 KB is more than enough.
    response_queue = Queue(maxsize=1, block_size=2048)

    # 2. Build and store request in DDict (in-runtime logging)
    request = HumanApprovalRequest(
        tool_name=tool_name,
        tool_args=tool_args,
        context=context,
        agent_id=agent_id,
        task_id=task_id,
        dispatch_id=dispatch_id,
    )
    accessor.put(request_key, request)

    # 3. Update status to WAITING
    accessor.put(status_key, TaskStatus.WAITING)

    _log.info(
        "Agent '%s' requesting approval for tool '%s' [%s]",
        agent_id, tool_name, dispatch_id[:8],
    )

    # 4. Send (request, response_queue) to the HITL client via the shared queue.
    # The client receives the full request object directly — no DDict access
    # needed on the client side (DDict/FLI is incompatible with the proxy).
    await asyncio.to_thread(hitl_queue.put, (request, response_queue))

    try:
        # 5. Await response — suspends this coroutine, event loop stays free.
        response: HumanApprovalResponse = await asyncio.to_thread(
            response_queue.get,
        )

        status = "APPROVED" if response.approved else f"REJECTED ({response.reason})"
        _log.info(
            "Agent '%s' tool '%s' -> %s [%s]",
            agent_id, tool_name, status, dispatch_id[:8],
        )

        # 6. Store response in DDict (in-runtime logging)
        accessor.put(response_key, response)

        # 7. Restore status
        accessor.put(status_key, TaskStatus.PROCESSING)
        # HITL_REQUEST_KEY and HITL_RESPONSE_KEY are intentionally preserved
        # in DDict for post-mortem analysis and trace viewer visibility.
    except Exception:
        # On timeout or any other failure, restore status so the agent is
        # not stuck in WAITING and re-raise so the caller can handle it.
        accessor.put(status_key, TaskStatus.PROCESSING)
        raise
    finally:
        # Always destroy the per-request response queue to avoid GS leaks.
        response_queue.destroy()

    return response
