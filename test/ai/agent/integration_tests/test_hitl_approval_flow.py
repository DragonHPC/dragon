"""Integration tests for the HITL (Human-in-the-Loop) approval flow.

These tests verify the full HITL pipeline: agent pauses on tool call →
approval request written to DDict → request forwarded via Queue → TCP bridge
relays to client → client approves/rejects → response flows back → agent
resumes or adapts.

Run with: dragon python -m unittest test.ai.agent.integration_tests.test_hitl_approval_flow -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import socket
import threading
import time
import uuid
from unittest import TestCase, main

from dragon.data.ddict import DDict
from dragon.native.queue import Queue

from dragon.ai.agent.config import (
    TaskStatus,
    STATUS_KEY,
    HITL_REQUEST_KEY,
    HITL_RESPONSE_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.hitl.approval import request_human_approval
from dragon.ai.agent.hitl.models import HumanApprovalRequest, HumanApprovalResponse
from dragon.ai.agent.hitl.tcp_bridge import HitlTcpBridge


# ========================================================================
# Direct approval flow (no TCP bridge)
# ========================================================================

class TestDirectApprovalFlow(TestCase):
    """Verify request_human_approval using real Dragon Queue and DDict."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_approved_flow(self):
        """Approved response is returned; DDict records request and response."""
        agent_id = "agent_a"
        dispatch_id = str(uuid.uuid4())

        # Create the shared HITL queue (as orchestrator would)
        hitl_queue = Queue()

        # Simulate a human operator approving in a background thread
        def _operator():
            request, response_queue = hitl_queue.get(timeout=5)
            self.assertIsInstance(request, HumanApprovalRequest)
            self.assertEqual(request.tool_name, "deploy")
            response_queue.put(HumanApprovalResponse(approved=True, reason="LGTM"))

        operator_thread = threading.Thread(target=_operator)
        operator_thread.start()

        response = asyncio.run(request_human_approval(
            ddict=self.ddict,
            hitl_queue=hitl_queue,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
            tool_name="deploy",
            tool_args={"target": "production"},
            context="Deploying to production",
        ))

        operator_thread.join(timeout=5)

        self.assertTrue(response.approved)
        self.assertEqual(response.reason, "LGTM")

        # Verify DDict records
        fmt = dict(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        req = self.ddict[HITL_REQUEST_KEY.format(**fmt)]
        self.assertIsInstance(req, HumanApprovalRequest)
        self.assertEqual(req.tool_name, "deploy")

        resp = self.ddict[HITL_RESPONSE_KEY.format(**fmt)]
        self.assertIsInstance(resp, HumanApprovalResponse)
        self.assertTrue(resp.approved)

        # Verify status was restored to PROCESSING
        status_key = STATUS_KEY.format(**fmt)
        self.assertEqual(self.ddict[status_key], TaskStatus.PROCESSING)

        hitl_queue.destroy()

    def test_rejected_flow(self):
        """Rejected response is returned with reason preserved."""
        agent_id = "agent_b"
        dispatch_id = str(uuid.uuid4())
        hitl_queue = Queue()

        def _operator():
            request, response_queue = hitl_queue.get(timeout=5)
            response_queue.put(HumanApprovalResponse(
                approved=False, reason="Too risky",
            ))

        operator_thread = threading.Thread(target=_operator)
        operator_thread.start()

        response = asyncio.run(request_human_approval(
            ddict=self.ddict,
            hitl_queue=hitl_queue,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
            tool_name="delete_db",
            tool_args={"table": "users"},
        ))

        operator_thread.join(timeout=5)

        self.assertFalse(response.approved)
        self.assertEqual(response.reason, "Too risky")

        hitl_queue.destroy()

    def test_status_transitions(self):
        """Status transitions: WAITING during approval, PROCESSING after."""
        agent_id = "agent_c"
        dispatch_id = str(uuid.uuid4())
        hitl_queue = Queue()
        fmt = dict(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        status_key = STATUS_KEY.format(**fmt)

        observed_statuses = []

        def _operator():
            request, response_queue = hitl_queue.get(timeout=5)
            # At this point, status should be WAITING
            time.sleep(0.1)  # small delay for DDict write to propagate
            observed_statuses.append(self.ddict[status_key])
            response_queue.put(HumanApprovalResponse(approved=True))

        operator_thread = threading.Thread(target=_operator)
        operator_thread.start()

        asyncio.run(request_human_approval(
            ddict=self.ddict,
            hitl_queue=hitl_queue,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
            tool_name="test_tool",
            tool_args={},
        ))

        operator_thread.join(timeout=5)

        # Operator observed WAITING
        self.assertEqual(observed_statuses[0], TaskStatus.WAITING)

        # After approval, status should be PROCESSING
        self.assertEqual(self.ddict[status_key], TaskStatus.PROCESSING)

        hitl_queue.destroy()


# ========================================================================
# TCP bridge end-to-end flow
# ========================================================================

class TestTcpBridgeApprovalFlow(TestCase):
    """Verify the full TCP bridge flow: Queue → TCP bridge → TCP client → response."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_tcp_approve_flow(self):
        """TCP client receives request via bridge, approves, response flows back."""
        agent_id = "agent_tcp"
        dispatch_id = str(uuid.uuid4())

        hitl_queue = Queue()
        bridge = HitlTcpBridge(hitl_queue, host="127.0.0.1", port=0)
        bridge.start()
        time.sleep(0.2)  # let bridge bind

        host, port = bridge.address

        # Simulate agent putting a request on the HITL queue
        response_queue = Queue(maxsize=1, block_size=2048)
        request = HumanApprovalRequest(
            tool_name="run_command",
            tool_args={"cmd": "ls -la"},
            context="Listing files",
            agent_id=agent_id,
            task_id=self.task_id,
            dispatch_id=dispatch_id,
        )
        hitl_queue.put((request, response_queue))

        # Simulate TCP client connecting and approving
        sock = socket.create_connection((host, port), timeout=5)
        rfile = sock.makefile("r")
        wfile = sock.makefile("w")

        # Read the forwarded request
        line = rfile.readline()
        data = json.loads(line)
        self.assertEqual(data["tool_name"], "run_command")
        self.assertEqual(data["tool_args"]["cmd"], "ls -la")

        # Send approval
        wfile.write(json.dumps({"approved": True, "reason": "ok"}) + "\n")
        wfile.flush()

        # Read back the response from the response queue
        response = response_queue.get(timeout=5)
        self.assertIsInstance(response, HumanApprovalResponse)
        self.assertTrue(response.approved)

        sock.close()
        bridge.stop()
        bridge.join(timeout=3)
        response_queue.destroy()
        hitl_queue.destroy()

    def test_tcp_reject_with_feedback(self):
        """TCP client rejects with feedback, response flows back with is_feedback=True."""
        agent_id = "agent_tcp2"
        dispatch_id = str(uuid.uuid4())

        hitl_queue = Queue()
        bridge = HitlTcpBridge(hitl_queue, host="127.0.0.1", port=0)
        bridge.start()
        time.sleep(0.2)

        host, port = bridge.address

        response_queue = Queue(maxsize=1, block_size=2048)
        request = HumanApprovalRequest(
            tool_name="search",
            tool_args={"query": "test"},
            context="Searching",
            agent_id=agent_id,
            task_id=self.task_id,
            dispatch_id=dispatch_id,
        )
        hitl_queue.put((request, response_queue))

        sock = socket.create_connection((host, port), timeout=5)
        rfile = sock.makefile("r")
        wfile = sock.makefile("w")

        rfile.readline()  # consume request

        # Send feedback (rejection with feedback flag)
        wfile.write(json.dumps({
            "approved": False,
            "reason": "Use a different query",
            "is_feedback": True,
        }) + "\n")
        wfile.flush()

        response = response_queue.get(timeout=5)
        self.assertFalse(response.approved)
        self.assertTrue(response.is_feedback)
        self.assertEqual(response.reason, "Use a different query")

        sock.close()
        bridge.stop()
        bridge.join(timeout=3)
        response_queue.destroy()
        hitl_queue.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
