"""Tests for HITL data models (HumanApprovalRequest / Response)."""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp

import pickle
import time
from unittest import TestCase, main

from dragon.ai.agent.hitl.models import (
    HumanApprovalRequest,
    HumanApprovalResponse,
)


class TestHumanApprovalRequest(TestCase):
    """Verify HumanApprovalRequest fields, timestamp, and pickle support."""

    def test_construction(self):
        """All required fields are stored correctly."""
        req = HumanApprovalRequest(
            tool_name="deploy",
            tool_args={"target": "prod"},
            context="Deploying to production",
            agent_id="agent1",
            task_id="task1",
            dispatch_id="dispatch1",
        )
        self.assertEqual(req.tool_name, "deploy")
        self.assertEqual(req.tool_args, {"target": "prod"})
        self.assertEqual(req.context, "Deploying to production")
        self.assertEqual(req.agent_id, "agent1")
        self.assertEqual(req.task_id, "task1")
        self.assertEqual(req.dispatch_id, "dispatch1")

    def test_timestamp_auto_generated(self):
        """Request timestamp is auto-set to current time at creation."""
        before = time.time()
        req = HumanApprovalRequest(
            tool_name="t", tool_args={}, context="",
            agent_id="a", task_id="t", dispatch_id="d",
        )
        after = time.time()
        self.assertLessEqual(before, req.timestamp)
        self.assertLessEqual(req.timestamp, after)

    def test_pickle_roundtrip(self):
        """Request survives pickle serialization (needed for Dragon Queue IPC)."""
        req = HumanApprovalRequest(
            tool_name="search", tool_args={"q": "test"}, context="ctx",
            agent_id="a", task_id="t", dispatch_id="d",
        )
        restored = pickle.loads(pickle.dumps(req))
        self.assertEqual(restored.tool_name, req.tool_name)
        self.assertEqual(restored.tool_args, req.tool_args)
        self.assertEqual(restored.timestamp, req.timestamp)


class TestHumanApprovalResponse(TestCase):
    """Verify HumanApprovalResponse for approved, rejected, and feedback modes."""

    def test_approved(self):
        """Approved response with default empty reason and no feedback."""
        resp = HumanApprovalResponse(approved=True)
        self.assertTrue(resp.approved)
        self.assertEqual(resp.reason, "")
        self.assertFalse(resp.is_feedback)

    def test_rejected_with_reason(self):
        """Rejected response stores the rejection reason."""
        resp = HumanApprovalResponse(approved=False, reason="Too risky")
        self.assertFalse(resp.approved)
        self.assertEqual(resp.reason, "Too risky")

    def test_feedback_mode(self):
        """Feedback response carries corrective instructions for the LLM."""
        resp = HumanApprovalResponse(
            approved=False, reason="Try with limit=5", is_feedback=True,
        )
        self.assertTrue(resp.is_feedback)
        self.assertEqual(resp.reason, "Try with limit=5")

    def test_timestamp_auto_generated(self):
        """Timestamp is auto-set to current time at creation."""
        before = time.time()
        resp = HumanApprovalResponse(approved=True)
        after = time.time()
        self.assertLessEqual(before, resp.timestamp)
        self.assertLessEqual(resp.timestamp, after)

    def test_pickle_roundtrip(self):
        """Response survives pickle serialization (needed for Dragon Queue IPC)."""
        resp = HumanApprovalResponse(approved=False, reason="no", is_feedback=True)
        restored = pickle.loads(pickle.dumps(resp))
        self.assertEqual(restored.approved, resp.approved)
        self.assertEqual(restored.reason, resp.reason)
        self.assertEqual(restored.is_feedback, resp.is_feedback)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
