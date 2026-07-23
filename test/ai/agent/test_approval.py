"""Tests for HITL approval flow — request_human_approval()."""

import dragon
import multiprocessing as mp


import asyncio
import queue as _queue
from unittest import IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch, PropertyMock

from dragon.ai.agent.hitl.models import HumanApprovalResponse
from dragon.ai.agent.config import TaskStatus


class TestRequestHumanApproval(IsolatedAsyncioTestCase):
    """Test the async approval flow with mocked Dragon Queue."""

    def _make_ddict(self) -> dict:
        return {}

    def _make_hitl_queue(self) -> MagicMock:
        q = MagicMock()
        q.put = MagicMock()
        return q

    async def test_approved_flow(self):
        """Approved response is returned and queue is destroyed."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(approved=True, reason="Looks good")

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            result = await request_human_approval(
                ddict=ddict,
                hitl_queue=hitl_queue,
                task_id="t1",
                agent_id="a1",
                dispatch_id="d1",
                tool_name="search",
                tool_args={"q": "test"},
                context="Testing approval",
            )

        self.assertTrue(result.approved)
        self.assertEqual(result.reason, "Looks good")
        hitl_queue.put.assert_called_once()
        mock_rq.destroy.assert_called_once()

    async def test_rejected_flow(self):
        """Rejected response is returned with reason preserved."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(approved=False, reason="Too risky")

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            result = await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="deploy", tool_args={},
            )

        self.assertFalse(result.approved)
        self.assertEqual(result.reason, "Too risky")

    async def test_feedback_flow(self):
        """Feedback response carries is_feedback=True for LLM retry."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(
            approved=False, reason="Use limit=5", is_feedback=True,
        )

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            result = await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="query", tool_args={},
            )

        self.assertTrue(result.is_feedback)

    async def test_status_transitions(self):
        """Status should go WAITING → PROCESSING."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            from dragon.ai.agent.config import STATUS_KEY

            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="tool", tool_args={},
            )

        # Status should end as PROCESSING
        status_key = STATUS_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        self.assertEqual(ddict[status_key], TaskStatus.PROCESSING)

    async def test_queue_created_with_right_size(self):
        """Response queue is created with maxsize=1 and block_size=2048."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq) as queue_cls:
            from dragon.ai.agent.hitl.approval import request_human_approval
            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="tool", tool_args={},
            )
            queue_cls.assert_called_once_with(maxsize=1, block_size=2048)

    async def test_exception_restores_status_and_destroys_queue(self):
        """On failure, status must be restored and queue destroyed."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()

        mock_rq = MagicMock()
        mock_rq.get.side_effect = _queue.Empty()
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            from dragon.ai.agent.config import STATUS_KEY

            with self.assertRaises(_queue.Empty):
                await request_human_approval(
                    ddict=ddict, hitl_queue=hitl_queue,
                    task_id="t1", agent_id="a1", dispatch_id="d1",
                    tool_name="tool", tool_args={},
                )

        # Status restored to PROCESSING
        status_key = STATUS_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        self.assertEqual(ddict[status_key], TaskStatus.PROCESSING)
        # Queue destroyed in finally
        mock_rq.destroy.assert_called_once()

    async def test_ddict_writes_request_and_response(self):
        """Both the approval request and response are persisted to DDict."""
        ddict = self._make_ddict()
        hitl_queue = self._make_hitl_queue()
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            from dragon.ai.agent.config import HITL_REQUEST_KEY, HITL_RESPONSE_KEY

            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="search", tool_args={"q": "test"},
            )

        req_key = HITL_REQUEST_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        resp_key = HITL_RESPONSE_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        self.assertIn(req_key, ddict)
        self.assertEqual(ddict[req_key].tool_name, "search")
        self.assertIn(resp_key, ddict)
        self.assertTrue(ddict[resp_key].approved)


class TestRequestHumanApprovalEdgeCases(IsolatedAsyncioTestCase):
    """Verify approval request edge cases."""

    async def test_request_stores_all_fields(self):
        """The approval request object has all expected fields."""
        ddict = {}
        hitl_queue = MagicMock()
        hitl_queue.put = MagicMock()
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            from dragon.ai.agent.config import HITL_REQUEST_KEY

            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="deploy", tool_args={"env": "prod"},
                context="Deploying to production",
            )

        req_key = HITL_REQUEST_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        req = ddict[req_key]
        self.assertEqual(req.tool_name, "deploy")
        self.assertEqual(req.tool_args, {"env": "prod"})
        self.assertEqual(req.context, "Deploying to production")
        self.assertEqual(req.agent_id, "a1")
        self.assertEqual(req.task_id, "t1")
        self.assertEqual(req.dispatch_id, "d1")

    async def test_hitl_queue_receives_request_and_response_queue(self):
        """The hitl_queue.put receives a (request, response_queue) tuple."""
        ddict = {}
        hitl_queue = MagicMock()
        put_args = []
        hitl_queue.put = MagicMock(side_effect=lambda x: put_args.append(x))
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval

            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="tool", tool_args={},
            )

        self.assertEqual(len(put_args), 1)
        req, rq = put_args[0]
        self.assertEqual(req.tool_name, "tool")
        self.assertIs(rq, mock_rq)

    async def test_empty_context_default(self):
        """Context defaults to empty string when not provided."""
        ddict = {}
        hitl_queue = MagicMock()
        hitl_queue.put = MagicMock()
        response = HumanApprovalResponse(approved=True)

        mock_rq = MagicMock()
        mock_rq.get.return_value = response
        mock_rq.destroy = MagicMock()

        with patch("dragon.ai.agent.hitl.approval.Queue", return_value=mock_rq):
            from dragon.ai.agent.hitl.approval import request_human_approval
            from dragon.ai.agent.config import HITL_REQUEST_KEY

            await request_human_approval(
                ddict=ddict, hitl_queue=hitl_queue,
                task_id="t1", agent_id="a1", dispatch_id="d1",
                tool_name="tool", tool_args={},
            )

        req_key = HITL_REQUEST_KEY.format(task_id="t1", agent_id="a1", dispatch_id="d1")
        self.assertEqual(ddict[req_key].context, "")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
