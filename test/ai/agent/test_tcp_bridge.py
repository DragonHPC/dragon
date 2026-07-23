"""Tests for HitlTcpBridge — init, shutdown, and guarded queue put."""

import dragon
import multiprocessing as mp


import json
import socket
import threading
import time
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

from dragon.ai.agent.hitl.models import HumanApprovalRequest, HumanApprovalResponse


class TestHitlTcpBridgeInit(TestCase):
    """Verify HitlTcpBridge construction, address binding, and daemon flag."""

    def test_construction_and_address(self):
        """Bridge binds to a free port; address is a (host, port) tuple."""
        from dragon.ai.agent.hitl.tcp_bridge import HitlTcpBridge

        mock_queue = MagicMock()
        bridge = HitlTcpBridge(mock_queue, host="127.0.0.1", port=0)
        host, port = bridge.address
        self.assertEqual(host, "127.0.0.1")
        self.assertIsInstance(port, int)
        self.assertGreater(port, 0)
        # Not started — only set the shutdown flag (don't join)
        bridge._shutdown.set()

    def test_is_daemon_thread(self):
        """Bridge runs as a daemon thread (dies with the parent process)."""
        from dragon.ai.agent.hitl.tcp_bridge import HitlTcpBridge

        mock_queue = MagicMock()
        bridge = HitlTcpBridge(mock_queue, host="127.0.0.1", port=0)
        self.assertTrue(bridge.daemon)
        bridge._shutdown.set()


class TestHitlTcpBridgeStop(TestCase):
    """Verify stop() sets the shutdown event."""

    def test_stop_sets_shutdown(self):
        """stop() sets _shutdown event and the thread exits."""
        from dragon.ai.agent.hitl.tcp_bridge import HitlTcpBridge

        mock_queue = MagicMock()
        bridge = HitlTcpBridge(mock_queue, host="127.0.0.1", port=0)
        bridge.start()  # must start before stop() can join
        self.assertFalse(bridge._shutdown.is_set())
        bridge.stop()
        self.assertTrue(bridge._shutdown.is_set())
        bridge.join(timeout=3)


class TestHitlTcpBridgeClientInteraction(TestCase):
    """Integration-style test: start the bridge, connect a TCP client, and
    exercise the approve/reject flow.
    """

    def _start_bridge(self, hitl_queue: MagicMock):
        import queue as _queue
        from dragon.ai.agent.hitl.tcp_bridge import HitlTcpBridge

        bridge = HitlTcpBridge(hitl_queue, host="127.0.0.1", port=0)
        bridge.start()
        return bridge

    def test_approve_flow(self):
        """Bridge relays request to TCP client, client approves, bridge puts response."""
        import queue as _queue

        request = HumanApprovalRequest(
            tool_name="deploy", tool_args={"target": "prod"},
            context="Deploying", agent_id="a", task_id="t", dispatch_id="d",
        )
        response_queue = MagicMock()

        hitl_queue = MagicMock()

        def _get_side_effect(timeout=None):
            if not hasattr(_get_side_effect, "_called"):
                _get_side_effect._called = True
                return (request, response_queue)
            raise _queue.Empty()

        hitl_queue.get = MagicMock(side_effect=_get_side_effect)

        bridge = self._start_bridge(hitl_queue)
        time.sleep(0.1)  # let bridge bind

        try:
            host, port = bridge.address
            sock = socket.create_connection((host, port), timeout=2)
            rfile = sock.makefile("r")
            wfile = sock.makefile("w")

            # Read the forwarded request
            line = rfile.readline()
            data = json.loads(line)
            self.assertEqual(data["tool_name"], "deploy")

            # Send approval
            wfile.write(json.dumps({"approved": True, "reason": "ok"}) + "\n")
            wfile.flush()

            # Give bridge time to process
            time.sleep(0.3)

            # Verify response_queue.put was called with an approved response
            response_queue.put.assert_called_once()
            resp = response_queue.put.call_args[0][0]
            self.assertIsInstance(resp, HumanApprovalResponse)
            self.assertTrue(resp.approved)

            sock.close()
        finally:
            bridge.stop()
            bridge.join(timeout=3)

    def test_destroyed_queue_put_does_not_crash_bridge(self):
        """Bridge survives when response_queue.put raises (queue destroyed by agent)."""
        import queue as _queue

        request = HumanApprovalRequest(
            tool_name="tool", tool_args={}, context="",
            agent_id="a", task_id="t", dispatch_id="d",
        )
        response_queue = MagicMock()
        # Simulate destroyed queue — put raises
        response_queue.put = MagicMock(side_effect=RuntimeError("destroyed"))

        hitl_queue = MagicMock()

        def _get_side_effect(timeout=None):
            if not hasattr(_get_side_effect, "_called"):
                _get_side_effect._called = True
                return (request, response_queue)
            raise _queue.Empty()

        hitl_queue.get = MagicMock(side_effect=_get_side_effect)

        bridge = self._start_bridge(hitl_queue)
        time.sleep(0.1)

        try:
            host, port = bridge.address
            sock = socket.create_connection((host, port), timeout=2)
            rfile = sock.makefile("r")
            wfile = sock.makefile("w")

            # Read request
            rfile.readline()

            # Send response (will fail on put because queue is "destroyed")
            wfile.write(json.dumps({"approved": True}) + "\n")
            wfile.flush()
            time.sleep(0.3)

            # Bridge should still be alive (not crashed)
            self.assertTrue(bridge.is_alive())

            sock.close()
        finally:
            bridge.stop()
            bridge.join(timeout=3)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
