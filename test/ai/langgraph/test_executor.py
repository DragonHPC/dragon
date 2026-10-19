"""Tests for DragonExecutor — manages Dragon AgentHost processes."""

import dragon
import multiprocessing as mp

import queue as _queue
from unittest import TestCase, main
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_agent(state: dict) -> dict:
    """Simple test agent that echoes input."""
    return {"messages": ["processed"]}


def _make_mock_queue() -> MagicMock:
    """Create a mock Queue that returns serialized bytes on get()."""
    mock_queue = MagicMock()
    mock_queue.get.return_value = b"mock_input_queue_bytes"
    return mock_queue


def _make_mock_process() -> MagicMock:
    """Create a mock Process."""
    mock_proc = MagicMock()
    mock_proc.start = MagicMock()
    mock_proc.join = MagicMock()
    mock_proc.kill = MagicMock()
    return mock_proc


# ========================================================================
# DragonExecutor Construction
# ========================================================================

class TestDragonExecutorConstruction(TestCase):
    """Verify DragonExecutor construction and parameter validation."""

    def test_default_construction(self):
        """Executor can be created with default parameters."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher"):
            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            self.assertIsNotNone(executor)

    def test_max_concurrent_tasks_validation(self):
        """max_concurrent_tasks < 1 raises ValueError."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher"):
            from dragon.ai.langgraph.executor import DragonExecutor
            with self.assertRaisesRegex(ValueError, "max_concurrent_tasks"):
                DragonExecutor(max_concurrent_tasks=0)

    def test_num_shards_validation(self):
        """num_shards < 1 raises ValueError."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher"):
            from dragon.ai.langgraph.executor import DragonExecutor
            with self.assertRaisesRegex(ValueError, "num_shards"):
                DragonExecutor(num_shards=0)

    def test_stores_configuration(self):
        """Executor stores provided configuration parameters."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            mock_watcher.max_concurrent_tasks = 128
            mock_watcher.num_shards = 4
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor(max_concurrent_tasks=128, num_shards=4)

            self.assertEqual(executor.max_concurrent_tasks, 128)
            self.assertEqual(executor.num_shards, 4)


# ========================================================================
# DragonExecutor.launch_host
# ========================================================================

class TestDragonExecutorLaunchHost(TestCase):
    """Verify launch_host spawns AgentHost processes and registers nodes."""

    def test_starts_dragon_process(self):
        """launch_host starts a Dragon Process."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"test": _simple_agent})

            MockProcess.assert_called_once()
            mock_proc.start.assert_called_once()

    def test_registers_nodes(self):
        """launch_host registers DragonAgentNode for each agent."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={
                "researcher": _simple_agent,
                "writer": _simple_agent,
            })

            # Both nodes should be registered
            self.assertIn("researcher", executor._nodes)
            self.assertIn("writer", executor._nodes)

    def test_duplicate_node_name_raises(self):
        """Registering same node_name twice raises ValueError."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"agent_a": _simple_agent})

            with self.assertRaisesRegex(ValueError, "already registered"):
                executor.launch_host(agents={"agent_a": _simple_agent})

    def test_returns_process(self):
        """launch_host returns the Dragon Process object."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            proc = executor.launch_host(agents={"test": _simple_agent})

            self.assertIs(proc, mock_proc)

    def test_async_agent_allowed(self):
        """Async (async def) node functions are accepted — the host drives them
        to completion, so an existing async LangGraph graph runs unchanged."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            async def _async_agent(state):
                return {"messages": ["async ok"]}

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"async_node": _async_agent})

            self.assertIn("async_node", executor._nodes)
            MockProcess.assert_called_once()


# ========================================================================
# DragonExecutor.node
# ========================================================================

class TestDragonExecutorNode(TestCase):
    """Verify node() returns LangGraph-compatible callable."""

    def test_returns_runnable_callable(self):
        """node() returns a RunnableCallable wrapping the agent node."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue, \
             patch("langgraph.utils.runnable.RunnableCallable") as MockRunnable:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            mock_runnable = MagicMock()
            MockRunnable.return_value = mock_runnable

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"test_node": _simple_agent})

            result = executor.node("test_node")

            MockRunnable.assert_called_once()
            self.assertIs(result, mock_runnable)

    def test_unknown_node_raises_keyerror(self):
        """node() raises KeyError for unregistered node names."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            with self.assertRaisesRegex(KeyError, "No agent"):
                executor.node("nonexistent")

    def test_node_lists_available_in_error(self):
        """KeyError message lists available nodes."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"available": _simple_agent})

            with self.assertRaises(KeyError) as ctx:
                executor.node("missing")

            self.assertIn("available", str(ctx.exception))


# ========================================================================
# DragonExecutor.shutdown
# ========================================================================

class TestDragonExecutorShutdown(TestCase):
    """Verify shutdown gracefully terminates hosts and watcher."""

    def test_graceful_shutdown_sends_sentinels(self):
        """Graceful shutdown sends None sentinel to each host queue."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"test": _simple_agent})

            with patch("dragon.ai.langgraph.executor.Queue") as MockQueueAttach:
                mock_host_queue = MagicMock()
                MockQueueAttach.attach.return_value = mock_host_queue

                executor.shutdown(graceful=True)

                mock_host_queue.put.assert_called_once_with(None)

    def test_shutdown_stops_watcher(self):
        """Shutdown calls stop() on the watcher."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            # The watcher is built in __init__, so it exists to stop.
            executor = DragonExecutor()
            executor.shutdown()

            mock_watcher.stop.assert_called_once()

    def test_shutdown_clears_state(self):
        """Shutdown clears internal process and node registries."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"test": _simple_agent})

            # Verify state exists
            self.assertEqual(len(executor._nodes), 1)
            self.assertEqual(len(executor._procs), 1)

            executor.shutdown()

            # State should be cleared
            self.assertEqual(len(executor._nodes), 0)
            self.assertEqual(len(executor._procs), 0)

    def test_forced_shutdown_kills_processes(self):
        """Non-graceful shutdown kills processes immediately."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"test": _simple_agent})

            executor.shutdown(graceful=False)

            mock_proc.kill.assert_called()


# ========================================================================
# DragonExecutor Context Manager
# ========================================================================

class TestDragonExecutorContextManager(TestCase):
    """Verify DragonExecutor works as a context manager."""

    def test_enter_returns_self(self):
        """__enter__ returns the executor instance."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            result = executor.__enter__()

            self.assertIs(result, executor)

    def test_exit_calls_shutdown(self):
        """__exit__ calls shutdown()."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            with patch.object(executor, "shutdown") as mock_shutdown:
                executor.__exit__(None, None, None)

            mock_shutdown.assert_called_once()

    def test_with_statement(self):
        """Executor can be used in a with statement."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor

            shutdown_called = []

            with DragonExecutor() as executor:
                # Patch shutdown after entering
                original_shutdown = executor.shutdown
                def track_shutdown(*args, **kwargs):
                    shutdown_called.append(True)
                    original_shutdown(*args, **kwargs)
                executor.shutdown = track_shutdown

            self.assertEqual(len(shutdown_called), 1)


# ========================================================================
# DragonExecutor Multiple Hosts
# ========================================================================

class TestDragonExecutorMultipleHosts(TestCase):
    """Verify DragonExecutor handles multiple AgentHost processes."""

    def test_multiple_hosts_separate_processes(self):
        """Multiple launch_host calls create separate processes."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_procs = [_make_mock_process(), _make_mock_process()]
            MockProcess.side_effect = mock_procs

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"agent_a": _simple_agent})
            executor.launch_host(agents={"agent_b": _simple_agent})

            self.assertEqual(len(executor._procs), 2)
            self.assertEqual(MockProcess.call_count, 2)

    def test_unique_names_across_hosts(self):
        """Agent names must be unique across all hosts."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"shared_name": _simple_agent})

            with self.assertRaisesRegex(ValueError, "already registered"):
                executor.launch_host(agents={"shared_name": _simple_agent})


# ========================================================================
# DragonExecutor.launch_hosts (batch convenience)
# ========================================================================

class TestDragonExecutorLaunchHosts(TestCase):
    """Verify launch_hosts() spawns multiple hosts with corresponding policies."""

    def test_launches_multiple_hosts(self):
        """launch_hosts creates one process per entry in hosts list."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_procs = [_make_mock_process(), _make_mock_process(), _make_mock_process()]
            MockProcess.side_effect = mock_procs

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            procs = executor.launch_hosts(
                hosts=[
                    {"agent_a": _simple_agent},
                    {"agent_b": _simple_agent},
                    {"agent_c": _simple_agent},
                ],
            )

            self.assertEqual(len(procs), 3)
            self.assertEqual(len(executor._procs), 3)
            self.assertEqual(MockProcess.call_count, 3)

    def test_applies_policies_to_corresponding_hosts(self):
        """Each policy in the list is applied to the corresponding host."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_procs = [_make_mock_process(), _make_mock_process()]
            MockProcess.side_effect = mock_procs

            policy_0 = MagicMock(name="policy_node0")
            policy_1 = MagicMock(name="policy_node1")

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            procs = executor.launch_hosts(
                hosts=[
                    {"researcher": _simple_agent},
                    {"writer": _simple_agent},
                ],
                policies=[policy_0, policy_1],
            )

            # Verify policies were passed to Process
            calls = MockProcess.call_args_list
            self.assertEqual(calls[0].kwargs["policy"], policy_0)
            self.assertEqual(calls[1].kwargs["policy"], policy_1)

    def test_policies_none_uses_default_placement(self):
        """When policies=None, all hosts use default (None) placement."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_procs = [_make_mock_process(), _make_mock_process()]
            MockProcess.side_effect = mock_procs

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            procs = executor.launch_hosts(
                hosts=[
                    {"agent_a": _simple_agent},
                    {"agent_b": _simple_agent},
                ],
            )

            # Both should have policy=None
            calls = MockProcess.call_args_list
            self.assertIsNone(calls[0].kwargs["policy"])
            self.assertIsNone(calls[1].kwargs["policy"])

    def test_mismatched_policies_length_raises(self):
        """ValueError raised when len(policies) != len(hosts)."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            with self.assertRaisesRegex(ValueError, "len\\(policies\\)=1 must equal len\\(hosts\\)=2"):
                executor.launch_hosts(
                    hosts=[
                        {"agent_a": _simple_agent},
                        {"agent_b": _simple_agent},
                    ],
                    policies=[MagicMock()],
                )

    def test_registers_all_agents_across_hosts(self):
        """All agents from all hosts are registered and accessible via node()."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_procs = [_make_mock_process(), _make_mock_process()]
            MockProcess.side_effect = mock_procs

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_hosts(
                hosts=[
                    {"researcher": _simple_agent},
                    {"analyzer": _simple_agent, "writer": _simple_agent},
                ],
            )

            self.assertIn("researcher", executor._nodes)
            self.assertIn("analyzer", executor._nodes)
            self.assertIn("writer", executor._nodes)

    def test_duplicate_name_across_hosts_raises(self):
        """Duplicate agent name across hosts raises ValueError."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            mock_proc = _make_mock_process()
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            with self.assertRaisesRegex(ValueError, "already registered"):
                executor.launch_hosts(
                    hosts=[
                        {"agent_a": _simple_agent},
                        {"agent_a": _simple_agent},  # duplicate
                    ],
                )

    def test_returns_processes_in_order(self):
        """Returned process list matches the order of hosts."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:

            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            mock_queue = _make_mock_queue()
            MockQueue.return_value = mock_queue

            proc_a = _make_mock_process()
            proc_b = _make_mock_process()
            MockProcess.side_effect = [proc_a, proc_b]

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            procs = executor.launch_hosts(
                hosts=[
                    {"agent_a": _simple_agent},
                    {"agent_b": _simple_agent},
                ],
            )

            self.assertIs(procs[0], proc_a)
            self.assertIs(procs[1], proc_b)


# ========================================================================
# DragonExecutor.shutdown — edge cases & cleanup resilience
# ========================================================================

class TestDragonExecutorShutdownEdgeCases(TestCase):
    """Verify shutdown kills un-exited hosts and never lets cleanup errors escape."""

    def test_graceful_shutdown_kills_process_that_did_not_exit(self):
        """If a host does not exit within the join timeout, it is killed."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            mock_proc = _make_mock_process()
            mock_proc.join.return_value = None  # never exited
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})
            executor.shutdown(graceful=True)

            mock_proc.kill.assert_called()

    def test_graceful_shutdown_skips_kill_when_process_exited(self):
        """If join returns an exit code, the process is left alone (not killed)."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            mock_proc = _make_mock_process()
            mock_proc.join.return_value = 0  # exited cleanly
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})
            executor.shutdown(graceful=True)

            mock_proc.kill.assert_not_called()

    def test_shutdown_swallows_sentinel_error_and_still_stops_watcher(self):
        """A failure sending the shutdown sentinel must not abort cleanup."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})

            with patch("dragon.ai.langgraph.executor.Queue") as MockQ2:
                MockQ2.attach.side_effect = RuntimeError("attach boom")
                # Must not raise despite the sentinel failure — but a host that
                # never got its sentinel is a leak, so it has to be reported.
                with self.assertLogs("LANGGRAPH.executor", level="WARNING") as logs:
                    executor.shutdown(graceful=True)

            self.assertIn("attach boom", "\n".join(logs.output))
            mock_watcher.stop.assert_called_once()

    def test_shutdown_reports_a_host_it_could_not_clean_up(self):
        """A host left running after shutdown is an orphaned process, so the
        failure is an error rather than a warning."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher
            MockQueue.return_value = _make_mock_queue()
            mock_proc = _make_mock_process()
            mock_proc.join.return_value = None
            mock_proc.kill.side_effect = RuntimeError("kill boom")
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})

            with self.assertLogs("LANGGRAPH.executor", level="ERROR") as logs:
                executor.shutdown(graceful=True)

            self.assertIn("kill boom", "\n".join(logs.output))
            # Cleanup still finished for everything else.
            mock_watcher.stop.assert_called_once()

    def test_shutdown_swallows_reply_queue_destroy_error(self):
        """A failure destroying a handshake queue must not abort cleanup."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher
            mock_reply = _make_mock_queue()
            mock_reply.destroy.side_effect = RuntimeError("destroy boom")
            MockQueue.return_value = mock_reply
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})
            # Must not raise — but a Dragon Queue left behind is a leak, so the
            # failure has to reach the log.
            with self.assertLogs("LANGGRAPH.executor", level="WARNING") as logs:
                executor.shutdown(graceful=True)

            self.assertIn("destroy boom", "\n".join(logs.output))
            mock_watcher.stop.assert_called_once()

    def test_shutdown_stops_watcher_without_hosts(self):
        """The watcher is built in __init__, so shutdown stops it even when no
        host was launched."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.shutdown()

            mock_watcher.stop.assert_called_once()

    def test_double_shutdown_is_safe(self):
        """Calling shutdown twice is a no-op the second time (watcher already
        stopped and state already cleared)."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            mock_watcher = MagicMock()
            MockWatcher.return_value = mock_watcher
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"t": _simple_agent})

            executor.shutdown()
            executor.shutdown()  # must not raise

            mock_watcher.stop.assert_called_once()
            self.assertEqual(len(executor._nodes), 0)
            self.assertEqual(len(executor._procs), 0)

    def test_launch_host_after_shutdown_raises(self):
        """Shutdown drops the watcher, so a later launch would hand every node a
        dead executor and only fail deep inside graph.invoke. Fail here instead."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.shutdown()

            with self.assertRaisesRegex(RuntimeError, "shut down"):
                executor.launch_host(agents={"t": _simple_agent})
            # And no orphan process or handshake queue was created.
            MockProcess.assert_not_called()
            self.assertEqual(executor._reply_queues, [])

    def test_launch_hosts_after_shutdown_raises(self):
        """The plural form goes through launch_host, so it fails the same way."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.shutdown()

            with self.assertRaisesRegex(RuntimeError, "shut down"):
                executor.launch_hosts(hosts=[{"t": _simple_agent}])


# ========================================================================
# DragonExecutor — AgentHost startup handshake
# ========================================================================

class TestDragonExecutorHandshake(TestCase):
    """A host that dies during startup must not hang launch_host forever.

    Import errors and bad model paths kill the host before it ever sends its
    input-queue handle; a plain blocking get() would then wait for ever with
    nothing to say why.
    """

    def test_dead_host_raises_naming_its_agents(self):
        """The error identifies the host and points at its log."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.executor._HANDSHAKE_POLL_INTERVAL", 0.01):
            MockWatcher.return_value = MagicMock()
            mock_queue = MagicMock()
            mock_queue.get.side_effect = _queue.Empty
            MockQueue.return_value = mock_queue
            mock_proc = _make_mock_process()
            # is_alive is a property on Process, so set it as a plain attribute;
            # a method mock (.return_value) reads truthy and hangs the handshake.
            mock_proc.is_alive = False
            mock_proc.returncode = 1
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()

            with self.assertRaises(RuntimeError) as ctx:
                executor.launch_host(agents={"researcher": _simple_agent})

            message = str(ctx.exception)
            self.assertIn("researcher", message)
            self.assertIn("exited before it was ready", message)
            self.assertIn("log", message)

    def test_slow_but_healthy_host_still_completes(self):
        """Liveness polling is not a deadline — a host that takes several polls
        to import its model still registers normally."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.executor._HANDSHAKE_POLL_INTERVAL", 0.01):
            MockWatcher.return_value = MagicMock()
            mock_queue = MagicMock()
            mock_queue.get.side_effect = [
                _queue.Empty, _queue.Empty, b"mock_input_queue_bytes",
            ]
            MockQueue.return_value = mock_queue
            mock_proc = _make_mock_process()
            mock_proc.is_alive = True
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"researcher": _simple_agent})

            self.assertIn("researcher", executor._nodes)
            self.assertEqual(executor._host_queues, [b"mock_input_queue_bytes"])

    def test_handle_that_landed_as_the_host_exited_is_still_used(self):
        """A host that sends its handle and then dies has done its job; losing
        the race must not turn a good launch into a failure."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.executor._HANDSHAKE_POLL_INTERVAL", 0.01):
            MockWatcher.return_value = MagicMock()
            mock_queue = MagicMock()
            mock_queue.get.side_effect = [_queue.Empty, b"late_bytes"]
            MockQueue.return_value = mock_queue
            mock_proc = _make_mock_process()
            mock_proc.is_alive = False
            MockProcess.return_value = mock_proc

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor()
            executor.launch_host(agents={"researcher": _simple_agent})

            self.assertEqual(executor._host_queues, [b"late_bytes"])


# ========================================================================
# DragonExecutor.max_concurrent_tasks — the concurrency cap
# ========================================================================

class TestDragonExecutorConcurrencyCap(TestCase):
    """Verify the single max_concurrent_tasks knob and its fixed default."""

    def test_default_cap_when_unset(self):
        """With no argument, the cap is the fixed default and sizes the watcher."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            MockWatcher.return_value = MagicMock()
            from dragon.ai.langgraph.executor import (
                DragonExecutor,
                _DEFAULT_MAX_CONCURRENT_TASKS,
            )
            executor = DragonExecutor()
            self.assertEqual(
                executor.max_concurrent_tasks, _DEFAULT_MAX_CONCURRENT_TASKS
            )
            # Watcher is built eagerly in __init__, sized to the default.
            MockWatcher.assert_called_once()
            self.assertEqual(
                MockWatcher.call_args.kwargs["max_concurrent_tasks"],
                _DEFAULT_MAX_CONCURRENT_TASKS,
            )

    def test_explicit_cap(self):
        """An explicit cap sizes the watcher and is reported unchanged."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher:
            MockWatcher.return_value = MagicMock()
            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor(max_concurrent_tasks=100)
            self.assertEqual(executor.max_concurrent_tasks, 100)
            MockWatcher.assert_called_once()
            self.assertEqual(
                MockWatcher.call_args.kwargs["max_concurrent_tasks"], 100
            )

    def test_cap_is_independent_of_hosts(self):
        """Launching hosts never changes the cap (no auto-sizing)."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            MockProcess.side_effect = [_make_mock_process(), _make_mock_process()]

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor(max_concurrent_tasks=32)
            executor.launch_host(agents={"a": _simple_agent}, max_threads=99)
            executor.launch_host(agents={"b": _simple_agent}, max_threads=7)

            # Built exactly once, in __init__, and never resized.
            MockWatcher.assert_called_once()
            self.assertEqual(executor.max_concurrent_tasks, 32)

    def test_launch_host_sizes_input_queue_to_cap(self):
        """launch_host passes the cap to the host so it can size its input
        queue (worst case: all in-flight tasks route to it)."""
        with patch("dragon.ai.langgraph.executor.DragonWatcher") as MockWatcher, \
             patch("dragon.ai.langgraph.executor.Process") as MockProcess, \
             patch("dragon.ai.langgraph.executor.Queue") as MockQueue:
            MockWatcher.return_value = MagicMock()
            MockQueue.return_value = _make_mock_queue()
            MockProcess.return_value = _make_mock_process()

            from dragon.ai.langgraph.executor import DragonExecutor
            executor = DragonExecutor(max_concurrent_tasks=50)
            executor.launch_host(agents={"a": _simple_agent}, max_threads=5)

            host_kwargs = MockProcess.call_args.kwargs["kwargs"]
            self.assertEqual(host_kwargs["input_maxsize"], 50)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
