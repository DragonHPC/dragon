"""Tests for config dataclasses, Pipeline validation, and TaskResult."""

import dragon
import multiprocessing as mp

from unittest import TestCase, main

from dragon.ai.agent.config import (
    AgentConfig,
    MCPServerConfig,
    OrchestratorConfig,
    MemoryConfig,
    MemoryStrategy,
    TaskStatus,
    PipelineNode,
    Pipeline,
    TaskResult,
)


# ========================================================================
# AgentConfig
# ========================================================================

class TestAgentConfig(TestCase):
    """Verify AgentConfig construction, defaults, and optional fields."""

    def test_minimal_construction(self):
        """Only required fields (agent_id, name, role) are needed."""
        cfg = AgentConfig(agent_id="a", name="Agent A", role="Do stuff")
        self.assertEqual(cfg.agent_id, "a")
        self.assertEqual(cfg.name, "Agent A")
        self.assertEqual(cfg.role, "Do stuff")

    def test_defaults(self):
        """All optional fields default to None / sensible values."""
        cfg = AgentConfig(agent_id="a", name="A", role="r")
        self.assertIsNone(cfg.input_queue)
        self.assertIsNone(cfg.inference_queue)
        self.assertIsNone(cfg.max_concurrent_requests)
        self.assertIsNone(cfg.summarizer_inference_queue)
        self.assertEqual(cfg.node_affinity, "")
        self.assertIsNone(cfg.approval_filter)
        self.assertEqual(cfg.max_tool_call_iterations, 20)
        self.assertIsNone(cfg.memory)

    def test_approval_filter_attached(self):
        """A callable approval_filter is stored and invocable."""
        filt = lambda name, args: name == "dangerous_tool"
        cfg = AgentConfig(agent_id="a", name="A", role="r", approval_filter=filt)
        self.assertIs(cfg.approval_filter, filt)
        self.assertTrue(cfg.approval_filter("dangerous_tool", {}))
        self.assertFalse(cfg.approval_filter("safe_tool", {}))

    def test_memory_config_attached(self):
        """A MemoryConfig object is stored on the agent config."""
        mem = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=5)
        cfg = AgentConfig(agent_id="a", name="A", role="r", memory=mem)
        self.assertIs(cfg.memory, mem)
        self.assertEqual(cfg.memory.max_kept_turns, 5)


# ========================================================================
# MCPServerConfig
# ========================================================================

class TestMCPServerConfig(TestCase):
    """Verify MCPServerConfig construction and alias validation."""

    def test_valid_construction(self):
        """Valid url + alias accepted; defaults for token, retries, timeout."""
        cfg = MCPServerConfig(url="http://mcp:8000", alias="jupyter")
        self.assertEqual(cfg.url, "http://mcp:8000")
        self.assertEqual(cfg.alias, "jupyter")
        self.assertIsNone(cfg.token)
        self.assertEqual(cfg.max_retries, 3)
        self.assertEqual(cfg.retry_delay, 0.5)
        self.assertEqual(cfg.timeout, 5.0)

    def test_empty_alias_rejected(self):
        """Empty string alias is rejected at construction."""
        with self.assertRaisesRegex(ValueError, "non-empty"):
            MCPServerConfig(url="http://x", alias="")

    def test_whitespace_alias_rejected(self):
        """Whitespace-only alias is rejected at construction."""
        with self.assertRaisesRegex(ValueError, "non-empty"):
            MCPServerConfig(url="http://x", alias="   ")


# ========================================================================
# OrchestratorConfig
# ========================================================================

class TestOrchestratorConfig(TestCase):
    """Verify OrchestratorConfig defaults and duplicate agent rejection."""

    def test_defaults(self):
        """Default values for poll_interval, poll_timeout, tracing, ddict_kwargs."""
        cfg = OrchestratorConfig()
        self.assertEqual(cfg.agents, [])
        self.assertEqual(cfg.poll_interval, 0.5)
        self.assertEqual(cfg.poll_timeout, 120.0)
        self.assertFalse(cfg.tracing)
        self.assertEqual(cfg.ddict_kwargs, {})

    def test_duplicate_agent_id_rejected(self):
        """Two agents with the same agent_id raise ValueError."""
        a1 = AgentConfig(agent_id="dup", name="A", role="r")
        a2 = AgentConfig(agent_id="dup", name="B", role="r")
        with self.assertRaisesRegex(ValueError, "duplicate agent_id"):
            OrchestratorConfig(agents=[a1, a2])

    def test_unique_agents_accepted(self):
        """Agents with distinct IDs are accepted."""
        a1 = AgentConfig(agent_id="a", name="A", role="r")
        a2 = AgentConfig(agent_id="b", name="B", role="r")
        cfg = OrchestratorConfig(agents=[a1, a2])
        self.assertEqual(len(cfg.agents), 2)


# ========================================================================
# MemoryConfig / MemoryStrategy
# ========================================================================

class TestMemoryConfig(TestCase):
    """Verify MemoryConfig defaults, MemoryStrategy enum, and resolve()."""

    def test_strategy_string_comparison(self):
        """MemoryStrategy enum values compare equal to their string forms."""
        self.assertEqual(MemoryStrategy.FULL, "full")
        self.assertEqual(MemoryStrategy.SLIDING_WINDOW, "sliding_window")
        self.assertEqual(MemoryStrategy.SUMMARIZE, "summarize")

    def test_defaults(self):
        """Default strategy is SLIDING_WINDOW with sensible turn/char limits."""
        cfg = MemoryConfig()
        self.assertEqual(cfg.strategy, MemoryStrategy.SLIDING_WINDOW)
        self.assertEqual(cfg.max_kept_turns, 8)
        self.assertEqual(cfg.max_tool_result_chars, 5000)
        self.assertEqual(cfg.summarize_after_turns, 6)

    def test_resolve_none(self):
        """resolve(None) returns None (no memory management)."""
        self.assertIsNone(MemoryConfig.resolve(None))

    def test_resolve_full_becomes_none(self):
        """FULL strategy resolves to None (keep everything, no management)."""
        cfg = MemoryConfig(strategy=MemoryStrategy.FULL)
        self.assertIsNone(MemoryConfig.resolve(cfg))

    def test_resolve_sliding_window_passthrough(self):
        """SLIDING_WINDOW passes through unchanged."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW)
        self.assertIs(MemoryConfig.resolve(cfg), cfg)

    def test_resolve_summarize_passthrough(self):
        """SUMMARIZE passes through unchanged."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE)
        self.assertIs(MemoryConfig.resolve(cfg), cfg)

    def test_resolve_invalid_type(self):
        """Non-MemoryConfig input raises TypeError."""
        with self.assertRaisesRegex(TypeError, "MemoryConfig"):
            MemoryConfig.resolve("invalid")


# ========================================================================
# TaskStatus
# ========================================================================

class TestTaskStatus(TestCase):
    """Verify TaskStatus enum string comparison and reconstruction."""

    def test_string_comparison(self):
        """Enum values compare equal to their lowercase string forms."""
        self.assertEqual(TaskStatus.READY, "ready")
        self.assertEqual(TaskStatus.PROCESSING, "processing")
        self.assertEqual(TaskStatus.WAITING, "waiting")
        self.assertEqual(TaskStatus.DONE, "done")
        self.assertEqual(TaskStatus.ERROR, "error")

    def test_reconstruction_from_string(self):
        """TaskStatus can be reconstructed from a raw string."""
        self.assertIs(TaskStatus("done"), TaskStatus.DONE)
        self.assertIs(TaskStatus("error"), TaskStatus.ERROR)


# ========================================================================
# PipelineNode
# ========================================================================

class TestPipelineNode(TestCase):
    """Verify PipelineNode construction for agent-backed and function nodes."""

    def test_agent_backed_node(self):
        """Node with agent_id and task_description, no function."""
        node = PipelineNode(agent_id="researcher", task_description="Find facts")
        self.assertEqual(node.agent_id, "researcher")
        self.assertEqual(node.task_description, "Find facts")
        self.assertIsNone(node.fn)
        self.assertEqual(node.depends_on, [])

    def test_function_node(self):
        """Node backed by a callable function."""
        fn = lambda *args: "result"
        node = PipelineNode(agent_id="merge", fn=fn)
        self.assertIs(node.fn, fn)

    def test_dependencies(self):
        """depends_on stores the list of upstream agent IDs."""
        node = PipelineNode(agent_id="c", depends_on=["a", "b"])
        self.assertEqual(node.depends_on, ["a", "b"])


# ========================================================================
# Pipeline validation and topological sort
# ========================================================================

class TestPipeline(TestCase):
    """Verify Pipeline topological sort, cycle detection, and terminal nodes."""

    def test_single_node(self):
        """Single-node pipeline is accepted as-is."""
        p = Pipeline(nodes=[PipelineNode(agent_id="a")])
        self.assertEqual(len(p.nodes), 1)
        self.assertEqual(p.nodes[0].agent_id, "a")

    def test_topological_sort(self):
        """Nodes provided out of order are sorted by dependencies."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="c", depends_on=["b"]),
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="b", depends_on=["a"]),
        ])
        ids = [n.agent_id for n in p.nodes]
        self.assertLess(ids.index("a"), ids.index("b"))
        self.assertLess(ids.index("b"), ids.index("c"))

    def test_diamond_dag(self):
        """A → B, A → C, B → D, C → D."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="D", depends_on=["B", "C"]),
            PipelineNode(agent_id="B", depends_on=["A"]),
            PipelineNode(agent_id="C", depends_on=["A"]),
            PipelineNode(agent_id="A"),
        ])
        ids = [n.agent_id for n in p.nodes]
        self.assertLess(ids.index("A"), ids.index("B"))
        self.assertLess(ids.index("A"), ids.index("C"))
        self.assertLess(ids.index("B"), ids.index("D"))
        self.assertLess(ids.index("C"), ids.index("D"))

    def test_duplicate_agent_id_rejected(self):
        """Two nodes with the same agent_id raise ValueError."""
        with self.assertRaisesRegex(ValueError, "duplicate"):
            Pipeline(nodes=[
                PipelineNode(agent_id="a"),
                PipelineNode(agent_id="a"),
            ])

    def test_missing_dependency_rejected(self):
        """Referencing a non-existent upstream agent raises ValueError."""
        with self.assertRaisesRegex(ValueError, "no node with that agent_id is declared"):
            Pipeline(nodes=[
                PipelineNode(agent_id="a", depends_on=["missing"]),
            ])

    def test_cycle_detected(self):
        """Mutual dependency (A→B, B→A) raises ValueError."""
        with self.assertRaisesRegex(ValueError, "cycle"):
            Pipeline(nodes=[
                PipelineNode(agent_id="a", depends_on=["b"]),
                PipelineNode(agent_id="b", depends_on=["a"]),
            ])

    def test_self_cycle_detected(self):
        """Node depending on itself raises ValueError."""
        with self.assertRaisesRegex(ValueError, "cycle"):
            Pipeline(nodes=[
                PipelineNode(agent_id="a", depends_on=["a"]),
            ])

    def test_terminal_nodes(self):
        """terminal_nodes() returns nodes with no downstream dependents."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="b", depends_on=["a"]),
            PipelineNode(agent_id="c", depends_on=["a"]),
        ])
        terminals = p.terminal_nodes()
        terminal_ids = {n.agent_id for n in terminals}
        self.assertEqual(terminal_ids, {"b", "c"})


# ========================================================================
# TaskResult
# ========================================================================

class TestTaskResult(TestCase):
    """Verify TaskResult construction and status coercion."""

    def test_construction(self):
        """Basic construction with required fields."""
        r = TaskResult(task_id="t", agent_id="a", status=TaskStatus.DONE)
        self.assertEqual(r.task_id, "t")
        self.assertEqual(r.agent_id, "a")
        self.assertIs(r.status, TaskStatus.DONE)
        self.assertEqual(r.serialized_ddict, "")
        self.assertEqual(r.metadata, {})

    def test_status_coerced_from_string(self):
        """String 'done' is coerced to TaskStatus.DONE in __post_init__."""
        r = TaskResult(task_id="t", agent_id="a", status="done")
        self.assertIs(r.status, TaskStatus.DONE)
        self.assertIsInstance(r.status, TaskStatus)

    def test_status_coerced_from_error_string(self):
        """String 'error' is coerced to TaskStatus.ERROR."""
        r = TaskResult(task_id="t", agent_id="a", status="error")
        self.assertIs(r.status, TaskStatus.ERROR)


# ========================================================================
# Pipeline — additional edge cases
# ========================================================================

class TestPipelineEdgeCases(TestCase):
    """Verify Pipeline edge cases and terminal_nodes() behavior."""

    def test_terminal_nodes_single_node(self):
        """Single-node pipeline's only node is terminal."""
        p = Pipeline(nodes=[PipelineNode(agent_id="a")])
        terminals = p.terminal_nodes()
        self.assertEqual(len(terminals), 1)
        self.assertEqual(terminals[0].agent_id, "a")

    def test_terminal_nodes_linear_chain(self):
        """In a linear chain A→B→C, only C is terminal."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="b", depends_on=["a"]),
            PipelineNode(agent_id="c", depends_on=["b"]),
        ])
        terminals = p.terminal_nodes()
        self.assertEqual(len(terminals), 1)
        self.assertEqual(terminals[0].agent_id, "c")

    def test_terminal_nodes_fan_out(self):
        """A→B, A→C: both B and C are terminal."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="b", depends_on=["a"]),
            PipelineNode(agent_id="c", depends_on=["a"]),
        ])
        terminal_ids = {n.agent_id for n in p.terminal_nodes()}
        self.assertEqual(terminal_ids, {"b", "c"})

    def test_terminal_nodes_diamond(self):
        """Diamond DAG: A→B, A→C, B→D, C→D — only D is terminal."""
        p = Pipeline(nodes=[
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="b", depends_on=["a"]),
            PipelineNode(agent_id="c", depends_on=["a"]),
            PipelineNode(agent_id="d", depends_on=["b", "c"]),
        ])
        terminals = p.terminal_nodes()
        self.assertEqual(len(terminals), 1)
        self.assertEqual(terminals[0].agent_id, "d")

    def test_function_node_in_pipeline(self):
        """Function-backed nodes participate in the DAG normally."""
        def merge(*results):
            return "merged"

        p = Pipeline(nodes=[
            PipelineNode(agent_id="a"),
            PipelineNode(agent_id="merge", fn=merge, depends_on=["a"]),
        ])
        self.assertEqual(len(p.nodes), 2)
        terminals = p.terminal_nodes()
        self.assertEqual(terminals[0].agent_id, "merge")


# ========================================================================
# OrchestratorConfig — additional edge cases
# ========================================================================

class TestOrchestratorConfigEdgeCases(TestCase):
    """Verify OrchestratorConfig edge cases."""

    def test_custom_ddict_kwargs(self):
        """ddict_kwargs are stored and accessible."""
        cfg = OrchestratorConfig(ddict_kwargs={"managers_per_node": 2, "total_mem": 1 << 30})
        self.assertEqual(cfg.ddict_kwargs["managers_per_node"], 2)

    def test_tracing_enabled(self):
        """tracing=True is stored correctly."""
        cfg = OrchestratorConfig(tracing=True)
        self.assertTrue(cfg.tracing)

    def test_custom_poll_parameters(self):
        """Custom poll_interval and poll_timeout are stored."""
        cfg = OrchestratorConfig(poll_interval=1.0, poll_timeout=300.0)
        self.assertEqual(cfg.poll_interval, 1.0)
        self.assertEqual(cfg.poll_timeout, 300.0)


# ========================================================================
# MCPServerConfig — additional edge cases
# ========================================================================

class TestMCPServerConfigEdgeCases(TestCase):
    """Verify MCPServerConfig edge cases."""

    def test_custom_retry_parameters(self):
        """Custom retry parameters are stored correctly."""
        cfg = MCPServerConfig(
            url="http://mcp:8000", alias="jupyter",
            token="secret", max_retries=5, retry_delay=1.0, timeout=10.0,
        )
        self.assertEqual(cfg.token, "secret")
        self.assertEqual(cfg.max_retries, 5)
        self.assertEqual(cfg.retry_delay, 1.0)
        self.assertEqual(cfg.timeout, 10.0)


# ========================================================================
# TaskResult — additional edge cases
# ========================================================================

class TestTaskResultEdgeCases(TestCase):
    """Verify TaskResult edge cases."""

    def test_metadata_default_is_independent(self):
        """Each TaskResult gets its own metadata dict (no shared default)."""
        r1 = TaskResult(task_id="t1", agent_id="a", status=TaskStatus.DONE)
        r2 = TaskResult(task_id="t2", agent_id="b", status=TaskStatus.DONE)
        r1.metadata["key"] = "val"
        self.assertNotIn("key", r2.metadata)

    def test_serialized_ddict_stored(self):
        """serialized_ddict is stored when provided."""
        r = TaskResult(
            task_id="t", agent_id="a", status=TaskStatus.DONE,
            serialized_ddict="handle-123",
        )
        self.assertEqual(r.serialized_ddict, "handle-123")

    def test_all_status_values_coerced(self):
        """All TaskStatus string values are correctly coerced."""
        for status_str in ["ready", "processing", "waiting", "done", "error"]:
            r = TaskResult(task_id="t", agent_id="a", status=status_str)
            self.assertIsInstance(r.status, TaskStatus)
            self.assertEqual(r.status, status_str)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
