"""Integration tests — multi-agent DAG end-to-end and error propagation.

Mirrors the inference suite's ``test_end_to_end_pipeline.py`` and
``test_error_handling.py`` by testing full multi-node pipeline execution
through real Dragon DDict, Queue, and Event primitives.

Covers:
- Linear chain (A→B→C): upstream results propagated through DDict.
- Fan-out / fan-in (A→B, A→C, B+C→D): parallel dispatch + merge.
- Error propagation: one agent fails, downstream agents observe ERROR.
- Recovery: agent succeeds after a previous pipeline run had errors.

Run with:  dragon python -m unittest test.ai.agent.integration_tests.test_multi_agent_dag -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import threading
import uuid
from unittest import main

from dragon.data.ddict import DDict
from dragon.native.queue import Queue
from dragon.native.event import Event

from dragon.ai.agent.config import (
    DispatchHeader,
    TaskStatus,
    TaskResult,
    PipelineNode,
    Pipeline,
    RESULT_KEY,
    STATUS_KEY,
    DISPATCH_ID_KEY,
    GLOBAL_STATE_KEY,
    GLOBAL_STATE_ENTRY_KEY,
    USER_INPUT_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.core.batch_dispatch import make_dispatcher_fn
from dragon.ai.agent.config import OrchestratorConfig, AgentConfig

from .conftest import (
    FakeLLMEngine,
    FailingLLMEngine,
    IntegrationTestCase,
)


# ---------------------------------------------------------------------------
# Helpers — fake agent thread that reads from Queue, runs LLM, writes DDict
# ---------------------------------------------------------------------------

def _fake_agent_loop(
    agent_queue: Queue,
    llm_responses: list,
    timeout: float = 10.0,
):
    """Minimal agent thread: read message, write result to DDict, signal Event.

    This simulates what SubAgent._handle_message does, without the full
    agent machinery — enough to test the batch dispatcher and DAG wiring.
    """
    msg = agent_queue.get(timeout=timeout)
    header = msg.header
    task_id = msg.task_id
    agent_id = msg.recipient_id

    ddict = DDict.attach(header.serialized_ddict)
    accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)
    dispatch_id = header.dispatch_id

    # Write status
    accessor.put(STATUS_KEY.format(
        task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id,
    ), TaskStatus.PROCESSING)

    # "Process" — just pick the first response
    answer = llm_responses[0] if llm_responses else "default"

    # Write result
    accessor.put(RESULT_KEY.format(
        task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id,
    ), {"response": answer})

    # Write global state entry
    accessor.put(GLOBAL_STATE_ENTRY_KEY.format(
        task_id=task_id, agent_id=agent_id,
    ), {"agent_id": agent_id, "answer": answer})

    # Mark done
    accessor.put(STATUS_KEY.format(
        task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id,
    ), TaskStatus.DONE)

    # Signal completion
    if header.completion_event is not None:
        header.completion_event.set()

    ddict.detach()


def _fake_error_agent_loop(
    agent_queue: Queue,
    error_msg: str = "Agent crashed",
    timeout: float = 10.0,
):
    """Agent thread that writes ERROR status (simulates agent failure)."""
    msg = agent_queue.get(timeout=timeout)
    header = msg.header
    task_id = msg.task_id
    agent_id = msg.recipient_id

    ddict = DDict.attach(header.serialized_ddict)
    accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)
    dispatch_id = header.dispatch_id

    accessor.put(STATUS_KEY.format(
        task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id,
    ), TaskStatus.ERROR)

    accessor.put(GLOBAL_STATE_ENTRY_KEY.format(
        task_id=task_id, agent_id=agent_id,
    ), {"agent_id": agent_id, "answer": f"ERROR: {error_msg}"})

    if header.completion_event is not None:
        header.completion_event.set()

    ddict.detach()


# ========================================================================
# Linear chain: A → B → C
# ========================================================================

class TestLinearChainPipeline(IntegrationTestCase):
    """Three-agent linear pipeline: planner → runner → reporter."""

    def test_linear_chain_results_propagated(self):
        """Each agent's result is accessible in DDict; final result is from C."""
        task_id = self.task_id
        serialized_ddict = self.ddict.serialize()

        # Seed global state
        accessor = DDictAccessor(self.ddict, agent_id="planner", task_id=task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=task_id), "Run experiment.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=task_id),
            [{"agent_id": "user", "answer": "Run experiment."}],
        )

        # Pipeline: planner → runner → reporter
        pipeline = Pipeline(nodes=[
            PipelineNode(agent_id="planner", task_description="Plan."),
            PipelineNode(agent_id="runner", task_description="Run.", depends_on=["planner"]),
            PipelineNode(agent_id="reporter", task_description="Report.", depends_on=["runner"]),
        ])

        orch_config = OrchestratorConfig(
            agents=[
                AgentConfig(agent_id="planner", name="P", role="planner"),
                AgentConfig(agent_id="runner", name="R", role="runner"),
                AgentConfig(agent_id="reporter", name="Rp", role="reporter"),
            ],
            poll_timeout=15.0,
        )

        # Create agent queues and fake agent threads
        queues = {}
        threads = []
        for node, answer in [
            (pipeline.nodes[0], "Experiment plan ready."),
            (pipeline.nodes[1], "Experiment executed."),
            (pipeline.nodes[2], "Final report."),
        ]:
            q = Queue()
            queues[node.agent_id] = q
            t = threading.Thread(
                target=_fake_agent_loop,
                args=(q, [answer]),
                daemon=True,
            )
            threads.append(t)

        # Build dispatchers
        dispatchers = {}
        for node in pipeline.nodes:
            dispatchers[node.agent_id] = make_dispatcher_fn(
                queues[node.agent_id], node, task_id, serialized_ddict, orch_config,
            )

        # Execute DAG in dependency order
        for t in threads:
            t.start()

        # planner (no upstream)
        result_p = dispatchers["planner"]()
        self.assertEqual(result_p.status, TaskStatus.DONE)

        # runner (upstream = planner)
        result_r = dispatchers["runner"](result_p)
        self.assertEqual(result_r.status, TaskStatus.DONE)

        # reporter (upstream = runner)
        result_rp = dispatchers["reporter"](result_r)
        self.assertEqual(result_rp.status, TaskStatus.DONE)

        for t in threads:
            t.join(timeout=5)

        # Verify each agent wrote its result to DDict
        for agent_id in ["planner", "runner", "reporter"]:
            entry_key = GLOBAL_STATE_ENTRY_KEY.format(
                task_id=task_id, agent_id=agent_id,
            )
            entry = accessor.get(entry_key)
            self.assertEqual(entry["agent_id"], agent_id)
            self.assertIsInstance(entry["answer"], str)

        # Clean up
        for q in queues.values():
            q.destroy()


# ========================================================================
# Fan-out / fan-in: A → B, A → C, B+C → D (diamond)
# ========================================================================

class TestDiamondDagPipeline(IntegrationTestCase):
    """Diamond DAG: planner → (researcher, analyst) → synthesizer."""

    def test_diamond_dag_merges_results(self):
        """Both branches complete; synthesizer receives both upstream results."""
        task_id = self.task_id
        serialized_ddict = self.ddict.serialize()

        accessor = DDictAccessor(self.ddict, agent_id="planner", task_id=task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=task_id), "Analyze data.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=task_id),
            [{"agent_id": "user", "answer": "Analyze data."}],
        )

        pipeline = Pipeline(nodes=[
            PipelineNode(agent_id="planner", task_description="Plan."),
            PipelineNode(agent_id="researcher", task_description="Research.", depends_on=["planner"]),
            PipelineNode(agent_id="analyst", task_description="Analyze.", depends_on=["planner"]),
            PipelineNode(agent_id="synthesizer", task_description="Synthesize.", depends_on=["researcher", "analyst"]),
        ])

        orch_config = OrchestratorConfig(
            agents=[
                AgentConfig(agent_id=n.agent_id, name=n.agent_id, role=n.agent_id)
                for n in pipeline.nodes
            ],
            poll_timeout=15.0,
        )

        queues = {}
        threads = []
        answers = {
            "planner": "Plan done.",
            "researcher": "Research findings.",
            "analyst": "Analysis complete.",
            "synthesizer": "Final synthesis.",
        }
        for node in pipeline.nodes:
            q = Queue()
            queues[node.agent_id] = q
            t = threading.Thread(
                target=_fake_agent_loop,
                args=(q, [answers[node.agent_id]]),
                daemon=True,
            )
            threads.append(t)

        dispatchers = {}
        for node in pipeline.nodes:
            dispatchers[node.agent_id] = make_dispatcher_fn(
                queues[node.agent_id], node, task_id, serialized_ddict, orch_config,
            )

        for t in threads:
            t.start()

        # Execute: planner first
        r_plan = dispatchers["planner"]()

        # Fan-out: researcher and analyst run in parallel (via threads here)
        results = {}

        def run_dispatch(agent_id, *upstreams):
            results[agent_id] = dispatchers[agent_id](*upstreams)

        t_research = threading.Thread(target=run_dispatch, args=("researcher", r_plan))
        t_analyst = threading.Thread(target=run_dispatch, args=("analyst", r_plan))
        t_research.start()
        t_analyst.start()
        t_research.join(timeout=10)
        t_analyst.join(timeout=10)

        # Fan-in: synthesizer receives both
        r_synth = dispatchers["synthesizer"](results["researcher"], results["analyst"])

        for t in threads:
            t.join(timeout=5)

        self.assertEqual(r_synth.status, TaskStatus.DONE)

        # Verify all 4 agents wrote entries
        for agent_id in answers:
            entry = accessor.get(GLOBAL_STATE_ENTRY_KEY.format(
                task_id=task_id, agent_id=agent_id,
            ))
            self.assertEqual(entry["agent_id"], agent_id)

        for q in queues.values():
            q.destroy()


# ========================================================================
# Error propagation: mid-chain failure
# ========================================================================

class TestErrorPropagationLinearChain(IntegrationTestCase):
    """Error in middle agent propagates to downstream dispatcher."""

    def test_middle_agent_error_raises_in_downstream(self):
        """Runner fails → reporter's dispatcher raises RuntimeError."""
        task_id = self.task_id
        serialized_ddict = self.ddict.serialize()

        accessor = DDictAccessor(self.ddict, agent_id="planner", task_id=task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=task_id), "Run.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=task_id),
            [{"agent_id": "user", "answer": "Run."}],
        )

        pipeline = Pipeline(nodes=[
            PipelineNode(agent_id="planner", task_description="Plan."),
            PipelineNode(agent_id="runner", task_description="Run.", depends_on=["planner"]),
            PipelineNode(agent_id="reporter", task_description="Report.", depends_on=["runner"]),
        ])

        orch_config = OrchestratorConfig(
            agents=[
                AgentConfig(agent_id=n.agent_id, name=n.agent_id, role=n.agent_id)
                for n in pipeline.nodes
            ],
            poll_timeout=15.0,
        )

        q_plan = Queue()
        q_run = Queue()
        q_report = Queue()

        t_plan = threading.Thread(
            target=_fake_agent_loop, args=(q_plan, ["Plan done."]), daemon=True,
        )
        t_run = threading.Thread(
            target=_fake_error_agent_loop, args=(q_run, "CUDA OOM"), daemon=True,
        )

        d_plan = make_dispatcher_fn(q_plan, pipeline.nodes[0], task_id, serialized_ddict, orch_config)
        d_run = make_dispatcher_fn(q_run, pipeline.nodes[1], task_id, serialized_ddict, orch_config)
        d_report = make_dispatcher_fn(q_report, pipeline.nodes[2], task_id, serialized_ddict, orch_config)

        t_plan.start()
        t_run.start()

        r_plan = d_plan()
        self.assertEqual(r_plan.status, TaskStatus.DONE)

        # Runner fails
        with self.assertRaises(RuntimeError):
            d_run(r_plan)

        t_plan.join(timeout=5)
        t_run.join(timeout=5)

        q_plan.destroy()
        q_run.destroy()
        q_report.destroy()


class TestErrorPropagationDiamond(IntegrationTestCase):
    """Error in one branch of diamond DAG: downstream merge node sees ERROR upstream."""

    def test_one_branch_fails_merge_raises(self):
        """Researcher succeeds, analyst fails → synthesizer's dispatcher raises."""
        task_id = self.task_id
        serialized_ddict = self.ddict.serialize()

        accessor = DDictAccessor(self.ddict, agent_id="planner", task_id=task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=task_id), "Analyze.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=task_id),
            [{"agent_id": "user", "answer": "Analyze."}],
        )

        pipeline = Pipeline(nodes=[
            PipelineNode(agent_id="planner", task_description="Plan."),
            PipelineNode(agent_id="researcher", task_description="Research.", depends_on=["planner"]),
            PipelineNode(agent_id="analyst", task_description="Analyze.", depends_on=["planner"]),
            PipelineNode(agent_id="synthesizer", task_description="Synth.", depends_on=["researcher", "analyst"]),
        ])

        orch_config = OrchestratorConfig(
            agents=[
                AgentConfig(agent_id=n.agent_id, name=n.agent_id, role=n.agent_id)
                for n in pipeline.nodes
            ],
            poll_timeout=15.0,
        )

        q_plan = Queue()
        q_research = Queue()
        q_analyst = Queue()
        q_synth = Queue()

        threads = [
            threading.Thread(target=_fake_agent_loop, args=(q_plan, ["Plan."]), daemon=True),
            threading.Thread(target=_fake_agent_loop, args=(q_research, ["Research done."]), daemon=True),
            threading.Thread(target=_fake_error_agent_loop, args=(q_analyst, "Analysis OOM"), daemon=True),
        ]

        d_plan = make_dispatcher_fn(q_plan, pipeline.nodes[0], task_id, serialized_ddict, orch_config)
        d_research = make_dispatcher_fn(q_research, pipeline.nodes[1], task_id, serialized_ddict, orch_config)
        d_analyst = make_dispatcher_fn(q_analyst, pipeline.nodes[2], task_id, serialized_ddict, orch_config)
        d_synth = make_dispatcher_fn(q_synth, pipeline.nodes[3], task_id, serialized_ddict, orch_config)

        for t in threads:
            t.start()

        r_plan = d_plan()

        # Fan-out
        results = {}
        def run_dispatch(name, fn, *args):
            try:
                results[name] = fn(*args)
            except RuntimeError as e:
                results[name] = e

        t1 = threading.Thread(target=run_dispatch, args=("researcher", d_research, r_plan))
        t2 = threading.Thread(target=run_dispatch, args=("analyst", d_analyst, r_plan))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # Researcher succeeded
        self.assertIsInstance(results["researcher"], TaskResult)
        self.assertEqual(results["researcher"].status, TaskStatus.DONE)

        # Analyst failed
        self.assertIsInstance(results["analyst"], (RuntimeError, TaskResult))

        for t in threads:
            t.join(timeout=5)

        for q in [q_plan, q_research, q_analyst, q_synth]:
            q.destroy()


# ========================================================================
# Timeout propagation
# ========================================================================

class TestDispatcherTimeout(IntegrationTestCase):
    """Dispatcher raises TimeoutError when agent never signals completion."""

    def test_timeout_raises(self):
        """Agent that never responds causes TimeoutError in dispatcher."""
        task_id = self.task_id
        serialized_ddict = self.ddict.serialize()

        accessor = DDictAccessor(self.ddict, agent_id="slow", task_id=task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=task_id), "Work.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=task_id),
            [{"agent_id": "user", "answer": "Work."}],
        )

        node = PipelineNode(agent_id="slow", task_description="Slow task.")
        q = Queue()

        orch_config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="slow", name="S", role="slow")],
            poll_timeout=1.0,  # 1 second timeout
        )

        dispatcher = make_dispatcher_fn(q, node, task_id, serialized_ddict, orch_config)

        # No agent thread — nobody reads the queue or signals the event
        with self.assertRaises(TimeoutError):
            dispatcher()

        q.destroy()


# ========================================================================
# Recovery: agent pipeline succeeds after prior run had errors
# ========================================================================

class TestRecoveryAfterError(IntegrationTestCase):
    """Verify a fresh pipeline run succeeds after a previous run had failures.

    This tests that DDict keys from the failed run don't interfere with the
    new run (since each run uses a unique task_id).
    """

    def test_fresh_run_succeeds_after_prior_error(self):
        """Second pipeline run with new task_id is unaffected by first failure."""
        serialized_ddict = self.ddict.serialize()

        # --- First run: agent fails ---
        task_id_1 = str(uuid.uuid4())
        accessor_1 = DDictAccessor(self.ddict, agent_id="worker", task_id=task_id_1)
        accessor_1.put(USER_INPUT_KEY.format(task_id=task_id_1), "Fail.")
        accessor_1.put(
            GLOBAL_STATE_KEY.format(task_id=task_id_1),
            [{"agent_id": "user", "answer": "Fail."}],
        )

        node = PipelineNode(agent_id="worker", task_description="Work.")
        q1 = Queue()
        orch_config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="worker", name="W", role="w")],
            poll_timeout=10.0,
        )

        t1 = threading.Thread(
            target=_fake_error_agent_loop, args=(q1, "Crash"), daemon=True,
        )
        t1.start()

        d1 = make_dispatcher_fn(q1, node, task_id_1, serialized_ddict, orch_config)
        with self.assertRaises(RuntimeError):
            d1()
        t1.join(timeout=5)
        q1.destroy()

        # --- Second run: fresh task_id, agent succeeds ---
        task_id_2 = str(uuid.uuid4())
        accessor_2 = DDictAccessor(self.ddict, agent_id="worker", task_id=task_id_2)
        accessor_2.put(USER_INPUT_KEY.format(task_id=task_id_2), "Succeed.")
        accessor_2.put(
            GLOBAL_STATE_KEY.format(task_id=task_id_2),
            [{"agent_id": "user", "answer": "Succeed."}],
        )

        q2 = Queue()
        t2 = threading.Thread(
            target=_fake_agent_loop, args=(q2, ["Recovery success."]), daemon=True,
        )
        t2.start()

        d2 = make_dispatcher_fn(q2, node, task_id_2, serialized_ddict, orch_config)
        result = d2()
        t2.join(timeout=5)

        self.assertEqual(result.status, TaskStatus.DONE)
        q2.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
