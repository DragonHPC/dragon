.. _agent_tutorial:

Dragon AI Agent Framework — User Guide
+++++++++++++++++++++++++++++++++++++++

This guide covers everything you need to build, configure, and run multi-agent
AI pipelines on HPC clusters using the Dragon AI Agent Framework. It is written
for first-time users. By the end you will understand every available API, every
configuration option, and enough architecture to debug performance issues.

For the internal architecture details (Dragon primitives, source-level code
paths), see :ref:`developer-guide-agent`. For the auto-generated API reference,
see :ref:`AgentAPI`.


.. contents:: In This Guide
   :local:
   :depth: 2


Overview — What the Framework Does
===================================

The Dragon AI Agent Framework runs LLM-powered multi-agent workflows on HPC
clusters. You define:

1. **Agents** — persistent processes with a role (system prompt), tools, and an
   LLM backend.
2. **A pipeline** — a directed acyclic graph (DAG) of tasks, each assigned to
   an agent.
3. **An orchestrator** — the entry point that creates shared state, builds the
   DAG, dispatches tasks to agents, and collects results.

At runtime the framework:

- Creates a shared :py:class:`~dragon.data.ddict.DDict` (Distributed Dictionary)
  called the **Scoreboard** that stores all task state, results, and
  observability data.
- Builds a Dragon Batch DAG where each node is a lightweight *dispatcher
  closure* that puts a message on the target agent's input
  :py:class:`~dragon.native.queue.Queue` and blocks on a completion
  :py:class:`~dragon.native.event.Event`.
- Each agent runs an ``asyncio`` event loop, picks up messages from its Queue,
  runs an LLM tool-calling loop, writes results to the Scoreboard, and signals
  the dispatcher.
- Agents are **stateless** — zero per-task state is held between tasks. All
  context lives in the Scoreboard DDict.

.. code-block:: text

    ┌─ Orchestrator Process ──────────────────────────────────────────────┐
    │  DDict (Scoreboard)  ←→  Dragon Batch DAG  ←→  HITL TCP Bridge      │
    │                           │    │    │           Trace TCP Bridge    │
    └───────────────────────────┼────┼────┼───────────────────────────────┘
                                │    │    │
                 ┌──────────────┘    │    └──────────────┐
                 ▼                   ▼                   ▼
    ┌─ Agent Process ───┐  ┌─ Agent Process ───┐  ┌─ Agent Process ───┐
    │  Input Queue      │  │  Input Queue      │  │  Input Queue      │
    │  LLM Proxy        │  │  LLM Proxy        │  │  LLM Proxy        │
    │  Tool Registry    │  │  Tool Registry    │  │  Tool Registry    │
    │  MCP Clients      │  │  MCP Clients      │  │  MCP Clients      │
    │  asyncio loop     │  │  asyncio loop     │  │  asyncio loop     │
    └───────────────────┘  └───────────────────┘  └───────────────────┘


Step 1 — Set Up the Inference Backend
======================================

Before creating agents you need an LLM inference service. The ``Inference``
class manages the vLLM backend with multi-GPU tensor parallelism, request
batching, and dynamic worker management.

.. code-block:: python
    :linenos:
    :caption: **Inference pipeline setup**

    from dragon.native.queue import Queue
    from dragon.ai.inference import (
      Inference, InferenceConfig, ModelConfig, HardwareConfig,
      BatchingConfig, GuardrailsConfig, DynamicWorkerConfig,
    )

    inference_queue = Queue()

    inference = Inference(
        config=InferenceConfig(
            model=ModelConfig(
                model_name="meta-llama/Llama-3.1-70B-Instruct",
                hf_token="hf_...",
                tp_size=4,              # tensor parallelism across 4 GPUs
                max_tokens=4096,
                max_model_len=8192,
            ),
            hardware=HardwareConfig(
                num_gpus=4,
                num_nodes=1,
            ),
            batching=BatchingConfig(
                enabled=True,
                batch_wait_seconds=0.1,
                max_batch_size=32,
            ),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="",
        ),
        input_queue=inference_queue,
    )
    inference.initialize()

The ``inference_queue`` is the shared :py:class:`~dragon.native.queue.Queue`
that agents will use to send LLM requests. Multiple agents can share the same
queue — the inference backend handles batching and routing internally.


InferenceConfig Reference
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 70

   * - Sub-config
     - Key fields
   * - ``ModelConfig``
     - ``model_name`` (str), ``hf_token`` (str), ``tp_size`` (int — tensor
       parallelism), ``max_tokens`` (int — max new tokens), ``dtype``
       (str, default ``"bfloat16"``), ``top_k`` (int, default 50),
       ``top_p`` (float, default 0.95), ``temperature`` (float, default 0.5),
       ``repetition_penalty`` (float, default 1.1), ``ignore_eos``
       (bool, default False), ``skip_special_tokens`` (bool, default False),
       ``system_prompt`` (list[str])
   * - ``HardwareConfig``
     - ``num_nodes`` (int, -1 = auto), ``num_gpus`` (int, -1 = all),
       ``num_inf_workers_per_cpu`` (int, -1 = auto ``num_gpus // tp_size``),
       ``node_offset`` (int, default 0 — skip first N nodes),
       ``inf_wrkr_queue_maxsize`` (int, -1 = auto ``num_inf_workers_per_cpu * 2``)
   * - ``BatchingConfig``
     - ``enabled`` (bool, default True), ``batch_type`` (``"dynamic"`` or
       ``"pre-batch"``), ``batch_wait_seconds`` (float, default 0.1),
       ``max_batch_size`` (int, default 60)
   * - ``GuardrailsConfig``
     - ``enabled`` (bool, default True), ``prompt_guard_model`` (str),
       ``prompt_guard_sensitivity`` (float, 0.0–1.0)
   * - ``DynamicWorkerConfig``
     - ``enabled`` (bool, default True), ``min_active_workers_per_cpu`` (int),
       ``spin_down_threshold_seconds`` (int), ``spin_up_threshold_seconds``
       (int), ``spin_up_prompt_threshold`` (int)


Separate Summarizer Model
--------------------------

For memory management with the ``SUMMARIZE`` strategy (see
:ref:`agent-tutorial-memory`), you can run a second, smaller model on a
separate GPU partition:

.. code-block:: python
    :linenos:
    :caption: **Summarizer on a second node**

    summarizer_queue = Queue()
    summarizer_inference = Inference(
        config=InferenceConfig(
            model=ModelConfig(
                model_name="meta-llama/Llama-3.1-8B-Instruct",
                hf_token="hf_...", tp_size=1, max_tokens=2048,
            ),
            hardware=HardwareConfig(num_gpus=1, num_nodes=1, node_offset=1),
            batching=BatchingConfig(enabled=True, batch_wait_seconds=0.05),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="",
        ),
        input_queue=summarizer_queue,
    )
    summarizer_inference.initialize()


Step 2 — Define Tools
======================

Tools are callable functions that agents invoke during their reasoning loop. The
LLM decides which tool to call and with what arguments based on the tool's
name, description, and parameter schema.

The ``ToolRegistry`` holds all tools for a single agent.


Registration Methods
---------------------

There are three ways to register tools:

**1. ``@registry.tool`` decorator** (recommended for new code):

.. code-block:: python
    :linenos:

    from dragon.ai.agent.tools.registry import ToolRegistry

    registry = ToolRegistry()

    @registry.tool
    def search_database(query: str, max_results: int = 10) -> dict:
        """Search the experiment database for matching records.

        :param query: The search query string.
        :param max_results: Maximum number of results to return.
        """
        # ... perform search ...
        return {"results": records}

The decorator wraps the function in a ``FunctionTool`` and registers it
automatically. The tool name is ``fn.__name__``, the description is the first
line of the docstring, and parameters are derived from type hints and
``:param:`` docstring entries.

**2. ``registry.register(callable)``** (for existing functions):

.. code-block:: python
    :linenos:

    from tools.analyzer import analyze_convergence
    registry.register(analyze_convergence)

**3. Subclass ``BaseTool``** (for full control over schema):

.. code-block:: python
    :linenos:

    from dragon.ai.agent.tools.base import BaseTool

    class RunSimulation(BaseTool):
        name = "run_simulation"
        description = "Run a Monte Carlo simulation with the given parameters."

        def run(self, input: dict) -> dict:
            n_samples = input["n_samples"]
            # ... run simulation ...
            return {"status": "complete", "result": result}

        def to_schema(self) -> dict:
            return {
                "type": "function",
                "function": {
                    "name": self.name,
                    "description": self.description,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "n_samples": {"type": "integer", "description": "Number of samples"},
                        },
                        "required": ["n_samples"],
                    },
                },
            }

    registry.register(RunSimulation())


Async Tools
-----------

Async callables are detected automatically. The dispatcher ``await``\ s them
instead of calling synchronously:

.. code-block:: python
    :linenos:

    @registry.tool
    async def fetch_remote_data(url: str, timeout: float = 30.0) -> dict:
        """Fetch data from a remote HTTP endpoint."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as resp:
                return await resp.json()

Async tools yield the event loop while waiting for I/O, allowing other
concurrent tasks on the same agent to make progress.


ToolRegistry API
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 65

   * - Method
     - Description
   * - ``register(tool)``
     - Register a ``BaseTool`` instance or a plain callable (auto-wrapped
       in ``FunctionTool``). Overwrites existing tools with the same name.
   * - ``unregister(name)``
     - Remove a tool by name. No-op if not present.
   * - ``get(name)``
     - Return the tool registered under *name*. Raises ``KeyError`` if
       not found.
   * - ``has(name)``
     - Return ``True`` if a tool with *name* is registered.
   * - ``list_tools()``
     - Return a list of OpenAI-compatible tool/function schema dicts.
   * - ``tool_names()``
     - Return a sorted list of registered tool names.
   * - ``tool(fn)``
     - Decorator that wraps *fn* as a ``FunctionTool`` and registers it.


Step 3 — Configure Agents
==========================

Each agent needs an ``AgentConfig`` that defines its identity, role, and
connections.

.. code-block:: python
    :linenos:
    :caption: **AgentConfig example**

    from dragon.ai.agent.config.agent_config import AgentConfig
    from dragon.ai.agent.config.memory_config import MemoryConfig, MemoryStrategy

    planner_config = AgentConfig(
        agent_id="planner",
        name="Experiment Planner",
        role="You are an experiment planner. Given a research question, "
             "design a set of experiments to answer it.",
        inference_queue=inference_queue,
        summarizer_inference_queue=summarizer_queue,   # optional
        memory=MemoryConfig(
            strategy=MemoryStrategy.SUMMARIZE,
            max_kept_turns=8,
            summarize_after_turns=6,
        ),
        max_tool_call_iterations=15,
        max_concurrent_requests=16,
        approval_filter=lambda name, args: name == "propose_experiment",
    )


AgentConfig Reference
----------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 50

   * - Field
     - Default
     - Description
   * - ``agent_id``
     - (required)
     - Unique identifier within the pipeline.
   * - ``name``
     - (required)
     - Human-readable display name.
   * - ``role``
     - (required)
     - System prompt — the LLM's persona for this agent.
   * - ``inference_queue``
     - ``None``
     - Dragon Queue feeding the inference backend. When set, a
       ``DragonQueueLLMProxy`` is auto-created.
   * - ``input_queue``
     - ``None``
     - The agent's input Queue — populated automatically via the reply
       queue handshake during agent startup. Do not set manually.
   * - ``max_concurrent_requests``
     - ``None`` (=32)
     - Maximum concurrent in-flight LLM requests per agent. Controls the
       size of the ``ResponseQueuePool``.
   * - ``summarizer_inference_queue``
     - ``None``
     - Separate inference queue for a dedicated summarizer model. Used when
       ``memory.strategy=SUMMARIZE``.
   * - ``node_affinity``
     - ``""``
     - Placement hint (hostname or node index). Informational — actual
       placement is controlled by ``Policy`` during process launch.
   * - ``approval_filter``
     - ``None``
     - HITL gating predicate: ``(tool_name: str, tool_args: dict) -> bool``.
       When set, tool calls matching the filter are paused for human
       approval before execution.
   * - ``max_tool_call_iterations``
     - ``20``
     - Maximum LLM iterations in the tool-calling loop. Each iteration is
       one LLM call that may produce a tool request or final answer.
       Increase for agents with many tool calls or HITL feedback rounds.
   * - ``memory``
     - ``None``
     - Memory management config. ``None`` = keep everything (no pruning).
       See :ref:`agent-tutorial-memory`.


.. _agent-tutorial-memory:

MemoryConfig Reference
-----------------------

Controls how the agent manages its conversation history during the tool-calling
loop. Without memory management, the message list grows without bound and can
exceed the model's context window.

.. code-block:: python
    :linenos:

    from dragon.ai.agent.config.memory_config import MemoryConfig, MemoryStrategy

    # No pruning (default — keeps everything)
    AgentConfig(..., memory=None)

    # Sliding window — drop old turns
    AgentConfig(..., memory=MemoryConfig(
        strategy=MemoryStrategy.SLIDING_WINDOW,
        max_kept_turns=8,
    ))

    # Summarize old turns via LLM
    AgentConfig(..., memory=MemoryConfig(
        strategy=MemoryStrategy.SUMMARIZE,
        max_kept_turns=8,
        summarize_after_turns=6,
    ))

**MemoryStrategy enum:**

.. list-table::
   :header-rows: 1
   :widths: 20 75

   * - Value
     - Behavior
   * - ``FULL``
     - Keep every message. Same as ``memory=None``. Simple but risks
       context overflow on long tasks.
   * - ``SLIDING_WINDOW``
     - Drop older turns beyond ``max_kept_turns``. Inserts a synthetic
       note: ``[Memory: N earlier tool-call pairs were removed]``.
   * - ``SUMMARIZE``
     - When ``summarize_after_turns`` pruneable turns have accumulated,
       call the LLM to condense them into a rolling summary. Prior
       summaries are incrementally updated, not regenerated.

**MemoryConfig fields:**

.. list-table::
   :header-rows: 1
   :widths: 30 15 50

   * - Field
     - Default
     - Description
   * - ``strategy``
     - ``SLIDING_WINDOW``
     - Memory management strategy.
   * - ``max_kept_turns``
     - ``8``
     - Number of recent turn-pairs to keep in full. A turn-pair is one
       assistant tool-call request + its tool result(s).
   * - ``max_tool_result_chars``
     - ``5000``
     - Maximum characters per tool result content field.
   * - ``summarize_after_turns``
     - ``6``
     - (``SUMMARIZE`` only) Trigger summarization when this many pruneable
       turns have accumulated.
   * - ``summarizer_max_tool_chars``
     - ``None``
     - Maximum chars per tool-result when building the summarizer input.
       ``None`` = no truncation.
   * - ``summarizer_max_content_chars``
     - ``None``
     - Maximum chars per non-tool message when building the summarizer
       input. ``None`` = no truncation.


MCPServerConfig Reference
---------------------------

Connect agents to remote MCP (Model Context Protocol) servers for additional
tools:

.. code-block:: python
    :linenos:

    from dragon.ai.agent.config.agent_config import MCPServerConfig

    mcp_servers = [
        MCPServerConfig(
            url="http://mcp-server:8080/mcp",
            alias="jupyter",
            token="my-api-token",
        ),
    ]

Tools from MCP servers are automatically discovered at agent startup and exposed
with scoped names: ``{alias}__{tool_name}`` (e.g., ``jupyter__create_notebook``).

.. list-table::
   :header-rows: 1
   :widths: 20 15 60

   * - Field
     - Default
     - Description
   * - ``url``
     - (required)
     - Full HTTP/HTTPS URL of the MCP server endpoint.
   * - ``alias``
     - (required)
     - Short unique label used as the tool name prefix.
   * - ``token``
     - ``None``
     - Bearer token for authentication.
   * - ``max_retries``
     - ``3``
     - Connection attempts before raising ``ConnectionError``.
   * - ``retry_delay``
     - ``0.5``
     - Seconds between retry attempts.
   * - ``timeout``
     - ``5.0``
     - Per-attempt connection timeout in seconds.


Step 4 — Build a Pipeline
==========================

A ``Pipeline`` defines the DAG topology. Each ``PipelineNode`` specifies which
agent handles it and which nodes it depends on.

.. code-block:: python
    :linenos:
    :caption: **Multi-agent pipeline DAG**

    from dragon.ai.agent.config.pipeline import Pipeline, PipelineNode

    pipeline = Pipeline(nodes=[
        PipelineNode(
            agent_id="planner",
            task_description="Design experiments to test the hypothesis.",
        ),
        PipelineNode(
            agent_id="runner",
            task_description="Execute the planned experiments.",
            depends_on=["planner"],
        ),
        PipelineNode(
            agent_id="analyzer",
            task_description="Analyze the experiment results.",
            depends_on=["runner"],
        ),
        PipelineNode(
            agent_id="reporter",
            task_description="Write a summary report.",
            depends_on=["analyzer"],
        ),
    ])

Nodes are topologically sorted automatically. Nodes with no dependencies run
first; nodes whose dependencies are all satisfied run in parallel.


PipelineNode Reference
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 20 15 60

   * - Field
     - Default
     - Description
   * - ``agent_id``
     - (required)
     - Unique node identifier (also the agent id for agent-backed nodes).
   * - ``task_description``
     - ``""``
     - Human-readable task sent to the agent. Ignored for function nodes.
   * - ``fn``
     - ``None``
     - Optional callable for function nodes (no LLM). Must accept
       ``(*upstream_results: TaskResult) -> TaskResult``.
   * - ``depends_on``
     - ``[]``
     - List of ``agent_id`` values whose results must be available first.

**Validation:** ``Pipeline.__post_init__`` rejects duplicate ``agent_id``
values, undefined ``depends_on`` references, and cycles.


Function Nodes
--------------

Not every node needs an LLM. A ``PipelineNode`` with ``fn=<callable>`` is wired
directly into the Batch graph:

.. code-block:: python
    :linenos:
    :caption: **Function node for deterministic post-processing**

    from dragon.data.ddict import DDict
    from dragon.ai.agent.config.pipeline import TaskResult, TaskStatus
    from dragon.ai.agent.ddict import DDictAccessor
    from dragon.ai.agent.config import DISPATCH_ID_KEY, RESULT_KEY

    def save_report(*upstreams: TaskResult) -> TaskResult:
        """Write the report to disk — no LLM needed."""
        ddict = DDict.attach(upstreams[0].serialized_ddict)
        accessor = DDictAccessor(ddict, agent_id="reporter",
                                 task_id=upstreams[0].task_id)
        dispatch_id = accessor.get(DISPATCH_ID_KEY.format(
            task_id=upstreams[0].task_id, agent_id="reporter"))
        result = accessor.get(RESULT_KEY.format(
            task_id=upstreams[0].task_id, agent_id="reporter",
            dispatch_id=dispatch_id))
        with open("report.md", "w") as f:
            f.write(result.get("response", ""))
        ddict.detach()
        return TaskResult(
            task_id=upstreams[0].task_id, agent_id="save_report",
            status=TaskStatus.DONE,
            serialized_ddict=upstreams[0].serialized_ddict)

    pipeline = Pipeline(nodes=[
        PipelineNode(agent_id="reporter",
                     task_description="Write the report."),
        PipelineNode(agent_id="save_report", fn=save_report,
                     depends_on=["reporter"]),
    ])


TaskResult and TaskStatus
--------------------------

``TaskResult`` is the lightweight token passed between Batch nodes:

.. list-table::
   :header-rows: 1
   :widths: 20 75

   * - Field
     - Description
   * - ``task_id``
     - Pipeline run identifier.
   * - ``agent_id``
     - Identifier of the agent that produced this result.
   * - ``status``
     - ``TaskStatus`` enum value.
   * - ``serialized_ddict``
     - Serialized DDict handle — function nodes use this to attach.
   * - ``metadata``
     - Arbitrary extra data (dict).

``TaskStatus`` enum values: ``READY``, ``PROCESSING``, ``WAITING`` (HITL),
``DONE``, ``ERROR``.


Step 5 — Start Agent Processes
===============================

Agents must be started as persistent Dragon Processes **before** running the
orchestrator.

.. code-block:: python
    :linenos:
    :caption: **Launching agent processes with node placement**

    from dragon.native.process import Process
    from dragon.native.queue import Queue
    from dragon.native.event import Event
    from dragon.infrastructure.policy import Policy
    from dragon.infrastructure.facts import HOST_NAME
    from dragon.native.machine import System, Node

    from dragon.ai.agent.core.sub_agent import create_sub_agent

    # Discover cluster topology
    all_nodes = System().nodes
    head_node = Node(all_nodes[0]).hostname
    compute_node = Node(all_nodes[1]).hostname

    shutdown_event = Event()

    # Launch each agent
    agents = {}  # agent_id -> (Process, reply_queue)
    for config, tools, mcps in [
        (planner_config, planner_tools, None),
        (runner_config, runner_tools, None),
        (analyzer_config, analyzer_tools, None),
        (reporter_config, reporter_tools, mcp_servers),
    ]:
        reply_q = Queue()
        p = Process(
            target=create_sub_agent,
            args=(config, tools, mcps, shutdown_event, reply_q),
            policy=Policy(placement=HOST_NAME, host_name=compute_node),
        )
        p.start()
        agents[config.agent_id] = (p, reply_q)

    # Collect input queues via reply queue handshake
    for agent_id, (proc, reply_q) in agents.items():
        input_queue = reply_q.get(timeout=60)
        # Store the queue on the config so the orchestrator can find it
        for cfg in [planner_config, runner_config, analyzer_config, reporter_config]:
            if cfg.agent_id == agent_id:
                cfg.input_queue = input_queue

The **reply queue handshake** is how the parent discovers each agent's input
queue: the agent creates a :py:class:`~dragon.native.queue.Queue` inside its
own process, serializes it, and puts it on the reply queue.


``create_sub_agent`` Parameters
--------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 70

   * - Parameter
     - Description
   * - ``config``
     - ``AgentConfig`` — agent identity, role, inference queue.
   * - ``tool_registry``
     - ``ToolRegistry`` — local tools. ``None`` for no tools.
   * - ``mcp_servers``
     - ``list[MCPServerConfig]`` — remote MCP server connections. ``None``
       for no MCP.
   * - ``shutdown_event``
     - ``dragon.native.event.Event`` — set by the parent to stop the agent.
   * - ``reply_queue``
     - ``dragon.native.queue.Queue`` — agent puts its input queue here after
       construction so the parent can retrieve it.


Step 6 — Configure and Run the Orchestrator
=============================================

OrchestratorConfig Reference
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 15 60

   * - Field
     - Default
     - Description
   * - ``agents``
     - ``[]``
     - List of ``AgentConfig`` instances. Duplicate ``agent_id`` values are
       rejected.
   * - ``poll_interval``
     - ``0.5``
     - Seconds between queue polls (used for HITL bridge).
   * - ``poll_timeout``
     - ``120.0``
     - Maximum seconds to wait per agent task. The dispatcher blocks on
       ``completion_event.wait(timeout=poll_timeout)``. Increase for
       long-running tasks.
   * - ``tracing``
     - ``False``
     - Enable span-based observability. ``True`` creates per-task
       ``DictTracingProcessor`` instances, writes per-event DDict keys, and
       starts a TCP bridge for live viewing.
   * - ``ddict_kwargs``
     - ``{}``
     - Passed to ``DDict(**kwargs)``. Tune ``managers_per_node``,
       ``n_nodes``, ``total_mem`` for large pipelines. Defaults applied when
       keys are absent: ``managers_per_node=1, n_nodes=1, trace=False``.


Running a Pipeline
-------------------

.. code-block:: python
    :linenos:
    :caption: **Full orchestrator setup and run**

    from dragon.ai.agent.config.agent_config import OrchestratorConfig
    from dragon.ai.agent.orchestrator.orchestrator import DAGOrchestrator
    from dragon.workflows.batch import Batch

    orch_config = OrchestratorConfig(
        agents=[planner_config, runner_config, analyzer_config, reporter_config],
        poll_timeout=300.0,
        tracing=True,
        ddict_kwargs={"managers_per_node": 2, "total_mem": 1 << 30},
    )

    orchestrator = DAGOrchestrator(config=orch_config, pipeline=pipeline)

    # Connection instructions — available immediately after construction
    if orchestrator.hitl_address:
        host, port = orchestrator.hitl_address
        print(f"HITL client:  python -m dragon.ai.agent.hitl --tcp {host}:{port}")
    if orchestrator.trace_address:
        host, port = orchestrator.trace_address
        print(f"Trace viewer: python -m dragon.ai.agent.observability --tcp {host}:{port} -i")

    try:
        batch = Batch()
        result = orchestrator.run(
            user_input="Test whether increasing learning rate improves convergence.",
            batch=batch,
        )
        print(result)
    finally:
        orchestrator.destroy()
        batch.join()
        # Shutdown agents
        shutdown_event.set()
        for proc, _ in agents.values():
            proc.join(timeout=30)
        inference.destroy()

Launch the script with the Dragon runtime:

.. code-block:: console

    dragon my_pipeline.py


DAGOrchestrator API
--------------------

.. list-table::
   :header-rows: 1
   :widths: 30 65

   * - Method / Property
     - Description
   * - ``__init__(config, pipeline)``
     - Creates shared DDict, starts HITL and trace TCP bridges. All
       properties below are available immediately after construction.
   * - ``run(user_input, batch)``
     - Execute the pipeline. Returns the terminal node's result (single
       terminal) or a ``dict[agent_id, result]`` (multiple terminals).
   * - ``destroy()``
     - Stop TCP bridges, destroy HITL Queue and DDict. Safe to call
       multiple times.
   * - ``hitl_address``
     - ``(host, port)`` of the HITL TCP bridge, or ``None``.
   * - ``trace_address``
     - ``(host, port)`` of the trace TCP bridge, or ``None``.
   * - ``ddict``
     - The shared ``DDict`` instance.
   * - ``serialized_ddict``
     - Serialized DDict descriptor (str). External clients can attach with
       ``DDict.attach(orchestrator.serialized_ddict)``.


What Happens Inside ``run()``
------------------------------

Understanding the execution flow helps with debugging and performance analysis:

1. Writes the user prompt to the DDict and seeds global state.
2. Builds a Dragon Batch DAG — each agent-backed node becomes a *dispatcher
   closure* that:

   a. Attaches to DDict, creates a completion Event.
   b. Puts a ``Message(header=DispatchHeader(...))`` on the agent's input Queue.
   c. Blocks on ``completion_event.wait(timeout=poll_timeout)`` — zero polling.
   d. Reads status from DDict (``DONE`` or ``ERROR``).
   e. Returns a ``TaskResult`` that Dragon Batch passes to downstream nodes.

3. Calls ``task_handle.get()`` on terminal nodes — blocks until done.
4. Assembles results from per-agent DDict keys.

If an agent task fails, the dispatcher reads ``status=ERROR`` from DDict and
raises ``RuntimeError``, which Dragon Batch propagates to downstream nodes.


.. _agent-tutorial-concurrency:

Agent Concurrency
=================

A single agent process can handle multiple tasks concurrently. Understanding
the concurrency model is important for tuning performance and debugging.


How It Works
------------

Each agent runs an ``asyncio`` event loop:

1. ``SubAgent.listen()`` calls ``await comm.receive(timeout=1.0)`` — the
   blocking ``Queue.get()`` is offloaded to a thread via
   ``asyncio.to_thread()`` so the event loop stays free.
2. On message arrival, the agent dispatches ``_handle_message(msg)`` as an
   independent ``asyncio.Task`` via ``asyncio.create_task()`` and immediately
   loops back to polling.
3. Multiple tasks run concurrently — the event loop is free during all LLM,
   MCP, and tool I/O waits.

.. code-block:: text
    :caption: **Concurrent task processing within a single agent**

    ┌─ Agent Process (asyncio event loop) ──────────────────────────────┐
    │                                                                   │
    │  listen loop:  receive → create_task → receive → create_task → …  │
    │                    │                      │                       │
    │    Task A: attach DDict → LLM call ⏳ → tool call → LLM → done    │
    │    Task B:              attach DDict → LLM call ⏳ → tool → done  │
    │    Task C:                          attach DDict → LLM call ⏳ …  │
    │                                                                   │
    │  ⏳ = awaiting asyncio.to_thread(queue.get) — event loop is free  │
    └───────────────────────────────────────────────────────────────────┘


Task Isolation
--------------

Tasks are fully isolated from each other:

- Each ``_handle_message()`` attaches to the DDict independently and detaches
  in a ``finally`` block.
- Exceptions are caught at the task boundary and **never re-raised** — a
  failing task writes ``status=ERROR`` to the DDict and signals the dispatcher's
  completion event, but the agent process and all other concurrent tasks
  continue running.
- Each task gets its own ``DDictAccessor`` scoped by ``dispatch_id``, so
  concurrent tasks never collide on DDict keys.


LLM Request Pool
-----------------

Each agent's ``DragonQueueLLMProxy`` maintains a ``ResponseQueuePool`` — a pool
of pre-allocated response Queues (default size: 32). When the agent sends a
prompt, it borrows a response queue; when the response arrives, the queue is
returned. Configure via ``AgentConfig.max_concurrent_requests``:

.. code-block:: python
    :linenos:

    config = AgentConfig(
        agent_id="runner", name="Runner", role="...",
        inference_queue=inference_queue,
        max_concurrent_requests=16,   # default is 32
    )

If the pool is exhausted, the next LLM request blocks until a queue is returned.


Graceful Shutdown
-----------------

When ``shutdown_event.set()`` is called, the listen loop stops accepting new
messages and awaits all in-flight tasks via
``asyncio.gather(*inflight, return_exceptions=True)``. Every task completes its
DDict writes and fires its completion event before the process exits.


Human-in-the-Loop (HITL)
=========================

The HITL system provides a human approval gate for tool calls that require
oversight.

Configuring HITL
-----------------

Set an ``approval_filter`` on the agent config — a callable that receives the
tool name and arguments and returns ``True`` if approval is needed:

.. code-block:: python
    :linenos:

    def needs_approval(tool_name: str, tool_args: dict) -> bool:
        """Require approval for any tool that launches experiments."""
        return tool_name in {"propose_experiment", "launch_experiment"}

    config = AgentConfig(
        agent_id="runner", name="Runner", role="...",
        inference_queue=inference_queue,
        approval_filter=needs_approval,
    )

When any agent in the pipeline has an ``approval_filter``, the orchestrator
automatically creates a HITL Queue and TCP bridge on construction.


Running the HITL Client
------------------------

Start the terminal client from a separate terminal (no Dragon runtime required):

.. code-block:: console

    python -m dragon.ai.agent.hitl --tcp HOST:PORT

The client presents each pending request with colored formatting and prompts
the operator to:

- **Approve** — the tool call proceeds.
- **Reject** — the tool call is skipped and the LLM receives a rejection
  message, allowing it to try an alternative approach.
- **Provide feedback** — the LLM receives the operator's text as a tool result
  and retries with the feedback incorporated.


How HITL Works (Architecture)
------------------------------

Dragon primitives (Queue, Channel, DDict) cannot be deserialized outside the
Dragon runtime. The TCP bridge keeps Dragon primitives intra-runtime and sends
only JSON to the external client:

1. Agent creates a per-request response Queue and puts
   ``(request, response_queue)`` on the shared HITL Queue.
2. ``HitlTcpBridge`` (daemon thread in orchestrator) reads from the HITL Queue,
   sends JSON over TCP.
3. TCP client reads JSON, gets operator decision, sends JSON response.
4. Bridge puts ``HumanApprovalResponse`` on the per-request response Queue.
5. Agent's coroutine unblocks and proceeds.

If the client disconnects, the bridge re-queues the request. If it cannot read
a valid response, it sends a synthetic rejection to prevent indefinite blocking.


Tracing and Observability
=========================

When ``OrchestratorConfig.tracing=True``, every LLM call, tool execution, HITL
decision, and memory operation emits structured span events.


Live Trace Viewer
------------------

.. code-block:: console

    python -m dragon.ai.agent.observability --tcp HOST:PORT -i

The viewer renders a live Gantt-bar tree of spans in the terminal. Use ``-i``
for interactive curses mode with keyboard navigation.

Traces can also be exported as JSONL files for offline analysis or as readable
``.txt`` reports that are auto-saved when the viewer exits.


Tracing Architecture
---------------------

1. **``DictTracingProcessor``** — per-task processor that writes span start/end
   events to per-agent DDict keys (``TRACE_KEY``) and per-event-type keys
   (``LLM_EVENT_KEY``, ``TOOL_EVENT_KEY``, etc.) with atomic counters.
2. **``TraceTcpBridge``** — daemon thread that polls DDict for new span data
   and streams it as newline-delimited JSON over TCP.
3. **Terminal Viewer** — pure Python (no Dragon) — connects via TCP.

When ``tracing=False``, all event writes are guarded and produce zero overhead.


Error Handling
==============

The framework defines a structured exception hierarchy:

.. list-table::
   :header-rows: 1
   :widths: 30 65

   * - Exception
     - When raised
   * - ``AgentError``
     - Base class for all framework errors.
   * - ``ToolExecutionError``
     - A tool call failed. Wraps the original exception plus the tool name
       and arguments. Fed back to the LLM so it can retry.
   * - ``AgentLoopError``
     - LLM produced unparseable JSON, or ``max_tool_call_iterations`` was
       exceeded.
   * - ``HITLBridgeError``
     - HITL TCP bridge encountered a queue or network failure.
   * - ``CompletionSignalError``
     - ``completion_event.set()`` failed — the dispatcher will time out.
   * - ``AgentObservabilityWarning``
     - Non-fatal DDict/trace write issue (emitted as ``UserWarning``).
       Promote to error: ``warnings.filterwarnings("error",
       category=AgentObservabilityWarning)``.

Key patterns:

- **Task isolation**: one failing task never crashes other concurrent tasks.
  Errors are published to DDict and propagated via the dispatcher.
- **Tool error tolerance**: ``ToolExecutionError`` is fed back to the LLM,
  which can reason about the failure and retry or try an alternative.
- **Graceful degradation**: summarization failure falls back to sliding-window
  pruning; HITL bridge disconnect re-queues requests.


DDict Key Reference
====================

All task state is stored in the Scoreboard DDict using structured key templates.
Knowing these keys is useful for debugging and building custom monitoring tools.

**Per-invocation keys** (scoped by ``{task_id}:{agent_id}:{dispatch_id}``):

.. list-table::
   :header-rows: 1
   :widths: 30 65

   * - Key template
     - Content
   * - ``RESULT_KEY``
     - Agent's final answer (dict).
   * - ``STATUS_KEY``
     - ``TaskStatus`` value (``ready``, ``processing``, ``waiting``,
       ``done``, ``error``).
   * - ``TRACE_KEY``
     - Ordered list of span dicts for this invocation.
   * - ``LLM_EVENT_KEY``
     - Per-LLM-call event (indexed by ``{index}``).
   * - ``TOOL_EVENT_KEY``
     - Per-tool-call event (indexed by ``{index}``).
   * - ``HITL_EVENT_KEY``
     - Per-HITL-decision event (indexed by ``{index}``).
   * - ``MEMORY_EVENT_KEY``
     - Per-memory-operation event (indexed by ``{index}``).
   * - ``HITL_REQUEST_KEY``
     - ``HumanApprovalRequest`` (for audit logging).
   * - ``HITL_RESPONSE_KEY``
     - ``HumanApprovalResponse`` (for audit logging).

**Per-run keys** (scoped by ``{task_id}``):

.. list-table::
   :header-rows: 1
   :widths: 30 65

   * - Key template
     - Content
   * - ``DISPATCH_ID_KEY``
     - Maps ``{task_id}:{agent_id}`` → ``dispatch_id``.
   * - ``USER_INPUT_KEY``
     - The original user prompt.
   * - ``GLOBAL_STATE_KEY``
     - Ordered list of ``{agent_id, answer}`` dicts.
   * - ``GLOBAL_STATE_ENTRY_KEY``
     - Per-agent atomic entry (avoids read-modify-write races).
   * - ``HITL_QUEUE_KEY``
     - Serialized Dragon Queue handle for the HITL channel.


Complete Example
================

Here is a minimal end-to-end script:

.. code-block:: python
    :linenos:
    :caption: **Single-agent pipeline**

    import dragon
    from dragon.native.process import Process
    from dragon.native.queue import Queue
    from dragon.native.event import Event
    from dragon.workflows.batch import Batch

    from dragon.ai.inference import (
      Inference, InferenceConfig, ModelConfig, HardwareConfig,
      BatchingConfig, GuardrailsConfig, DynamicWorkerConfig,
    )
    from dragon.ai.agent.config.agent_config import AgentConfig, OrchestratorConfig
    from dragon.ai.agent.config.pipeline import Pipeline, PipelineNode
    from dragon.ai.agent.tools.registry import ToolRegistry
    from dragon.ai.agent.core.sub_agent import create_sub_agent
    from dragon.ai.agent.orchestrator.orchestrator import DAGOrchestrator

    # 1. Inference backend
    inference_queue = Queue()
    inference = Inference(
        config=InferenceConfig(
            model=ModelConfig(model_name="meta-llama/Llama-3.1-8B-Instruct",
                              hf_token="hf_...", tp_size=1, max_tokens=2048),
            hardware=HardwareConfig(num_gpus=1, num_nodes=1),
            batching=BatchingConfig(enabled=True),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="",
        ),
        input_queue=inference_queue,
    )
    inference.initialize()

    # 2. Tools
    registry = ToolRegistry()

    @registry.tool
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    # 3. Agent config
    config = AgentConfig(
        agent_id="assistant",
        name="Math Assistant",
        role="You are a math assistant. Use the add tool to compute sums.",
        inference_queue=inference_queue,
    )

    # 4. Start agent process
    shutdown = Event()
    reply_q = Queue()
    p = Process(target=create_sub_agent,
                args=(config, registry, None, shutdown, reply_q))
    p.start()
    config.input_queue = reply_q.get(timeout=60)

    # 5. Pipeline and orchestrator
    pipeline = Pipeline(nodes=[
        PipelineNode(agent_id="assistant",
                     task_description="Compute the sum of 17 and 25."),
    ])
    orch = DAGOrchestrator(
        config=OrchestratorConfig(agents=[config]),
        pipeline=pipeline,
    )

    try:
        batch = Batch()
        result = orch.run("What is 17 + 25?", batch=batch)
        print(result)
    finally:
        orch.destroy()
        batch.join()
        shutdown.set()
        p.join(timeout=30)
        inference.destroy()


Related Cookbook Examples
========================

- :ref:`cbook_agent_basic` — Minimal single-agent example
- :ref:`cbook_agent_multiagent_dag` — Multi-agent DAG orchestration
- :ref:`cbook_agent_hitl` — Human-in-the-loop approval workflow
- :ref:`cbook_agent_memory` — Memory management strategies
- :ref:`cbook_agent_mcp` — Connecting to remote MCP servers
- :ref:`cbook_agent_full_pipeline` — Full production pipeline
