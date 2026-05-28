.. _cbook_agent_basic:

Single Agent with Tool Registration
++++++++++++++++++++++++++++++++++++

This is the first and simplest example in the Agent framework series. It demonstrates
how to stand up a single Dragon agent with an LLM backend, register a sync tool,
and execute a task. This example is the foundation for all subsequent examples —
read it first to understand the basic lifecycle and configuration.

**What you'll learn:**

* How to configure an ``AgentConfig`` with minimal required parameters
* How to register a sync tool using ``registry.register(fn)``
* How to set up an inference pipeline and queue
* How to create a ``Pipeline`` with a single agent node
* How to launch and run a task through the ``DAGOrchestrator``

**Architecture:**

The agent process receives a task via Dragon Queue, calls the LLM, uses tools,
and returns results via the Scoreboard DDict.

Main Code
=========

Below is the complete example:

.. literalinclude:: ../../examples/dragon_ai/ai_agent/01_single_agent.py
    :language: python
    :linenos:
    :caption: **01_single_agent.py: Minimal single-agent example**


Key Concepts
============

**Agent Lifecycle:**

1. ``AgentConfig`` defines the agent's identity, tools, and inference backend
2. ``PipelineNode`` registers the agent in the workflow DAG
3. ``DAGOrchestrator`` spawns the agent as a Dragon process
4. Agent listens on its input queue for ``DispatchHeader`` messages
5. For each task, agent processes via the LLM + tool loop
6. Results are written to the Scoreboard DDict
7. Agent signals completion via an Event

**Tool Registration:**

Use ``registry.register(fn)`` to register a simple callable. The framework
extracts parameter annotations and generates a JSON schema for the LLM.

**Inference Setup:**

Pass an inference queue to the agent so it can send requests to the
vLLM backend. Multiple agents in the same Dragon runtime share this queue,
allowing them to submit inference requests to the same GPU worker(s).
The backend can run on the same machine or a different node.

Installation
============

After installing Dragon, ensure you have:

.. code-block:: console

    pip install torch torchvision torchaudio
    pip install vllm

System Description
===================

* For a minimal run: 1 node, 1 GPU (for vLLM), any CPU available

How to Run
==========

**Step 1: Edit the model path**

Open ``01_single_agent.py`` and set ``MODEL_NAME`` to your vLLM-compatible
checkpoint (e.g., ``meta-llama/Llama-2-7b-hf``).

**Step 2: Set HuggingFace token (if using gated models)**

.. code-block:: console

    export HF_TOKEN="hf_your_token_here"

**Step 3: Run**

.. code-block:: console

    dragon 01_single_agent.py

**Example output:**

.. code-block:: console

    $ dragon 01_single_agent.py
    Agent 'planner' started
    Task 'Estimate the convergence rate' received
    Calling LLM with tool: estimate_convergence_rate
    Result: {'convergence_rate': 0.95, 'confidence': 0.87}
    Agent 'planner' completed

Next Steps
==========

Once this example works, proceed to:

* **02 — Multi-Agent DAG** (multi-agent orchestration, function nodes, registration styles)
* **03 — Human-in-the-Loop** (approval gates before tool execution)
* **04 — Memory Management** (history strategies, dedicated summarizer LLM)
* **05 — MCP Tools** (integrate remote MCP servers)
* **06 — Full Pipeline** (all features combined with tracing)
        # ... setup code ...
        return inference_queue


    def main():
        set_start_method("dragon")

        # 1. Define tools
        registry = ToolRegistry()

        @registry.tool
        def lookup_value(key: str) -> dict:
            """Look up a value in the configuration store.

            Args:
                key: The configuration key to look up.

            Returns:
                A dict with the key and its corresponding value.
            """
            store = {"learning_rate": "0.001", "batch_size": "64", "epochs": "100"}
            value = store.get(key, "not found")
            return {"key": key, "value": value}

        # 2. Configure the agent
        inference_queue = setup_inference_queue()

        agent_config = AgentConfig(
            agent_id="assistant",
            name="Config Assistant",
            role="You are a helpful assistant that looks up configuration values. "
                 "Use the lookup_value tool to find requested settings.",
            inference_queue=inference_queue,
        )

        # 3. Build a single-node pipeline
        pipeline = Pipeline(nodes=[
            PipelineNode(
                agent_id="assistant",
                task_description="Look up the requested configuration values.",
            ),
        ])

        # 4. Run the orchestrator
        orch_config = OrchestratorConfig(agents=[agent_config])
        orchestrator = DAGOrchestrator(config=orch_config, pipeline=pipeline)

        try:
            batch = Batch()
            result = orchestrator.run(
                user_input="What are the learning_rate and batch_size settings?",
                batch=batch,
            )
            print("Agent result:", result)
        finally:
            orchestrator.destroy()


    if __name__ == "__main__":
        main()


How to run
==========

.. code-block:: console

    dragon basic_agent_pipeline.py
