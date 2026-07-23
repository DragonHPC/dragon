.. _cbook_agent_full_pipeline:

Full Production Pipeline
++++++++++++++++++++++++++

This is the capstone example, combining **all features** from the previous five
examples into a production-ready reference implementation. It demonstrates:
multi-agent DAG, HITL approval gates, memory strategies, MCP tool servers, and
real-time tracing / observability. This is your template for building complex
agentic systems on Dragon.

**Prerequisites:** Read Examples 01–05 first.

**What you'll learn:**

* How to integrate all five techniques into one system
* How to enable tracing to log every LLM call, tool invocation, approval, and
  memory operation to a structured JSONL file
* How to use the trace viewer to inspect and replay execution
* Best practices for production agent deployment on HPC clusters

**Architecture:**

* Node 0, GPUs [0,1]: Main inference service (large reasoning LLM)
* Node 1, GPU [0]: Dedicated summarizer LLM
* DAG: planner → runner → analyzer → reporter → save_report (function node)
* All agents have memory management, some have HITL approval, one has MCP tools
* Tracing enabled for full observability

Main Code
=========

Below is the complete example:

.. literalinclude:: ../../examples/dragon_ai/ai_agent/06_full_pipeline.py
    :language: python
    :linenos:
    :caption: **06_full_pipeline.py: All features combined with tracing**


Key Concepts
============

**Feature Integration:**

1. **Multi-agent DAG** (from 02): Four agents form a pipeline with automatic
   upstream result flow
2. **HITL Approval** (from 03): Planner and runner require human review for
   tool calls
3. **Memory Management** (from 04): Planner uses sliding window, runner uses
   summarization with dedicated small LLM
4. **MCP Tools** (from 05): Reporter can call tools from remote MCP servers
5. **Tracing** (new feature): Every operation logged to `./traces/{run_id}.jsonl`

**Trace Structure:**

Each line in the trace file is a JSON object:

.. code-block:: json

    {"timestamp": "...", "type": "llm_call", "agent": "planner", "model": "llama-70b", ...}
    {"timestamp": "...", "type": "tool_call", "agent": "planner", "tool": "propose_strategy", ...}
    {"timestamp": "...", "type": "hitl_approval", "agent": "planner", "status": "approved", ...}
    {"timestamp": "...", "type": "memory_op", "agent": "runner", "op": "summarize", ...}
    {"timestamp": "...", "type": "dag_transition", "from": "planner", "to": "runner", ...}

**Trace Viewer:**

Interactively explore the trace to understand agent reasoning, tool execution
paths, approval decisions, and memory operations in real-time.

Installation
============

See Example 01 (same dependencies).

System Description
===================

Tested on HPE Cray EX:

* **Node 0** (main inference): 2 Nvidia A100 GPUs, AMD EPYC 64-core CPU
* **Node 1** (summarizer): 1 Nvidia A100 GPU, AMD EPYC 64-core CPU
* Total: 2 compute nodes required

How to Run
==========

**Step 1: Edit configuration**

Open ``06_full_pipeline.py`` and set:

* ``MODEL_NAME`` (main reasoning model)
* ``SUMMARIZER_MODEL_NAME`` (small model for summarization)
* MCP server URLs (if using external tools)

**Step 2: Generate auth token (if using MCP)**

.. code-block:: console

    echo "your-mcp-server-token" > token.txt

**Step 3: Allocate nodes**

.. code-block:: console

    salloc --nodes=2 --exclusive

**Step 4: Run**

.. code-block:: console

    # Without MCP:
    dragon 06_full_pipeline.py

    # With MCP (e.g., Jupyter server):
    dragon 06_full_pipeline.py http://jupyter-mcp-server:8888

**Example output:**

.. code-block:: console

    $ dragon 06_full_pipeline.py
    Starting 2-node infrastructure...
    Node 0: Main inference service (70B model) initialized
    Node 1: Summarizer service (7B model) initialized
    Agent 'planner' started
    Agent 'runner' started
    Agent 'analyzer' started
    Agent 'reporter' started (with MCP: jupyter)
    Task: 'Execute complex analysis workflow'
    Planner proposing strategy... ✓
    HITL: Approve planner's tool call? [A/R/F]: A
    Runner executing strategy... ✓
    HITL: Approve runner's tool call? [A/R/F]: A
    Analyzer reviewing results... ✓
    Reporter generating final report... ✓
    Function node: save_report() executed
    Workflow completed
    Trace written to: ./traces/20260421-163045.jsonl

**Step 5: Inspect trace (optional, in another terminal)**

.. code-block:: console

    python -m dragon.ai.agent.observability --trace-file ./traces/20260421-163045.jsonl

**HITL approval client (in another terminal, during run)**

.. code-block:: console

    python -m dragon.ai.agent.hitl_client --tcp 127.0.0.1:9000

Use to approve/reject tool calls in real-time or add feedback.

Next Steps
==========

* **Deploy to production** using this example as a blueprint
* **Customize memory strategies** for your task's characteristics
* **Add more MCP servers** for access to domain-specific tools
* **Monitor trace files** to detect failure patterns and refine agents
* **Replay traces** for debugging and testing recovery scenarios
