.. _cbook_agent_hitl:

Human-in-the-Loop Approval
+++++++++++++++++++++++++++

Building on Examples 01–02, this example demonstrates the human-in-the-loop (HITL)
approval system. A human operator reviews and approves or rejects tool calls before
they execute. This is essential for high-stakes operations: deleting data, deploying
to production, running expensive jobs, or any action requiring oversight.

**Prerequisites:** Read ``02_multi_agent_dag.py`` first.

**What you'll learn:**

* How to configure an ``ApprovalFilter`` to specify which tools need review
* How to run the HITL approval client in a separate terminal
* How operators approve, reject, or provide feedback on tool calls
* How the agent adjusts its approach based on rejections
* How to audit and replay HITL decisions

**Architecture:**

The agent calls the LLM, receives tool proposals, sends dangerous ones to the
HITL gateway (TCP), and awaits human decision before executing. Safe tools
execute immediately.

Main Code
=========

Below is the complete example:

.. literalinclude:: ../../examples/dragon_ai/ai_agent/03_hitl_approval.py
    :language: python
    :linenos:
    :caption: **03_hitl_approval.py: Agent with human approval gate**


Key Concepts
============

**Approval Filter:**

Define a function that returns ``True`` for tool names that need review:

.. code-block:: python

    DANGEROUS_TOOLS = {"delete_experiment", "launch_expensive_job"}

    def approval_filter(tool_name: str, tool_args: dict) -> bool:
        return tool_name in DANGEROUS_TOOLS

Pass it to ``AgentConfig(approval_filter=approval_filter)``.

**HITL Client:**

The operator runs a separate Python process that connects via TCP and displays
pending approvals with:

* Tool name and arguments
* Agent's reasoning
* Options to approve, reject, or add feedback

**Audit Log:**

Every HITL decision is logged. You can replay sessions for training, auditing,
or testing recovery paths.

Installation
============

See Example 01 (same dependencies).

System Description
===================

Tested on HPE Cray EX:

* 1 node, 1 GPU (vLLM backend)
* Can run on single development machine

How to Run
==========

**Terminal 1 — Launch the pipeline:**

.. code-block:: console

    dragon 03_hitl_approval.py

The pipeline will output something like:

.. code-block:: console

    Agent 'manager' started
    Task received: 'Delete failed experiments and launch new batch'
    LLM proposing: delete_experiment(experiment_id='exp-001')
    Waiting for approval at 127.0.0.1:9000
    ...

**Terminal 2 — Start the approval client:**

.. code-block:: console

    python -m dragon.ai.agent.hitl_client --tcp HOST:PORT

Replace ``HOST:PORT`` with the address shown by the pipeline (e.g., ``localhost:9000``).

The client presents pending requests. The operator can:

- **Approve (A)** — tool executes
- **Reject (R)** — tool is skipped, LLM receives rejection feedback
- **Feedback (F)** — add a message the LLM sees, e.g., "Insufficient permissions"

**Audit log (optional):**

After a run, replay decisions for testing or training:

.. code-block:: console

    python -m dragon.ai.agent.hitl_client --replay audit_log.jsonl

Next Steps
==========

* **04 — Memory Management** (history strategies for long-running agents)
