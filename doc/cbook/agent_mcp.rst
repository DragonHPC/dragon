.. _cbook_agent_mcp:

MCP Server Integration
++++++++++++++++++++++

Building on Examples 01–04, this example demonstrates connecting a Dragon agent
to remote `Model Context Protocol (MCP) <https://modelcontextprotocol.io/>`__
servers. The agent can discover and invoke tools exposed by any MCP-compatible
server (e.g., data stores, compute clusters, external APIs) alongside locally
registered tools. Tool calls are automatically routed based on a namespace prefix.

**Prerequisites:** Read ``04_memory.py`` first.

**What you'll learn:**

* How to connect to one or more MCP servers via HTTP/SSE
* How to configure authentication tokens and timeouts
* How tool names are scoped (local tools vs. MCP tools via ``alias__tool_name``)
* How the ``ToolDispatcher`` routes calls to the correct server
* How to mix local and remote tools in the same agent

**Architecture:**

The agent's ``ToolDispatcher`` maintains connections to multiple MCP servers
and routes tool calls transparently: calls without a prefix go to local tools;
calls like ``datastore__query`` route to the MCP server aliased ``datastore``.

Main Code
=========

Below is the complete example:

.. literalinclude:: ../../examples/dragon_ai/ai_agent/05_mcp_tools.py
    :language: python
    :linenos:
    :caption: **05_mcp_tools.py: Agent with MCP server tools**


Key Concepts
============

**MCP Server Configuration:**

Define one or more MCP servers in ``MCPServerConfig``:

.. code-block:: python

    mcp_servers = [
        MCPServerConfig(
            url="http://datastore-service:8080/sse",
            alias="datastore",
            token="my-secret-token",
            timeout=10.0,
            max_retries=3,
        ),
        MCPServerConfig(
            url="http://compute-cluster:9090/sse",
            alias="compute",
        ),
    ]

**Tool Naming and Routing:**

* **Local tools**: registered via ``registry.register()`` or ``@registry.tool``;
  accessible as ``tool_name``
* **MCP tools**: auto-discovered from servers; accessible as ``alias__tool_name``
  (e.g., ``datastore__query``, ``compute__submit_job``)

The ``ToolDispatcher`` parses the prefix and routes the call to the correct
server (or local registry).

**Tool Discovery:**

When the agent connects to an MCP server, it auto-discovers all tools exposed
by that server. No manual schema registration needed.

Installation
============

See Example 01 (same dependencies). For MCP servers, install compatible
implementations (e.g., `mcp-server-filesystem`, `mcp-server-postgres`, etc.).

System Description
===================

Tested on HPE Cray EX:

* 1 compute node with 1–2 GPUs
* MCP servers can run on same or different nodes (TCP/HTTP connectivity required)

How to Run
==========

**Step 1: Start MCP servers (in separate terminals)**

Example with filesystem MCP server:

.. code-block:: console

    python -m mcp_server_filesystem /data/experiments

Example with custom data service:

.. code-block:: console

    python my_datastore_mcp_server.py --port 8080

**Step 2: Edit MCP URLs in the example**

Open ``05_mcp_tools.py`` and set ``MCPServerConfig`` URLs to match your servers.

**Step 3: Run the agent**

.. code-block:: console

    dragon 05_mcp_tools.py

**Example output:**

.. code-block:: console

    $ dragon 05_mcp_tools.py
    Agent 'researcher' started
    MCP server 'datastore' connected (6 tools available)
    MCP server 'compute' connected (4 tools available)
    Task: 'Find all experiments and analyze trends'
    Calling: datastore__query(filter='last_week')
    → Result: 42 experiments found
    Calling: compute__analyze(experiment_ids=[...])
    → Result: Trends calculated
    Calling: summarize_results(trends=...)
    → Result: Final summary written to report.json
    Agent completed

Next Steps
==========

* **06 — Full Pipeline** (all features combined: DAG + HITL + Memory + MCP + Tracing)
