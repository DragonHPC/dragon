# Dragon AI Agent — Progressive Examples

Six numbered examples that teach the Dragon agent framework one concept at a
time. **Read them in order.** Every example builds on the previous one, using
the same Monte Carlo convergence theme throughout.

## Overview

| # | File | Concepts introduced | Agents |
|---|------|---------------------|--------|
| 01 | `01_single_agent.py` | Agent lifecycle, tool registration, inference setup | 1 |
| 02 | `02_multi_agent_dag.py` | DAG orchestration, function nodes, async tools, 3 registration styles | 4 + fn |
| 03 | `03_hitl_approval.py` | Human-in-the-loop approval gates, three-way decisions | 4 |
| 04 | `04_memory.py` | Memory strategies (sliding window, summarize), dedicated summarizer LLM | 4 |
| 05 | `05_mcp_tools.py` | MCP server integration, scoped tool names, mixed local+remote tools | 4 |
| 06 | `06_full_pipeline.py` | All features combined + tracing/observability | 4 + fn |

## Prerequisites

1. **Dragon runtime** — all examples run under `dragon <script>.py`
2. **Model path** — edit `MODEL_NAME` at the top of each file to point at your
   local model (any vLLM-compatible checkpoint)
3. **GPU allocation** — examples 01–03 and 05 need ≥ 2 GPUs on 1 node;
   examples 04 and 06 need 2 nodes (one for the summarizer LLM)
4. **MCP server** (05, 06 only) — optional; examples fall back to local tools
5. **Python packages** — `pip install -r requirements.txt` in the repo root

## Quick start

```bash
# Simplest possible run — single agent, single tool:
dragon 01_single_agent.py
```

## Per-example guide

---

### 01 — Single Agent

**What you learn:** the minimal building blocks of a Dragon agent.

| Concept | Where to look |
|---------|---------------|
| Set model path and HF token | `01_single_agent.py` line 67 |
| Configure the inference pipeline (`InferenceConfig`, `ModelConfig`, `HardwareConfig`) | lines 77–97 |
| Register a sync tool with `registry.register(fn)` | line 106 |
| Define a single-node `Pipeline` with `PipelineNode` | lines 113–126 |
| Build `AgentConfig` and launch the agent as a Dragon Process | lines 154–163 |
| Create `DAGOrchestrator` and call `orchestrator.run()` | lines 182–206 |

**Run it:**
```bash
dragon 01_single_agent.py
```

---

### 02 — Multi-Agent DAG

**What's new:** connect multiple agents into a dependency graph, add a
plain-function node, and explore all three tool registration styles.

| Concept | Where to look |
|---------|---------------|
| Style 1 — `registry.register(fn)` for imported sync tools | `02_multi_agent_dag.py` lines 120–127 |
| Style 2 — `@registry.tool` for an inline **async** tool | lines 129–165 |
| Style 3 — `@registry.tool` for an inline sync tool | lines 167–215 |
| `PipelineNode(fn=...)` — function node (no LLM) | lines 218–288, line 337 |
| `depends_on` — declaring DAG edges between agents | lines 294–338 |
| `DAGOrchestrator` with a 4-agent + 1 fn-node pipeline | line 446 |

**Run it:**
```bash
dragon 02_multi_agent_dag.py
```

---

### 03 — HITL Approval Gates

**What's new:** pause the agentic loop and route specific tool calls to a
human operator before execution.  The operator can approve, reject, or give
feedback (the agent retries incorporating the feedback).

| Concept | Where to look |
|---------|---------------|
| `approval_filter` parameter on `AgentConfig` | `03_hitl_approval.py` line 192 |
| Gate `propose_experiment` on the planner agent | line 271 |
| Gate `launch_experiment` on the runner agent | line 282 |
| Print HITL TCP address at startup for the client | lines 331–341 |
| HITL client options (`--jsonl`, `--report`, `--no-color`) | lines 335–340 |

**Run it:**
```bash
# Terminal 1:
dragon 03_hitl_approval.py

# Terminal 2 (HITL client — HOST:PORT printed at startup):
python -m dragon.ai.agent.hitl --tcp HOST:PORT
```

---

### 04 — Memory Strategies

**What's new:** control how each agent manages its growing conversation
history, and spin up a dedicated summarizer LLM on a separate GPU so the main
model stays uncontended.

| Concept | Where to look |
|---------|---------------|
| Second `InferenceConfig` for the summarizer model (`node_offset=1`) | `04_memory.py` lines 121–143 |
| `MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=3)` on planner | lines 333–336 |
| `MemoryConfig(strategy=MemoryStrategy.SUMMARIZE, summarize_after_turns=2)` on runner | lines 348–352 |
| Pass `summarizer_inference_queue` to the runner agent | line 354 |
| `FULL` strategy (default) — no `MemoryConfig` needed | analyzer/reporter agents |

**Memory strategy comparison:**

| Agent | Strategy | Effect |
|-------|----------|--------|
| planner | `SLIDING_WINDOW` | Drops turns beyond `max_kept_turns` |
| runner | `SUMMARIZE` | Compresses old turns via a second LLM |
| analyzer / reporter | `FULL` (default) | Keeps everything |

**Run it:**
```bash
# Needs 2 nodes:
dragon 04_memory.py

# HITL client (same as 03):
python -m dragon.ai.agent.hitl --tcp HOST:PORT
```

---

### 05 — MCP Tools

**What's new:** connect an agent to a remote Model Context Protocol server.
MCP tools are auto-discovered and merged into the LLM's tool list, prefixed
with the alias to avoid collisions with local tools.

| Concept | Where to look |
|---------|---------------|
| Read MCP bearer token from file (never CLI) | `05_mcp_tools.py` lines 274–285 |
| `MCPServerConfig(url=..., alias="jupyter", token=...)` | line 294 |
| Pass `mcp_servers` to the reporter agent | line 337 |
| Graceful fallback when no MCP URL is provided | lines 291–303 |
| Scoped tool names — `jupyter__create_notebook`, etc. | reporter task description |

**Run it:**
```bash
# Without MCP (local tools only):
dragon 05_mcp_tools.py

# With MCP — write your token to token.txt in the working directory first:
echo '<your-token>' > token.txt
dragon 05_mcp_tools.py https://your-mcp-server/
```

> **Token file:** the script reads `token.txt` from the current working
> directory (`os.getcwd()`).  Never pass the token on the command line.
> The file is read as raw bytes and UTF-8 decoded, so a trailing newline or
> BOM is stripped automatically.

---

### 06 — Full Pipeline

**What's new:** enable tracing to capture a complete JSONL log of every LLM
call, tool call, approval decision, memory operation, and DAG state transition.
All features from examples 02–05 are combined.

| Concept | Where to look |
|---------|---------------|
| `tracing=True` on `OrchestratorConfig` | `06_full_pipeline.py` line 531 |
| Print trace viewer TCP address at startup | lines 546–555 |
| Trace viewer options (`-i`, `--jsonl`, `--report`, `--no-color`) | lines 549–554 |
| Function node — `save_report` writes `.md` + `.json` to disk | lines 185–257 |
| `REPORT_DIR` env-var override for output directory | line 182 |
| Combining all features in `_make_agent_kwargs` | lines 379–397 |

**Run it:**
```bash
# Needs 2 nodes; MCP optional — write your token to token.txt first if using MCP:
echo '<your-token>' > token.txt
dragon 06_full_pipeline.py [mcp_url]

# HITL client:
python -m dragon.ai.agent.hitl --tcp HOST:PORT

# Trace viewer (HOST:PORT printed at startup):
python -m dragon.ai.agent.observability --tcp HOST:PORT -i
```

---

## Shared tools

All examples import from the `tools/` package in this directory. The tools
simulate a Monte Carlo experiment pipeline:

| Module | Tools |
|--------|-------|
| `tools/planner.py` | `propose_experiment` |
| `tools/runner.py` | `launch_experiment`, `check_progress`, `collect_results` |
| `tools/analyzer.py` | `analyze_convergence` |
| `tools/reporter.py` | `format_results_table` |
| `tools/worker.py` | Internal — Monte Carlo worker process |

## Directory layout

```
examples/dragon_ai/ai_agent/
├── README.md               ← you are here
├── 01_single_agent.py
├── 02_multi_agent_dag.py
├── 03_hitl_approval.py
├── 04_memory.py
├── 05_mcp_tools.py
├── 06_full_pipeline.py
└── tools/
    ├── __init__.py
    ├── planner.py
    ├── runner.py
    ├── analyzer.py
    ├── reporter.py
    └── worker.py
```

