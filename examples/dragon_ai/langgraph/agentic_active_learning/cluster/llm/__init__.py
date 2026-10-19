"""Where the LLM is, and what it is allowed to do.

Two uses. `brief` asks once for settings, with a JSON schema. `analyze` runs a
short ReAct loop in `tools`: the model writes a thought, names a tool, Python
runs it, the result comes back as the next message. Every tool is read-only, the turn
and call budgets are enforced in code, and the only thing that reaches the
graph is a region to sample next. The model cannot launch or cancel anything.
"""
