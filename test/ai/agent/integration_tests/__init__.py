"""
Integration tests for the Dragon Agent framework.

These tests verify the correct interaction between multiple components
of the agent pipeline, including LLM reasoning, tool dispatch, HITL
approval, batch orchestration, and memory management.

Unlike the unit tests (which mock Dragon primitives), these tests require
a running Dragon runtime and use real Dragon Queues, DDicts, and Events.

Run with: dragon python -m unittest discover -s test/ai/agent/integration_tests -p "test_*.py" -v
"""
