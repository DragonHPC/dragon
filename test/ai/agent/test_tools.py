"""Tests for ToolRegistry, FunctionTool, and BaseTool."""

import dragon
import multiprocessing as mp


import inspect
from typing import Any
from unittest import TestCase, main

from dragon.ai.agent.tools.base import BaseTool
from dragon.ai.agent.tools.function_tool import FunctionTool
from dragon.ai.agent.tools.registry import ToolRegistry


# ========================================================================
# BaseTool
# ========================================================================

class TestBaseTool(TestCase):
    """Verify BaseTool abstract interface and schema generation."""

    def test_cannot_instantiate_abstract(self):
        """Direct instantiation of BaseTool raises TypeError."""
        with self.assertRaises(TypeError):
            BaseTool()

    def test_concrete_subclass(self):
        """Concrete subclass with name, description, and run() works."""
        class Adder(BaseTool):
            name = "add"
            description = "Add two numbers."

            def run(self, input: dict[str, Any]) -> dict[str, Any]:
                return {"sum": input["a"] + input["b"]}

        tool = Adder()
        self.assertEqual(tool.name, "add")
        result = tool.run({"a": 1, "b": 2})
        self.assertEqual(result, {"sum": 3})

    def test_default_schema(self):
        """to_schema() returns OpenAI-compatible function schema."""
        class Noop(BaseTool):
            name = "noop"
            description = "Does nothing."

            def run(self, input):
                return {}

        schema = Noop().to_schema()
        self.assertEqual(schema["type"], "function")
        self.assertEqual(schema["function"]["name"], "noop")
        self.assertEqual(schema["function"]["description"], "Does nothing.")
        self.assertIn("parameters", schema["function"])


# ========================================================================
# FunctionTool
# ========================================================================

class TestFunctionTool(TestCase):
    """Verify FunctionTool wraps callables with auto-extracted metadata."""

    def test_sync_function(self):
        """Sync function is wrapped; name and description from function/docstring."""
        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        tool = FunctionTool(add)
        self.assertEqual(tool.name, "add")
        self.assertIn("Add two numbers", tool.description)
        self.assertEqual(tool.run({"a": 2, "b": 3}), 5)

    def test_async_function(self):
        """Async function is wrapped; run() stays a coroutine."""
        async def fetch(url: str) -> dict:
            """Fetch data."""
            return {"url": url}

        tool = FunctionTool(fetch)
        self.assertEqual(tool.name, "fetch")
        self.assertTrue(inspect.iscoroutinefunction(tool.run))

    def test_name_override(self):
        """Explicit name= overrides the function's __name__."""
        def foo():
            pass

        tool = FunctionTool(foo, name="bar")
        self.assertEqual(tool.name, "bar")

    def test_description_override(self):
        """Explicit description= overrides the function's docstring."""
        def foo():
            """Original doc."""
            pass

        tool = FunctionTool(foo, description="Custom description")
        self.assertEqual(tool.description, "Custom description")

    def test_no_docstring(self):
        """Function without docstring uses the function name as description."""
        def nodoc():
            pass

        tool = FunctionTool(nodoc)
        self.assertEqual(tool.description, "nodoc")

    def test_schema_type_hints(self):
        """Type hints are reflected in the JSON schema (str, int, required vs optional)."""
        def search(query: str, limit: int = 10) -> list:
            """Search for something.

            :param query: The search term.
            :param limit: Max results to return.
            """
            return []

        tool = FunctionTool(search)
        schema = tool.to_schema()
        func = schema["function"]
        self.assertEqual(func["name"], "search")
        params = func["parameters"]
        self.assertEqual(params["type"], "object")
        self.assertIn("query", params["properties"])
        self.assertEqual(params["properties"]["query"]["type"], "string")
        self.assertIn("limit", params["properties"])
        self.assertEqual(params["properties"]["limit"]["type"], "integer")
        self.assertIn("query", params["required"])
        self.assertNotIn("limit", params["required"])  # has default

    def test_param_descriptions_extracted(self):
        """:param docstring tags are extracted into schema property descriptions."""
        def tool(x: int):
            """Do something.

            :param x: The input value.
            """
            return x

        ft = FunctionTool(tool)
        schema = ft.to_schema()
        self.assertEqual(schema["function"]["parameters"]["properties"]["x"]["description"], "The input value.")

    def test_no_type_hints_default_string(self):
        """Parameters without type hints default to 'string' in the schema."""
        def mystery(x):
            """Mysterious."""
            pass

        tool = FunctionTool(mystery)
        schema = tool.to_schema()
        self.assertEqual(schema["function"]["parameters"]["properties"]["x"]["type"], "string")


# ========================================================================
# ToolRegistry
# ========================================================================

class TestToolRegistry(TestCase):
    """Verify ToolRegistry CRUD, auto-wrapping, and decorator support."""

    def _make_tool(self, name: str) -> BaseTool:
        class T(BaseTool):
            description = f"Tool {name}"

            def run(self, input):
                return {}

        t = T()
        t.name = name
        return t

    def test_register_and_get(self):
        """Registered tool is retrievable by name."""
        reg = ToolRegistry()
        tool = self._make_tool("alpha")
        reg.register(tool)
        self.assertIs(reg.get("alpha"), tool)

    def test_has(self):
        """has() returns False before registration, True after."""
        reg = ToolRegistry()
        self.assertFalse(reg.has("x"))
        reg.register(self._make_tool("x"))
        self.assertTrue(reg.has("x"))

    def test_get_missing_raises(self):
        """Getting an unregistered tool raises KeyError."""
        reg = ToolRegistry()
        with self.assertRaisesRegex(KeyError, "not registered"):
            reg.get("missing")

    def test_register_callable_auto_wraps(self):
        """Plain callables are auto-wrapped in FunctionTool on register."""
        def my_func():
            """My function."""
            return "ok"

        reg = ToolRegistry()
        reg.register(my_func)
        self.assertTrue(reg.has("my_func"))
        tool = reg.get("my_func")
        self.assertIsInstance(tool, FunctionTool)

    def test_list_tools(self):
        """list_tools() returns OpenAI-format schemas for all tools."""
        reg = ToolRegistry()
        reg.register(self._make_tool("a"))
        reg.register(self._make_tool("b"))
        schemas = reg.list_tools()
        self.assertEqual(len(schemas), 2)
        self.assertTrue(all(s["type"] == "function" for s in schemas))

    def test_tool_names_sorted(self):
        """tool_names() returns names in alphabetical order."""
        reg = ToolRegistry()
        reg.register(self._make_tool("beta"))
        reg.register(self._make_tool("alpha"))
        self.assertEqual(reg.tool_names(), ["alpha", "beta"])

    def test_len(self):
        """len(registry) returns the number of registered tools."""
        reg = ToolRegistry()
        self.assertEqual(len(reg), 0)
        reg.register(self._make_tool("x"))
        self.assertEqual(len(reg), 1)

    def test_unregister(self):
        """unregister() removes the tool by name."""
        reg = ToolRegistry()
        reg.register(self._make_tool("x"))
        reg.unregister("x")
        self.assertFalse(reg.has("x"))

    def test_unregister_missing_noop(self):
        """Unregistering a non-existent tool is a silent no-op."""
        reg = ToolRegistry()
        reg.unregister("nonexistent")  # no error

    def test_overwrite_same_name(self):
        """Re-registering with the same name replaces the old tool."""
        reg = ToolRegistry()
        reg.register(self._make_tool("x"))
        new_tool = self._make_tool("x")
        reg.register(new_tool)
        self.assertIs(reg.get("x"), new_tool)

    def test_decorator(self):
        """@reg.tool decorator registers the function and returns a FunctionTool."""
        reg = ToolRegistry()

        @reg.tool
        def calculate(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        self.assertTrue(reg.has("calculate"))
        self.assertIsInstance(calculate, FunctionTool)
        self.assertEqual(calculate.run({"a": 1, "b": 2}), 3)


# ========================================================================
# FunctionTool — edge cases
# ========================================================================

class TestFunctionToolEdgeCases(TestCase):
    """Verify FunctionTool handling of edge cases and unusual inputs."""

    def test_multiline_docstring_first_line_only(self):
        """Only the first non-empty line of the docstring becomes the description."""
        def multi():
            """First line is the summary.

            Extended description goes here.
            """
            pass

        tool = FunctionTool(multi)
        self.assertEqual(tool.description, "First line is the summary.")

    def test_bool_type_hint_maps_to_boolean(self):
        """bool type hint maps to JSON 'boolean'."""
        def check(flag: bool) -> str:
            """Check a flag."""
            return str(flag)

        tool = FunctionTool(check)
        schema = tool.to_schema()
        self.assertEqual(
            schema["function"]["parameters"]["properties"]["flag"]["type"],
            "boolean",
        )

    def test_float_type_hint_maps_to_number(self):
        """float type hint maps to JSON 'number'."""
        def calc(value: float) -> float:
            """Calculate."""
            return value * 2

        tool = FunctionTool(calc)
        schema = tool.to_schema()
        self.assertEqual(
            schema["function"]["parameters"]["properties"]["value"]["type"],
            "number",
        )

    def test_list_type_hint_maps_to_array(self):
        """list type hint maps to JSON 'array'."""
        def batch(items: list) -> list:
            """Process a batch."""
            return items

        tool = FunctionTool(batch)
        schema = tool.to_schema()
        self.assertEqual(
            schema["function"]["parameters"]["properties"]["items"]["type"],
            "array",
        )

    def test_dict_type_hint_maps_to_object(self):
        """dict type hint maps to JSON 'object'."""
        def process(data: dict) -> dict:
            """Process data."""
            return data

        tool = FunctionTool(process)
        schema = tool.to_schema()
        self.assertEqual(
            schema["function"]["parameters"]["properties"]["data"]["type"],
            "object",
        )

    def test_all_params_required_when_no_defaults(self):
        """Parameters without defaults are all marked required."""
        def strict(a: int, b: str, c: float) -> str:
            """Strict function."""
            return ""

        tool = FunctionTool(strict)
        schema = tool.to_schema()
        required = schema["function"]["parameters"]["required"]
        self.assertEqual(sorted(required), ["a", "b", "c"])

    def test_no_params_required_when_all_have_defaults(self):
        """Parameters with defaults are excluded from required."""
        def flexible(a: int = 1, b: str = "x") -> str:
            """Flexible function."""
            return ""

        tool = FunctionTool(flexible)
        schema = tool.to_schema()
        required = schema["function"]["parameters"].get("required", [])
        self.assertEqual(required, [])

    def test_repr(self):
        """__repr__ includes name and fn name."""
        def my_func():
            """Test."""
            pass

        tool = FunctionTool(my_func, name="custom_name")
        r = repr(tool)
        self.assertIn("custom_name", r)
        self.assertIn("my_func", r)

    def test_multiple_param_descriptions(self):
        """Multiple :param tags are all extracted."""
        def multi_doc(a: int, b: str, c: float):
            """Do many things.

            :param a: The first param.
            :param b: The second param.
            :param c: The third param.
            """
            pass

        tool = FunctionTool(multi_doc)
        schema = tool.to_schema()
        props = schema["function"]["parameters"]["properties"]
        self.assertEqual(props["a"]["description"], "The first param.")
        self.assertEqual(props["b"]["description"], "The second param.")
        self.assertEqual(props["c"]["description"], "The third param.")

    def test_run_passes_kwargs(self):
        """run() passes dict values as keyword arguments to the wrapped function."""
        def greet(name: str, greeting: str = "Hello") -> str:
            """Greet someone."""
            return f"{greeting}, {name}!"

        tool = FunctionTool(greet)
        result = tool.run({"name": "Alice", "greeting": "Hi"})
        self.assertEqual(result, "Hi, Alice!")

    def test_run_with_default_only_required(self):
        """run() with only required args uses defaults for the rest."""
        def greet(name: str, greeting: str = "Hello") -> str:
            """Greet someone."""
            return f"{greeting}, {name}!"

        tool = FunctionTool(greet)
        result = tool.run({"name": "Bob"})
        self.assertEqual(result, "Hello, Bob!")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
