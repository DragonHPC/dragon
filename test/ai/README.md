# Dragon AI Tests

This directory contains tests for Dragon AI components:

- `test_collective_group.py`: collective process group helpers.
- `test_public_api.py`: package-level `dragon.ai` public API smoke tests.
- `inference/`: Dragon inference service tests.
- `agent/`: Dragon agent framework tests.
- `torch/`: DragonTorch patch tests.

All AI tests are standard `unittest.TestCase` tests and run with the Python
standard-library `unittest` runner under the Dragon runtime. Each suite runs in
its own Dragon process so heavy optional dependencies are loaded only for the
suite that needs them and memory stays bounded.

## Default `make test`

The `TESTS_AI` variable in `test/Makefile` lists the AI targets that run as part
of the default test suite.

`TESTS_AI` contains:

- `test_collective_group.py`: collective process group helpers.
- `test_public_api.py`: package-level `dragon.ai` public API smoke tests. The
  `dragon.ai.torch` check is skipped automatically when PyTorch is not installed.
- `inference`: full Dragon inference service suite.
- `agent`: full Dragon agent framework suite.
- `torch`: full DragonTorch patch suite.

Install the `ai` extra so the full suites have their dependencies. From a
source checkout, use the editable `src` install:

```shell
pip3 install -e "src[ai]"
```

When installing from a published package instead of a source checkout, request
the same extra on the `dragonhpc` package:

```shell
pip3 install "dragonhpc[ai]"
```

Run everything with the default target (which also runs the rest of the test
suite):

```shell
make -C test test
```

Each entry is also its own target. The `.py` smoke tests run through the
`unittest` module runner, and the suite directories run through `unittest`
discovery (`torch` uses its own top-level directory because it is not a
package):

```shell
make -C test test_collective_group.py
make -C test test_public_api.py
make -C test inference
make -C test agent
make -C test torch
```

The suite directories also have dedicated aliases that invoke `unittest`
discovery directly:

```shell
make -C test test_ai_inference
make -C test test_ai_agent
make -C test test_ai_torch
```

The equivalent direct commands are:

```shell
cd test/ai && dragon python3 -m unittest discover -s inference -p "test_*.py" -t .
cd test/ai && dragon python3 -m unittest discover -s agent -p "test_*.py" -t .
cd test/ai && dragon python3 -m unittest discover -s torch -p "test_*.py" -t torch
```

The inference suite contains tests that inspect the real Dragon hardware view.
In CPU-only environments, tests that request GPU tensor parallelism are skipped
when Dragon reports zero visible GPUs.

The performance tests under `inference/performance_tests/` are not collected by
the discovery pattern. Run them manually when the required models, dependencies,
and hardware are present.