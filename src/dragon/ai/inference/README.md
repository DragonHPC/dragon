# Dragon Inference Service

This package implements the Dragon-native inference service under
`dragon.ai.inference`. It provides a shared vLLM-backed LLM backend for Dragon
applications, agents, and workflows using Dragon queues, process groups,
placement policies, telemetry, and optional dynamic batching and guardrails.

The service is experimental and its public API may change.

## Documentation

The central Dragon documentation is the canonical source for installation,
configuration, examples, and API details:

- [Dragon Inference Service - User Guide](https://dragonhpc.github.io/dragon/doc/_build/html/uses/inference.html)
- [Inference Service Examples](https://dragonhpc.github.io/dragon/doc/_build/html/cbook/ai_inference/index.html)
- [Inference Service - Developer Guide](https://dragonhpc.github.io/dragon/doc/_build/html/devguide/inference.html)
- [Inference API Reference](https://dragonhpc.github.io/dragon/doc/_build/html/ref/ai/inference/index.html)

## Local Files

- `config.sample` provides a starting YAML configuration.
- `patch_vllm/` contains the `vllm_dragonhpc` Dragon vLLM compatibility
	plugin.

## Configuration

Use `config.sample` as the starting point for YAML-based configuration. Copy it
to your own configuration file, then update the required values under the
`required` section:

- `model_name`: Hugging Face model name or local model path.
- `hf_token`: Hugging Face token for gated models, or another token string for
	open/local models.
- `tp_size`: Number of GPU devices used by each tensor-parallel model worker.
- `flask_secret_key`: Unique secret string for Flask-based integrations.

Optional sections in `config.sample` configure hardware selection, vLLM model
settings, batching, guardrails, and dynamic inference-worker management. See the
central user guide for the full configuration walkthrough.

## Installation

Install Dragon with the `ai` optional dependency set. The `ai` extra installs a
supported vLLM (`vllm>=0.11.0,<0.18.0`) and bundles the Dragon vLLM
compatibility plugin, which is registered automatically through vLLM's
`vllm.general_plugins` entry point group.

```shell
pip3 install "dragonhpc[ai]"
```

For a source checkout, install from the repository root with:

```shell
pip3 install -e "src[ai]"
```

The plugin patches vLLM at runtime when an `LLM()` instance starts inside the
Dragon inference service. The patches are a no-op outside Dragon, and nothing
needs to be re-run after changing or reinstalling vLLM.