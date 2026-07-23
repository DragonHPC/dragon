# Dragon vLLM Compatibility Plugin

This directory contains the Dragon compatibility plugin for running vLLM inside
the Dragon inference service. The plugin ships inside the `dragonhpc` package as
`dragon.ai.inference.patch_vllm`.

The plugin registers Dragon-specific vLLM general plugins that patch the small
set of vLLM internals that need Dragon-aware behavior:

- engine startup waiting for Dragon-managed process sentinels
- multiprocess worker startup and readiness handling
- multiprocessing-context selection so vLLM inherits the active Dragon context
- open-port selection for co-located vLLM instances

## Install

The plugin is bundled with `dragonhpc` and its entry points are declared by the
package. The `ai` extra installs a supported vLLM (`vllm>=0.11.0,<0.18.0`)
alongside it, so installing the extra is all that is required.

```shell
pip3 install "dragonhpc[ai]"
```

vLLM discovers the plugin through its `vllm.general_plugins` entry point group
and loads it automatically when an `LLM()` instance starts. Each patch checks
for the `_DRAGON_DEVICE_OFFSET` environment variable that the Dragon inference
service sets before constructing `LLM()`, and is a no-op when vLLM runs outside
Dragon. Nothing needs to be re-run after changing or reinstalling vLLM.

## Compatibility

The plugin supports `vllm>=0.11.0,<0.18.0`. At runtime, it selects the Dragon
patch implementation that matches the installed vLLM version.

## Entry Points

`dragonhpc` registers these `vllm.general_plugins` entry points:

- `dragon_engine = dragon.ai.inference.patch_vllm:patch_engine_utils`
- `dragon_multiproc_executor = dragon.ai.inference.patch_vllm:patch_multiproc_executor`
- `dragon_get_open_port = dragon.ai.inference.patch_vllm:patch_get_open_port`
- `dragon_mp_context = dragon.ai.inference.patch_vllm:patch_mp_context`
- `dragon_engine_core = dragon.ai.inference.patch_vllm:patch_engine_core`