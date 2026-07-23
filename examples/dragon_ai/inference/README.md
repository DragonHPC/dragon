# Dragon Inference Examples

This directory contains standalone examples for the Dragon inference service.
These examples use `dragon.ai.inference` directly, without the agent framework.

## Single Prompt Service

`single_prompt.py` starts one shared inference backend, submits one prompt, and
prints the assistant response.

Before running, edit the configuration constants at the top of
`single_prompt.py` (model path, Hugging Face token, tensor-parallel size, and
hardware). These map to the configuration dataclasses in
[`config.py`](../../../src/dragon/ai/inference/config.py). A YAML-based
alternative using [`config.sample`](../../../src/dragon/ai/inference/config.sample)
and `InferenceConfig.from_dict` is described in the Dragon inference user guide.

```shell
dragon single_prompt.py
```

The example requires a Dragon allocation with at least one visible GPU for each
selected node.

For the complete user guide, see the
[Dragon Inference Service documentation](https://dragonhpc.github.io/dragon/doc/_build/html/uses/inference.html).