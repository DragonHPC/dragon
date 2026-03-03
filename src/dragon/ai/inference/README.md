# Dragon Distributed ML Inference

This module is experimental and not in its final state.


## Install package dependencies & VLLM

```shell
pip3 install -r requirements.txt
pip3 install --force-reinstall vllm==0.12.0
```

### Patch vllm to make compatible with Dragon

```shell
python3 patch_vllm/vllm_utils.py
```

### Install vllm_dragon plugin

```shell
cd patch_vllm
pip3 install -e .
```


## Configuration

### Rename/Copy config.sample to config.yaml and add your custom configs

Rename the config.sample file into config.yaml.

There are 4 required key-value pairs in the config.yaml file.

- llm_model: "Your HuggingFace model or custom model path". Ex: meta-llama/Llama-3.1-8B-Instruct
- hf_token: "Your HuggingFace token". Note: Only required if your model is "closed". If open model, then add an arbitrary hugging-face token.
- tp_size: "Your model tensor-parallel size". What is tensor parallelism? <https://huggingface.co/docs/text-generation-inference/en/conceptual/tensor_parallelism>
- flask_secret_key: "Your flask application token". Note: This key can be arbitrarily generated. Ex: 123

### Optional args in config.yaml

- There are pleanty of optional configs that you can modify in config.yaml based on desired custom behavior. Details about the configuration, default values, and field-type, are all specified in the config.yaml file.