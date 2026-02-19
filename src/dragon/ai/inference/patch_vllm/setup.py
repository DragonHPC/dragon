from setuptools import setup

setup(
    name="vllm_dragonhpc",
    version="0.1",
    description="Dragon multiprocessing plugin for vLLM",
    author="DragonHPC",
    packages=["patch_vllm"],
    package_dir={"patch_vllm": "."},
    install_requires=[
        "vllm>=0.11.0",
    ],
    entry_points={
        "vllm.general_plugins": [
            "dragon_engine = patch_vllm:patch_engine_utils",
            "dragon_multiproc_executor = patch_vllm:patch_multiproc_executor",
        ]
    },
)
