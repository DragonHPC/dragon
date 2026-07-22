"""Dragon AI integrations.

This package groups Dragon's AI-facing components:

* ``dragon.ai.torch``: PyTorch integration and DragonDataset support.
* ``dragon.ai.inference``: Dragon-native shared LLM inference service.
* ``dragon.ai.agent``: Agent framework built on Dragon queues and inference.
* ``dragon.ai.collective_group``: Collective process group helpers.

Subpackages are imported lazily so importing ``dragon.ai`` does not trigger
optional AI dependencies or PyTorch monkey patches. Import the component you
need directly, for example ``import dragon.ai.torch`` or
``from dragon.ai.inference import Inference``.
"""

from importlib import import_module


__all__ = [
	"agent",
	"collective_group",
	"inference",
	"torch",
]


def __getattr__(name):
	"""Lazily import Dragon AI subpackages."""
	if name in __all__:
		module = import_module(f"{__name__}.{name}")
		globals()[name] = module
		return module
	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
	"""Return module attributes for interactive discovery."""
	return sorted(list(globals()) + __all__)