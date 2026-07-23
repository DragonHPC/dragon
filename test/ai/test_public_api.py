"""Tests for the dragon.ai package public surface."""

from unittest import TestCase, skipUnless

import dragon.ai as ai


# ``dragon.ai.torch`` imports PyTorch, which ships in the ``ai``/``examples``
# optional dependency sets and is not present in the base test environment.
# Skip the torch wiring check when PyTorch (and therefore the Dragon torch
# integration) cannot be imported.
try:
    import dragon.ai.torch as _dragon_torch  # noqa: F401

    _DRAGON_TORCH_AVAILABLE = True
except Exception:
    _DRAGON_TORCH_AVAILABLE = False


class TestDragonAIPublicAPI(TestCase):
    """Test package-level Dragon AI discovery behavior."""

    def test_all_lists_ai_subpackages(self):
        """__all__ should list the supported Dragon AI subpackages."""
        self.assertEqual(
            sorted(ai.__all__),
            [
                "agent",
                "collective_group",
                "inference",
                "torch",
            ],
        )

    def test_lazy_imports_resolve_dragon_subpackages(self):
        """Package attributes should resolve to Dragon AI subpackages."""
        self.assertEqual(ai.inference.__name__, "dragon.ai.inference")
        self.assertEqual(ai.agent.__name__, "dragon.ai.agent")
        self.assertEqual(ai.collective_group.__name__, "dragon.ai.collective_group")

    @skipUnless(_DRAGON_TORCH_AVAILABLE, "PyTorch is not installed (ai/examples extra)")
    def test_torch_attribute_resolves_local_dragon_package(self):
        """dragon.ai.torch should be the Dragon subpackage, not PyTorch."""
        self.assertEqual(ai.torch.__name__, "dragon.ai.torch")

    def test_unknown_attribute_raises_attribute_error(self):
        """Unknown package attributes should raise AttributeError."""
        with self.assertRaises(AttributeError):
            ai.not_a_real_ai_component