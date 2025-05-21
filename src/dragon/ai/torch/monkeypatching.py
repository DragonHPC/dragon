"""A class to monkeypatch PyTorch Multiprocessing reductions that do not work with Dragon.

PyTorch's multiprocessing introduces specialized reductions for PyTorch data objects like tensors. These reductions utilize shared memory and duplicate file-descriptors. This is incompatible with Dragon. This monkeypatch disables the ability to register these reductions with the ForkingPickler, imports torch, and then returns the ability to register reductions with the ForkingPickler.
"""

import dragon
import os
from multiprocessing.reduction import ForkingPickler


def dragon_fp_register(cls, type, reduce):
    return


class FPregisterMonkeypatcher:
    """Class managing the ForkingPickler monkeypatching functionality.

    We temporarily replace the ForkingPickler.register function to be a no-op. We save the original ForkingPickler.register function so that we can switch it back in once PyTorch has been imported.
    """

    def __init__(self):
        """Save original ForkingPickler.register function"""
        self.fp_register = ForkingPickler.register

    def switch_out(self) -> None:
        """Switch out ForkingPickler.register function for no-op"""
        ForkingPickler.register = classmethod(dragon_fp_register)

    def switch_in(self) -> None:
        """Switch back in ForkingPickler.register function with original"""
        ForkingPickler.register = self.fp_register


original_fp_register = FPregisterMonkeypatcher()


def patch_torch():
    """This is called when dragon.ai.torch is imported. This needs to be done before torch is imported. The environment variable ensures that all subprocesses also patch torch.

    The mechanics of the patch are as follows:
        1. Make ForkingPickler.register a no-op so that reductions registered with the ForkingPickler are ignored. This function is used in torch.multiprocessing.reductions.init_reductions to register the specialized reductions.
        2. Import PyTorch while the ForkingPickler.register function is a no-op
        3. Switch back in the ForkingPickler.register's original functionality so that downstream functions are actually registered.
    """
    # set env variable so that subprocesses get patched torch
    os.environ["DRAGON_PATCH_TORCH"] = str(True)
    # patch torch multiprocessing by making ForkingPickler.register a no-op, importing torch, and then returning the ForkingPickler.register to its original functionality
    original_fp_register.switch_out()
    import torch

    # Avoid some versions of PyTorch defaulting to trying to share a file
    # descriptor associated with single-node cpu model sharing whether
    # cpu compute is requested or not.
    torch.multiprocessing.set_sharing_strategy("file_system")

    original_fp_register.switch_in()
