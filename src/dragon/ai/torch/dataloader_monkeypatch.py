"""A class to monkeypatch PyTorch Multiprocessing DataLoader that does not work with Dragon."""

import dragon
import os
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, current
import dragon.ai.torch
import torch


class _DragonMultiProcessingDataLoaderIter(torch.utils.data.dataloader._MultiProcessingDataLoaderIter):
    """Class managing the Dragon Multiprocessing DataLoader monkeypatching functionality.

    The processes are run on the same node using policy. The DragonLoader is monkeypatched with DragonMultiprocessingDataLoader. The original functionality is saved and switched back in once PyTorch is imported.
    """

    def __init__(self, loader, *args, **kwargs):
        if System().nnodes > 1:
            with Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname):
                super().__init__(loader, **kwargs)
        else:
            super().__init__(loader, **kwargs)


class DataloaderIterMonkeypatcher:
    def __init__(self):
        """Save original torch utils DataLoader MultiprocessingDataLoader"""
        self._DataloaderIter = torch.utils.data.dataloader._MultiProcessingDataLoaderIter

    def switch_out(self) -> None:
        """Switch out Multiprocessing Dataloader for Dragon Multiprocessing DataLoader"""
        torch.utils.data.dataloader._MultiProcessingDataLoaderIter = _DragonMultiProcessingDataLoaderIter

    def switch_in(self) -> None:
        """Switch back in Dragon Multiprcocessing DataLoader with original"""
        torch.utils.data.dataloader._MultiProcessingDataLoaderIter = self._DataloaderIter


original_dataloader_iter = DataloaderIterMonkeypatcher()


def patch_mpdataloader_torch():
    # set env variable so that subprocesses get patched torch
    os.environ["DRAGON_PATCH_TORCH"] = str(True)
    # patch torch multiprocessing by switching out the Multiprocessing Dataloader for Dragon Multiprocessing DataLoader
    original_dataloader_iter.switch_out()
