from .monkeypatching import patch_torch

patch_torch()

from .dataloader_monkeypatch import patch_mpdataloader_torch

patch_mpdataloader_torch()

from .dictdataset import DragonDataset