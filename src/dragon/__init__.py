"""This code is first executed when the `import dragon` statement is encountered
in the user code.

If the environment variable `DRAGON_PATCH_MP` is set, we assume the user wants
to use Dragon below the standard Multiprocessing module. We then monkeypatch the
multiprocessing module to hook in our own context into the context hierarchy:

    BaseContext <|- DefaultContext <|- AugmentedDefaultContext

Our `AugmentedDefaultContext` replaces the `DefaultContext`, and replaces the
`get_start_method` and `set_start_method` to make our context available.

We collect the public API of the Multiprocessing API and point it towards
our context, that contains simple functions returning our Dragon classes.

Finally, we need to make sure that imports like this one:
```
import multiprocessing.context.wait
```
or inheritance like this:
```
class Foo(multiprocessing.synchronize.Event):
```
work with our Dragon classes. To do so, we save all Multiprocessing classes in
the variable `original_multiprocessing` that can be imported from the
dragon.mpbridge.monkeypatching module and replace them with our own Dragon
classes.

Note that this final step cannot be completely undone when switching contexts.

I.e. switching from start method "dragon" back to a start method "spawn" will
break user code that has inherited or imported directly from Multiprocessing.

General advice to users is to use the context to get the Multiprocessing
functionality.
"""

import os
import sys
import importlib.util
from .version import __version__ as raw_version

class LazyClass:
    def __init__(self, module_path: str, class_name: str):
        self._module_path = module_path
        self._class_name = class_name
        self._cls = None

    def _get_class(self):
        if self._cls is None:
            module = importlib.import_module(self._module_path, package="dragon")
            self._cls = getattr(module, self._class_name)
        return self._cls

    def __call__(self, *args, **kwargs):
        return self._get_class()(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._get_class(), item)

# When performing a clean build, there is an edge case where
# we get a circular import error. If the direct import fails,
# fall back to using the Lazy import option.

try:
    from .data.ddict.ddict import DDict
except ImportError as e:
    DDict = LazyClass(".data.ddict.ddict", "DDict")

try:
    from .infrastructure.policy import Policy
except ImportError as e:
    Policy = LazyClass(".infrastructure.policy", "Policy")

try:
    from .native.machine import current, System
except ImportError as e:
    System = LazyClass(".native.machine", "System")
    current = LazyClass(".native.machine", "current")

try:
    from .native.process import ProcessTemplate
except ImportError as e:
    ProcessTemplate = LazyClass(".native.process", "ProcessTemplate")

try:
    from .native.process_group import ProcessGroup
except ImportError as e:
    ProcessGroup = LazyClass(".native.process_group", "ProcessGroup")

try:
    from .workflows.batch.batch import Batch
except ImportError as e:
    Batch = LazyClass(".workflows.batch.batch", "Batch")


# Re-exporting the constant with a type hint and static docstring
__version__ : str = raw_version
""" The current release version of Dragon """



def in_dragon():
    try:
        from dragon.globalservices.api_setup import get_gs_ret_cuid
    except:
        return False
    try:
        _ = get_gs_ret_cuid()
    except:
        return False
    return True


def _patch_multiprocessing():
    # Set DRAGON_PATCH_MP so multiprocessing is automatically patched when
    # imported in a subprocess.
    os.environ["DRAGON_PATCH_MP"] = str(True)

    from .mpbridge.monkeypatching import patch_multiprocessing

    patch_multiprocessing()


def _patch_torch():

    from .ai.torch.monkeypatching import patch_torch

    patch_torch()

    from .ai.torch.dataloader_monkeypatch import patch_mpdataloader_torch

    patch_mpdataloader_torch()


if os.environ.get("DRAGON_PATCH_MP", False):
    _patch_multiprocessing()

if os.environ.get("DRAGON_PATCH_TORCH", False):
    _patch_torch()
