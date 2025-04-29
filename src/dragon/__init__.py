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
