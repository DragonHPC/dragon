import os
from importlib.metadata import version, PackageNotFoundError

try:
    DRAGON_VERSION = version("dragonhpc")
except PackageNotFoundError:
    DRAGON_VERSION = os.environ.get("DRAGON_VERSION", "(unknown)")

__version__ = DRAGON_VERSION