"""Logging setup for Dragon runtime infrastructure

This is to bring in the logging package and allow the infrastructure
to log in a consistent way.
"""

import logging
import os
import time


def setup_logging(*, basename, basedir=None, the_level=logging.INFO, force=False, add_console=False):
    """Global logging setup.

    Sets up logging with specified logging level.

    The logfile has the timestamp in the name so it's easy to disambiguate
    between multiple test runs, etc.
    """

    if basedir is None:
        basedir = os.getcwd()

    now = time.strftime("%m_%d_%H_%M_%S", time.localtime())
    fn = "{}_{}.log".format(basename, now)
    full_fn = os.path.join(basedir, fn)
    fmt = "%(asctime)-15s %(levelname)-6s  %(name)s :: %(message)s"
    logging.basicConfig(format=fmt, filename=full_fn, level=the_level, force=force)

    if add_console:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
        console.setFormatter(formatter)
        logging.getLogger("").addHandler(console)
