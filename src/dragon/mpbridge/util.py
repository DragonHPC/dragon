"""Dragon's replacement for Multiprocessing's logging methods."""

import logging

from dragon.dlogging import util as dlog

LOG = logging.getLogger(__name__)


def get_logger(*args, original=None, use_base_impl=True):
    if use_base_impl:
        if original == None:
            raise NameError(f"Dragon patch of Multiprocessing not correct.")
        else:
            return original(*args)
    else:
        return LOG  # PE-41692
        # return log_to_dragon()


def log_to_stderr(*args, level=None, original=None, use_base_impl=True):
    if use_base_impl:
        if original == None:
            raise NameError(f"Dragon patch of Multiprocessing not correct.")
        else:
            return original(*args, level=level)
    else:
        raise NotImplementedError(f"log_to_stderr is not implemented, yet.")
        return dlog.dragon_logger(0)  # Returning DragonLogger() indexed at node 0
