"""Module contains dragonError_t values, for convenience."""

from functools import singledispatch
from dragon.rc import DragonError


DRAGON_SUCCESS = DragonError.SUCCESS
DRAGON_TIMEOUT = DragonError.TIMEOUT
DRAGON_FAILURE = DragonError.FAILURE
DRAGON_INVALID_ARGUMENT = DragonError.INVALID_ARGUMENT
DRAGON_NOT_IMPLEMENTED = DragonError.NOT_IMPLEMENTED
DRAGON_INTERNAL_MALLOC_FAIL = DragonError.INTERNAL_MALLOC_FAIL


def get_errno(e):
    return getattr(e, "lib_err", _errno(e))


@singledispatch
def _errno(e):
    return DRAGON_FAILURE


@_errno.register
def _(e: ValueError):
    return DRAGON_INVALID_ARGUMENT


@_errno.register
def _(e: NotImplementedError):
    return DRAGON_NOT_IMPLEMENTED


@_errno.register
def _(e: TimeoutError):
    return DRAGON_TIMEOUT
