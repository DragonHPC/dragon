"""A quick and dirty scheme for doing single node channels by themselves."""

import atexit
import logging
import os
import subprocess

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.infrastructure.connection as dic
import dragon.infrastructure.parameters as dparm
import dragon.utils as du

_STANDALONE_POOL_EV_NAME = "DRAGON_STANDALONE_POOL"
_STANDALONE_POOL = None
_MY_CHANNELS = set()
_IS_INIT = False
_ID_BASE = None

LOG = logging.getLogger("connection")


def destroy_standalone():
    global _STANDALONE_POOL
    _STANDALONE_POOL.destroy()
    _STANDALONE_POOL = None
    os.environ.pop(_STANDALONE_POOL_EV_NAME)


def destroy_my_channels():
    global _MY_CHANNELS

    for chan in _MY_CHANNELS:
        try:
            chan.destroy()
        except dch.ChannelError as ce:
            logging.exception("destroy of {} failed: {}".format(chan.cuid, ce))

    _MY_CHANNELS.clear()


def get_going(pool_size=2**30, pool_attr=None, pool_uid=None):
    global _STANDALONE_POOL
    if _STANDALONE_POOL_EV_NAME in os.environ:
        # get the pool descriptor from the environment
        pool_desc = du.B64.str_to_bytes(os.environ[_STANDALONE_POOL_EV_NAME])
        _STANDALONE_POOL = dmm.MemoryPool.attach(pool_desc)
    else:
        # make the pool descriptor
        username = os.environ.get("USER", str(os.getuid()))
        pool_name = f"sa_{username}_{os.getpid()!s}"
        if pool_uid is None:
            pool_uid = os.getpid()

        _STANDALONE_POOL = dmm.MemoryPool(pool_size, pool_name, pool_uid)
        os.environ[_STANDALONE_POOL_EV_NAME] = du.B64.bytes_to_str(_STANDALONE_POOL.serialize())

        # whoever was the first to make the standalone pool has the
        # responsibility for cleaning up the pool.
        # here, it just cleans it up; slightly kinder might be
        # to go into a polling loop to verify nothing is in the pool.
        # But: for use in testbenches this should probably be ok.
        atexit.register(destroy_standalone)

    # fake up some uniqueness for c_uid we make from this process.
    # the PID is probably not the very worst choice.
    global _ID_BASE
    _ID_BASE = os.getpid() * (2**20)
    atexit.register(destroy_my_channels)

    global _IS_INIT
    _IS_INIT = True


def get_new_id():
    global _ID_BASE
    tmp = _ID_BASE
    _ID_BASE += 1
    return tmp


def Pipe(duplex=True):
    global _IS_INIT
    global _MY_CHANNELS

    if not _IS_INIT:
        get_going()

    first_chan = dch.Channel(_STANDALONE_POOL, get_new_id())
    _MY_CHANNELS.add(first_chan)

    if duplex:
        second_chan = dch.Channel(_STANDALONE_POOL, get_new_id())
        _MY_CHANNELS.add(second_chan)

        first = dic.Connection(inbound_initializer=first_chan, outbound_initializer=second_chan)
        second = dic.Connection(inbound_initializer=second_chan, outbound_initializer=first_chan)
    else:
        first = dic.Connection(inbound_initializer=first_chan)
        second = dic.Connection(outbound_initializer=first_chan)

    return first, second
