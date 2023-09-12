#!/usr/bin/env python3

import pdb
import sys

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.infrastructure.parameters as dp
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.debug_support as dds
import dragon.utils as du


def make_fake_inf_pool():
    pool = dmm.MemoryPool.create(2 ** 30, 'mtp', dfacts.infrastructure_pool_muid_from_index(0))
    dp.this_process.inf_pd = du.B64.bytes_to_str(pool.serialize())
    be_ch = dch.Channel(mem_pool=pool, c_uid=dfacts.launcher_cuid_from_index(0))
    dp.this_process.local_be_cd = du.B64.bytes_to_str(be_ch.serialize())


def main():
    print('start')
    x = 17
    sys.breakpointhook = dds.dragon_debug_hook
    dds._TESTING_DEBUG_HOOK = True
    make_fake_inf_pool()

    print('breakpoint the first')
    breakpoint()

    y = 27

    print('breakpoint the second')
    breakpoint()

    print('done')
    return 0


if __name__ == '__main__':
    exit(main())
