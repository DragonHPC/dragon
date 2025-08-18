#!/usr/bin/env python3

import dragon
import multiprocessing as mp
import dragon.perf as dperf

if __name__ == "__main__":
    mp.set_start_method('dragon')

    num_procs = 32

    with dperf.Session(num_procs) as session:
        kernel = session.new_kernel('large msg all-to-all')

        small_msg_size = 64
        large_msg_size = 1 * 1024 * 1024
        timeout_in_sec = 999

        # create an all-to-all kernel

        for src_ch_idx in range(num_procs):
            for dst_ch_idx in range(num_procs):
                kernel.append(dperf.Opcode.SEND_MSG, src_ch_idx, dst_ch_idx, large_msg_size, timeout_in_sec)

        for src_ch_idx in range(num_procs):
            for _ in range(num_procs):
                kernel.append(dperf.Opcode.GET_MSG, src_ch_idx, src_ch_idx, timeout_in_sec)

        # run the kernel

        kernel.run()

