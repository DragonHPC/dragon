import dragon.perf as dperf

def get_args(arg_dict=None):
    parser = argparse.ArgumentParser(description="All-to-all Channels Performance Benchmark")
    parser.add_argument(
        "--size_min",
        type=int,
        default=8,
        help="minimum size of data exchanged between processes",
    )
    parser.add_argument(
        "--size_max",
        type=int,
        default=(512 * 1024),
        help="maximum size of data exchanged between processes",
    )
    parser.add_argument(
        "--nprocs",
        type=int,
        default=2,
        help="number of processes communicating",
    )
    parser.add_argument(
        "--managers_per_node",
        type=int,
        default=1,
        help="number of managers per node for the dragon dict",
    )
    parser.add_argument(
        "--total_mem_size",
        type=float,
        default=0.1,
        help="total managed memory size for dictionary in GB",
    )
    parser.add_argument(
        "--mem_frac",
        type=float,
        default=0.1,
        help="fraction of total_mem_size to use for keys+values",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="number of iterations at the minimum value size",
    )

    if arg_dict is None:
        return parser.parse_args()



if __name__ == "__main__":

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

