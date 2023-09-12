import subprocess
from pathlib import Path
import dragon.infrastructure.parameters as dparms

CH_P2P_LATENCY = 'ch_p2p_latency'
CH_P2P_MSG_RATE = 'ch_p2p_msg_rate'
CH_P2P_BANDWIDTH = 'ch_p2p_bandwidth'

CH_P2P_TESTS = [
    CH_P2P_LATENCY,
    CH_P2P_MSG_RATE,
    CH_P2P_BANDWIDTH
]


def main():
    try:
        nodes_to_channel_map = eval(input())
        num_nodes = len(nodes_to_channel_map)

        if num_nodes < 2:
            raise ValueError('There must be at least 2 nodes allocated to run this test.', flush=True)

        if num_nodes % 2 != 0:
            raise ValueError('The number of allocated nodes must be a multiple of 2.', flush=True)

        this_node_index = dparms.this_process.index
        next_node_index = (this_node_index + 1) % num_nodes

        channel_0_index = next_node_index if this_node_index % 2 == 0 else this_node_index
        channel_0_descr = nodes_to_channel_map[channel_0_index][0]

        channel_1_index = this_node_index if this_node_index % 2 == 0 else next_node_index
        channel_1_descr = nodes_to_channel_map[channel_1_index][0]

        for ch_p2p_test in CH_P2P_TESTS:
            print(f'{this_node_index}: Starting {ch_p2p_test}', flush=True)

            proc = subprocess.Popen(
                [f'{Path.cwd() / ch_p2p_test}',
                 str(this_node_index % 2),         # 0 / 1 node id
                 dparms.this_process.default_pd,   # descriptor for default memory pool
                 channel_0_descr,                  # descriptor for channel 0
                 channel_1_descr],                 # descriptor for channel 1
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            stdout, stderr = proc.communicate()

            for stream_name, stream_handle in [('stdout', stdout), ('stderr', stderr)]:
                for line in stream_handle.splitlines():
                    if isinstance(line, bytes):
                        line = line.decode()
                    print(f'{this_node_index}-{stream_name}: {line}')

            print(f'{this_node_index}: app exited with {proc.returncode}')
            print(f'{this_node_index}: Done', flush=True)

    except Exception as ex:
        print("!!!!There was an EXCEPTION!!!!!")
        print(repr(ex))


if __name__ == "__main__":
    main()
