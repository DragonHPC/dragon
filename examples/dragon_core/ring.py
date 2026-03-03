import dragon
import multiprocessing as mp
import subprocess
import sys
import dragon.channels as dch
import dragon.managed_memory as dm
import dragon.infrastructure.parameters as dp
import dragon.infrastructure.facts as df
import dragon.utils as du
import time

def start_ringproc(iterations, cuid, receive_from_channel_sdesc, ret_queue):
    proc = subprocess.Popen(['ringproc', str(iterations), str(cuid), receive_from_channel_sdesc], stdout=subprocess.PIPE)
    send_to_channel_sdesc = proc.stdout.readline()
    while len(send_to_channel_sdesc.strip()) == 0:
        send_to_channel_sdesc = proc.stdout.readline()
    ret_queue.put(send_to_channel_sdesc)
    proc.wait()
    if proc.returncode != 0:
        print('*******Proc exited with rc=', proc.returncode, flush=True)

def usage():
    print('usage: dragon ring.py <num_procs> <iterations>')
    print('    <num_procs> is the number of processes to start, one per node.')
    print('    <iterations> is the number of times each process forwards a message')
    print('                to the next node.')
    print('    The program creates a ring across the user specified number of')
    print('    nodes and sends a message around a ring of nodes. The num_procs')
    print('    and iterations must be greater than 0.')
    sys.exit(1)

def main():
    try:
        if len(sys.argv) != 3:
            raise ValueError()

        mp.set_start_method('dragon')
        ring_size = int(sys.argv[1])
        iterations = int(sys.argv[2])
        if iterations <= 0 or ring_size <= 0:
            raise ValueError()
    except:
        usage()

    pool = dm.MemoryPool.attach(du.B64.str_to_bytes(dp.this_process.default_pd))
    origin_channel = dch.Channel(pool, df.BASE_USER_MANAGED_CUID)
    receive_sdesc = du.B64.bytes_to_str(origin_channel.serialize())
    final_channel = dch.Channel(pool, df.BASE_USER_MANAGED_CUID+1)
    final_sdesc = du.B64.bytes_to_str(final_channel.serialize())
    origin_send_sdesc = receive_sdesc

    ret_queue = mp.Queue()
    mp_procs = []
    for i in range(1,ring_size):
        proc = mp.Process(target=start_ringproc, args=(str(iterations), str(i+df.BASE_USER_MANAGED_CUID+1), receive_sdesc, ret_queue))
        proc.start()
        mp_procs.append(proc)
        receive_sdesc = ret_queue.get().strip()

    # This final process starts on the current node and completes the ring. It
    # also provides the destination for the final message to be returned.
    proc = subprocess.Popen(['ringproc', str(iterations), str(df.BASE_USER_MANAGED_CUID), receive_sdesc, origin_send_sdesc, final_sdesc], stdout=subprocess.PIPE)

    reader = dch.ChannelRecvH(final_channel)
    writer = dch.ChannelSendH(origin_channel)
    reader.open()
    writer.open()
    start = time.perf_counter()
    writer.send_bytes(b'hello', timeout=None, blocking=True)
    last_msg = reader.recv_bytes(timeout=None, blocking=True)
    stop = time.perf_counter()

    avg_time = (stop - start) / (iterations*ring_size)
    proc.wait()
    print('Ring proc exited...', flush=True)
    for proc in mp_procs:
        proc.join()
        print('Ring proc exited...', flush=True)
    if last_msg == b'hello':
        print('Test Passed.', flush=True)
        print(f'The average time per message transfer was {avg_time*1e6} microseconds.')
    else:
        print('Test Failed.', flush=True)
    print('Main proc exiting...', flush=True)


if __name__ == '__main__':
    main()