#!/usr/bin/env python3

'''
@MCB:
Intended to be used as a benchmark script to compare pipe multiprocessing vs dragon connections
'''

import multiprocessing as mp
import dragon.infrastructure.standalone_conn as dconn
import sys
import time
import csv

# This needs to be modified based on message size so we're not running a 15 minute test at 4MB with 8 duplex pipes
N_REPEATS = 1000
N_WARMUPS = int(N_REPEATS / 4)

# Divide ping-pong latency by 2 because MPI measurements are silly
# Adjust N_REPEAT const based on message size (huge for small, small for huge)
# Ping-pong latency will probably be wonky because of sleepy polling
# Mess with poll times and default block sizes in connection.py
# TODO: Look into fan & all to all latency tests

timings = []

def pingpong(writer, reader, ping_time, msg_size, write_first=True):
    x = bytearray(msg_size)

    # Warmup
    if write_first:
        for i in range(N_WARMUPS):
            writer.send(x)
            msg = reader.recv()
    else:
        for i in range(N_WARMUPS):
            msg = reader.recv()
            writer.send(x)

    # Measure
    start_time = time.monotonic_ns()
    if write_first:
        for i in range(N_REPEATS):
            writer.send(x)
            msg = reader.recv()
    else:
        for i in range(N_REPEATS):
            msg = reader.recv()
            writer.send(x)

    end_time = time.monotonic_ns()
    total = 1.0 * (end_time - start_time) * 1e-9 # Convert to seconds
    ping_time.value = total
    #writer.close()
    #reader.close()

# Unilateral write actor
def unilateral(actor, uni_time, msg_size, writer=True):
    x = bytearray(msg_size)

    # Warmup
    if writer:
        for i in range(N_WARMUPS):
            actor.send(x)
    else:
        for i in range(N_WARMUPS):
            msg = actor.recv()

    # Measure
    start_time = time.monotonic_ns()
    if writer:
        for i in range(N_REPEATS):
            actor.send(x)
    else:
        for i in range(N_REPEATS):
            msg = actor.recv()
    end_time = time.monotonic_ns()
    total = 1.0 * (end_time - start_time) * 1e-9 # Convert to seconds
    uni_time.value = total
    #actor.close() # this causes dragon to break but not mp.Pipe


def main(conn_class, num_pairs=1, msg_size=1024, is_pingpong=False):
    # Generate our read/write handles
    conns = []
    times = []
    procs = []
    for i in range(num_pairs):
        reader, writer = conn_class.Pipe(duplex=is_pingpong)
        # Store read/write pair as tuple
        conns.append((writer, reader))
        # Generate multiprocessing values to track times
        r_time = mp.Value("d", 0.0, lock=False)
        w_time = mp.Value("d", 0.0, lock=False)
        # Store as tuples similarly
        times.append((w_time, r_time))

    # Dumb way to do it but reiterate over pairs to match connections to processes
    for i in range(num_pairs):
        w_process = None
        r_process = None
        # Spawn our processes
        # TODO: This is a bit hard to read
        if is_pingpong:
            w_process = mp.Process(target=pingpong, args=(conns[i][0], conns[i+1][1], times[i][0], msg_size, True))
            r_process = mp.Process(target=pingpong, args=(conns[i+1][0], conns[i][1], times[i][1], 1, False))
        else:
            w_process = mp.Process(target=unilateral, args=(conns[i][0], times[i][0], msg_size, True))
            r_process = mp.Process(target=unilateral, args=(conns[i][1], times[i][1], 1, False))

        procs.append((w_process, r_process))

    for i in range(num_pairs):
        # Get processes
        w_process = procs[i][0]
        r_process = procs[i][1]
        # Start said processes
        w_process.start()
        r_process.start()

    total_0 = 0.0
    total_1 = 0.0

    for i in range(num_pairs):
        w_process = procs[i][0]
        r_process = procs[i][1]
        # Stop said processes
        w_process.join()
        r_process.join()
        # Routine finished. smiley_face.png
        total_0 += times[i][0].value
        total_1 += times[i][1].value

    # Average times
    total_0 = total_0 / num_pairs
    total_1 = total_1 / num_pairs

    timings.append((num_pairs, msg_size, total_0, total_1))
    # Spit them to stdout
    print("Size(b): {:8s} | {:0.5f}{:2s} | {:0.5f}".format(str(msg_size), total_0, " ", total_1))

if __name__ == "__main__":
    mp.set_start_method('spawn')
    conn_class = None
    is_pingpong = False
    if "-d" in sys.argv: # Check for dragon argument
        conn_class = dconn
        print("Using dragon connections")
    else: # Default to multiprocessing
        conn_class = mp
        print("Using multiprocessing connections")

    if "-pp" in sys.argv: # Check if we want to test pingpong
        is_pingpong = True

    # Chnage this to start at 8 bytes
    sizes = [2**x for x in range(6, 22)] # 8 bytes to 2MB
    #sizes = [2**x for x in range(10, 19)]
    #sizes = [1024, 2048, 4096]
    #sizes = [2**x for x in range(21, 26)]
    num_pairs = [1, 2, 4, 8]
    for p in num_pairs:
        print(f"Using {p} pairs")
        for sz in sizes:
            main(conn_class, p, sz, is_pingpong)

    with open("pipe.csv", 'w', newline='') as testfile:
        writer = csv.writer(testfile)
        for timing in timings:
            writer.writerow(timing)
