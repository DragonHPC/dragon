import dragon
import multiprocessing
import time
import sys

import dragon.infrastructure.parameters as dparm
import random


def fp(x):
    print(x)
    sys.stdout.flush()


def debug_it():
    delay = random.uniform(1, 10)
    fp(f'p_uid = {dparm.this_process.my_puid} will be delaying {delay} seconds')
    time.sleep(delay)
    breakpoint()  # people might want tools to conditionally enable breakpoints.
    fp(f'How fun it is here in {dparm.this_process.my_puid}')


class IHateFives(Exception):
    pass


def except_it():
    delay = random.uniform(1, 10)
    fp(f'p_uid = {dparm.this_process.my_puid} will be delaying {delay} seconds')
    time.sleep(delay)

    if dparm.this_process.my_puid == 5:
        raise IHateFives

    fp(f'Leaving {dparm.this_process.my_puid}')


def main():
    multiprocessing.set_start_method('dragon')
    # multiprocessing.set_start_method('spawn')

    nproc = 3

    procs = [multiprocessing.Process(target=debug_it) for _ in range(nproc)]

    # procs = [multiprocessing.Process(target=except_it) for _ in range(nproc)]

    for p in procs:
        p.start()

    # will need to join and release all of these in the debugger
    for p in procs:
        p.join()

    return 0


if __name__ == '__main__':
    exit(main())
