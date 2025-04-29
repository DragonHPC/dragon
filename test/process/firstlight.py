#!/usr/bin/env python3

import dragon.mpbridge

import multiprocessing
import sys
import time


def by_hyenas(what_eaten):
    print("hyenas ate your {}".format(what_eaten))


def main(flag):
    print("> hello from the head process")

    if "-u" == flag:
        print("> using mpbridge.DragonProcess")
        method = "dragon"

        print(f"> setting start method to {method}")
        multiprocessing.set_start_method(method)
        print(f"> making process")
        p = dragon.mpbridge.DragonProcess(target=dragon.mpbridge.CloudyWrapFunc(by_hyenas), args=("pineapples",))

    elif "-s" == flag:
        method = "spawn"
        print(f"> setting start method to {method}")
        multiprocessing.set_start_method(method)
        print(f"> making process")
        p = multiprocessing.Process(target=by_hyenas, args=("pineapples",))
    elif "-d" == flag:
        print("> using mpbridge.DragonProcess")
        method = "impbridge"
        print(f"> setting start method to {method}")
        multiprocessing.set_start_method(method)
        print(f"> making process")
        p = dragon.mpbridge.DragonProcess(target=dragon.mpbridge.CloudyWrapFunc(by_hyenas), args=("pineapples",))

    else:
        raise NotImplementedError("derp")

    print(f"> starting process")
    p.start()
    print("> process is started\n")
    sys.stdout.flush()
    print("> trying to join()")
    p.join()
    print("> goodbye from the head process")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        flag = sys.argv[1]
    else:
        flag = "-s"

    # 'spawn' Process vs 'mpbridge' Process vs eventual dragon process
    assert flag in {"-s", "-u", "-d"}

    main(flag)
