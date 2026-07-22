#!/usr/bin/env python3

import unittest
import os
import dragon
import sys
import multiprocessing as mp
from dragon.fli import FLInterface, DragonFLIError, DragonFLIEOT
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel
from dragon.globalservices import channel
from dragon.localservices.options import ChannelOptions
from dragon.native.process import Popen
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as facts
import dragon.infrastructure.parameters as parameters
from dragon.utils import b64decode, b64encode


def main():
    try:
        ser_fli = sys.argv[1]
        decoded_ser_fli = b64decode(ser_fli)
        fli = FLInterface.attach(decoded_ser_fli)
        msg = dmsg.DDRegisterClient(42, "Hello World", "Dragon is the best")
        sendh = fli.sendh()
        sendh.send_bytes(msg.serialize())
        sendh.close()
        print("OK")
    except Exception as ex:
        print(f"Got Exception in flimsgfrom.py {ex}", flush=True, file=sys.stderr)


if __name__ == "__main__":
    main()
