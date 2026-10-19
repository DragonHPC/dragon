#!/usr/bin/env python3
import os
import socket
import time


def main():
    """Open a file, write, sleep, and exit

    Just a simple test to confirm local services is
    capable of bringing up a serial process on a given compute node.

    """
    _user = os.environ.get("USER", str(os.getuid()))
    _hostname = socket.gethostname()
    try:
        filename = os.path.join(os.environ.get("DRAGON_LA_LOG_DIR"), f"simple_compute_{_user}.txt")
    except TypeError:
        filename = f"/tmp/simple_compute_{_user}.txt"

    if os.path.exists(filename):
        os.remove(filename)

    for i in range(5):
        with open(filename, "a") as f:
            f.write(f"hello from {_hostname}\n")
        time.sleep(5)


if __name__ == "__main__":
    main()
