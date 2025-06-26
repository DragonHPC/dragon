import os
import random
import logging
import multiprocessing as mp

from dragon.managed_memory import MemoryPool
from dragon.dlogging.logger import DragonLogger

POOL_SIZE = 4194304
DEFAULT_UID = 123456


def worker(log_ser):
    logger = DragonLogger.attach(log_ser)  # Attach to an existing logger
    msg = logger.get()
    print(f"Worker process attached, retrieved message: {msg}")
    logger.put("Salutations!")


if __name__ == "__main__":
    mpool = MemoryPool(
        POOL_SIZE, f"log_test_{os.getuid()}_{os.getpid()}", DEFAULT_UID, None
    )  # Create a memory pool for the logger
    logger = DragonLogger(mpool)  # Initialize the logger
    logger.put("Hello world!")  # Insert a log, default priority is INFO
    msg = logger.get()  # Retrieve a log, default priority is INFO
    print(msg)  # Should print "Hello World!"

    logger.put("Hello slightly more important world!", logging.WARNING)
    msg = logger.get(logging.INFO)  # Get a message of at least INFO priority
    print(msg)

    logger.put("Hello not very important world!", logging.DEBUG)
    msg = logger.get(logging.WARNING)  # Try to get a message of at least WARNING priority
    print(msg)  # msg returned will be None since the next message was below WARNING priority

    logger.put("Hello world (from somewhere else!)")
    log_ser = logger.serialize()  # Get a serialized descriptor of the logger
    proc = mp.Process(target=worker, args=(log_ser,))
    proc.start()
    proc.join()
    msg = logger.get()  # Retrieve the log we inserted from the other process
    print(f"Got message from worker process: {msg}")

    logger.destroy()
