import os
import socket
import sys

import logging

from .src.dragon_run import DragonRunBE

my_hostname = socket.gethostname()

logger = logging.getLogger(__name__)


def main():

    # TODO Add commandline parser and options for logging level

    my_rank = int(os.environ.get("DRAGON_RUN_RANK", 0))

    try:
        logger.debug("++main")
        with DragonRunBE(my_rank) as drbe:
            drbe.run()
    except Exception as exc:
        logger.error("unhandled exception in main %s", exc, exc_info=True)
        raise
    finally:
        logger.debug("--main")


if __name__ == "__main__":
    sys.exit(main())
