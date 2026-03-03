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

    drbe_log_level = os.environ.get("DRAGON_RUN_LOG_LEVEL", logging.getLevelName(logging.NOTSET)).upper()
    if drbe_log_level.isdigit():
        drbe_log_level = int(drbe_log_level)
    else:
        drbe_numeric_log_level = getattr(logging, drbe_log_level, None)
        if not isinstance(drbe_numeric_log_level, int):
            raise ValueError(f"Invalid log level: {drbe_log_level}")
        drbe_log_level = drbe_numeric_log_level

    if drbe_log_level != logging.NOTSET:
        logging.basicConfig(
            filename=f"drbe_{socket.gethostname()}.log",
            encoding="utf-8",
            level=drbe_log_level,
            format="%(relativeCreated)6d %(threadName)s %(thread)d %(levelname)s:%(name)s:%(message)s",
        )

    try:
        logger.debug("++main")
        with DragonRunBE(my_rank, drbe_log_level) as drbe:
            drbe.run()
    except Exception as exc:
        logger.error("unhandled exception in main %s", exc, exc_info=True)
        raise
    finally:
        logger.debug("--main")


if __name__ == "__main__":
    sys.exit(main())
