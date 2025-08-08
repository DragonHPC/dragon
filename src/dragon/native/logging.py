"""The Dragon native classes to log messages from the runtime.

This is a stub.

"""

import os
from logging import Handler, Logger, LogRecord, getLogger
from dragon.dlogging.logger import DragonLogger


def log_to_stderr(level=None) -> None:
    raise NotImplementedError(f"{__name__} is not implemented, yet.")


class DragonHandler(Handler):
    """Logging handler to enable python logging to be sent to the Dragon Logging channel."""

    def __init__(self, serialized_log_descr: bytes) -> None:
        Handler.__init__(self)
        self._dlog = DragonLogger.attach(serialized_log_descr)

    def emit(self, record: LogRecord) -> None:
        """Emit a Dragon log record

        :param record: the record to emit
        :type record: LogRecord
        """

        try:
            msg = self.format(record)
            self._dlog.put(msg, record.levelno)
        except Exception:
            self.handleError(record)


def log_to_dragon(serialized_log_descr: bytes, level: int = None) -> Logger:
    """Turn on logging and add a handler which routes log messages to Dragon logging channels

    :param serialized_log_descr: The serialized descriptor
    :type serialized_log_descr: bytes
    :param level: the log level, defaults to None
    :type level: int, optional
    :return: the Logger
    :rtype: Logger
    """

    logger = getLogger()
    handler = DragonHandler(serialized_log_descr)
    logger.addHandler(handler)

    if level:
        logger.setLevel(level)
    return logger
