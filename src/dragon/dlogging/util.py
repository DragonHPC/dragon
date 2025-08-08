"""Logging setup for Dragon runtime infrastructure"""

import logging
import os
import time
import socket
import enum
from typing import Tuple, Union
import argparse

import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
from dragon.utils import B64
from dragon.managed_memory import MemoryPool
from dragon.dlogging.logger import DragonLogger, DragonLoggingError

# This is random and exists to conform to the messaging infrastructure.
# It is discarded once logs are written out
LOGGING_TAG = 2**8

LOGGING_OUTPUT_DEVICE_STDERR = "stderr"
LOGGING_OUTPUT_DEVICE_DRAGON_FILE = "dragon_file"
LOGGING_OUTPUT_DEVICE_ACTOR_FILE = "actor_file"

LOGGING_OUTPUT_DEVICES = [
    LOGGING_OUTPUT_DEVICE_STDERR,
    LOGGING_OUTPUT_DEVICE_DRAGON_FILE,
    LOGGING_OUTPUT_DEVICE_ACTOR_FILE,
]

LOGGING_LEVEL_NONE = "NONE"
LOGGING_LEVEL_NAMES_MAPPING = {
    logging.getLevelName(level): level
    for level in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
}

LOGGING_STDERR_MAX_LOG_LEVEL = logging.INFO
LOGGING_DRAGON_FILE_MAX_LOG_LEVEL = logging.INFO

LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING = {
    f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_STDERR.upper()}": LOGGING_LEVEL_NONE,
    f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_DRAGON_FILE.upper()}": LOGGING_LEVEL_NONE,
    f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_ACTOR_FILE.upper()}": LOGGING_LEVEL_NONE,
}

# Default formatting for logging
default_single_fmt = "%(asctime)-15s %(levelname)-8s %(name)s :: %(message)s"
default_FE_fmt = "%(asctime)-15s %(levelname)-8s %(service)s:%(name)s (%(hostname)s) :: %(message)s"
default_services_fmt = "%(time)-15s %(levelname)-8s %(service)s:%(name)s (%(hostname)s) :: %(message)s"


class LoggingValue(argparse.Action):
    def __init__(
        self,
        option_strings,
        dest,
        nargs=None,
        const=None,
        default=None,
        type=None,
        choices=None,
        required=False,
        help=None,
        metavar=None,
    ):

        if default == dict():
            default = LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING

        if choices == None:
            choices = []
            choices.append(LOGGING_LEVEL_NONE)
            choices.extend(LOGGING_LEVEL_NAMES_MAPPING.keys())
            for device in LOGGING_OUTPUT_DEVICES:
                choices.append(f"{device}={LOGGING_LEVEL_NONE}")
                for level in LOGGING_LEVEL_NAMES_MAPPING.keys():
                    choices.append(f"{device}={level}")

        super(LoggingValue, self).__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=nargs,
            const=const,
            default=default,
            type=type,
            choices=choices,
            required=required,
            help=help,
            metavar=metavar,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        level_names = LOGGING_LEVEL_NAMES_MAPPING.keys()

        log_device_level_map = getattr(namespace, self.dest)

        for value in values:
            if value.upper() == LOGGING_LEVEL_NONE:
                setattr(namespace, self.dest, LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING)

            elif value.upper() in level_names:
                level_name = value.upper()
                log_device_level_map[f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_STDERR.upper()}"] = level_name
                log_device_level_map[f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_DRAGON_FILE.upper()}"] = level_name
                log_device_level_map[f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_ACTOR_FILE.upper()}"] = level_name

            elif "=" in value:
                k, v = value.split("=")
                output_device = k.lower()
                level_name = v.upper()

                if output_device not in LOGGING_OUTPUT_DEVICES:
                    raise ValueError(f"Unsupported log output device '{k}' in parameter '{value}'")

                if level_name not in level_names:
                    raise ValueError(f"Unsupported log level '{v}' in parameter '{value}'")

                log_device_level_map[f"DRAGON_LOG_DEVICE_{output_device.upper()}"] = level_name

            else:
                raise ValueError(f"Unsupported parameter {value}.")


class DragonLoggingServices(str, enum.Enum):
    """Enumerator for logging names of backend services

    Attributes:
        :LA_FE: 'LA_FE'
        :LA_BE: 'LA_BE'
        :GS: 'GS'
        :TA: 'TA'
        :ON: 'ON'
        :OOB: 'OOB'
        :LS: 'LS'
        :DD: 'DD'
        :TEST: 'TEST'
        :PERF: 'PERF'
        :PG: 'PG'
        :TELEM: 'TELEM'
    """

    LA_FE = "LA_FE"
    LA_BE = "LA_BE"
    GS = "GS"
    TA = "TA"
    ON = "ON"
    OOB = "OOB"
    LS = "LS"
    DD = "DD"
    TEST = "TEST"
    PERF = "PERF"
    PG = "PG"
    TELEM = "TELEM"

    def __str__(self):
        return str(self.value)


def next_tag():
    global LOGGING_TAG
    tmp = LOGGING_TAG
    LOGGING_TAG += 1
    return tmp


class DragonFEFilter(logging.Filter):

    def __init__(self) -> None:
        """Create logging filter to add record attributes to FE logger

        Adds hostname, IP address, port, and service name to all FE logging
        entries. These are needed to satisfy the ``dragon.dlogging.util.default_FE_fmt``
        format string
        """

        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter out BE messages and let through all FE messages while defining record attributes

        Args:
            record (logging.LogRecord): record that will be populated

        Returns:
            bool: Log message if True. Don't if False
        """

        # time is only defined for our backend services. If the attr exists, we will
        # filter out the message as another handler will log the backend logs
        if hasattr(record, "time"):
            return False
        else:
            record.hostname = socket.gethostname()
            record.ip_address = socket.gethostbyname(record.hostname)
            record.port = "      "
            record.service = DragonLoggingServices.LA_FE
        return True


class DragonLoggingHandler(logging.StreamHandler):

    def __init__(
        self,
        serialized_log_descr: B64,
        mpool: MemoryPool = None,
        hostname: str = None,
        ip_address: str = None,
        port: str = None,
        service: Union[DragonLoggingServices, str] = None,
    ) -> None:
        """Logging handler to enable python logging to be sent to the Dragon Logging channel on BE

        Ultimately a `logging.StreamHandler` that sends stream output to ``/dev/null`` and also
        emits log records into a  :ref:`dragon.infrastructure.messages.LoggingMsg <loggingmsg>` object that
        is serialized into a :class:`dragon.dlogging.logger.DragonLogger`

        :param serialized_log_descr: serialized descriptor from
            :class:`dragon.dlogging.logger.DragonLogger`
        :type serialized_log_descr: B64
        :param mpool: Memory pool to use for attaching to logging channel, defaults to None
        :type mpool: MemoryPool, optional
        :param hostname: hostname of logging service, defaults to None
        :type hostname: str, optional
        :param ip_address: IP address of logging service, defaults to None
        :type ip_address: str, optional
        :param port: Port of logging service, defaults to None
        :type port: str, optional
        :param service: Name of logging service. Acceptable names are defined
            in :class:`DragonLoggingServices`
        :type service: Union[DragonLoggingServices, str], optional
        """
        with open(os.devnull, "w") as f:
            super().__init__(f)
        self._dlog = DragonLogger.attach(serialized_log_descr.decode(), mpool=mpool)
        self.hostname = hostname
        self.ip_address = ip_address
        self.port = port
        self.service = service

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a dragon.insfrauction.messages.LoggingMsg message

        Args:
            record (logging.LogRecord): Record populated by classes format method

        Raises:
            err: DragonLoggingError if message could not be emitted into logging channel
        """
        try:
            self.format(record)
            msg = dmsg.LoggingMsg(
                tag=next_tag(),
                name=record.name,
                msg=record.msg,
                time=record.asctime,
                func=record.funcName,
                hostname=self.hostname,
                ip_address=self.ip_address,
                port=self.port,
                service=self.service,
                level=record.levelno,
            )
            self._dlog.put(msg.serialize(), record.levelno)
        except (Exception, DragonLoggingError) as err:
            if isinstance(err, DragonLoggingError):
                raise err
            else:
                self.handleError(record)


def _clear_root_log_handlers():
    log = logging.getLogger()
    for handler in log.handlers:
        handler.close()
        log.removeHandler(handler)


def _setup_actor_logging(service: Union[DragonLoggingServices, str], level: int, fname: str = None) -> Tuple[int, str]:
    """Get the logging level and setup file logging if in DEBUG mode

    Args:
        service (str, optional): logging service, options defined in :class:`DragonLoggingServices`. Defaults to 'dragon'.

        fname (str, optional): Filename to log to if debug logging is enabled. Defaults to None.

    Returns:
        int: logging level

        str: logging filename if created or None if not
    """
    if service is None or service is DragonLoggingServices.LA_FE:
        service_name = "dragon"
    else:
        service_name = service

    full_fn = None
    if service is not DragonLoggingServices.LA_FE:
        try:
            basedir = os.environ.get("DRAGON_LA_LOG_DIR")
        except AttributeError:
            basedir = None
        if fname is None:
            full_fn = _setup_file_logging(
                basename=f"{service_name}_{socket.gethostname()}", basedir=basedir, level=level
            )
        else:
            full_fn = _setup_file_logging(fname=os.path.basename(fname), basedir=basedir, level=level)

    return full_fn


def _configure_console_logging(level, fmt=None):
    """Add logging to stderr handler"""

    console = logging.StreamHandler()
    console.setLevel(level)

    if fmt is None:
        formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    else:
        formatter = logging.Formatter(fmt)

    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)


def setup_logging(
    *,
    basename: str = None,
    basedir: str = None,
    level: int = logging.INFO,
    force: bool = False,
    add_console: bool = False,
):
    """Original dragon logging function to configure root logger.

    Args:
        basename (str, optional): Logging file basename. Date and time will be appended. Defaults to 'dragon'. Defaults to None.

        basedir (str, optional): Directory to save log file to os.getcwd(). Defaults to None.

        level (int, optional): Logging level. Defaults to logging.INFO.

        force (bool, optional): Remove any handlers attached to the root logger. Defaults to False.

        add_console (bool, optional): Enable loggng to stderr. Defaults to False.
    """
    if basedir is None:
        basedir = os.getcwd()
    else:
        # Make sure the path exists. If not, just put to /tmp,
        # so we have a defined place for it to go and look for
        # it
        if not os.path.exists(basedir):
            basedir = "/tmp"

    fmt = default_single_fmt

    now = time.strftime("%m_%d_%H_%M_%S", time.localtime())
    fn = "{}_{}.log".format(basename, now)
    full_fn = os.path.join(basedir, fn)

    logging.basicConfig(format=fmt, filename=full_fn, level=level, force=force)

    if add_console:
        _configure_console_logging(level=level)


def _setup_file_logging(
    basename: str = None,
    fname: str = None,
    basedir: str = None,
    level: int = logging.INFO,
    force: bool = False,
    fmt: str = None,
) -> str:
    """Configure file-based logging

    Sets up the root file logging with specified logging level.
    The logfile has the timestamp in the name so it's easy to disambiguate
    between multiple test runs, etc.

    Args:
        basename (str, optional): Logging file basename. Date and time will be appended. Defaults to 'dragon'

        fname (str, optional): Full path and file name of log to create. Overrides fname and basedir. Defaults to None.

        basedir (str, optional): Directory to save log file to. Defaults to pwd

        level (int, optional): Logging level. Defaults to logging.INFO.

        force (bool, optional): Remove all existing handlers from root logger. Defaults to False.

        fmt (str, optional): Formatting string for file log messages. Defaults to None.

    Returns:
        str: file and path of log file
    """
    if basedir is None:
        basedir = os.getcwd()
    else:
        # Make sure the path exists. If not, just put to /tmp,
        # so we have a defined place for it to go and look for
        # it
        if not os.path.exists(basedir):
            basedir = "/tmp"

    if fmt is None:
        fmt = default_single_fmt

    if fname is not None:
        full_fn = os.path.join(basedir, fname)
    else:
        now = time.strftime("%m_%d_%H_%M_%S", time.localtime())
        fn = "{}_{}.log".format(basename, now)
        full_fn = os.path.join(basedir, fn)

    file_handler = logging.FileHandler(filename=full_fn, mode="a")
    formatter = logging.Formatter(fmt=fmt)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().setLevel(level=level)

    return full_fn


def setup_FE_logging(
    log_device_level_map: dict = {},
    basename: str = None,
    basedir: str = None,
    force: bool = False,
    add_console: bool = False,
    test_BE_dir: str = None,
    multi_node_mode: bool = False,
) -> Tuple[int, str]:
    """Set-up launcher frontend logging infrastructure. Will log to file.

    Configures the individual service loggers the FE will use to log all messages
    sent to it from the backend services of the MRNet overlay and creates the FileHandler
    they will all write to

    Args:
        basename (str, optional): Logging file basename. Date and time will be appended. Defaults to 'dragon'.

        basedir (str, optional): Directory to save log file to. Defaults to pwd

        force (bool, optional): Remove all existing handlers from root logger. Defaults to False.

        add_console (bool, optional): Turn on logging to stderr. Defaults to False.

        test_BE_dir (str, optional): Directory to save backend log files to. Used for testing. Defaults to None.

        multi_node_mode (bool, optional): Enable multi-node filtering and formatting.
    Returns:
        int: logging level

        str: full path and filename of logging file
    """

    # Make sure the environment is updated with the directory to log to:
    if test_BE_dir is not None:
        os.environ.update({"DRAGON_LA_LOG_DIR": test_BE_dir})
    elif os.environ.get("DRAGON_LA_LOG_DIR") is None:
        os.environ.update({"DRAGON_LA_LOG_DIR": os.getcwd()})
    for log_device, log_level in log_device_level_map.items():
        os.environ.update({log_device: log_level})

    log_to_stderr, stderr_log_level = _get_dragon_log_device_level(log_device_level_map, LOGGING_OUTPUT_DEVICE_STDERR)
    log_to_dragon_file, dragon_file_log_level = _get_dragon_log_device_level(
        log_device_level_map, LOGGING_OUTPUT_DEVICE_DRAGON_FILE
    )

    _clear_root_log_handlers()

    logger = logging.getLogger()

    if any([log_to_stderr, log_to_dragon_file]):

        root_log_level = max(stderr_log_level, dragon_file_log_level)
        logger.setLevel(root_log_level)

        if log_to_stderr:
            _configure_console_logging(level=stderr_log_level, fmt=default_single_fmt)

        if log_to_dragon_file:
            file_formatter = default_FE_fmt if multi_node_mode else default_single_fmt
            full_fn = _setup_file_logging(
                basename=basename, basedir=basedir, level=dragon_file_log_level, force=force, fmt=file_formatter
            )

        if multi_node_mode:
            # Add a filter to all the root handlers to only log on the root if it's a FE service
            for handler in logging.getLogger().handlers:
                handler.addFilter(DragonFEFilter())
    else:
        logger.addHandler(logging.NullHandler())


def close_FE_logging():
    """Close individual service loggers used for logging on frontend"""
    for service in list(DragonLoggingServices):
        log = logging.getLogger(service)
        for handle in log.handlers[:]:
            handle.close()
            log.removeHandler(handle)


def _get_logging_mpool(node_index: int):
    """Create memory pool for logging infrastructure

    Args:
        node_index (int): node ID of caller
    """
    _user = os.environ.get("USER", str(os.getuid()))
    lps = int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ)
    lpn = f"{_user}_{os.getpid()}_{dfacts.LOGGING_POOL_SUFFIX}"
    lp_muid = dfacts.logging_pool_muid_from_index(node_index)
    logging_mpool = MemoryPool(lps, lpn, lp_muid, None)
    return logging_mpool


def setup_dragon_logging(node_index: int) -> DragonLogger:
    """Create a :class:`dragon.dlogging.logger.DragonLogger` instance

    Args:
        node_index (int): node index logger is being created on

    Returns:
        DragonLogger: created DragonLogger instance
    """
    logging_mpool = _get_logging_mpool(node_index)
    dragon_logger = DragonLogger(logging_mpool)

    return dragon_logger


def detach_from_dragon_handler(service: Union[DragonLoggingServices, str]):
    """Detach logger from dragon handler

    This only needs to be called for processes that will outlive the
    MRNet backend service, which "owns" the logging memory pool
    and relevant backend infrastructure

    Args:
        service (str): logging service, options defined in :class:`DragonLoggingServices`
    """
    log = logging.getLogger(service)
    root_log = logging.getLogger()

    for handle in log.handlers[:]:
        if isinstance(handle, DragonLoggingHandler):
            log.debug("removing dragonlogginghandler")
            log.removeHandler(handle)

    for handle in root_log.handlers[:]:
        if isinstance(handle, DragonLoggingHandler):
            log.debug("removing dragonlogginghandler from root")
            root_log.removeHandler(handle)


def _get_dragon_log_device_level(env_map: dict(), device_name: str) -> Tuple[bool, int]:
    log_level_value = 0
    got_dragon_log_level = False
    dragon_log_device_env = f"DRAGON_LOG_DEVICE_{device_name.upper()}"

    if dragon_log_device_env in env_map:
        try:
            log_level_name = env_map[dragon_log_device_env]
            log_level_value = LOGGING_LEVEL_NAMES_MAPPING[log_level_name]
            got_dragon_log_level = log_level_value != LOGGING_LEVEL_NONE
        except KeyError:
            # Unsupported level was set, don't enable logging
            pass

    return got_dragon_log_level, log_level_value


def setup_BE_logging(
    service: Union[DragonLoggingServices, str],
    logger_sdesc: B64 = None,
    mpool: MemoryPool = None,
    hostname: str = None,
    ip_address: str = None,
    port: str = None,
    fname: str = None,
) -> Tuple[int, str]:
    """Configure backend logging for requested backend service

    Create a handler using the :class:`dragon.dlogging.logger.DragonLogger` serialized
    descriptor and add it to the given service's logger
    :param service: logging service, options defined in :class:`DragonLoggingServices`
    :type service: Union[DragonLoggingServices, str]
    :param logger_sdesc: serialized logging descriptor, defaults to None
    :type logger_sdesc: B64, optional
    :param mpool: memory pool to use for attaching to channel, defaults to None
    :type mpool: MemoryPool, optional
    :param hostname:  Hostname of logging service, defaults to None
    :type hostname: str, optional
    :param ip_address: IP address of logging service, defaults to None
    :type ip_address: str, optional
    :param port: Port of logging service, defaults to None
    :type port: str, optional
    :param fname: Filename for file logging, defaults to None
    :type fname: str, optional
    :return: logging level, filename logging will be saved to
    :rtype: Tuple[int, str]
    """
    full_fn = ""
    log_level = 0

    # Make sure there isn't a duplicate handler already attached to this root instance
    _clear_root_log_handlers()

    log_to_stderr, stderr_log_level = _get_dragon_log_device_level(os.environ, LOGGING_OUTPUT_DEVICE_STDERR)
    log_to_dragon_file, dragon_file_log_level = _get_dragon_log_device_level(
        os.environ, LOGGING_OUTPUT_DEVICE_DRAGON_FILE
    )
    log_to_actor_file, actor_file_log_level = _get_dragon_log_device_level(os.environ, LOGGING_OUTPUT_DEVICE_ACTOR_FILE)

    logger = logging.getLogger()

    if any([log_to_stderr, log_to_dragon_file, log_to_actor_file]):

        # determine the maximum log level that we want to the logger to handle
        if log_to_stderr:
            log_level = stderr_log_level
        if log_to_dragon_file:
            log_level = max(log_level, dragon_file_log_level)
        if log_to_actor_file:
            log_level = max(log_level, actor_file_log_level)

        # Set the loglevel on the logger
        logger.setLevel(log_level)

        # determine the maximum log level that we want to send over MRNet
        if all([log_to_stderr, log_to_dragon_file]):
            front_end_log_level = max(stderr_log_level, dragon_file_log_level)
        elif log_to_stderr:
            front_end_log_level = stderr_log_level
        else:
            front_end_log_level = dragon_file_log_level

        # If we're logging to either/both stderr or the combined dragon file,
        # we then need to send appropriate log messages to the FrontEnd via
        # the DragonLoggingHandler.
        if logger_sdesc and any([log_to_stderr, log_to_dragon_file]):

            # Determine our hostname if we weren't told it
            if hostname is None:
                hostname = socket.gethostname()

            # Create the Dragon Logging Handler
            handler = DragonLoggingHandler(
                logger_sdesc, mpool=mpool, hostname=hostname, ip_address=ip_address, port=port, service=service
            )

            # Give it a formatter with asctime so time gets populated in the log record
            handler.setFormatter(logging.Formatter(fmt=default_single_fmt + "%(funcName)s"))

            # The intent of this next block is to prevent sending DEBUG or higher log
            # messages over the MRNet connection if those log messages will be logged
            # to the actor file.
            if log_to_actor_file and (front_end_log_level <= actor_file_log_level):
                if logging.INFO > front_end_log_level:
                    front_end_log_level = logging.INFO

            # Set the loglevel on the handler
            handler.setLevel(front_end_log_level)

            # FIXME: This needs fixing.
            # The logger currently fills up the channel because no back pressure is applied
            # and instead of handling that internally, returns a CHANNEL_FULL error which
            # causes some tests to fail. It should instead, either block on sending or
            # discard log messages when full. We should have a way of configuring which
            # behavior we want.

            # Add the handler to the logger object
            logger.addHandler(handler)

        if log_to_actor_file:
            full_fn = _setup_actor_logging(service=service, level=actor_file_log_level, fname=fname)

    else:
        logger.addHandler(logging.NullHandler())

    return log_level, full_fn
