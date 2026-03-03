import dragon
from os import environ, getcwd
import multiprocessing as mp
from functools import partial

from socket import gethostname
from subprocess import Popen, PIPE, TimeoutExpired
import logging

# Pull in a lot of non-user facing dragon infrastructure to make logging pretty
from dragon.managed_memory import MemoryPool
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.parameters import this_process
from dragon.dlogging.util import setup_dragon_logging, setup_FE_logging, setup_BE_logging
from dragon.dlogging.util import LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING, DragonLoggingServices as dls
from dragon.dlogging.logger import DragonLogger
from dragon.utils import B64


class Shim:
    def __init__(self, rank, retries, timeout):
        self._rank = rank
        self._retries = retries
        self._timeout = timeout
        self._shutdown = False

    def _run_cmdline(self, cmdline):
        """Execute the user binary via subproces.Popen and return results"""

        log = logging.getLogger(f'shim_{gethostname()}_{self._rank}').getChild('run_cmdline)')
        emptystring_utf8 = "".encode("utf-8")
        # currently dragon.process.Popen is under development but seems close?
        proc = Popen(cmdline,
                     stderr=PIPE,
                     stdout=PIPE,
                     stdin=PIPE,
                     cwd=None,
                     env=None)
        try:
            out, err = proc.communicate(timeout=self._timeout)
        except TimeoutExpired:
            proc.kill()
            proc.communicate()
            log.info("FATAL: Failed to execute command, timeout reached")
            return -1, emptystring_utf8, emptystring_utf8
        except OSError:
            log.info("FATAL: Exception while attempting to start a process")
            return 1, emptystring_utf8, emptystring_utf8

        return proc.returncode, out.decode("utf-8"), err.decode("utf-8")

    def run_image(self, cmdline, logger_sdesc, use_dragon):

        # Configure handlers to make sure our log messages go to the DragonLogger channel
        def_mpool = None
        if use_dragon:
            def_mpool = MemoryPool.attach(B64.from_str(this_process.default_pd).decode())
        level, fname = setup_BE_logging(dls.TEST, logger_sdesc=B64.from_str(logger_sdesc), mpool=def_mpool)

        # Execute the user-provided application until it is successful or we run out
        # of retry attempts. Log as we do it
        log = logging.getLogger(f'shim_{gethostname()}_{self._rank}').getChild('run_image')
        log.info(f"Beginning execution, cmdline={' '.join(cmdline)}")

        while True:
            code, sout, serr = self._run_cmdline(cmdline)
            if code < 0:
                log.info(f"Execution FAILED; exit code={code}")
                log.info(f"Error output: {serr}")
            elif code > 0:
                log.info(f"Execution unscuccesful; exit code={code}")
                log.info(f"Error output: {serr}")
                break
            else:
                log.info("Execution complete")
                log.info(f'stdout: {sout}')
                break

            # see if we have any retries left
            if self._retries == 0:
                log.info("No more retries")
                break
            self._retries -= 1


def launch_shim(cmdline, logging_sdesc, use_dragon, shim):
    shim.run_image(cmdline, logging_sdesc, use_dragon)


def run_logging(logger_sdesc, LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING, use_dragon):
    """Run a proc pulling messages out of logging channel and logging them away"""

    # Configure logging handlers for our logging environment
    def_mpool = None
    if use_dragon:
        basename = 'dragon-shim'

        # Attach to the default memory pool
        def_mpool = MemoryPool.attach(B64.from_str(this_process.default_pd).decode())
    else:
        basename = 'regular-shim'


    setup_FE_logging(log_device_level_map=LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING,
                     basename=basename, basedir=getcwd())

    # Log a hello for this process
    local_log = logging.getLogger(f'logger_{gethostname()}')
    local_log.info('hello from logging thread')

    dragon_logger = DragonLogger.attach(B64.from_str(logger_sdesc).decode(), mpool=def_mpool)

    # Pull messages out of the DragonLogger and log them until told to stop
    while True:
        msg = dmsg.parse(dragon_logger.get(logging.INFO, timeout=None))
        if isinstance(msg, dmsg.HaltLoggingInfra):
            break
        elif isinstance(msg, dmsg.LoggingMsg):
            log = logging.getLogger(msg.name)
            log.log(msg.level, msg.msg, extra=msg.get_logging_dict())


if __name__ == "__main__":
    import argparse

    # This looks awful, but it's how we do all our infrastructure logging
    LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING['DRAGON_LOG_DEVICE_DRAGON_FILE'] = 'DEBUG'
    environ['DRAGON_LOG_DEVICE_DRAGON_FILE'] = 'DEBUG'

    # parse args
    parser = argparse.ArgumentParser()

    # Args to this driver program
    parser.add_argument("--retries", type=int, default=-1, help="Number of attempts to run an executable before exiting")
    parser.add_argument("--timeout", type=int, default=15, help="Time to wait on executable to complete")
    parser.add_argument("--num_workers", type=int, default=4, help="Number of pool workers for deploying executables")
    parser.add_argument("--dragon", action="store_true", help="Run with dragon multiprocessing")

    # The binary executable and its args
    parser.add_argument('prog', type=str, nargs='?', help="Executable pool workers will launch")
    parser.add_argument('args', type=str, nargs=argparse.REMAINDER, default=[], help="Option to the executable")

    args = parser.parse_args()

    # Opt to use dragon or not
    if args.dragon:
        print("using Dragon-augmented multiprocessing")
        mp.set_start_method("dragon")
    else:
        print("using base multiprocessing")
        mp.set_start_method("spawn")

    cmdline = [args.prog] + args.args
    print(f'cmdline = {cmdline}')

    # Create my dragon logger and run it in its own process.
    print(f'creating logger on {gethostname()}', flush=True)

    if args.dragon:
        dragon_logger = DragonLogger(mpool=MemoryPool.attach(B64.from_str(this_process.default_pd).decode()))
    else:
        dragon_logger = setup_dragon_logging(0)
    logging_sdesc = B64(dragon_logger.serialize())
    logging_proc = mp.Process(target=run_logging, args=(str(logging_sdesc), LOGGING_DEFAULT_DEVICE_LEVEL_MAPPING, args.dragon))
    print('starting dragon logger receiver', flush=True)
    logging_proc.start()

    # set up my experiments as a pool and run them
    worker_pool = mp.Pool(args.num_workers)
    print(f"created pool with {args.num_workers} workers")
    launch_partial = partial(launch_shim, cmdline, str(logging_sdesc), args.dragon)
    shims = [Shim(idx, args.retries, args.timeout) for idx in range(args.num_workers)]
    print("entering shim.run_image", flush=True)
    pool_proc = worker_pool.map(launch_partial, shims, 1)
    print("Pool work is complete.", flush=True)

    # Tell the dragon logger to halt
    dragon_logger.put(dmsg.HaltLoggingInfra(tag=0).serialize(), logging.INFO)
    print("signaled logging exit", flush=True)
    logging_proc.join()
    print('joined on logging thread', flush=True)

    if args.dragon:
        dragon_logger.destroy(destroy_pool=False)
    else:
        dragon_logger.destroy()

