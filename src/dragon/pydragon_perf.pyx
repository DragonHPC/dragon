from dragon.dtypes_inc cimport *
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import logging
import multiprocessing as mp

from . import channels as dch
from . import managed_memory as dmm
from . import utils as dutils
from .dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from .infrastructure import facts as dfacts
from .infrastructure import parameters as dparm


# placeholder global var for the log
log = None


################################
# Begin Cython definitions
################################


ch_ctr = 0

cpdef enum Opcode:
    SEND_MSG = DRAGON_PERF_OPCODE_SEND_MSG
    GET_MSG = DRAGON_PERF_OPCODE_GET_MSG
    PEEK = DRAGON_PERF_OPCODE_PEEK
    POP = DRAGON_PERF_OPCODE_POP
    POLL = DRAGON_PERF_OPCODE_POLL
    LAST = DRAGON_PERF_OPCODE_LAST


class ChPerfError(Exception):
    def __init__(self, msg, lib_err=None, lib_msg=None, lib_err_str=None):
        self._msg = msg
        self._lib_err = lib_err
        cdef char * errstr
        if lib_err is not None and lib_msg is None and lib_err_str is None:
            errstr = dragon_getlasterrstr()
            self._lib_msg = errstr[:].decode('utf-8')
            free(errstr)
            rcstr = dragon_get_rc_string(lib_err)
            self._lib_err_str = rcstr[:].decode('utf-8')
        else:
            self._lib_err = lib_err
            self._lib_msg = lib_msg
            self._lib_err_str = lib_err_str


    def __str__(self):
        if self._lib_err is None:
            return f'Perf Error: {self._msg}'

        return f'Perf Error: {self._msg} | Dragon Msg: {self._lib_msg} | Dragon Error Code: {self._lib_err_str}'


    @property
    def lib_err(self):
        return self._lib_err


    def __repr__(self):
        return f'{self.__class__}({self._msg!r}, {self._lib_err!r}, {self._lib_msg!r}, {self._lib_err_str!r})'


# Kernel's are broken up into two classes: KernelManager and KernelWorker.
# A KernelManager runs in the single process that sets up the test, whereas
# a KernelWorker runs in one of the worker processes started for the test.
# KernelManager objects talk to KernelWorker objects via a queue. New operations
# and commands are sent to the workers over the queue. When a worker receives
# a 'run' command, all operations are executed in the order in which they
# were received.


class KernelWorker:
    def __init__(self, int idx, int ch_idx, int num_procs):
        self.idx = idx

        cdef dragonError_t derr
        derr = dragon_chperf_kernel_new(idx, ch_idx, num_procs)
        if derr != DRAGON_SUCCESS:
            raise ChPerfError("Failed to create new kernel", derr)


    def append(self, dragonChPerfOpcode_t op_code, int dst_ch_idx, size_t size_in_bytes, double timeout_in_sec):
        cdef dragonError_t derr
        derr = dragon_chperf_kernel_append_op(self.idx, op_code, dst_ch_idx, size_in_bytes, timeout_in_sec)
        if derr != DRAGON_SUCCESS:
            raise ChPerfError("Failed to append new op to kernel", derr)


    def run(self):
        cdef:
            dragonError_t derr
            double run_time

        derr = dragon_chperf_kernel_run(self.idx, &run_time)
        if derr != DRAGON_SUCCESS:
            raise ChPerfError("Failure during kernel execution", derr)

        return run_time


class Kernel:
    def __init__(self, name, idx, session, manager_queue, workers):
        self.name = name
        self.idx = idx
        self.session = session
        self.manager_queue = manager_queue
        self.workers = workers
        self.run_times = []

        log.info(f'creating new kernel: name={self.name}')

        for worker in workers:
            worker.queue.put(['new_kernel', idx])

        for worker in workers:
            msg_type, msg = manager_queue.get()
            self.handle_response(worker, msg_type, msg)


    def handle_response(self, worker, msg_type, msg):
        if msg_type == 'error':
            log.debug(f'received error from worker {worker.ch_idx}: {msg}')
            raise
        elif msg_type == 'data':
            self.run_times.append(msg)
        elif msg_type == 'ack':
            # do nothing
            pass
        else:
            log.debug(f'invalid response type from worker {worker.ch_idx}: {msg_type}')
            raise

    def process_run_times(self):
        total_time = 0
        for time in self.run_times:
            total_time += time

        mean_time = total_time / len(self.run_times)
        min_time = min(self.run_times)
        max_time = max(self.run_times)

        strs_to_print = []
        strs_to_print.append(f'runtime (seconds):')
        strs_to_print.append(f'  mean   {mean_time}')
        strs_to_print.append(f'  min    {min_time}')
        strs_to_print.append(f'  mmax   {max_time}')
        print('\n'.join(strs_to_print), flush=True)
        

    def get_run_times(self):
        return self.run_times

    def append(self, op_code, src_ch_idx, dst_ch_idx, size_in_bytes=None, timeout_in_sec=30):
        worker = self.workers[src_ch_idx]
        if size_in_bytes == None or size_in_bytes < 0:
            size_in_bytes = 0
        # this list can't be pickled unless we convert op_code to an int (which is harmless)
        worker.queue.put(['new_operation', self.idx, int(op_code), dst_ch_idx, size_in_bytes, timeout_in_sec]) 

        msg_type, msg = self.manager_queue.get()
        self.handle_response(worker, msg_type, msg)


    def run(self):
        log.info(f'{self.name} kernel: run started')

        for worker in self.workers:
            worker.queue.put(['run', self.idx])

        self.run_times = []
        for worker in self.workers:
            msg_type, msg = self.manager_queue.get()
            self.handle_response(worker, msg_type, msg)

        log.info(f'{self.name} kernel: run complete')

        self.process_run_times()


class SessionWorker:
    def __init__(self, manager_queue, ch_ctr, ch_idx, num_procs):
        self.manager_queue = manager_queue
        self.queue = mp.Queue()
        self.ch_ctr = ch_ctr
        self.ch_idx = ch_idx
        self.num_procs = num_procs
        self.kernels = {}
        self.proc = mp.Process(target=self.work_loop)
        self.proc.start()


    def init_channels(self):
        # create a channel and send its serialized descriptor to the manager. the
        # manager will reply with a list of serialized descriptors for this session.

        inf_pool_descr = dutils.B64.from_str(dparm.this_process.inf_pd).decode()
        inf_pool = dmm.MemoryPool.attach(inf_pool_descr)

        channel_uid = dfacts.BASE_USER_MANAGED_CUID + self.ch_ctr + self.ch_idx
        channel = dch.Channel(inf_pool, channel_uid)
        sdesc = channel.serialize()

        self.manager_queue.put([self.ch_idx, sdesc])
        sdesc_dict = self.queue.get()

        cdef dragonChannelSerial_t *sdesc_array
        sdesc_array = <dragonChannelSerial_t *>malloc(self.num_procs * sizeof(dragonChannelSerial_t))
        if sdesc_array == NULL:
            raise MemoryError()

        cdef int i
        cdef uint8_t *sdesc_data_c = NULL

        for i in range(self.num_procs):
            sdesc_array[i].len = len(sdesc_dict[i])
            sdesc_array[i].data = <uint8_t *>malloc(len(sdesc_dict[i]))
            if sdesc_array[i].data == NULL:
                raise MemoryError()

            sdesc_data_c = sdesc_dict[i]
            memcpy(sdesc_array[i].data, sdesc_data_c, len(sdesc_dict[i]))

        cdef derr = dragon_chperf_session_new(sdesc_array, self.num_procs)
        if derr != DRAGON_SUCCESS:
            raise ChPerfError("Failed to create new session", derr)

        for i in range(self.num_procs):
            free(sdesc_array[i].data)

        free(sdesc_array)


    def do_work(self, work):
        if work[0] == 'new_kernel':
            idx = work[1]
            self.kernels[idx] = KernelWorker(idx, self.ch_idx, self.num_procs)
            self.manager_queue.put(['ack', 'new_kernel'])
        elif work[0] == 'new_operation':
            idx = work[1]
            kernel = self.kernels[idx]
            kernel.append(work[2], work[3], work[4], work[5])
            self.manager_queue.put(['ack', 'new_operation'])
        elif work[0] == 'run':
            idx = work[1]
            kernel = self.kernels[idx]
            run_time = kernel.run()
            self.manager_queue.put(['data', run_time])
        elif work[0] == 'done':
            self.manager_queue.put(['ack', 'done'])
            return True
        else:
            self.manager_queue.put(['error', f'invalid work request: {work}'])
            return True

        return False


    def work_loop(self):
        self.init_channels()
        done = False
        while not done:
            work = self.queue.get()
            try:
                done = self.do_work(work)
            except Exception as e:
                self.manager_queue.put(['error', f'exception while handling work item: {e}'])
                return

        cdef derr = dragon_chperf_session_cleanup()
        if derr != DRAGON_SUCCESS:
            raise ChPerfError("Failed to clean up stopped session", derr)


class Session:
    def __init__(self, int num_procs, name="default"):
        self.name = name
        self.kernel_idx_ctr = 0
        self.manager_queue = mp.Queue()
        self.workers = []

        global log
        if log == None:
            setup_BE_logging(service=dls.PERF)
            log = logging.getLogger(str(dls.PERF)).getChild(f'dragon.perf')

        log.info(f'starting new perf session: name={self.name}')

        sdesc_dict = {}

        # create workers and get serialized descriptors from them
        for ch_idx in range(num_procs):
            worker = SessionWorker(self.manager_queue, ch_ctr, ch_idx, num_procs)
            self.workers.append(worker)

            ch_idx, sdesc = self.manager_queue.get()
            sdesc_dict[ch_idx] = sdesc

        # send dict of serialized descriptors back to all workers
        for worker in self.workers:
            worker.queue.put(sdesc_dict)

        global ch_ctr
        ch_ctr += num_procs


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


    def stop(self):
        log.info(f'cleaning up stopped session: name={self.name}')
        for worker in self.workers:
            worker.queue.put(['done'])
            msg_type, msg = self.manager_queue.get()
            if msg_type == 'error':
                log.info(f'final message from worker: {msg}')
            worker.proc.join()


    def new_kernel(self, name="unknown"):
        kernel_idx = self.kernel_idx_ctr
        self.kernel_idx_ctr += 1
        return Kernel(name, kernel_idx, self, self.manager_queue, self.workers)

