from dragon.dtypes_inc cimport *
from dragon.return_codes cimport *
import ctypes
import os
import sys
import time
import threading
from socket import gethostname
from .infrastructure.facts import lazy_import

np = lazy_import("numpy")

cpdef host_id_from_k8s(str pod_uid):
    """This is used to get a hostid based on the k8s pod uid.
    It is called when a process in one node needs to know
    the host_id of another node.
    It can be called from a node/pod as many times as needed
    in order to translate a pod uid to a dragon hostid.
    For example, a backend pod can query the hostids of the other
    backend pods. For setting/assigning the hostid of the pod itself,
    each pod needs to call set_host_id().
    """
    pod_uuid = pod_uid.encode('utf-8')
    return dragon_host_id_from_k8s_uuid(<char *>pod_uuid)

cpdef host_id():
    return dragon_host_id()

cpdef set_host_id(unsigned long new_id):
    dnew_id = <dragonULInt> new_id
    dragon_set_host_id(new_id)

cpdef get_local_rt_uid():
    return dragon_get_local_rt_uid()

cpdef set_procname(str name):
    estr = name.encode('utf-8')
    cdef dragonError_t err = dragon_set_procname(<char *>estr)
    if err != DRAGON_SUCCESS:
        raise ValueError('Could not set process name')

cdef class B64:
    """ Cython wrapper for Dragon's byte <> string conversion routines. """
    cdef:
        char* _encoded_string

    def __cinit__(self):
        self._encoded_string = NULL

    def __del__(self):
        if self._encoded_string != NULL:
            free(self._encoded_string)
            self._encoded_string = NULL

    def __init__(self, data):
        """Convert a bytes array into a base64 encoded string.
        :param data: The list of bytes to convert.
        :return: A new B64String object containing the base64 encoded string.
        """
        self._encoded_string = dragon_base64_encode(data, len(data))

    def __str__(self):
        return self._encoded_string[:strlen(self._encoded_string)].decode('utf-8')

    def _initialize_from_str(self, serialized_str):
        cdef char * todataptr = <char*>malloc(len(serialized_str)+1)
        if todataptr == NULL:
            raise ValueError('Could not allocate space for B64 object.')

        data = serialized_str.encode('utf-8')
        size = len(serialized_str)+1
        cdef char * fromdataptr = data
        memcpy(todataptr, fromdataptr, size)
        self._encoded_string = todataptr

    def decode(self):
        cdef:
            uint8_t *data
            size_t decoded_length

        data = dragon_base64_decode(self._encoded_string, &decoded_length)
        if data == NULL:
            raise ValueError('Invalid Base64 Value')

        val = data[:decoded_length]
        free(data)
        return val

    @classmethod
    def from_str(cls, serialized_str):
        obj = cls.__new__(cls)
        obj._initialize_from_str(serialized_str)
        return obj

    @classmethod
    def bytes_to_str(cls, the_bytes):
        """Converts bytes into a string by base64 encoding it.
        Convenience function to convert bytes objects to base64
        encoded strings.
        :param the_bytes: bytes to get encoded
        :return: string
        """
        return str(cls(the_bytes))

    @classmethod
    def str_to_bytes(cls, the_str):
        """Converts a base64 encoded string to a bytes object.
        Convenience function to unpack strings.
        :param the_str: base64 encoded string.
        :return: original bytes representation.
        """
        data = cls.from_str(the_str).decode()
        if data is None:
            raise ValueError('Could not convert Base64 string to bytes.')

        return data

cpdef b64encode(the_bytes):
    return str(B64.bytes_to_str(the_bytes))

cpdef b64decode(the_str):
    return B64.str_to_bytes(str(the_str))

cpdef hash(byte_str:bytes):
    cdef:
        const unsigned char[:] buf = byte_str

    return dragon_hash(<void*>&buf[0], len(byte_str))

cpdef set_local_kv(key, value, timeout=None):
    cdef:
        const unsigned char* val_ptr
        unsigned char* empty_str = ""
        const unsigned char[:] key_str
        const unsigned char[:] val_str

        dragonError_t err
        timespec_t * time_ptr
        timespec_t val_timeout

    if len(key) == 0:
        raise KeyError('Key cannot be empty')

    if timeout is None:
        time_ptr = NULL
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0 to set_local_kv operation')
        # Anything > 0 means use that as seconds for timeout.
        time_ptr = & val_timeout
        val_timeout.tv_sec =  int(timeout)
        val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
    else:
        raise ValueError('make_process_local timeout must be a float or int')

    key_str = str.encode(key)

    if len(value) > 0:
        val_str = str.encode(value)
        val_ptr = &val_str[0]
    else:
        val_ptr = empty_str

    with nogil:
        err = dragon_ls_set_kv(&key_str[0], val_ptr, time_ptr)

    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not set kv pair. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

cpdef get_local_kv(key, timeout=None):
    cdef:
        const unsigned char[:] key_str
        char* val_str

        dragonError_t err
        timespec_t * time_ptr
        timespec_t val_timeout

    if timeout is None:
        time_ptr = NULL
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0 to set_local_kv operation')
        # Anything > 0 means use that as seconds for timeout.
        time_ptr = & val_timeout
        val_timeout.tv_sec =  int(timeout)
        val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
    else:
        raise ValueError('make_process_local timeout must be a float or int')

    key_str = str.encode(key)

    with nogil:
        err = dragon_ls_get_kv(&key_str[0], &val_str, time_ptr)

    if err == DRAGON_NOT_FOUND:
        raise KeyError(key)

    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not set kv pair. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

    return val_str.decode('utf-8')

cpdef get_hugepage_mount():
    cdef:
        dragonError_t derr
        char *mount_dir
        bytes tmp_bytes

    derr = dragon_get_hugepage_mount(&mount_dir)
    if derr == DRAGON_SUCCESS:
        tmp_bytes = mount_dir
        return tmp_bytes.decode('utf-8')
    else:
        return None

cpdef get_cpu_count():
    cdef:
        int count

    try:
        count = dragon_get_cpu_count()
        if count <= 0:
            raise RuntimeError('Could not get the number of cpus.')
    except:
        raise RuntimeError('There was an exception while calling dragon_get_cpu_count')

    return count

cpdef set_core_affinity(core):
    cdef:
        char* core_val
        bytes core_bytes

    try:
        core_bytes = str(core).encode('utf-8')
        core_val = core_bytes
        dragon_set_my_core_affinity(core_val)
    except:
        raise RuntimeError("Unable to set core affinity")

cpdef getlasterrstr():
    cdef:
        char* traceback

    traceback = dragon_getlasterrstr()

    return traceback.decode('utf-8')

cpdef strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'. Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))

class TimeKeeper:
    """Tracks elapsed times by id and optionally streams them to the Dragon telemetry
    infrastructure on a background thread.

    A :class:`~dragon.telemetry.telemetry.Telemetry` instance is created
    internally.  When its ``level > 0`` (i.e. the runtime was launched with
    ``--telemetry-level`` greater than zero), a background thread is started
    that wakes every *collection_window* seconds, acquires an
    :class:`threading.RLock`, ships every accumulated timing to the telemetry
    service via ``add_data``, resets all accumulators, then releases the lock.

    * ``timekeeper_name`` becomes the ``ts_metric_name`` in every ``add_data``
      call (the OpenTSDB metric name).
    * Each timing *id* is sent as ``tagk="id", tagv=str(id)``.
    * The local hostname is sent as ``tagk="hostname", tagv=<hostname>``.

    When telemetry is inactive (``telemetry.level == 0``) no thread is started
    and no lock is created, preserving the original single-threaded behaviour.
    """

    def __init__(
        self,
        timekeeper_name: str = "",
        recording: bool = False,
        collection_window: float = 10.0,
        clear_all: bool = True,
    ):
        """
        :param timekeeper_name: Name used as the telemetry metric name.
        :type timekeeper_name: str
        :param recording: Enable timing accumulation.
        :type recording: bool
        :param collection_window: Seconds between telemetry flushes.
        :type collection_window: float
        """
        self._name = timekeeper_name
        self._recording = recording
        self._collection_window = collection_window
        self._clear_all = clear_all
        # Lock and thread are only created when telemetry is active.
        self._lock = None
        self._stop_event = None
        self._thread = None

        self.start_telemetry()
        self.reset_all()

    def start_telemetry(self):
        try:
            from dragon.telemetry.telemetry import Telemetry, DRAGON_INFRASTRUCTURE_TELEMETRY_STARTUP_TIMEOUT

            self._telemetry = Telemetry(timeout=DRAGON_INFRASTRUCTURE_TELEMETRY_STARTUP_TIMEOUT)

            if self._telemetry.level > 0:
                self._hostname = gethostname()
                self._lock = threading.RLock()
                self._stop_event = threading.Event()
                self._thread = threading.Thread(
                    target=self._telemetry_worker,
                    name=f"TimeKeeper-{self._name}",
                    daemon=True,
                )
                self._thread.start()
        except ModuleNotFoundError:
            pass

    def _telemetry_worker(self) -> None:
        """Periodically flush accumulated timings to the telemetry service.

        Sleeps for *collection_window* seconds, then—while holding the lock—
        sends each ``(id, duration)`` pair to ``add_data`` (once tagged by
        ``id``, once tagged by ``hostname``), and calls :meth:`reset_all`
        before releasing the lock.
        """
        while not self._stop_event.wait(self._collection_window):
            with self._lock:
                for timing_id, duration in self._timings.items():
                    self._telemetry.add_data(
                        self._name,
                        duration,
                        tagk="id",
                        tagv=str(timing_id),
                    )
                # Reset accumulators while still holding the lock.
                if self._clear_all:
                    self.reset_all()

    def stop(self) -> None:
        """Signal the background telemetry worker to stop and wait for it.

        Safe to call when telemetry is not active (no-op in that case).
        """
        if self._stop_event is not None:
            self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    class Recorder:
        def __init__(self, timekeeper, id, start=None):
            self._timekeeper = timekeeper
            self._id = id
            self._tic = start

        def __enter__(self):
            if self._tic is None:
                self._tic = time.perf_counter()

        def __exit__(self, exc_type, exc_value, traceback):
            toc = time.perf_counter()
            self._timekeeper.add(self._id, self._tic, toc)

    def record(self, id, start=None):
        return TimeKeeper.Recorder(self, id, start=start)

    def now(self):
        return time.perf_counter()

    def add(self, id, start, end=None):
        if not self._recording:
            return
        if end is None:
            end = time.perf_counter()
        delta = end - start
        if self._lock is not None:
            with self._lock:
                self._timings[id] = self._timings.get(id, 0) + delta
        else:
            self._timings[id] = self._timings.get(id, 0) + delta

    def add_elapsed(self, id, elapsed):
        if not self._recording:
            return
        if self._lock is not None:
            with self._lock:
                self._timings[id] = self._timings.get(id, 0) + elapsed
        else:
            self._timings[id] = self._timings.get(id, 0) + elapsed

    def reset(self, id):
        if self._lock is not None:
            with self._lock:
                self._timings[id] = 0
        else:
            self._timings[id] = 0

    def reset_all(self):
        # Uses RLock so it is safe to call from within _telemetry_worker
        # (which already holds the lock on the same thread).
        if self._lock is not None:
            with self._lock:
                self._timings = {}
        else:
            self._timings = {}

    def empty(self):
        if self._lock is not None:
            with self._lock:
                return len(self._timings) == 0
        return len(self._timings) == 0

    def get_timings(self):
        if self._lock is not None:
            with self._lock:
                return dict(self._timings)
        return dict(self._timings)

    def __str__(self):
        if self._lock is not None:
            with self._lock:
                timings = dict(self._timings)
        else:
            timings = self._timings

        if not timings:
            return "Nothing was recorded.\nTurn on recording by creating TimeKeeper with recording=True.\n"

        result = ""
        result += "TimeKeeper Timings\n"
        result += "==================\n"
        for key in timings:
            duration = timings[key]
            result += f"{key}:  {duration}\n"

        return result


class ExceptionalThread(threading.Thread):
    """Enhanced threading.Thread that can be killed from the outside by raising
    an exception inside the running instance.  The thread's running target
    function must reach a point where the exception can be noticed such as
    when the GIL swaps executing threads or some other wait state occurs.  Note
    that external native library (C/C++/Fortran) code invoked within the target
    function will not notice the exception being raised and so control must be
    returned to Python for the exception to be noticed.

    >>> import time
    >>> t1 = ExceptionalThread(
    ...     target=lambda n: sum(1 if y % 2 == 0 else -1 for y in range(n)),
    ...     args=(10_000_000_000,)
    ... )  # Compute should take several minutes on modern processor core
    >>> start_time = time.monotonic(); t1.start()
    >>> t1.kill_by_exception()
    1
    >>> t1.join()
    >>> (time.monotonic() - start_time) < 1  # Killed and joined in under 1s
    True
    >>> # time.sleep ignores exception until very end like native code would
    >>> t2 = ExceptionalThread(target=time.sleep, args=(1,))
    >>> start_time = time.monotonic(); t2.start()
    >>> t2.kill_by_exception()  # Still indicates exception trigger success
    1
    >>> t2.join()
    >>> (time.monotonic() - start_time) < 1  # No early termination of thread
    False

    """

    def kill_by_exception(self, exc_type=Exception):
        """When called, will raise the specified exception inside the running
        thread instance and will return an int to indicate success (1),
        failure to find the thread (0), or unhandled error followed by an
        attempt to revert the thread state change (2 or greater)."""

        thread_id = ctypes.c_long(self.ident)
        retval = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            thread_id,
            ctypes.py_object(exc_type),
        )
        if retval > 1:
            # Unexpected result; attempt to revert thread state change.
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, None)
        return retval

cpdef enum SerType:
    TY_NONE = SERTYPE_NONE
    TY_STR = SERTYPE_STR
    TY_INT = SERTYPE_INT
    TY_DOUBLE = SERTYPE_DOUBLE
    TY_INTVECTOR = SERTYPE_INTVECTOR
    TY_DOUBLEVECTOR = SERTYPE_DOUBLEVECTOR
    TY_INTMATRIX = SERTYPE_INTMATRIX
    TY_DOUBLEMATRIX = SERTYPE_DOUBLEMATRIX
    TY_BYTEBUFFER = SERTYPE_BYTEBUFFER

class XNumPyVectorPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableVector<Type>. Pickling a numpy array with this pickler can
    be read in C++ by deserializing using the SerializableVector<Type>.
    Likewise, a C++ matrix serialized with the SerializableVector<Type> can
    be unpickled into a numpy array as well. This is meant to be an
    efficient implementation for passing arrays/vectors to and from other
    languages (at present that means C++) through a pickling/unpickling
    interface like native Queue or DDict. """

    def __init__(self, data_type: np.dtype, type_val = TY_NONE):
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XNumPyVectorPickler.")
        self._ty_val = type_val
        self._data_type = data_type

    @property
    def ty_val(self):
        """
        This is the type value to identify the particular type of value
        to be unpickled. It is one of the SerType values.
        """
        return self._ty_val

    def byte_size(self, nparr):
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        item_size = np.dtype(self._data_type).itemsize
        return size_t_size + item_size * len(nparr)

    def dumps(self, nparr) -> bytes:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        try:
            ncol, = nparr.shape
        except:
            raise ValueError("The array must be a 1-dimensional array")

        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)
        mv = memoryview(nparr)
        bobj = mv.tobytes()
        rv = bytes_ncol
        rv += bobj
        return rv

    def loads(self, val):
        obj = None
        total_size = len(val)
        # read the number of rows in the matrix
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        ncol = int.from_bytes(val[:size_t_size], sys.byteorder)
        idx = size_t_size
        item_size = np.dtype(self._data_type).itemsize

        # then grab the array
        data_size = ncol*item_size
        data = val[idx:idx+data_size]
        view = memoryview(data)
        obj = bytearray(view)
        idx+=data_size

        ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape((ncol,))
        return ret_arr

    def dump(self, nparr, file) -> None:

        # write the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        ncol, = nparr.shape
        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)
        file.write(bytes_ncol)

        mv = memoryview(nparr)
        bobj = mv.tobytes()
        file.write(bobj)

    def load(self, file):

        # read the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        ncol = int.from_bytes(file.read(size_t_size), sys.byteorder)
        item_size = np.dtype(self._data_type).itemsize

        data = file.read(ncol*item_size)
        # convert bytes to bytearray
        view = memoryview(data)
        obj = bytearray(view)

        ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape((ncol,))

        return ret_arr

class XNumPy2DMatrixPickler:
    """A Pickler that has X-language support and is compatible with the C++
    Serializable2DMatrix<Type>. Pickling a numpy array with this pickler can
    be read in C++ by deserializing using the Serializable2DMatrix<Type>.
    Likewise, a C++ matrix serialized with the Serializable2DMatrix<Type> can
    be unpickled into a numpy matrix was well. This is meant to be an
    efficient implementation for passing 2D matrices to and from other
    languages (at present that means C++) through a pickling/unpickling
    interface like native Queue or DDict. """

    def __init__(self, data_type: np.dtype, type_val = TY_NONE):
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XNumPy2DMatrixPickler.")
        self._ty_val = type_val
        self._data_type = data_type
        self._arr_pickler = XNumPyVectorPickler(data_type)

    @property
    def ty_val(self):
        return self._ty_val

    def byte_size(self, nparr):
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow, ncol = nparr.shape
        return size_t_size + nrow * XNumPyVectorPickler.byte_size(nparr[0])

    def dumps(self, nparr) -> bytes:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow, ncol = nparr.shape
        bytes_nrow = nrow.to_bytes(size_t_size, byteorder=sys.byteorder)
        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)
        rv = bytes_nrow
        for i in range(nrow):
            rv += self._arr_pickler.dumps(nparr[i])

        return rv

    def loads(self, val):
        obj = None

        # read the number of rows in the matrix
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow = int.from_bytes(val[:size_t_size], sys.byteorder)
        idx = size_t_size
        item_size = np.dtype(self._data_type).itemsize
        total_size = len(val)

        ret_arr = np.array([])
        ncol = 0

        for i in range(nrow):
            arr = self._arr_pickler.loads(val[idx:])
            ncol = len(arr)
            idx += self._arr_pickler.byte_size(arr)
            ret_arr = np.append(ret_arr, arr)

        ret_arr = ret_arr.reshape((nrow, ncol))
        return ret_arr

    def dump(self, nparr, file) -> None:

        # write the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow, ncol = nparr.shape
        bytes_nrow = nrow.to_bytes(size_t_size, byteorder=sys.byteorder)
        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)
        file.write(bytes_nrow)

        # Write the 2D array as a sequence of 1D array rows. Numpy's
        # default is that rows are guaranteed contiguous. If your
        # application had completely contiguous data, then crafting
        # a new pickler and writing your own C++ serializable class
        # would be in order. Otherwise, this should work for most
        # default numpy matrices.

        for i in range(nrow):
            self._arr_pickler.dump(nparr[i], file)


    def load(self, file):

        obj = None

        # read the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow = int.from_bytes(file.read(size_t_size), sys.byteorder)
        item_size = np.dtype(self._data_type).itemsize
        ret_arr = np.array([])
        ncol = 0

        for i in range(nrow):
            arr = self._arr_pickler.load(file)
            ncol = len(arr)
            ret_arr = np.append(ret_arr, arr)

        ret_arr = ret_arr.reshape((nrow, ncol))

        return ret_arr

class XByteBufferPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableByteBuffer. Pickling a byte buffer with this pickler can
    be read in C++ by deserializing using the SerializableByteBuffer class.
    Likewise, a C++ byte buffer serialized with the SerializableByteBuffer can
    be unpickled into a bytes object as well. This is meant to be used by code
    that wishes to access the bytes and perhaps provide code that interprets them
    in an application specific way. """

    def __init__(self):
        pass

    @property
    def ty_val(self):
        """
        This is the type value to identify the particular type of value
        to be unpickled. It is one of the SerType values.
        """
        return TY_BYTEBUFFER

    def byte_size(self, val: bytes) -> int:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        item_size = 1 # byte
        return size_t_size + item_size * len(val)

    def dumps(self, val: bytes) -> bytes:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        num_bytes = len(val)
        size_bytes = num_bytes.to_bytes(size_t_size, byteorder=sys.byteorder)
        rv = size_bytes
        rv += val
        return rv

    def loads(self, val: bytes) -> bytes:
        total_size = len(val)
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        data_size = int.from_bytes(val[:size_t_size], sys.byteorder)

        # then grab the data
        idx = size_t_size
        stop_idx = idx + data_size
        data = val[idx:stop_idx]

        if total_size != stop_idx:
            raise ValueError(f"The bytes size was {total_size} and the encoded size was {stop_idx}. They should match.")

        return data

    def dump(self, val:bytes, file) -> None:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        num_bytes = len(val)
        size_bytes = num_bytes.to_bytes(size_t_size, byteorder=sys.byteorder)
        file.write(size_bytes)
        file.write(val)

    def load(self, file):
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        data_size = int.from_bytes(file.read(size_t_size), sys.byteorder)
        data = file.read(data_size)
        return data


class XScalarPickler:
    """A Pickler that has X-language support and is compatible with the C++
    Scalars like int and double. Pickling a scalar with this pickler can be
    read in C++ by deserializing using the SeralizableInt or
    SerializableScalar class. Likewise, a C++ int/double serialized with
    SeralizableScalar and SerializableScalar can be unpickled into a int/float
    was well, enabling cross language communication between Python and other
    languages (at present that means C++) through a pickling/unpickling
    interface like native Queue or DDict. """

    def __init__(self, data_type: np.dtype, type_val = TY_NONE):
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XScalarPickler.")
        self._ty_val = type_val
        self._data_type = data_type
        self._num_bytes = np.dtype(self._data_type).itemsize

    @property
    def ty_val(self):
        return self._ty_val

    def byte_size(self, val):
        return self._num_bytes

    def dumps(self, val) -> bytes:
        val = self._data_type(val)
        return val.tobytes()

    def loads(self, val: bytes) -> object:
        try:
            return np.frombuffer(val, dtype=self._data_type)[0].item()
        except Exception as ex:
            raise ValueError(f"The value {val} was not decodable as a scalar of type {self._data_type}")

    def dump(self, val, file) -> None:
        file.write(self.dumps(val))

    def load(self, file) -> object:
        return self.loads(file.read(self._num_bytes))

class XStringPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableString. Pickling a string with this pickler can be read in
    C++ by deserializing using the SeralizableString class. Likewise, a C++
    string serialized with SeralizableString can be unpickled into a str was
    well, enabling cross communication between Python and other languages (at
    present that means C++) through a pickling/unpickling interface like
    native Queue or DDict. """

    def __init__(self):
        pass

    @property
    def ty_val(self):
        return TY_STR

    def byte_size(self, val):
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        strlen = len(val)
        return size_t_size + strlen

    def dumps(self, val) -> bytes:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        strlen = len(val)
        strlenbytes = strlen.to_bytes(size_t_size, byteorder=sys.byteorder)
        valbytes = val.encode()
        return strlenbytes + valbytes

    def loads(self, val: bytes) -> str:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        # unused: strlen = int.from_bytes(val[:size_t_size], sys.byteorder)
        rv = val[size_t_size:].decode('utf-8')
        return rv

    def dump(self, val, file) -> None:
        file.write(self.dumps(val))

    def load(self, file) -> str:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        strlen = int.from_bytes(file.read(size_t_size), sys.byteorder)
        val = file.read(strlen)
        rv = val.decode('utf-8')
        return rv

class XPickler:
    """A Pickler that has X-language support and is compatible with the C++
    Serializables. Objects to be shared between Python and C++ must be supported
    by XPickler on the Python side and the Serializable class on the C++ side.
    Pickling a supported object with this pickler can be read in
    C++ by deserializing using the Seralizable class. Likewise, a C++
    Serializable object can be unpickled into its corresponding Python object,
    enabling cross communication between Python and other languages (at
    present that means C++) through a pickling/unpickling interface like
    native Queue or DDict. """

    def __init__(self):
        self._str_pickler = XStringPickler()
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XPickler.")
        self._int_pickler = XScalarPickler(np.int32, TY_INT)
        self._double_pickler = XScalarPickler(np.float64, TY_DOUBLE)
        self._intvector_pickler = XNumPyVectorPickler(np.int32, TY_INTVECTOR)
        self._doublevector_pickler = XNumPyVectorPickler(np.float64, TY_DOUBLEVECTOR)
        self._intmatrix_pickler = XNumPy2DMatrixPickler(np.int32, TY_INTMATRIX)
        self._doublematrix_pickler = XNumPy2DMatrixPickler(np.float64, TY_DOUBLEMATRIX)
        self._bytebuffer_pickler = XByteBufferPickler()

        self._ty_pickler = {
            TY_STR: self._str_pickler,
            TY_INT: self._int_pickler,
            TY_DOUBLE: self._double_pickler,
            TY_INTVECTOR: self._intvector_pickler,
            TY_DOUBLEVECTOR: self._doublevector_pickler,
            TY_INTMATRIX: self._intmatrix_pickler,
            TY_DOUBLEMATRIX: self._doublematrix_pickler,
            TY_BYTEBUFFER: self._bytebuffer_pickler
        }


    def choose_pickler(self, val):
        if isinstance(val, str):
            return self._str_pickler

        if isinstance(val, int):
            return self._int_pickler

        if isinstance(val, float):
            return self._double_pickler

        if isinstance(val, bytes):
            return self._bytebuffer_pickler

        if isinstance(val, np.ndarray):
            if isinstance(val[0], int):
                return self._intvector_pickler

            if isinstance(val[0], float):
                return self._doublevector_pickler

            if isinstance(val[0], np.ndarray):
                if isinstance(val[0][0], int):
                    return self._intmatrix_pickler

                if isinstance(val[0][0], float):
                    return self._doublematrix_pickler


        raise ValueError(f"Value {val} of unknown type {type(val)} cannot be pickled by XPickler.")

    @classmethod
    def ty_bytes(cls, pickler):
        int_size = ctypes.sizeof(ctypes.c_int)
        return pickler.ty_val.to_bytes(int_size, byteorder=sys.byteorder)

    def dumps(self, val) -> bytes:
        pickler = self.choose_pickler(val)
        ty_bytes = XPickler.ty_bytes(pickler)
        return ty_bytes + pickler.dumps(val)

    def loads(self, val: bytes) -> object:
        int_size = ctypes.sizeof(ctypes.c_int)
        ty_val = np.frombuffer(val[:int_size], dtype=np.int32)[0].item()
        if ty_val not in self._ty_pickler:
            raise ValueError("The bytes are not XPickled and could not be decoded.")
        pickler = self._ty_pickler[ty_val]
        return pickler.loads(val[int_size:])

    def dump(self, val, file) -> None:
        file.write(self.dumps(val))

    def load(self, file) -> object:
        int_size = ctypes.sizeof(ctypes.c_int)
        ty_val = int.from_bytes(file.read(int_size), sys.byteorder)
        pickler = self._ty_pickler[ty_val]
        return pickler.load(file)