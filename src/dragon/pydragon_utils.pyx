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
    cdef:
        dragonULInt c_host_id
        dragonError_t err

    pod_uuid = pod_uid.encode('utf-8')
    err = dragon_host_id_from_k8s_uuid(<char *>pod_uuid, &c_host_id)
    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not get host ID from the Kubernetes pod UUID. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

    return c_host_id

cpdef host_id():
    cdef:
        dragonULInt c_host_id
        dragonError_t err

    err = dragon_host_id(&c_host_id)
    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not determine the host ID. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

    return c_host_id

cpdef set_host_id(unsigned long new_id):
    cdef dragonError_t err = dragon_set_host_id(<dragonULInt> new_id)
    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not set the host ID. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

cpdef get_local_rt_uid():
    return dragon_get_local_rt_uid()

# Explicitly override the C layer's cached local runtime UID. Proxy environment
# switches leave this process-local identity unchanged.
cpdef set_local_rt_uid(unsigned long new_rt_uid):
    dragon_set_local_rt_uid(<dragonULInt> new_rt_uid)

cpdef set_procname(str name):
    estr = name.encode('utf-8')
    cdef dragonError_t err = dragon_set_procname(<char *>estr)
    if err != DRAGON_SUCCESS:
        raise ValueError('Could not set process name')

cpdef shm_space():
    cdef:
        unsigned long total_space;
        unsigned long free_space;
        dragonError_t err;

    err = query_shm_space(&total_space, &free_space)
    if err != DRAGON_SUCCESS:
        raise RuntimeError("Could not query the free shared memory size.")

    return (free_space, total_space)

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
    TY_DOUBLENDARRAY = SERTYPE_DOUBLENDARRAY
    TY_FLOATNDARRAY = SERTYPE_FLOATNDARRAY
    TY_INTNDARRAY = SERTYPE_INTNDARRAY
    TY_LONGNDARRAY = SERTYPE_LONGNDARRAY
    TY_QUEUE = SERTYPE_QUEUE
    TY_DDICT = SERTYPE_DDICT
    TY_BARRIER = SERTYPE_BARRIER
    TY_SEMAPHORE = SERTYPE_SEMAPHORE

def nparray(arr):
    if not isinstance(arr, np.ndarray):
        arr = np.array(arr)

    return arr


try:
    class XNDArray(np.ndarray):
        def __new__(cls, input_array, dd_ser:str, ndarray_key:str = None, data_type:np.dtype=None, dim:tuple(int)=None, timeout:float=None):
            from dragon.data.ddict import DDict
            # Add the custom attribute
            if dd_ser is None:
                raise ValueError("You must provide a serialized DDict for serializable ndarrays.")

            dd = DDict.attach(serialized_dict=dd_ser, timeout=timeout)
            kpickler = XStringPickler()
            vpickler = XByteBufferPickler()
            dd = dd.pickler(key_pickler=kpickler, value_pickler=vpickler)
            store_needed = False

            if ndarray_key is None:
                key_tag = dd.fetch_add("dragon_serializable_ndarray")
                ndarray_key = "DRAGON_NDARRAY_" + str(key_tag)
                # Cast the input_array to our new class type
                obj = np.asarray(input_array).view(cls)
                store_needed = True
            else:
                data = dd[ndarray_key]
                view = memoryview(data)
                obj = bytearray(view)
                # view(cls) is required so the instance attributes below can be set.
                obj = np.frombuffer(obj, dtype=data_type).view(cls)

            if dim is not None:
                obj = obj.reshape(dim)

            obj._dd_ser = dd_ser
            obj._dd = dd
            obj._ndarray_key = ndarray_key
            obj._timeout = timeout

            if store_needed:
                mv = memoryview(obj)
                bobj = mv.tobytes()
                dd[ndarray_key] = bobj

            return obj

        def __array_finalize__(self, obj):
            # Ensures attributes persist through slicing and operations
            if obj is None: return
            self._dd_ser = getattr(obj, "_dd_ser", None)
            self._dd = getattr(obj, "_dd", None)
            self._ndarray_key = getattr(obj, "_ndarray_key", None)
            self._timeout = getattr(obj, "_timeout", None)

        def sync(self):
            mv = memoryview(self)
            bobj = mv.tobytes()
            self._dd[self._ndarray_key] = bobj

        def refresh(self):
            data = self._dd[self._ndarray_key]
            arr = np.frombuffer(bytearray(memoryview(data)), dtype=self.dtype)
            # Writes through to this instance's own buffer, leaving attributes intact.
            np.copyto(self, arr.reshape(self.shape))

        def destroy(self):
            try:
                del self._dd[self._ndarray_key]
            except:
                pass
except:
    class XNDArray:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("XNDArray is not available without numpy")


class XVectorPickler:
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
            raise ModuleNotFoundError("The numpy package is required when using XVectorPickler.")
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
        # Explicitly convert input to the expected data type to avoid dtype mismatches
        if not isinstance(nparr, np.ndarray):
            nparr = np.array(nparr, dtype=self._data_type)
        else:
            nparr = nparr.astype(self._data_type)

        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        try:
            ncol, = nparr.shape
        except ValueError:
            raise ValueError("The array must be a 1-dimensional array")

        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)

        # Ensure contiguous memory layout
        nparr = np.ascontiguousarray(nparr)
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

        if data_size > 0:
            data = val[idx:idx+data_size]
            view = memoryview(data)
            obj = bytearray(view)
            ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape((ncol,))
        else:
            # Handle empty array case
            ret_arr = np.array([], dtype=self._data_type).reshape((ncol,))

        return ret_arr

    def dump(self, nparr, file) -> None:
        # Explicitly convert input to the expected data type to avoid dtype mismatches
        if not isinstance(nparr, np.ndarray):
            nparr = np.array(nparr, dtype=self._data_type)
        else:
            nparr = nparr.astype(self._data_type)

        # write the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        try:
            ncol, = nparr.shape
        except ValueError:
            raise ValueError("The array must be a 1-dimensional array")

        bytes_ncol = ncol.to_bytes(size_t_size, byteorder=sys.byteorder)
        file.write(bytes_ncol)

        # Ensure contiguous memory layout for memoryview
        nparr = np.ascontiguousarray(nparr)
        mv = memoryview(nparr)
        bobj = mv.tobytes()
        file.write(bobj)

    def load(self, file):
        # read the dimension of the array
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        ncol = int.from_bytes(file.read(size_t_size), sys.byteorder)
        item_size = np.dtype(self._data_type).itemsize

        # Read data (0 bytes for empty arrays)
        data_size = ncol * item_size
        if data_size > 0:
            data = file.read(data_size)
            # convert bytes to bytearray
            view = memoryview(data)
            obj = bytearray(view)
            ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape((ncol,))
        else:
            # Handle empty array case
            ret_arr = np.array([], dtype=self._data_type).reshape((ncol,))

        return ret_arr

class X2DMatrixPickler:
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
            raise ModuleNotFoundError("The numpy package is required when using X2DMatrixPickler.")
        self._ty_val = type_val
        self._data_type = data_type
        self._arr_pickler = XVectorPickler(data_type)

    @property
    def ty_val(self):
        return self._ty_val

    def byte_size(self, nparr):
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow, ncol = nparr.shape
        return size_t_size + nrow * self._arr_pickler.byte_size(nparr[0])

    def dumps(self, nparr) -> bytes:
        nparr = nparray(nparr)
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

        ret_arr = np.array([], dtype=self._data_type)
        ncol = 0

        for i in range(nrow):
            arr = self._arr_pickler.loads(val[idx:])
            ncol = len(arr)
            idx += self._arr_pickler.byte_size(arr)
            ret_arr = np.append(ret_arr, arr)

        ret_arr = ret_arr.reshape((nrow, ncol))
        return ret_arr

    def dump(self, nparr, file) -> None:
        nparr = nparray(nparr)

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
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        nrow = int.from_bytes(file.read(size_t_size), sys.byteorder)

        ret_arr = np.array([], dtype=self._data_type)
        ncol = 0

        for i in range(nrow):
            arr = self._arr_pickler.load(file)
            ncol = len(arr)
            ret_arr = np.append(ret_arr, arr)

        return ret_arr.reshape((nrow, ncol))

class XNDArrayPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableNDArray. Pickling a higher dimensional numpy array with this
    pickler can be read in C++ by deserializing using the
    SerializableNDArray. Likewise, a C++ n-dimensional array serialized with
    SerializableNDArray can be unpickled into a numpy array was well. This is
    meant to be an efficient implementation for passing large
    multi-dimensional arrays to and from other languages (at present that
    means C++) through a pickling/unpickling interface like native Queue or
    DDict. """

    def __init__(self, data_type: np.dtype, type_val = TY_NONE):
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XNDArrayPickler.")
        self._ty_val = type_val

        self._vecpickler = XVectorPickler(np.dtype(ctypes.c_int))
        self._strpickler = XStringPickler()
        self._bufpickler = XByteBufferPickler()
        self._intpickler = XScalarPickler(ctypes.c_int)
        self._sizetpickler = XScalarPickler(ctypes.c_size_t)
        self._data_type = data_type

    @property
    def ty_val(self):
        return self._ty_val

    def dumps(self, xndarray) -> bytes:
        if not isinstance(xndarray, XNDArray):
            raise ValueError("You must provide an XNDArray instance to the XNDArrayPickler")

        # write the item/element size
        rv = self._intpickler.dumps(xndarray.dtype.itemsize)

        # write the dimension of the array
        rv += self._vecpickler.dumps(xndarray.shape)

        # write the indices which are empty when sent from Python
        rv += self._vecpickler.dumps([])

        # add the serialized ddict.
        rv += self._strpickler.dumps(xndarray._dd_ser)

        # add the ddict key for the ndarray
        rv += self._strpickler.dumps(xndarray._ndarray_key)

        if xndarray._timeout is None:
            tv_sec = -1
            tv_nsec = -1
        else:
            tv_sec = int(xndarray._timeout)
            tv_nsec = int((xndarray._timeout - tv_sec) * 1E9)

        # add the seconds and ns for timeout
        rv += self._intpickler.dumps(tv_sec)
        rv += self._intpickler.dumps(tv_nsec)

        return rv

    def loads(self, val):
        item_size = self._intpickler.loads(val)
        idx = self._intpickler.byte_size(item_size)

        dimensions = self._vecpickler.loads(val[idx:])
        idx += self._vecpickler.byte_size(dimensions)

        indices = self._vecpickler.loads(val[idx:])
        idx += self._vecpickler.byte_size(indices)

        dd_ser = self._strpickler.loads(val[idx:])
        idx += self._strpickler.byte_size(dd_ser)

        ndarray_key = self._strpickler.loads(val[idx:])
        idx += self._strpickler.byte_size(ndarray_key)

        secs = self._intpickler.loads(val[idx:])
        idx += self._intpickler.byte_size(secs)

        nsecs = self._intpickler.loads(val[idx:])
        idx += self._intpickler.byte_size(nsecs)

        if secs == -1:
            timeout = None
        else:
            timeout = secs + (nsecs / 1.0E9)

        return XNDArray([], dd_ser, ndarray_key, self._data_type, dimensions, timeout)

    def dump(self, xndarray, file) -> None:
        if not isinstance(xndarray, XNDArray):
            raise ValueError("You must provide an XNDArray instance to the XNDArrayPickler")

        # write the item/element size
        self._intpickler.dump(file, xndarray.dtype.itemsize)

        # write the dimension of the array
        self._vecpickler.dump(file, xndarray.shape)

        # write the indices which are empty when sent from Python
        self._vecpickler.dump(file, [])

        # add the serialized ddict.
        self._strpickler.dump(file, xndarray._dd_ser)

        # add the ddict key for the ndarray
        self._strpickler.dump(file, xndarray._ndarray_key)

        if xndarray._timeout is None:
            tv_sec = -1
            tv_nsec = -1
        else:
            tv_sec = int(xndarray._timeout)
            tv_nsec = int((xndarray._timeout - tv_sec) * 1E9)

        # add the seconds and ns for timeout
        self._intpickler.dump(file, tv_sec)
        self._intpickler.dump(file, tv_nsec)

    def load(self, file):
        item_size = self._intpickler.load(file)
        dimensions = self._vecpickler.load(file)
        indices = self._vecpickler.load(file)
        dd_ser = self._strpickler.load(file)
        ndarray_key = self._strpickler.load(file)
        secs = self._intpickler.load(file)
        nsecs = self._intpickler.load(file)

        if secs == -1:
            timeout = None
        else:
            timeout = secs + (nsecs / 1.0E9)

        return XNDArray([], dd_ser, ndarray_key, self._data_type, dimensions, timeout)

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
        return np.array(val, dtype=np.dtype(self._data_type)).tobytes()

    def loads(self, val: bytes) -> object:
        try:
            return np.frombuffer(val[:self._num_bytes], dtype=self._data_type)[0].item()
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
        strlen = len(val.encode())
        return size_t_size + strlen

    def dumps(self, val) -> bytes:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        valbytes = val.encode()
        strlenbytes = len(valbytes).to_bytes(size_t_size, byteorder=sys.byteorder)
        return strlenbytes + valbytes

    def loads(self, val: bytes) -> str:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        strlen = int.from_bytes(val[:size_t_size], sys.byteorder)
        rv = val[size_t_size:size_t_size+strlen].decode('utf-8')
        return rv

    def dump(self, val, file) -> None:
        file.write(self.dumps(val))

    def load(self, file) -> str:
        size_t_size = ctypes.sizeof(ctypes.c_size_t)
        strlen = int.from_bytes(file.read(size_t_size), sys.byteorder)
        val = file.read(strlen)
        rv = val.decode('utf-8')
        return rv

class XQueuePickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableQueue. Pickling a Dragon native Queue with this pickler can be
    read in C++ by deserializing using the SerializableQueue class. Likewise, a
    C++ Queue serialized with SerializableQueue can be unpickled into a Dragon
    native Queue as well. Only the serialized descriptor of the Queue is
    transferred, so the lifetime of the Queue is still managed by whoever
    created it. """

    def __init__(self, pickler=None):
        """
        :param pickler: The pickler given to the Queue that is created when a
        Queue descriptor is unpickled. An XPickler is used when one is not
        provided so the unpickled Queue can continue to talk to other languages.
        """
        self._pickler = pickler
        self._strpickler = XStringPickler()

    @property
    def ty_val(self):
        return TY_QUEUE

    def _attach(self, serialized):
        # Imported here because dragon.native.queue depends on this module.
        from dragon.native.queue import Queue

        pickler = self._pickler
        if pickler is None:
            pickler = XPickler()

        return Queue.attach(serialized, pickler=pickler)

    def byte_size(self, val) -> int:
        return self._strpickler.byte_size(val.serialize())

    def dumps(self, val) -> bytes:
        return self._strpickler.dumps(val.serialize())

    def loads(self, val: bytes):
        return self._attach(self._strpickler.loads(val))

    def dump(self, val, file) -> None:
        self._strpickler.dump(val.serialize(), file)

    def load(self, file):
        return self._attach(self._strpickler.load(file))

class XDDictPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableDDict. Pickling a Distributed Dictionary with this pickler can
    be read in C++ by deserializing using the SerializableDDict class.
    Likewise, a C++ DDict serialized with SerializableDDict can be unpickled
    into a Python DDict as well. Only the serialized descriptor of the DDict is
    transferred, so the lifetime of the DDict is still managed by whoever
    created it. """

    def __init__(self, key_pickler=None, value_pickler=None, timeout:float=None):
        """
        :param key_pickler: The key pickler given to the DDict that is created
        when a DDict descriptor is unpickled. An XPickler is used when one is
        not provided so the unpickled DDict can continue to talk to other
        languages.

        :param value_pickler: The value pickler for the unpickled DDict. An
        XPickler is used when one is not provided.

        :param timeout: The timeout applied to operations on the unpickled DDict.
        """
        self._key_pickler = key_pickler
        self._value_pickler = value_pickler
        self._timeout = timeout
        self._strpickler = XStringPickler()

    @property
    def ty_val(self):
        return TY_DDICT

    def _attach(self, serialized):
        # Imported here because dragon.data.ddict depends on this module.
        from dragon.data.ddict.ddict import DDict

        key_pickler = self._key_pickler
        if key_pickler is None:
            key_pickler = XPickler()

        value_pickler = self._value_pickler
        if value_pickler is None:
            value_pickler = XPickler()

        dd = DDict.attach(serialized, timeout=self._timeout)
        return dd.pickler(key_pickler=key_pickler, value_pickler=value_pickler)

    def byte_size(self, val) -> int:
        return self._strpickler.byte_size(val.serialize())

    def dumps(self, val) -> bytes:
        return self._strpickler.dumps(val.serialize())

    def loads(self, val: bytes):
        return self._attach(self._strpickler.loads(val))

    def dump(self, val, file) -> None:
        self._strpickler.dump(val.serialize(), file)

    def load(self, file):
        return self._attach(self._strpickler.load(file))

class XBarrierPickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableBarrier. Pickling a Dragon native Barrier with this pickler can
    be read in C++ by deserializing using the SerializableBarrier class.
    Likewise, a C++ Barrier serialized with SerializableBarrier can be
    unpickled into a Dragon native Barrier as well. Only the serialized
    descriptor of the Barrier is transferred, so the lifetime of the Barrier is
    still managed by whoever created it. """

    def __init__(self, action=None, timeout:float=None):
        """
        :param action: The action given to the Barrier that is created when a
        Barrier descriptor is unpickled. An action cannot be carried in the
        descriptor so provide it here when the unpickled Barrier needs one.

        :param timeout: The default timeout for waits on the unpickled Barrier.
        """
        self._action = action
        self._timeout = timeout
        self._strpickler = XStringPickler()

    @property
    def ty_val(self):
        return TY_BARRIER

    def _attach(self, serialized):
        # Imported here because dragon.native.barrier depends on this module.
        from dragon.native.barrier import Barrier

        return Barrier.attach(serialized, action=self._action, timeout=self._timeout)

    def byte_size(self, val) -> int:
        return self._strpickler.byte_size(val.serialize())

    def dumps(self, val) -> bytes:
        return self._strpickler.dumps(val.serialize())

    def loads(self, val: bytes):
        return self._attach(self._strpickler.loads(val))

    def dump(self, val, file) -> None:
        self._strpickler.dump(val.serialize(), file)

    def load(self, file):
        return self._attach(self._strpickler.load(file))

class XSemaphorePickler:
    """A Pickler that has X-language support and is compatible with the C++
    SerializableSemaphore. Pickling a Dragon native Semaphore with this pickler
    can be read in C++ by deserializing using the SerializableSemaphore class.
    Likewise, a C++ Semaphore serialized with SerializableSemaphore can be
    unpickled into a Dragon native Semaphore as well. Only the serialized
    descriptor of the Semaphore is transferred, so the lifetime of the
    Semaphore is still managed by whoever created it. """

    def __init__(self):
        self._strpickler = XStringPickler()

    @property
    def ty_val(self):
        return TY_SEMAPHORE

    def _attach(self, serialized):
        # Imported here because dragon.native.semaphore depends on this module.
        from dragon.native.semaphore import Semaphore

        return Semaphore.attach(serialized)

    def byte_size(self, val) -> int:
        return self._strpickler.byte_size(val.serialize())

    def dumps(self, val) -> bytes:
        return self._strpickler.dumps(val.serialize())

    def loads(self, val: bytes):
        return self._attach(self._strpickler.loads(val))

    def dump(self, val, file) -> None:
        self._strpickler.dump(val.serialize(), file)

    def load(self, file):
        return self._attach(self._strpickler.load(file))

class XPickler:
    """A Pickler that has X-language support and is compatible with the C++
    Serializables. Objects to be shared between Python and C++ must be supported
    by XPickler on the Python side and the Serializable class on the C++ side.
    Pickling a supported object with this pickler can be read in
    C++ by deserializing using the Seralizable class. Likewise, a C++
    Serializable object can be unpickled into its corresponding Python object,
    enabling cross communication between Python and other languages (at
    present that means C++) through a pickling/unpickling interface like
    native Queue or DDict. if only_ndarrays is True, then all vectors and
    matrices are treated and handled as ndarrays by the library which
    means their data will be stored in a Distributed Dictionary and only
    meta-data will be passed serialized/deserialized."""

    def __init__(self, only_ndarrays:bool=False):
        self._only_ndarrays = only_ndarrays
        self._str_pickler = XStringPickler()
        if np is None:
            raise ModuleNotFoundError("The numpy package is required when using XPickler.")
        self._int_pickler = XScalarPickler(np.int32, TY_INT)
        self._double_pickler = XScalarPickler(np.float64, TY_DOUBLE)
        self._intvector_pickler = XVectorPickler(np.int32, TY_INTVECTOR)
        self._doublevector_pickler = XVectorPickler(np.float64, TY_DOUBLEVECTOR)
        self._intmatrix_pickler = X2DMatrixPickler(np.int32, TY_INTMATRIX)
        self._doublematrix_pickler = X2DMatrixPickler(np.float64, TY_DOUBLEMATRIX)
        self._bytebuffer_pickler = XByteBufferPickler()
        self._double_ndarray_pickler = XNDArrayPickler(np.float64, TY_DOUBLENDARRAY)
        self._float_ndarray_pickler = XNDArrayPickler(np.float32, TY_FLOATNDARRAY)
        self._int_ndarray_pickler = XNDArrayPickler(np.int32, TY_INTNDARRAY)
        self._long_ndarray_pickler = XNDArrayPickler(np.int64, TY_LONGNDARRAY)
        self._queue_pickler = XQueuePickler()
        self._ddict_pickler = XDDictPickler()
        self._barrier_pickler = XBarrierPickler()
        self._semaphore_pickler = XSemaphorePickler()

        self._ty_pickler = {
            TY_STR: self._str_pickler,
            TY_INT: self._int_pickler,
            TY_DOUBLE: self._double_pickler,
            TY_INTVECTOR: self._intvector_pickler,
            TY_DOUBLEVECTOR: self._doublevector_pickler,
            TY_INTMATRIX: self._intmatrix_pickler,
            TY_DOUBLEMATRIX: self._doublematrix_pickler,
            TY_BYTEBUFFER: self._bytebuffer_pickler,
            TY_DOUBLENDARRAY: self._double_ndarray_pickler,
            TY_FLOATNDARRAY: self._float_ndarray_pickler,
            TY_INTNDARRAY: self._int_ndarray_pickler,
            TY_LONGNDARRAY: self._long_ndarray_pickler,
            TY_QUEUE: self._queue_pickler,
            TY_DDICT: self._ddict_pickler,
            TY_BARRIER: self._barrier_pickler,
            TY_SEMAPHORE: self._semaphore_pickler
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
            if not self._only_ndarrays and not isinstance(val, XNDArray):
                # numpy scalars are not Python ints, so dispatch on dtype/rank instead.
                if val.ndim == 1:
                    if np.issubdtype(val.dtype, np.integer):
                        return self._intvector_pickler

                    if np.issubdtype(val.dtype, np.floating):
                        return self._doublevector_pickler

                elif val.ndim == 2:
                    if np.issubdtype(val.dtype, np.integer):
                        return self._intmatrix_pickler

                    if np.issubdtype(val.dtype, np.floating):
                        return self._doublematrix_pickler

            if val.dtype == np.dtype('double'):
                return self._double_ndarray_pickler

            if val.dtype == np.dtype('float32'):
                return self._float_ndarray_pickler

            if val.dtype == np.dtype('int32'):
                return self._int_ndarray_pickler

            if val.dtype == np.dtype('int64'):
                return self._long_ndarray_pickler

        # Imported here because dragon.native.queue depends on this module.
        from dragon.native.queue import Queue

        if isinstance(val, Queue):
            return self._queue_pickler

        # Imported here because dragon.data.ddict depends on this module.
        from dragon.data.ddict.ddict import DDict

        if isinstance(val, DDict):
            return self._ddict_pickler

        # Imported here because dragon.native.barrier depends on this module.
        from dragon.native.barrier import Barrier

        if isinstance(val, Barrier):
            return self._barrier_pickler

        # Imported here because dragon.native.semaphore depends on this module.
        from dragon.native.semaphore import Semaphore

        if isinstance(val, Semaphore):
            return self._semaphore_pickler

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