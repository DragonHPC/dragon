from dragon.dtypes_inc cimport *
from dragon.channels cimport *
from libc.stdlib cimport malloc, free
from libc.string cimport strncpy

################################
# Begin Cython definitions
################################


class PMODError(Exception):
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
            return f'PMOD Error: {self._msg}'

        return f'PMOD Error: {self._msg} | Dragon Msg: {self._lib_msg} | Dragon Error Code: {self._lib_err_str}'

    @property
    def lib_err(self):
        return self._lib_err

    def __repr__(self):
        return f'{self.__class__}({self._msg!r}, {self._lib_err!r}, {self._lib_msg!r}, {self._lib_err_str!r})'


cdef class PMOD:
    """
    Cython wrapper for PMOD functionality
    """

    cdef dragonSendJobParams_t _job_params

    def __init__(self, int ppn, int nid, int nnodes, int nranks, nidlist, hostname_list, uint64_t job_id):
        # set scalar params

        self._job_params.ppn    = ppn
        self._job_params.nid    = nid
        self._job_params.nnodes = nnodes
        self._job_params.nranks = nranks
        self._job_params.id     = job_id

        # allocate and set nidlist

        self._job_params.nidlist = <int *> malloc(nranks * sizeof(int))

        if self._job_params.nidlist is NULL:
            raise MemoryError()

        cdef int i = 0

        for nid in nidlist:
            self._job_params.nidlist[i] = nidlist[i]
            i = i + 1

        # allocate and set hostnames array

        self._job_params.hostnames = <dragonHostname_t *> malloc(nnodes * sizeof(dragonHostname_t))

        if self._job_params.hostnames is NULL:
            raise MemoryError()

        cdef char *hostname_c

        i = 0

        for hostname in hostname_list:
            hostname_bytes = hostname.encode('utf-8')
            hostname_c = hostname_bytes
            strncpy(&self._job_params.hostnames[i].name[0], hostname_c, PMOD_MAX_HOSTNAME_LEN-1)
            i = i + 1


    @property
    def ppn(self):
        return self._job_params.ppn


    @property
    def nid(self):
        return self._job_params.nid


    @property
    def nnodes(self):
        return self._job_params.nnodes


    @property
    def nranks(self):
        return self._job_params.nranks


    @property
    def job_id(self):
        return self._job_params.id


    def __del__(self):

        free(self._job_params.nidlist)
        free(self._job_params.hostnames)


    @staticmethod
    def get_num_nics():
        cdef:
            int nnics
            dragonError_t derr

        derr = dragon_pmod_pals_get_num_nics(&nnics)
        if derr != DRAGON_SUCCESS:
            raise PMODError("Could not get number of nics", derr)

        return nnics


    def send_mpi_data(self, int lrank, Channel child_ch):
        cdef:
            dragonError_t derr

        self._job_params.lrank = lrank

        derr = dragon_pmod_send_mpi_data(&self._job_params, &child_ch._channel)
        if derr != DRAGON_SUCCESS:
            raise PMODError("Could not send MPI data to child process", derr)
