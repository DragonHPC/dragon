from dragon.dtypes_inc cimport *
import sys
import tempfile
import shutil

cdef class DragonPMIxJob:

    cdef:
        char *server_tmp_dir
        char *job_tmp_dir
        dragonG_UID_t guid

    def __cinit__(self):

        self.server_tmp_dir = NULL
        self.job_tmp_dir = NULL
        self.guid = 0


    cdef char* _put_bytestring_into_c_char(self, str_in: str):

        cdef char *out
        tmp_in = str_in.encode('utf-8') + b"\x00"
        lsize = len(tmp_in) * sizeof(char)
        out = <char*> malloc(lsize);
        if (out == NULL):
            raise MemoryError("Unable to allocate memory for c char in PMIx server initialization")
        memcpy(out, <char*> tmp_in, lsize)
        return out

    def __init__(self,
                 guid,
                 server_created,
                 ddict_sdesc,
                 local_mgr_sdesc,
                 ls_in,
                 ls_resp,
                 ls_buffered_resp,
                 node_rank,
                 proc_ranks,
                 ppns,
                 node_ranks,
                 hosts):

        """Initializes the server and sets up communciation between it and caller"""

        cdef:
            dragonChannelSerial_t c_ls_in
            dragonChannelSerial_t c_ls_resp
            dragonChannelSerial_t c_ls_buffered_resp
            dragonG_UID_t c_guid
            char *c_ddict_sdesc
            char *c_local_mgr_sdesc
            int c_node_rank, c_nhosts, c_nproc
            int *c_ppn
            int *c_node_ranks
            int *c_proc_ranks

        # Get a tmp directory for all the PMIx space. Save it to our space for
        # removal at finalization
        self.job_tmp_dir = self._put_bytestring_into_c_char(tempfile.mkdtemp())
        if not server_created:
            self.server_tmp_dir = self._put_bytestring_into_c_char(tempfile.mkdtemp())

        self.guid = guid
        # Construct a c list of hostnames in our PMIx group
        nhosts = len(hosts)
        cdef char **hostnames = <char**> malloc(nhosts * sizeof(char*))
        if (hostnames == NULL):
            raise MemoryError("Unable to allocate memory for hostnames in PMIx server initialization")

        for idx, host in enumerate(hosts):
            hostnames[idx] = self._put_bytestring_into_c_char(host)

        # Store the ddict descriptor and its local manager into C strings.
        c_ddict_sdesc = self._put_bytestring_into_c_char(ddict_sdesc)

        if local_mgr_sdesc is not None:
            c_local_mgr_sdesc = self._put_bytestring_into_c_char(local_mgr_sdesc)
        else:
            c_local_mgr_sdesc = NULL;

        # Put channel bytestrings into serialized descriptor struct
        c_ls_in.len = len(ls_in)
        cdef const unsigned char[:] ols_data = ls_in
        c_ls_in.data = <uint8_t *>&ols_data[0]

        c_ls_resp.len = len(ls_resp)
        cdef const unsigned char[:] ils_data = ls_resp
        c_ls_resp.data = <uint8_t *>&ils_data[0]

        c_ls_buffered_resp.len = len(ls_buffered_resp)
        cdef const unsigned char[:] ils_buffered_data = ls_buffered_resp
        c_ls_buffered_resp.data = <uint8_t *>&ils_buffered_data[0]

        c_node_rank = <int> node_rank
        c_nhosts = <int> nhosts
        c_nproc = <int> len(proc_ranks)
        c_guid = <dragonG_UID_t> guid

        # Construct the pointers containing ppns and node ranks into an array for the PMIx code
        c_ppn = <int*> malloc(nhosts * sizeof(int))
        if (c_ppn == NULL):
            raise MemoryError("Unable to allocate memory for ppn in PMIx server initialization")

        c_node_ranks = <int*> malloc(nhosts * sizeof(int))
        if (c_node_ranks == NULL):
            raise MemoryError("Unable to allocate memory for node ranks in PMIx server initialization")

        c_proc_ranks = <int*> malloc(c_nproc * sizeof(int))
        if (c_proc_ranks == NULL):
            raise MemoryError("Unable to allocate memory for proc ranks in PMIx server initialization")


        for idx, rank in enumerate(node_ranks):
            c_node_ranks[idx] = <int> rank;
            c_ppn[idx] = <int> ppns[idx]

        for idx, nid in enumerate(proc_ranks):
            c_proc_ranks[idx] = <int> nid

        derr = dragon_pmix_initialize_job(c_guid,
                                          c_ddict_sdesc,
                                          c_local_mgr_sdesc,
                                          c_ls_in,
                                          c_ls_resp,
                                          c_ls_buffered_resp,
                                          c_node_rank,
                                          c_nhosts,
                                          c_nproc,
                                          c_proc_ranks,
                                          c_ppn,
                                          c_node_ranks,
                                          hostnames,
                                          self.job_tmp_dir,
                                          self.server_tmp_dir)

        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Unable to initialize PMIx server for launch of MPI application")

        for i in range(nhosts):
            free(hostnames[i])
        free(hostnames)
        free(c_ppn)
        free(c_node_ranks)
        free(c_proc_ranks)
        free(c_ddict_sdesc)
        free(c_local_mgr_sdesc)

    def get_client_env(self,
                       the_env,
                       rank):
        """Set up environment variables for the client/worker process"""

        cdef dragonError_t derr
        cdef char **pmix_env
        cdef int nenv

        # Unpack "the_env" into char **env and then figure out how to return it as a dict. It'll be a headache.
        nitems = len(the_env) + 1
        pmix_env = <char**> malloc(nitems * sizeof(char*))
        if (pmix_env == NULL):
            raise MemoryError("Unable to allocate memory for PMIx client environment variables")

        for idx, (name, val) in enumerate(the_env.items()):
            full_env_var = name + "=" + val
            pmix_env[idx] = self._put_bytestring_into_c_char(full_env_var)

        # NULL terminate the list because OpenPMIX expects that for its appending
        pmix_env[nitems - 1] = NULL

        derr = dragon_pmix_get_client_env(self.guid, rank, &pmix_env, &nenv)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Unable to configure MPI client applicatiion to use PMIx")

        # Unpack env to return to Python as a dict
        p_env = {}
        for i in range(nenv):
            # Get the string
            py_env = pmix_env[i].decode('utf-8')
            k, v = py_env.split('=', maxsplit=1)
            p_env[k] = v
            free(pmix_env[i])
        free(pmix_env)

        return p_env

    def finalize_job(self):
        """Cleanup resources associated with this particular PMIx job"""

        cdef dragonError_t derr

        derr = dragon_pmix_finalize_job(self.guid)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError(f"Unable to finalize PMIx job for guid {self.guid}")

        try:
            if self.job_tmp_dir != NULL:
                shutil.rmtree(self.job_tmp_dir.decode('utf-8'))
                free(self.job_tmp_dir)
        except Exception as e:
            raise RuntimeError("Unable to clean up tmp directory for PMIx Server") from e


    def finalize_server(self):
        """Shutdown the PMIx server stood up on this node"""
        cdef dragonError_t derr

        derr = dragon_pmix_finalize_server()
        if derr != DRAGON_SUCCESS:
            raise RuntimeError(f"Unable to finalize PMIx server")

        try:
            if self.server_tmp_dir != NULL:
                shutil.rmtree(self.server_tmp_dir.decode('utf-8'))
                free(self.server_tmp_dir)
        except Exception as e:
            raise RuntimeError("Unable to clean up tmp directory for PMIx Server") from e

