from dragon.dtypes_inc cimport *
import sys
import tempfile
import shutil

cdef class DragonPMIxServer:

    cdef:
        dragonPMIxServer_t *d_server
        char *tmp_dir
        char *client_nspace
        char *server_nspace

    def __cinit__(self):

        self.d_server = NULL
        self.tmp_dir = NULL
        self.client_nspace = NULL
        self.server_nspace = NULL

    def __del__(self):

        try:
            shutil.rmtree(self.tmp_dir.decode('utf-8'))
        except Exception as e:
            raise RuntimeError("Unable to clean up tmp directory for PMIx Server") from e

        if self.tmp_dir != NULL:
            free(self.tmp_dir)

        if self.client_nspace != NULL:
            free(self.client_nspace)

        if self.server_nspace != NULL:
            free(self.server_nspace)


    cdef char* _put_bytestring_into_c_char(self, str_in: str):

        cdef char *out
        tmp_in = str_in.encode('utf-8') + b"\x00"
        lsize = len(tmp_in) * sizeof(char)
        out = <char*> malloc(lsize);
        memcpy(out, <char*> tmp_in, lsize)

        return out

    def __init__(self,
                 ddict_sdesc,
                 local_mgr_sdesc,
                 ls_in,
                 ls_resp,
                 ls_buffered_resp,
                 nproc,
                 node_rank,
                 ppn,
                 hosts,
                 server_nspace="server-nspace",
                 client_nspace="client-nspace"):

        """Initializes the server and sets up communciation between it and caller"""

        cdef:
            dragonChannelSerial_t c_ls_in
            dragonChannelSerial_t c_ls_resp
            dragonChannelSerial_t c_ls_buffered_resp
            char *c_ddict_sdesc, *c_local_mgr_sdesc
            int c_nrank, c_nhosts, c_nproc, c_ppn

        # Get a tmp directory for all the PMIx space. Save it to our space for
        # removal at finalization
        self.tmp_dir = self._put_bytestring_into_c_char(tempfile.mkdtemp())

        # Convert all python strings to null terminated byte chars
        self.server_nspace = self._put_bytestring_into_c_char(server_nspace)
        self.client_nspace = self._put_bytestring_into_c_char(client_nspace)

        # Construct a c list of hostnames in our PMIx group
        nhosts = len(hosts)
        cdef char **hostnames = <char**> malloc(nhosts * sizeof(char*))
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

        c_nrank = <int> node_rank
        c_nhosts = <int> nhosts
        c_nproc = <int> nproc
        c_ppn = <int> ppn

        with nogil:
            derr = dragon_initialize_pmix_server(&self.d_server,
                                                 c_ddict_sdesc,
                                                 c_local_mgr_sdesc,
                                                 c_ls_in,
                                                 c_ls_resp,
                                                 c_ls_buffered_resp,
                                                 c_nrank,
                                                 c_nhosts, c_nproc, c_ppn, hostnames,
                                                 self.server_nspace, self.client_nspace, self.tmp_dir)

        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Unable to initialize PMIx server for launch of MPI application")

        for i in range(nhosts):
            free(hostnames[i])
        free(hostnames)

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
        for idx, (name, val) in enumerate(the_env.items()):
            full_env_var = name + "=" + val
            pmix_env[idx] = self._put_bytestring_into_c_char(full_env_var)

        # NULL terminate the list because OpenPMIX expects that for its appending
        pmix_env[nitems - 1] = NULL

        derr = dragon_pmix_get_client_env(self.d_server, rank, &pmix_env, &nenv)
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

    def finalize(self):
        """Shutdown the PMIx server"""

        cdef dragonError_t derr

        derr = dragon_pmix_finalize_server(self.d_server)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Unable to finalize and shutdown PMIx server")
