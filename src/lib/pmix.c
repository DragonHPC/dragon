#define _GNU_SOURCE
#ifdef HAVE_PMIX_INCLUDE

#include <dirent.h>
#include <errno.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pty.h>
#include <unistd.h>
#include <pthread.h>
#include <dlfcn.h>

#include <src/include/pmix_globals.h>
#include "umap.h"
#include "ulist.h"
#include "_ddict.h"
#include "_pmix.h"

// Several static values to manage internal state
static pmix_list_t pubdata;  // Data provided in lookup functions by the server

#define SERVER_NSPACE_PREFIX "server-nspace-"
#define CLIENT_NSPACE_PREFIX "client-nspace-"
#define DRAGON_PMIX_UMAP_SEED 2212

/*****************************************************************/
/*                                                               */
/*  These typedefs appear here because we want to make           */
/*  sure no pmix structs appear in C++ codes since pmix.h        */
/*  is very incompatible with g++. Thus, they don't appear       */
/*  in header files that may appear in C++ files.                */
/*                                                               */
/*****************************************************************/
static uint64_t pmix_syms_loaded = 0UL;
void *lib_pmix_handle = NULL;
pmix_status_t (*PMIx_server_init_p)(pmix_server_module_t *module, pmix_info_t info[], size_t ninfo);
pmix_status_t (*PMIx_server_finalize_p)(void);
pmix_status_t (*PMIx_server_setup_fork_p)(const pmix_proc_t *proc, char ***env);
pmix_status_t (*PMIx_Register_event_handler_p)(pmix_status_t codes[], size_t ncodes, pmix_info_t info[], size_t ninfo,
        pmix_notification_fn_t evhdlr, pmix_hdlr_reg_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_server_setup_application_p)(const pmix_nspace_t nspace, pmix_info_t info[], size_t ninfo,
        pmix_setup_application_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_Info_xfer_p)(pmix_info_t *dest, pmix_info_t *src);
pmix_status_t (*PMIx_Notify_event_p)(pmix_status_t status, const pmix_proc_t *source, pmix_data_range_t range,
                                     pmix_info_t info[], size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_Info_load_p)(pmix_info_t *info, const char* key, const void *data, pmix_data_type_t type);
pmix_status_t (*PMIx_generate_regex_p)(const char *input, char **output);
pmix_status_t (*PMIx_server_register_client_p)(const pmix_proc_t *proc, uid_t uid, gid_t gid, void *server_object,
        pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_generate_ppn_p)(const char *input, char **ppn);
pmix_status_t (*PMIx_Deregister_event_handler_p)(size_t evhdlr_ref, pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_Data_pack_p)(const pmix_proc_t *target, pmix_data_buffer_t *buffer, void *src, int32_t num_vals,
                                  pmix_data_type_t type);
pmix_status_t (*PMIx_server_setup_local_support_p)(const pmix_nspace_t nspace, pmix_info_t info[], size_t ninfo,
        pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_server_register_nspace_p)(const pmix_nspace_t nspace, int nlocalprocs, pmix_info_t info[], size_t ninfo,
        pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t (*PMIx_Value_xfer_p)(pmix_value_t *dest, const pmix_value_t *src);
pmix_status_t (*PMIx_Data_unpack_p)(const pmix_proc_t *source, pmix_data_buffer_t *buffer, void *dest,
                                    int32_t *max_num_values, pmix_data_type_t type);

// OpenPMIx non-standard function pointers (defined as macros in standard)
void (*PMIx_Load_nspace_p)(pmix_nspace_t nspace, const char *str);
void (*PMIx_Data_buffer_unload_p)(pmix_data_buffer_t *b, char **bytes, size_t *sz);
void (*PMIx_Data_buffer_construct_p)(pmix_data_buffer_t *b);
void (*PMIx_Data_buffer_load_p)(pmix_data_buffer_t *b, char *bytes, size_t sz);
void (*PMIx_Load_key_p)(pmix_key_t key, const char *src);
void (*PMIx_Pdata_free_p)(pmix_pdata_t *p, size_t n);
void (*PMIx_Info_free_p)(pmix_info_t *p, size_t n);
pmix_info_t* (*PMIx_Info_create_p)(size_t n);
pmix_pdata_t* (*PMIx_Pdata_create_p)(size_t n);


typedef struct dragonPMIxCBData_st {
    char *nspace;
    pmix_info_t *info;
    pmix_status_t status;
    pmix_op_cbfunc_t cbfunc;
    pmix_proc_t caller;
    pmix_spawn_cbfunc_t spcbfunc;
    void *cbdata;
    size_t ninfo;
    volatile bool flag;
    volatile bool active;
} dragonPMIxCBData_t;


typedef struct {
    pmix_list_item_t super;
    pmix_pdata_t pdata;
} pmix_locdat_t;

typedef struct dragonPMIxNode_st {
    char *hostname;  // hostname of this node
    int nid;          // node rank of this node as provides via global services
    int *ranks;      // ranks executed on this node as part of this job nspace
    int ppn;         // number of processes running this node as part of this job nspace
} dragonPMIxNode_t;

typedef struct dragonPMIxProcess_st {
    int nid;           // node rank of this process as provided via global services
    int rank;          // rank of this process in the job nspace
    int lrank;         // local rank of process on its node
    int nodes_index;   // Reference to index of this proc's nodes in the dragonPMIxNode_t structure
} dragonPMIxProcess_t;

typedef struct dragonPMIxJob_st {
    bool job_captain, holding_ddict_attach;
    int node_rank;  // node rank this server is executing on. Equivalent to nid in places
    int ppn;
    int nprocs;
    int nnodes;
    dragonG_UID_t guid;
    char *nsdir;
    char *client_nspace;
    char *ddict_sdesc;
    dragonPMIxNode_t *nodes;
    dragonPMIxProcess_t *procs;
    dragonDDictDescr_t *ddict;
} dragonPMIxJob_t;

typedef struct dragonPMIxServer_st {
    int node_rank;  // node rank this server is executing on. Equivalent to nid in places
    uint32_t sess_id;
    uid_t uid;
    gid_t gid;
    uint64_t initialized;
    char *tmpdir, *hostname;
    char *server_nspace;
} dragonPMIxServer_t;



static dragonPMIxServer_t *dpmix_server = NULL;
static dragonMap_t *job_map = NULL;
static dragonList_t *guid_map_keys = NULL;

static dragonError_t
_dragon_get_job_from_nspace(char *nspace, dragonPMIxJob_t **d_job);

void
setup_cbfunc(pmix_status_t status, pmix_info_t info[], size_t ninfo,
             void *provided_cbdata, pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
connected(const pmix_proc_t *proc, void *server_object,
          pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
finalized(const pmix_proc_t *proc, void *server_object,
          pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
abort_fn(const pmix_proc_t *proc, void *server_object, int status,
         const char msg[], pmix_proc_t procs[], size_t nprocs,
         pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
fence_handler(const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
              size_t ninfo, char *data, size_t ndata, pmix_modex_cbfunc_t cbfunc,
              void *cbdata);

pmix_status_t
dmodex_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
          pmix_modex_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
publish_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
           pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
lookup_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
          size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
unpublish_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
             size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
spawn_fn(const pmix_proc_t *proc, const pmix_info_t job_info[], size_t ninfo,
         const pmix_app_t apps[], size_t napps, pmix_spawn_cbfunc_t cbfunc,
         void *cbdata);

pmix_status_t
connect_fn(const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
           size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_status_t
disconnect_fn(const pmix_proc_t procs[], size_t nprocs,
              const pmix_info_t info[], size_t ninfo, pmix_op_cbfunc_t cbfunc,
              void *cbdata);

pmix_status_t
job_control_handler(const pmix_proc_t *proc, const pmix_proc_t targets[],
                    size_t ntargets, const pmix_info_t directives[],
                    size_t ndirs, pmix_info_cbfunc_t cbfunc, void *cbdata);

void
errhandler(size_t evhdlr_registration_id, pmix_status_t status,
           const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
           pmix_info_t results[], size_t nresults,
           pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);

void
errhandler_reg_callbk(pmix_status_t status, size_t errhandler_ref, void *cbdata);

// Internal functions for using c++ code from PMIx C code
dragonError_t
_dragon_pmix_write_to_ddict(dragonDDictDescr_t * ddict, bool persist, char key[], char* val, size_t byte_len);

dragonError_t
_dragon_pmix_read_from_ddict(dragonDDictDescr_t * ddict, char key[], char **val, size_t *recv_sz);

dragonError_t
_dragon_pmix_put_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t ndata, char *data);

dragonError_t
_dragon_pmix_get_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t *ndata, char **data);

static pmix_server_module_t dragon_pmix_cback_module = {
    .client_connected = connected,
    .client_finalized = finalized,
    .abort = abort_fn,
    .fence_nb = fence_handler,
    .direct_modex = dmodex_fn,
    .publish = publish_fn,
    .lookup = lookup_fn,
    .unpublish = unpublish_fn,
    .spawn = spawn_fn,
    .connect = connect_fn,
    .disconnect = disconnect_fn,
    .job_control = job_control_handler
};


///////////////////////////////////////////////////////////////////////////////
// Regular okay code follows
///////////////////////////////////////////////////////////////////////////////

dragonError_t
_dragon_pmix_set_fn_ptrs()
{

    // See if we've already locally loaded everything
    if (pmix_syms_loaded == 1UL)
        no_err_return(DRAGON_SUCCESS);


    // Otherwise, get the pointers loaded
    lib_pmix_handle = dlopen("libpmix.so", RTLD_LAZY | RTLD_GLOBAL);
    if (lib_pmix_handle == NULL) {
        append_err_return(DRAGON_FAILURE, "Unable to open libpmix.so for PMIx code execution");
    }

    PMIx_server_init_p = dlsym(lib_pmix_handle, "PMIx_server_init");
    PMIx_server_finalize_p = dlsym(lib_pmix_handle, "PMIx_server_finalize");
    PMIx_server_setup_fork_p = dlsym(lib_pmix_handle, "PMIx_server_setup_fork");
    PMIx_Load_nspace_p = dlsym(lib_pmix_handle, "PMIx_Load_nspace");
    PMIx_Data_buffer_unload_p = dlsym(lib_pmix_handle, "PMIx_Data_buffer_unload");
    PMIx_Register_event_handler_p = dlsym(lib_pmix_handle, "PMIx_Register_event_handler");
    PMIx_Data_buffer_construct_p = dlsym(lib_pmix_handle, "PMIx_Data_buffer_construct");
    PMIx_Data_buffer_load_p = dlsym(lib_pmix_handle, "PMIx_Data_buffer_load");
    PMIx_Info_create_p = dlsym(lib_pmix_handle, "PMIx_Info_create");
    PMIx_server_setup_application_p = dlsym(lib_pmix_handle, "PMIx_server_setup_application");
    PMIx_Info_xfer_p = dlsym(lib_pmix_handle, "PMIx_Info_xfer");
    PMIx_Notify_event_p = dlsym(lib_pmix_handle, "PMIx_Notify_event");
    PMIx_Info_load_p = dlsym(lib_pmix_handle, "PMIx_Info_load");
    PMIx_generate_regex_p = dlsym(lib_pmix_handle, "PMIx_generate_regex");
    PMIx_server_register_client_p = dlsym(lib_pmix_handle, "PMIx_server_register_client");
    PMIx_generate_ppn_p = dlsym(lib_pmix_handle, "PMIx_generate_ppn");
    PMIx_Deregister_event_handler_p = dlsym(lib_pmix_handle, "PMIx_Deregister_event_handler");
    PMIx_Data_pack_p = dlsym(lib_pmix_handle, "PMIx_Data_pack");
    PMIx_Load_key_p = dlsym(lib_pmix_handle, "PMIx_Load_key");
    PMIx_Pdata_free_p = dlsym(lib_pmix_handle, "PMIx_Pdata_free");
    PMIx_Pdata_create_p = dlsym(lib_pmix_handle, "PMIx_Pdata_create");
    PMIx_server_setup_local_support_p = dlsym(lib_pmix_handle, "PMIx_server_setup_local_support");
    PMIx_server_register_nspace_p = dlsym(lib_pmix_handle, "PMIx_server_register_nspace");
    PMIx_Value_xfer_p = dlsym(lib_pmix_handle, "PMIx_Value_xfer");
    PMIx_Data_unpack_p = dlsym(lib_pmix_handle, "PMIx_Data_unpack");
    PMIx_Info_free_p = dlsym(lib_pmix_handle, "PMIx_Info_free");

    pmix_syms_loaded = 1UL;

    no_err_return(DRAGON_SUCCESS);
}

dragonPMIxCBData_t*
_dragon_pmix_cbdata_constructor()
{
    dragonPMIxCBData_t *p = malloc(sizeof(dragonPMIxCBData_t));
    if (p == NULL)
        return p;

    p->info = NULL;
    p->ninfo = 0;
    p->active = true;
    p->cbfunc = NULL;
    p->spcbfunc = NULL;
    p->cbdata = NULL;
    p->nspace = NULL;
    return p;

}


dragonError_t
_dragon_pmix_cbdata_deconstructor(dragonPMIxCBData_t *p)
{

    if (NULL != p->info) {
        PMIx_Info_free_p(p->info, p->ninfo);
    }
    if (p->nspace != NULL) {
        free(p->nspace);
    }
    no_err_return(DRAGON_SUCCESS);
}

void
_mkdir(const char *dir)
{
    char tmp[256];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", dir);
    len = strlen(tmp);
    if (tmp[len - 1] == '/')
        tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    mkdir(tmp, S_IRWXU);
}


void
abcbfunc(pmix_status_t status, void *cbdata)
{

    dragonPMIxCBData_t *state = (dragonPMIxCBData_t *) cbdata;

    /* be sure to release the caller */
    if (NULL != state->cbfunc) {
        state->cbfunc(status, state->cbdata);
    }
    PMIX_RELEASE(state);
}

void
opcbfunc(pmix_status_t status, void *cbdata)
{

    dragonPMIxCBData_t *d_x = (dragonPMIxCBData_t *) cbdata;
    (void) status;

    /* release the caller, if necessary */
    if (d_x->cbfunc != NULL) {
        d_x->cbfunc(PMIX_SUCCESS, d_x->cbdata);
    }
    d_x->active = false;
}


pmix_status_t
job_control_handler(const pmix_proc_t *proc, const pmix_proc_t targets[], size_t ntargets,
                    const pmix_info_t directives[], size_t ndirs, pmix_info_cbfunc_t cbfunc,
                    void *cbdata)
{

    (void)proc;
    (void)targets;
    (void)ntargets;
    (void)directives;
    (void)ndirs;
    (void)cbfunc;
    (void)cbdata;

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t
connect_fn(const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
           size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata)
{

    (void)procs;
    (void)nprocs;
    (void)info;
    (void)ninfo;
    (void)cbfunc;
    (void)cbdata;

    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }

    return PMIX_SUCCESS;
}

pmix_status_t
disconnect_fn(const pmix_proc_t procs[], size_t nprocs,
              const pmix_info_t info[], size_t ninfo, pmix_op_cbfunc_t cbfunc,
              void *cbdata)
{

    (void)procs;
    (void)nprocs;
    (void)info;
    (void)ninfo;
    (void)cbfunc;
    (void)cbdata;

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t
spawn_fn(const pmix_proc_t *proc, const pmix_info_t job_info[], size_t ninfo,
         const pmix_app_t apps[], size_t napps, pmix_spawn_cbfunc_t cbfunc,
         void *cbdata)
{
    (void) proc;
    (void) job_info;
    (void) ninfo;
    (void) apps;
    (void) napps;
    (void) cbfunc;
    (void) cbdata;

    return PMIX_ERR_NOT_SUPPORTED;
}


pmix_status_t
publish_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
           pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_locdat_t *p;
    size_t n;

    for (n = 0; n < ninfo; n++) {
        p = malloc(sizeof(pmix_locdat_t));
        if (p == NULL)
            return PMIX_ERROR;

        p->pdata.proc.rank = proc->rank;
        PMIx_Load_key_p(p->pdata.key, info[n].key);
        PMIx_Value_xfer_p(&p->pdata.value, (pmix_value_t *) &info[n].value);
        pmix_list_append(&pubdata, &p->super);
    }
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}


pmix_status_t
lookup_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
          size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    (void) info;
    (void) ninfo;

    pmix_locdat_t *p, *p2;
    pmix_list_t *results;
    size_t i, n;
    pmix_pdata_t *pd = NULL;
    pmix_status_t ret = PMIX_ERR_NOT_FOUND;

    results = malloc(sizeof(pmix_list_t));
    if (results == NULL)
        return PMIX_ERROR;

    for (n = 0; NULL != keys[n]; n++) {
        PMIX_LIST_FOREACH (p, &pubdata, pmix_locdat_t) {
            if (0 == strncmp(keys[n], p->pdata.key, PMIX_MAX_KEYLEN)) {
                p2 = malloc(sizeof(pmix_locdat_t));
                if (p2 == NULL)
                    return PMIX_ERROR;

                PMIx_Load_nspace_p(p2->pdata.proc.nspace, p->pdata.proc.nspace);

                p2->pdata.proc.rank = p->pdata.proc.rank;
                PMIx_Load_key_p(p2->pdata.key, p->pdata.key);
                PMIx_Value_xfer_p(&p2->pdata.value, &p->pdata.value);
                pmix_list_append(results, &p2->super);
                break;
            }
        }
    }
    if (0 < (n = pmix_list_get_size(results))) {
        ret = PMIX_SUCCESS;
        pd = PMIx_Pdata_create_p(n);
        for (i = 0; i < n; i++) {
            p = (pmix_locdat_t *) pmix_list_remove_first(results);
            if (p) {
                PMIx_Load_nspace_p(pd[i].proc.nspace, p->pdata.proc.nspace);
                pd[i].proc.rank = p->pdata.proc.rank;
                PMIx_Load_key_p(pd[i].key, p->pdata.key);
                PMIx_Value_xfer_p(&pd[i].value, &p->pdata.value);
            }
        }
    }
    //PMIX_LIST_DESTRUCT(&results);
    if (NULL != cbfunc) {
        cbfunc(ret, pd, n, cbdata);
    }
    if (0 < n) {
        PMIx_Pdata_free_p(pd, n);
    }
    return PMIX_SUCCESS;
}

pmix_status_t
unpublish_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
             size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_locdat_t *p, *p2;
    size_t n;
    (void) proc;
    (void) info;
    (void) ninfo;

    for (n = 0; NULL != keys[n]; n++) {
        PMIX_LIST_FOREACH_SAFE (p, p2, &pubdata, pmix_locdat_t) {
            if (0 == strncmp(keys[n], p->pdata.key, PMIX_MAX_KEYLEN)) {
                pmix_list_remove_item(&pubdata, &p->super);
                PMIX_RELEASE(p);
                break;
            }
        }
    }
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}


// TODO: Implement dmodex via C implementation of queuue
int
_send_dmodex_request(const pmix_info_t info[], size_t ninfo,
                     pmix_modex_cbfunc_t cbfunc, void *cbdata,
                     const pmix_proc_t *proc)
{

    return 0;
}

// TODO: Implement dmodex via C implementation of queue
pmix_status_t
dmodex_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
          pmix_modex_cbfunc_t cbfunc, void *cbdata)
{

    return PMIX_ERR_NOT_SUPPORTED;
}


void
setup_cbfunc(pmix_status_t status, pmix_info_t info[], size_t ninfo,
             void *provided_cbdata, pmix_op_cbfunc_t cbfunc, void *cbdata)
{

    dragonPMIxCBData_t *state = (dragonPMIxCBData_t *) provided_cbdata;

    dragonPMIxJob_t *d_job;
    dragonError_t derr = _dragon_get_job_from_nspace(state->nspace, &d_job);
    if (derr != DRAGON_SUCCESS) {
        cbfunc(PMIX_ERROR, cbdata);
    }
    size_t i;

    if (PMIX_SUCCESS == status && 0 < ninfo) {
        state->ninfo = ninfo;
        state->info = PMIx_Info_create_p(ninfo);
        state->info[ninfo - 1].flags = PMIX_INFO_ARRAY_END;
        for (i = 0; i < ninfo; i++) {
            PMIx_Info_xfer_p(&state->info[i], &info[i]);
        }
    }
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    state->status = status;
    state->active = false;
}


void
_dragon_pmix_free_fence_request(void *arg)
{

    dragonPMIxRequest_t *req = (dragonPMIxRequest_t*) arg;
    if (req->node_list != NULL)
        free(req->node_list);

    if (req->fence_data != NULL)
        free(req->fence_data);

    free(req);

}

static void *
_ddict_allgather(void *arg)
{

    dragonPMIxRequest_t *req = (dragonPMIxRequest_t *) arg;
    dragonDDictDescr_t *ddict = req->ddict;
    dragonError_t derr;

    // My key is the op name and my node id:
    char *allgather_key;
    asprintf(&allgather_key, "fence_ag_%d", req->src);

    // Put my data into the dictionary:
    derr = _dragon_pmix_put_fence_msg(ddict, allgather_key, req->fence_ndata, req->fence_data);
    free(allgather_key);
    if (derr != DRAGON_SUCCESS) {
        goto leave_thread;
    }

    // Now loop through getting data from all of my friends
    for (int i = 0; i < req->nnodes; i++) {
        // Do the ddict get.
        // TODO: randomize the key selection. I don't want everyone getting the exact same key at the same time leading to
        //       a poorly created DDoS.

        char *new_data = NULL;
        size_t new_ndata;
        asprintf(&allgather_key, "fence_ag_%d", req->node_list[i]);
        _dragon_pmix_get_fence_msg(ddict, allgather_key, &new_ndata, &new_data);
        free(allgather_key);

        // Pack it into my concat'd data char
        size_t old_ndata = req->fence_ndata;
        if (new_ndata > 0) {
            req->fence_ndata = req->fence_ndata + new_ndata;
            req->fence_data = realloc(req->fence_data, req->fence_ndata * sizeof(char));
            memcpy(&req->fence_data[old_ndata], new_data, new_ndata * sizeof(char));
        }
        if (new_data != NULL) {
            free(new_data);
        }
    }

    // Checkpoint the dictionary
    dragon_ddict_checkpoint(ddict);

    pmix_modex_cbfunc_t cbfunc = (pmix_modex_cbfunc_t) req->cbfunc;
    cbfunc(PMIX_SUCCESS, req->fence_data, req->fence_ndata, req->cbdata, _dragon_pmix_free_fence_request, req);


leave_thread:
    return NULL;
}

dragonPMIxRequest_t *
_dragon_pmix_init_fence_request(dragonPMIxJob_t *d_job,
                                const pmix_proc_t procs[],
                                size_t nprocs,
                                char *data,
                                size_t ndata,
                                pmix_modex_cbfunc_t cbfunc,
                                void *cbdata)
{

    // Initialize the request data structure
    dragonPMIxRequest_t *req = malloc(sizeof(dragonPMIxRequest_t));
    if (req == NULL)
        return NULL;


    req->ddict = d_job->ddict;
    req->id = NULL;
    req->node_list = NULL;
    req->fence_data = NULL;
    req->msg_type = REQ_COLLECTIVE;
    req->cbfunc = (void*) cbfunc;
    req->cbdata = (void*) cbdata;

    req->nnodes = 0;
    req->checked_in = 0;
    req->fence_ndata = 0;
    req->ninfo = 0;

    // Populate the target list(s) so we know who to ping and what to send and vice-versa
    if (nprocs != 0) {

        // Figure out if it's a wild card request or not
        int is_wildcard = 0;
        for (int i = 0; i < nprocs; i++) {
            if (PMIX_RANK_WILDCARD == procs[i].rank) {
                is_wildcard = 1;
            }
        }

        // If a wildcard, we need a response from everyone -- Figure out a way to do this
        // without needing to reference the server
        if (is_wildcard) {
            req->nnodes = d_job->nnodes - 1; // Exclude myself
            req->node_list = malloc((req->nnodes) * sizeof(int));
            if (req->node_list == NULL) {
                free(req);
                return NULL;
            }
            int node_counter = 0;
            for (int i = 0; i < d_job->nnodes; i++) {
                if ((int) d_job->nodes[i].nid != (int) d_job->node_rank) {
                    req->node_list[node_counter] = d_job->nodes[i].nid;
                    node_counter++;
                }
            }
        }
        // Otherwise, handle it on a process by process basis -- if we're given proc ranks, we may need to map this
        // to nodes hosting the given rank. Leaving it this way for the moment.
        else {
            req->nnodes = (int) nprocs;
            req->node_list = malloc(req->nnodes * sizeof(int));
            if (req->node_list == NULL) {
                free(req);
                return NULL;
            }
            for (int i = 0; i < nprocs; i++) {
                req->node_list[i] = procs[i].rank;
            }
        }
    }

    // Populate the data we'll be using
    req->fence_ndata = ndata;
    req->fence_data = NULL;
    if (req->fence_ndata > 0) {
        size_t data_size = req->fence_ndata * sizeof(char);
        req->fence_data = malloc(data_size);
        if (req->fence_data == NULL) {
            free(req);
            return NULL;
        }
        memcpy(req->fence_data, data, data_size);
    }

    // Lastly, store our node rank for the pthread to use in the dict
    req->src = d_job->node_rank;

    return req;
}



dragonError_t
_dragon_pmix_ddict_allgather(dragonPMIxJob_t *d_job,
                             pmix_proc_t *procs,
                             size_t nprocs,
                             char *data,
                             size_t ndata,
                             pmix_modex_cbfunc_t cbfunc,
                             void *cbdata)
{

    dragonPMIxRequest_t *req = _dragon_pmix_init_fence_request(d_job, procs, nprocs, data, ndata, cbfunc, cbdata);
    if (req == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for fence request");

    // Start the pthread that will call the cbfunc when complete
    int err = pthread_create(&(req->req_thread),  NULL, _ddict_allgather, (void *) req);

    if (err != 0) {
        append_err_return(DRAGON_FAILURE, "Unable to start PMIx allgather pthread");
    } else {
        no_err_return(DRAGON_SUCCESS);
    }

}

pmix_status_t
fence_handler(
    const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
    size_t ninfo, char *data, size_t ndata, pmix_modex_cbfunc_t cbfunc,
    void *cbdata)
{

    // Get a job ref
    dragonPMIxJob_t *d_job;
    dragonError_t derr = _dragon_get_job_from_nspace((char*) procs[0].nspace, &d_job);
    if (derr != DRAGON_SUCCESS) {
        return PMIX_ERROR;
    }

    // We only support PMIX_COLLECT_DATA
    for (size_t i = 0; i < ninfo; i++) {
        if (!strcmp(info[i].key, PMIX_COLLECT_DATA)) {
            continue;
        }
        if (info[i].flags & PMIX_INFO_REQD) {
            return PMIX_ERR_NOT_SUPPORTED;
        }
    }

    // Prepare the data request going to LS and send it off
    derr = _dragon_pmix_ddict_allgather(d_job, (pmix_proc_t*) procs, nprocs, data, ndata, cbfunc, cbdata);
    if (derr != DRAGON_SUCCESS) {
        return PMIX_ERROR;
    }

    // We'll call their callback once everything is complete
    return PMIX_SUCCESS;
}

pmix_status_t
connected(const pmix_proc_t *proc, void *server_object, pmix_op_cbfunc_t cbfunc,
          void *cbdata)
{
    //(void) proc;
    (void) server_object;

    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}


pmix_status_t
finalized(const pmix_proc_t *proc, void *server_object,
          pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    (void) server_object;

    /* ensure we call the cbfunc so the proc can exit! */
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}

pmix_status_t
abort_fn(const pmix_proc_t *proc, void *server_object, int status,
         const char msg[], pmix_proc_t procs[], size_t nprocs,
         pmix_op_cbfunc_t cbfunc, void *cbdata)
{

    pmix_status_t rc;
    dragonPMIxCBData_t *state;
    (void) server_object;
    (void) msg;
    (void) nprocs;

    /* instead of aborting the specified procs, notify them
     * (if they have registered their errhandler) */

    /* use the dragonPMIxCBData_t object to ensure we release
     * the caller when notification has been queued */
    state = _dragon_pmix_cbdata_constructor();
    if (state == NULL)
        return PMIX_ERROR;

    // Get the server reference via the nspace
    PMIx_Load_nspace_p(state->caller.nspace, proc->nspace);
    state->caller.rank = proc->rank;
    state->cbfunc = cbfunc;
    state->cbdata = cbdata;

    if (PMIX_SUCCESS
            != (rc = PMIx_Notify_event_p(status, &state->caller, PMIX_RANGE_NAMESPACE, state->info, 2, abcbfunc,
                                         state))) {
    }
    _dragon_pmix_cbdata_deconstructor(state);

    return PMIX_SUCCESS;
}

static dragonError_t
_add_local_peers(dragonPMIxJob_t *d_job,
                 char **local_peers)
{

    const uint nchars = 9; // Assume 7 chars per rank

    size_t len = d_job->ppn * (sizeof(char) * nchars);
    char *peers = malloc(len);
    if (peers == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for peers string");

    // Find the ranks that are on my node and string them together
    for (size_t h = 0; h < d_job->nnodes; h++) {
        if (d_job->node_rank == d_job->nodes[h].nid) {
            int n = snprintf(peers, len, "%d", d_job->nodes[h].ranks[0]);
            for (int r = 1; r < d_job->nodes[h].ppn; r++) {
                n += snprintf(peers + n, len - n, ",%d", d_job->nodes[h].ranks[r]);
            }
            break;
        }
    }

    *local_peers = peers;
    return 0;
}

dragonError_t
_dragon_pmix_configure_server_nspace(dragonPMIxServer_t *d_server,
                                     pmix_info_t **info,
                                     size_t *ninfo)
{

    int nitems = 4;
    size_t lninfo = 0;
    pmix_info_t *linfo;

    linfo = PMIx_Info_create_p(nitems);

    PMIx_Info_load_p(&linfo[lninfo], PMIX_SERVER_TMPDIR, d_server->tmpdir, PMIX_STRING);
    lninfo++;

    PMIx_Info_load_p(&linfo[lninfo], PMIX_HOSTNAME, d_server->hostname, PMIX_STRING);
    lninfo++;

    PMIx_Info_load_p(&linfo[lninfo], PMIX_SERVER_NSPACE, d_server->server_nspace, PMIX_STRING);
    lninfo++;

    // Create a node rank based off hostname
    PMIx_Info_load_p(&linfo[lninfo], PMIX_SERVER_RANK, &d_server->node_rank, PMIX_PROC_RANK);
    lninfo++;

    *info = linfo;
    *ninfo = lninfo;
    no_err_return(DRAGON_SUCCESS);

}


void
errhandler(size_t evhdlr_registration_id, pmix_status_t status,
           const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
           pmix_info_t results[], size_t nresults,
           pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata)
{
    (void) evhdlr_registration_id;
    (void) source;
    (void) info;
    (void) ninfo;
    (void) results;
    (void) nresults;
    (void) cbfunc;
    (void) cbdata;
}

void
errhandler_reg_callbk(pmix_status_t status, size_t errhandler_ref, void *cbdata)
{
    (void) status;
    (void) errhandler_ref;
    (void) cbdata;
    return;
}


static dragonError_t
_add_hosts(dragonPMIxJob_t *d_job,
           char **node_map)
{
    size_t space = 0;
    pmix_status_t rc;
    char *nodes = NULL;

    // Concat the lists into a single string. Get total length of concat'ed string
    for (int h = 0; h < d_job->nnodes; h++) {
        space += strlen(d_job->nodes[h].hostname) + 2; // comma and trailing nul
    }

    nodes = malloc(space * sizeof(char));
    if (nodes == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for generating host map in PMIx server");

    int n = snprintf(nodes, space, "%s", d_job->nodes[0].hostname);
    for (int h = 1; h < d_job->nnodes; h++) {
        n += snprintf(nodes + n, space - n, ",%s", d_job->nodes[h].hostname);
    }

    rc = PMIx_generate_regex_p(nodes, node_map);
    assert(PMIX_SUCCESS == rc);
    free(nodes);

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t
_add_ppn(dragonPMIxJob_t *d_job,
         char **ppn)
{
    const uint nchars = 12;
    uint32_t *count = calloc(d_job->nnodes, sizeof(uint32_t));
    if (count == NULL)
        append_err_return(DRAGON_FAILURE, "Failed to allocate on-the-fly space for ppn counter in PMIx server");

    size_t tlen = 0;
    char **arrays, *procs, *str = NULL;
    pmix_status_t rc;

    // Put together an array with how many proccesses are assigned to a node
    // with subscript defined as node rank
    for (int r = 0; r < d_job->nnodes; r++) {
        count[r] = d_job->nodes[r].ppn;
    }

    // Array for holding a node's ranks represented as a list
    // of comma-separeted ranks
    arrays = calloc(d_job->nnodes, sizeof(char *));
    if (arrays == NULL)
        append_err_return(DRAGON_FAILURE, "Failed to allocate on-the-fly space for ppn array holder in PMIx server");


    // Create a string for each host of the ranks on it
    for (int nid = 0; nid < d_job->nnodes; nid++) {
        for (int rid = 0, n = 0; rid < d_job->nodes[nid].ppn; rid++) {
            if (!arrays[nid]) {
                tlen = count[nid] * nchars;
                str = arrays[nid] = malloc(tlen * sizeof(char));
                if (str == NULL)
                    append_err_return(DRAGON_FAILURE, "Failed to allocate on-the-fly space for ppn regex creation in PMIx server");
                n = snprintf(str, tlen, "%d", d_job->nodes[nid].ranks[rid]);
            } else {
                assert(str);
                n += snprintf(str + n, tlen - n, ",%d",  d_job->nodes[nid].ranks[rid]);
            }
        }
    }

    // Concatenate all the node ranks into a single string with nodes
    // separated by a semi-colon
    for (int h = 0; h < d_job->nnodes; h++) {
        assert(arrays[h]);
        tlen += strlen(arrays[h]) + 1; // Add semicolon or trailing nul
    }
    procs = malloc(tlen * sizeof(char));
    if (procs == NULL)
        append_err_return(DRAGON_FAILURE, "Failed to allocate space for single procs string in ppn regex creation for PMIx server");

    assert(arrays[0]);
    int n = snprintf(procs, tlen, "%s", arrays[0]);
    free(arrays[0]);
    for (int h = 1; h < d_job->nnodes; h++) {
        assert(arrays[h]);
        n += snprintf(procs + n, tlen - n, ";%s", arrays[h]);
        free(arrays[h]);
    }

    // Turn it into PMIx's internal format
    rc = PMIx_generate_ppn_p(procs, ppn);
    assert(PMIX_SUCCESS == rc);

    free(arrays);
    free(procs);

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
_dragon_pmix_initialize_application_server(dragonPMIxJob_t *d_job)
{
    dragonError_t derr;
    size_t ninfo = 2;
    pmix_status_t rc;
    char *node_map, *c_ppn;
    pmix_info_t *info = calloc(ninfo, sizeof(pmix_info_t));
    if (info == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx info array in application server init");


    derr = _add_hosts(d_job, &node_map);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Unable to construct host map for PMIx application server");

    PMIx_Info_load_p(&info[0], PMIX_NODE_MAP, node_map, PMIX_STRING);

    // TODO: Add to dragon logging
    // fprintf(stderr, "Loading node map into application server: %s\n", node_map);fflush(stderr);

    derr = _add_ppn(d_job, &c_ppn);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Failed to construct ppn map for PMIx application server");

    PMIx_Info_load_p(&info[1], PMIX_PROC_MAP, c_ppn, PMIX_STRING);

    // TODO: Add to dragon logging
    // fprintf(stderr, "Setting up application server with proc map %s\n", c_ppn); fflush(stderr);

    // Set up application server
    dragonPMIxCBData_t *state = _dragon_pmix_cbdata_constructor();
    if (state == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx callback data");

    asprintf(&(state->nspace), "%s", d_job->client_nspace);
    if (PMIX_SUCCESS
            != (rc = PMIx_server_setup_application_p(d_job->client_nspace, info, ninfo, setup_cbfunc, state))) {

        _dragon_pmix_cbdata_deconstructor(state);
        append_err_return(DRAGON_FAILURE, "Failed to setup local config for PMIx application");
    }
    PMIX_WAIT_FOR_COMPLETION(state->active);
    _dragon_pmix_cbdata_deconstructor(state);

    // Format and serialize my data for the dict
    char *data_to_dict;
    pmix_data_buffer_t pbuf;
    size_t buf_size;

    // Serialize PMIx data via PMIx API
    PMIx_Data_buffer_construct_p(&pbuf);

    rc = PMIx_Data_pack_p(NULL, &pbuf, &ninfo, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        append_err_return(DRAGON_FAILURE, "Failed to pack PMIx ninfo data");
    }

    rc = PMIx_Data_pack_p(NULL, &pbuf, info, ninfo, PMIX_INFO);
    if (PMIX_SUCCESS != rc) {
        append_err_return(DRAGON_FAILURE, "Failed to pack PMIx info data");
    }
    PMIx_Data_buffer_unload_p(&pbuf, &data_to_dict, &buf_size);

    // b64 encode
    char* encoded = dragon_base64_encode((uint8_t*) data_to_dict, buf_size);
    if (encoded == NULL) {
        append_err_return(DRAGON_FAILURE, "Failed to encode PMIx blob to dragoon base64");
    }

    // Put my data into the dictionary via a persistent put:
    char *local_info_key = LOCAL_INFO_KEY_DEF;
    bool persist = true;
    derr = _dragon_pmix_write_to_ddict(d_job->ddict, persist, local_info_key, encoded, strlen(encoded) * sizeof(char));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "Unable to do a persistent put of PMIx local app into PMIx ddict");
    }


    // Forward along the info. Just use the shared filesystem for now
    PMIx_Info_free_p(info, ninfo);
    free(node_map);
    free(c_ppn);
    free(encoded);

    no_err_return(DRAGON_SUCCESS);
}

#define INFO_SPOTS 10 // # of fixed attributes in the top level collection
#define RANK_SPOTS 5 // # of attributes for each per-rank collection
#define HOST_SPOTS 8 // # of attributes for each host/node
dragonError_t
_dragon_pmix_configure_client_nspace(dragonPMIxJob_t *d_job, dragonPMIxServer_t *d_server)
{

    // Parameters to be used later
    char *node_map, *c_ppn;
    char *local_peers = NULL;

    // Set how many parameters are going to be in our info array:
    int ninfo = INFO_SPOTS + d_job->nprocs + d_job->nnodes;

    // Define an info array. We'll call it spot and info for reasons
    pmix_info_t *spot, *info;
    spot = info = calloc(ninfo, sizeof(pmix_info_t));
    if (spot == NULL) {
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx info array in nspace configuration");
    }

    // Create string of global ranks on this specific node
    dragonError_t derr = _add_local_peers(d_job, &local_peers);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Unable to determine local peers in PMIx server");

    // TODO: Add to dragon logging
    // fprintf(stderr, "local peers on nid %d (ppn %d): %s\n", d_job->node_rank, (int)d_job->ppn, local_peers); fflush(stderr);

    //////// Session-specific attributes /////////////////////
    uint32_t univ_size = (uint32_t) d_job->nprocs;
    PMIx_Info_load_p(spot, PMIX_UNIV_SIZE, &univ_size, PMIX_UINT32);
    spot++;

    uint32_t spawned = 0;
    PMIx_Info_load_p(spot, PMIX_SPAWNED, &spawned, PMIX_UINT32);
    spot++;

    PMIx_Info_load_p(spot, PMIX_NSPACE, &d_job->client_nspace, PMIX_UINT32);
    spot++;

    derr = _add_hosts(d_job, &node_map);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Unable to construct host map for PMIx server namespace");
    PMIx_Info_load_p(spot, PMIX_NODE_MAP, node_map, PMIX_STRING);
    spot++;

    _add_ppn(d_job, &c_ppn);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Failed to construct ppn map for PMIx application server");
    PMIx_Info_load_p(spot, PMIX_PROC_MAP, c_ppn, PMIX_STRING);
    free(c_ppn);
    spot++;

    uint32_t job_nproc = (uint32_t) d_job->nprocs;
    PMIx_Info_load_p(spot, PMIX_JOB_SIZE, &job_nproc, PMIX_UINT32);
    spot++;

    char *job_id = &d_job->client_nspace[strlen(CLIENT_NSPACE_PREFIX)];
    PMIx_Info_load_p(spot, PMIX_JOBID, job_id, PMIX_STRING);
    spot++;

    // Session ID will be tied to the server head GUID
    PMIx_Info_load_p(spot, PMIX_SESSION_ID, &d_server->sess_id, PMIX_UINT32);
    spot++;

    PMIx_Info_load_p(spot, PMIX_TMPDIR, d_server->tmpdir, PMIX_STRING);
    spot++;

    PMIx_Info_load_p(spot, PMIX_NSDIR, d_job->nsdir, PMIX_STRING);
    spot++;


    ////// Collection of all attributes for every rank in all the jobs this server is involved in//////////
    for (int32_t i = 0; i < d_job->nprocs; i++) {
        pmix_info_t *iter;
        pmix_data_array_t *darray = calloc(1, sizeof(pmix_data_array_t));
        if (darray == NULL)
            append_err_return(DRAGON_FAILURE, "Failed to allocate space for per-rank nspace data array in PMIx server");

        darray->type = PMIX_INFO;
        iter = darray->array = calloc(RANK_SPOTS, sizeof(pmix_info_t));
        if (iter == NULL)
            append_err_return(DRAGON_FAILURE, "Failed to allocate space for array in per-rank nspace info in PMIx server");

        int32_t pmix_job_rank = (int32_t) d_job->procs[i].rank;
        int32_t global_rank = (int32_t) d_job->procs[i].rank;
        uint32_t node_rank = (uint32_t) d_job->procs[i].nid;

        PMIx_Info_load_p(iter, PMIX_RANK, &pmix_job_rank, PMIX_PROC_RANK);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODEID, &node_rank, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_GLOBAL_RANK, &global_rank, PMIX_PROC_RANK);
        iter++;

        // If this proc will be on my node, I need to set stuff local/node ranks up
        if (d_job->node_rank == (int) node_rank) {
            uint16_t l = (uint16_t) d_job->procs[i].lrank;  // local rank within this job
            uint16_t n = l;

            PMIx_Info_load_p(iter, PMIX_LOCAL_RANK, &l, PMIX_UINT16);
            iter++;

            PMIx_Info_load_p(iter, PMIX_NODE_RANK, &n, PMIX_UINT16);
            iter++;
        }

        // Point the "spot" pointer to this data array of crap
        strncpy(spot->key, PMIX_PROC_INFO_ARRAY, PMIX_MAX_KEYLEN);
        spot->value.type = PMIX_DATA_ARRAY;
        darray->size = iter - (pmix_info_t *) darray->array;
        spot->value.data.darray = darray;
        spot++;
        assert(RANK_SPOTS >= darray->size);

    }

    //// Collection of attributes for every node in this job/namespace
    for (int32_t host_rank = 0; host_rank < d_job->nnodes; host_rank++) {

        pmix_data_array_t *darray = calloc(1, sizeof(pmix_data_array_t));
        if (darray == NULL)
            append_err_return(DRAGON_FAILURE, "Failed to allocate space for per-host nspace data array in PMIx server");

        darray->array = calloc(HOST_SPOTS, sizeof(pmix_info_t));
        pmix_info_t *iter = darray->array;
        if (iter == NULL)
            append_err_return(DRAGON_FAILURE, "Failed to allocate space for array in per-host nspace info in PMIx server");

        darray->type = PMIX_INFO;

        uint32_t nid = (uint32_t) d_job->nodes[host_rank].nid;
        uint32_t ppn = (uint32_t) d_job->nodes[host_rank].ppn;

        PMIx_Info_load_p(iter, PMIX_HOSTNAME, d_job->nodes[host_rank].hostname, PMIX_STRING);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODEID, &nid, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODE_SIZE, &ppn, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_LOCAL_SIZE, &ppn, PMIX_UINT32);
        iter++;

        // Allocate extra stuff if this node is my own
        if ((int) d_job->node_rank == (int) d_job->nodes[host_rank].nid) {

            PMIx_Info_load_p(iter, PMIX_TMPDIR, d_server->tmpdir, PMIX_STRING);
            iter++;

            PMIx_Info_load_p(iter, PMIX_NSDIR, d_job->nsdir, PMIX_STRING);
            iter++;

            PMIx_Info_load_p(iter, PMIX_LOCAL_PEERS, local_peers, PMIX_STRING);
            iter++;

            pmix_proc_t *procs = malloc(d_job->nodes[host_rank].ppn * sizeof(pmix_proc_t));
            if (procs == NULL)
                append_err_return(DRAGON_FAILURE, "Failed to allocate space for local procs data array in PMIx server");

            for (int32_t lr = 0; lr < d_job->nodes[host_rank].ppn; lr++) {
                strncpy(procs[lr].nspace, d_job->client_nspace, PMIX_MAX_NSLEN + 1);
                procs[lr].rank = d_job->nodes[host_rank].ranks[lr];
            }

            // All local procs, not just those in our job-step
            PMIx_Info_load_p(iter, PMIX_LOCAL_PROCS, procs, PMIX_PROC);
            iter++;
            free(procs);
        }

        // Fill the next entry in our spot info array
        strncpy(spot->key, PMIX_NODE_INFO_ARRAY, PMIX_MAX_KEYLEN);
        spot->value.type = PMIX_DATA_ARRAY;
        spot->value.data.darray = darray;
        darray->size = iter - (pmix_info_t*)darray->array;
        spot++;
        assert(HOST_SPOTS >= darray->size);
    }
    free(local_peers);

    // Set this node's namespace:
    pmix_status_t rc;
    dragonPMIxCBData_t *state = _dragon_pmix_cbdata_constructor();
    if (state == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx callback data");

    if (PMIX_SUCCESS
            != (rc = PMIx_server_register_nspace_p(d_job->client_nspace, d_job->ppn, info, spot - info, opcbfunc, state))) {
        append_err_return(DRAGON_FAILURE, "PMIx: failed to register nspace!\n");
    }
    PMIX_WAIT_FOR_COMPLETION(state->active);
    _dragon_pmix_cbdata_deconstructor(state);

    free(info);

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t
_dragon_pmix_configure_local_support(dragonPMIxJob_t *d_job)
{

    dragonDDictDescr_t *ddict = d_job->ddict;
    dragonError_t derr;
    pmix_info_t *info;
    size_t ninfo;

    // Read the application info from the ddict
    char *local_info_key = LOCAL_INFO_KEY_DEF;
    char *tmp, *b64_data;
    size_t data_len;
    derr = _dragon_pmix_read_from_ddict(ddict, local_info_key, &tmp, &data_len);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(DRAGON_FAILURE, "Unable to read data from dict for PMIx local app support");
    }

    // Make sure there's a null terminator at end of data
    b64_data = malloc((data_len + 1) * sizeof(char));
    if (b64_data == NULL)
        append_err_return(DRAGON_FAILURE, "Failed to allocate b64 space for packing PMIx local app support data into ddict");
    memcpy(b64_data, tmp, data_len);
    b64_data[data_len] = '\0';
    free(tmp);

    // Get the data from the buffer
    char *input;
    size_t inputlen;
    pmix_data_buffer_t pbuf = {0};
    pmix_status_t rc;
    int32_t count = 1;

    // Decode the data
    if (!b64_data)
        append_err_return(DRAGON_FAILURE, "Data from dict for PMIx local app suppprt was empty");

    input = (char*) dragon_base64_decode(b64_data, &inputlen);
    free(b64_data);
    if (!input) {
        append_err_return(DRAGON_FAILURE, "Failed to base64 decode PMIx data buffer");
    }

    PMIx_Data_buffer_load_p(&pbuf, (void*) input, inputlen);

    // Find out how many Infos they sent us
    rc = PMIx_Data_unpack_p(NULL, &pbuf, &ninfo, &count, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        append_err_return(DRAGON_FAILURE, "Failed to unpack PMIx buffer's info array size");
    }
    count = (int32_t) ninfo;

    info = malloc(ninfo * sizeof(pmix_info_t));
    if (info == NULL)
        append_err_return(DRAGON_FAILURE, "Failed to allocate space for unpacking PMIx local app support info array");

    // Convert from BLOB to an array of Infos
    rc = PMIx_Data_unpack_p(NULL, &pbuf, info, &count, PMIX_INFO);
    if (PMIX_SUCCESS != rc) {
        PMIx_Info_free_p(info, ninfo);
        append_err_return(DRAGON_FAILURE, "Failed to unpack PMIx buffer's info array");
    }


    // And finally use it to set everything up.
    dragonPMIxCBData_t *state = _dragon_pmix_cbdata_constructor();
    if (state == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx callback data");


    // TODO: Add to dragon logging
    // fprintf(stderr, "Setting up local server support with ninfo == %d in nspace %s\n", (int) ninfo, d_job->client_nspace); fflush(stderr);
    // fprintf(stderr, "server info %d: %s\n", 0, (char*) info[0].value.data.string); fflush(stderr);
    // fprintf(stderr, "server info %d: %s\n", 1, (char*) info[1].value.data.string); fflush(stderr);
    if (PMIX_SUCCESS != (rc = PMIx_server_setup_local_support_p(d_job->client_nspace, info,
                                                                ninfo, opcbfunc, state))) {
        PMIx_server_finalize_p();
        append_err_return(DRAGON_FAILURE, "Could not setup local server support for PMIx");
    }

    PMIX_WAIT_FOR_COMPLETION(state->active);
    _dragon_pmix_cbdata_deconstructor(state);

    PMIx_Info_free_p(info, ninfo);
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_dragon_get_job_ref(dragonG_UID_t guid, dragonPMIxJob_t **d_job)
{

    dragonError_t err = dragon_umap_getitem(&job_map, (uint64_t) guid, (void**) d_job);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in Dragon PMIx job umap.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_dragon_get_job_from_nspace(char *nspace, dragonPMIxJob_t **d_job)
{

    // Extract the guid from the nspace
    dragonG_UID_t guid = (dragonG_UID_t) strtoull(&nspace[strlen(CLIENT_NSPACE_PREFIX)], NULL, 10);
    if ((uint64_t) guid == 0UL)
        append_err_return(DRAGON_FAILURE, "Failed to extract GUID from PMIx client nspace");

    dragonError_t derr = _dragon_get_job_ref(guid, d_job);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to get PMIx job reference from nspace");

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t
_dragon_pmix_create_job_ref(dragonPMIxJob_t **d_job, dragonG_UID_t guid)
{

    dragonPMIxJob_t *l_job = malloc(sizeof(dragonPMIxJob_t));
    if (l_job == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for new PMIx job structuure");

    l_job->guid = guid;

    // If job_map is NULL then this is our first attempt to do any of this, allocate the space
    // for it
    dragonError_t err;
    if (job_map == NULL) {
        job_map = (dragonMap_t*) malloc(sizeof(dragonMap_t));
        if (job_map == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate umap for PMIx job structures.");

        err = dragon_umap_create(&job_map, DRAGON_PMIX_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to create umap for PMIx job structures.");
    }

    if (guid_map_keys == NULL) {
        guid_map_keys = (dragonList_t*) malloc(sizeof(dragonList_t));
        if (guid_map_keys == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate umap for PMIx job structures.");

        err = dragon_ulist_create(&guid_map_keys);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to create ulist for PMIx guid tracking.");
    }

    *d_job = l_job;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_dragon_pmix_create_server_tmpdir(dragonPMIxServer_t *d_server, char *server_tmp)
{
    struct stat buf;

    // Construct path
    if (0 > asprintf(&d_server->tmpdir, "%s/pmix.%lu/%s", server_tmp, (long unsigned) d_server->uid, d_server->hostname)) {
        exit(1);
    }

    /* create the directory */
    if (0 != stat(d_server->tmpdir, &buf)) {
        _mkdir(d_server->tmpdir);
        if (0 != stat(d_server->tmpdir, &buf))
            err_return(DRAGON_FAILURE, "Unable to create PMIx tmp directory");
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
_dragon_pmix_create_job_nsdir(dragonPMIxJob_t *d_job, char *client_tmp, char *hostname, uid_t uid)
{
    struct stat buf;

    // Construct path
    if (0 > asprintf(&d_job->nsdir, "%s/pmix.%lu/%s/%s.%lu", client_tmp, (long unsigned) uid, hostname,
                     "nsdir-job", d_job->guid)) {
        exit(1);
    }

    /* create the directory */
    if (0 != stat(d_job->nsdir, &buf)) {
        _mkdir(d_job->nsdir);
            // Check it exists now
        if (0 != stat(d_job->nsdir, &buf))
            err_return(DRAGON_FAILURE, "Unable to create PMIx namespace directory");
    }

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t
_dragon_pmix_initialize_server(dragonPMIxServer_t **d_server,
                               dragonG_UID_t guid,
                               int node_rank,
                               char *server_tmpdir)

{
    dragonPMIxServer_t *l_server = *d_server;

    // Make sure we're not already initialized
    if (l_server != NULL) {
        if (l_server->initialized == 1UL) {
            no_err_return(DRAGON_SUCCESS);
        }
        else {
            append_err_return(DRAGON_NOT_FOUND, "Dragon PMIx server in an undefined state");
        }
    }

    l_server = malloc(sizeof(dragonPMIxServer_t));
    l_server->initialized = 0UL;

    l_server->node_rank = node_rank;
    l_server->uid = getuid();
    l_server->gid = getgid();

    // uint32_t for ID-ing the server session
    l_server->sess_id = (uint32_t) ((guid & 0xFFFFFFFF00000000UL) >> 32);

    // Fill the hostname
    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, sizeof(hostname)) == -1) {
        err_return(DRAGON_FAILURE, "Unable to use gethostname() to get PMIx server hostname");
    }
    asprintf(&(l_server->hostname), "%s", hostname);

    // Give some name to the server namespace
    asprintf(&(l_server->server_nspace), "%s%lu", SERVER_NSPACE_PREFIX, (long unsigned) l_server->uid);

    // Create tmp directory for server
    dragonError_t derr = _dragon_pmix_create_server_tmpdir(l_server, server_tmpdir);

    // Actually start the PMIx server now
    pmix_info_t *info = NULL;
    size_t ninfo;

    derr = _dragon_pmix_configure_server_nspace(l_server, &info, &ninfo);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "Failed to load server info into PMIx server namespace");
    }

    pmix_status_t rc = PMIx_server_init_p(&dragon_pmix_cback_module, info, ninfo);
    if (rc != PMIX_SUCCESS) {
        append_err_return(DRAGON_FAILURE, "PMIx_server_init failed");
    }
    PMIx_Info_free_p(info, ninfo);
    l_server->initialized = 1UL;

    *d_server = l_server;

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t
_find_already_attached_ddict(char* pmix_sdesc, dragonDDictDescr_t **ddict)
{

    dragonPMIxJob_t *d_job = NULL;;
    dragonError_t derr;
    *ddict = NULL;
    bool locking = false;

    // We store the keys to the umap in the guid ulist. We'll use that to
    // traverse the jobs in the umap
    derr = dragon_ulist_lock(&guid_map_keys);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to lock Dragon PMIx guid ulist while searching for ddict state");


    size_t nguids = dragon_ulist_get_size(&guid_map_keys, locking);
    for (size_t gidx = 0; gidx < nguids; gidx++) {

        dragonG_UID_t *guid;
        derr = dragon_ulist_get_by_idx(&guid_map_keys, (int) gidx, (void**) &guid, locking);
        if (derr != DRAGON_SUCCESS) {
            dragon_ulist_unlock(&guid_map_keys);
            append_err_return(derr, "Failed to get GUID from PMIx job guid list");
        }

        if (guid != NULL) {
            derr = dragon_umap_getitem(&job_map, (uint64_t) *guid, (void**) &d_job);
            if (derr != DRAGON_SUCCESS) {
                dragon_ulist_unlock(&guid_map_keys);
                append_err_return(derr, "Failed to get PMIx job from job umap during ddict search");
            }

            if (d_job->holding_ddict_attach && d_job->ddict_sdesc != NULL) {
                if (strcmp(pmix_sdesc, d_job->ddict_sdesc) == 0) {
                    *ddict = d_job->ddict;
                    dragon_ulist_unlock(&guid_map_keys);
                    no_err_return(DRAGON_SUCCESS);
                }
            }
        }
    }
    derr = dragon_ulist_unlock(&guid_map_keys);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to unlock Dragon PMIx guid ulist while searching for ddict state");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_dragon_pmix_initialize_job_struct(dragonPMIxJob_t **d_job,
                                   dragonPMIxServer_t *d_server,
                                   char *ddict_sdesc,
                                   char *local_mgr_sdesc,
                                   dragonChannelSerial_t out_to_ls,
                                   dragonChannelSerial_t in_from_ls,
                                   dragonChannelSerial_t buffered_from_ls,
                                   dragonG_UID_t guid,
                                   int nhosts,
                                   int nprocs,
                                   int *proc_ranks,
                                   int *ppn,
                                   int *node_ranks,
                                   char **hosts,
                                   char *client_tmpdir)
{

    dragonError_t derr = _dragon_pmix_create_job_ref(d_job, guid);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to create PMIx job structure");

    dragonPMIxJob_t *l_job = *d_job;
    l_job->nsdir = NULL;
    l_job->ddict_sdesc = NULL;

    // Create the nspace tmpdir
    derr = _dragon_pmix_create_job_nsdir(l_job, client_tmpdir, d_server->hostname, d_server->uid);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to crate PMIx tmpdir for client apps");

    asprintf(&(l_job->client_nspace), "%s%lu", CLIENT_NSPACE_PREFIX, guid);

    // Check to see if we have already seen this dictionary. If we have, then we are necessarily attached
    // to it because the ProcessGroup manager creates 1 dictionary for its lifetime. If we're seeing
    // the dict a second time it necessarily is from the same manager and is just a re-execution
    // of the process templates tied to that particular manager.
    derr = _find_already_attached_ddict(ddict_sdesc, &l_job->ddict);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Error occurred while searching umap for PMIx dictionary");

    // We could have gotten a success but just not found the ddict. If so, it's NULL
    if (l_job->ddict == NULL) {
        timespec_t *timeout = NULL;
        l_job->ddict = malloc(sizeof(dragonDDictDescr_t));
        if (l_job->ddict == NULL)
            append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx ddict_sdesc");
        derr = _dragon_ddict_attach(ddict_sdesc,
                                    l_job->ddict,
                                    timeout,
                                    &out_to_ls,
                                    &in_from_ls,
                                    &buffered_from_ls,
                                    local_mgr_sdesc,
                                    false);

        if (derr != DRAGON_SUCCESS)
            append_err_return(derr, "Unable to attach to dictionary");

        // Store the reference to the serialized descriptor for future comparisons.
        l_job->ddict_sdesc = strdup(ddict_sdesc);

        l_job->holding_ddict_attach = true;
    }
    else {
        l_job->holding_ddict_attach = false;
    }

    // Fill the node and process structures:
    l_job->node_rank = d_server->node_rank;
    l_job->nnodes = (size_t) nhosts;
    l_job->nprocs = (size_t) nprocs;

    l_job->nodes = malloc(l_job->nnodes * sizeof(dragonPMIxNode_t));
    if (l_job->nodes == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx nodes structure");

    l_job->procs = malloc(l_job->nprocs * sizeof(dragonPMIxProcess_t));
    if (l_job->procs == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx procs structure");

    // The node ranks come in sorted, so the first value becomes the caption
    l_job->job_captain = false;
    if (l_job->node_rank == node_ranks[0]) {
        l_job->job_captain = true;
    }

    // Get the max node id:
    int max_nid = 0;
    for (size_t nid = 0; nid < l_job->nnodes; nid++) {
        if (node_ranks[nid] > max_nid) {
            max_nid = node_ranks[nid];
        }
    }

    // Since we're using this only for allocating pointer space, add 1 to account for 0-based indexing
    max_nid++;

    // Track the node index
    int *node_map = malloc(max_nid * sizeof(int));
    if (node_map == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for node map during PMIx server/job construction");

    // Initialize the nodes structure
    for (size_t node_count = 0; node_count < l_job->nnodes; node_count++) {
        l_job->nodes[node_count].nid = node_ranks[node_count];
        l_job->nodes[node_count].ppn = ppn[node_count];

        if (l_job->nodes[node_count].nid == l_job->node_rank) {
            l_job->ppn = l_job->nodes[node_count].ppn;
        }
        l_job->nodes[node_count].hostname = strdup(hosts[node_count]);

            // Set stuff up to fill in the ranks array when we process the ranks structure
        l_job->nodes[node_count].ranks = malloc(sizeof(int) * l_job->nodes[node_count].ppn);
        if (l_job->nodes[node_count].ranks == NULL)
            append_err_return(DRAGON_FAILURE, "Unable to allocate space for ranks inside the PMIx nodes structure");

        node_map[l_job->nodes[node_count].nid] = node_count;

    }
    // initialize the ranks structure
    int *lrank_count = calloc(max_nid, sizeof(int));
    if (lrank_count == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for local rank counter during PMIx procs structure");

    for (size_t proc = 0; proc < nprocs; proc++) {
        l_job->procs[proc].rank = proc;
        l_job->procs[proc].nid = proc_ranks[proc];
        l_job->procs[proc].lrank = lrank_count[l_job->procs[proc].nid];

        // Fill in the ranks structure for the approriate node
        l_job->nodes[node_map[l_job->procs[proc].nid]].ranks[l_job->procs[proc].lrank] = l_job->procs[proc].rank;
        lrank_count[l_job->procs[proc].nid]++;

        // TODO: Hook the below up with dragon logging
        // fprintf(stderr, "rank %d | nid %d | ppn %d | lrank %d | host %s\n",
        //         l_job->procs[proc].rank,
        //         l_job->procs[proc].nid,
        //         l_job->nodes[node_map[l_job->procs[proc].nid]].ppn,
        //         l_job->procs[proc].lrank,
        //         l_job->nodes[node_map[l_job->procs[proc].nid]].hostname);fflush(stderr);


    }
    free(lrank_count);
    free(node_map);

    // Add it to the umap
    derr = dragon_umap_additem(&job_map, (uint64_t) l_job->guid, (void*) l_job);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to insert item into Dragon PMIx umap.");

    // And add the guid to our list of known keys. This is needed to let us
    // find what ddict we may already be attached which exists inside the job_map
    // data structures.
    dragonG_UID_t* l_guid = malloc(sizeof(dragonG_UID_t));
    if (l_guid == NULL)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "unable to allocate space to store PMIx guid in ulist");

    *l_guid = l_job->guid;
    derr = dragon_ulist_additem(&guid_map_keys, (void*) l_guid, true);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to insert guid into Dragon PMIx ulist.");


    no_err_return(DRAGON_SUCCESS);

}


dragonError_t
dragon_pmix_initialize_job(dragonG_UID_t guid,
                           char *ddict_sdesc,
                           char *local_mgr_sdesc,
                           dragonChannelSerial_t out_to_ls,
                           dragonChannelSerial_t in_from_ls,
                           dragonChannelSerial_t buffered_from_ls,
                           int node_rank,
                           int nhosts,
                           int nprocs,
                           int *proc_ranks,
                           int *ppn,
                           int *node_ranks,
                           char **hosts,
                           char *client_tmpdir,
                           char *server_tmpdir)
{

    dragonError_t derr;

    // Make sure we have libmpix beforeqpmix anything
    derr = _dragon_pmix_set_fn_ptrs();
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to access PMIx function pointers");

    // If the server hasn't been created for this runtime instance, make sure it is
    derr = _dragon_pmix_initialize_server(&dpmix_server,
                                          guid,
                                          node_rank,
                                          server_tmpdir);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Unable to initialize dragonPMIxServer_t data structure");

    // Get ourselves a job structure to use
    dragonPMIxJob_t *d_job;
    derr = _dragon_pmix_initialize_job_struct(&d_job,
                                              dpmix_server,
                                              ddict_sdesc,
                                              local_mgr_sdesc,
                                              out_to_ls,
                                              in_from_ls,
                                              buffered_from_ls,
                                              guid,
                                              nhosts,
                                              nprocs,
                                              proc_ranks,
                                              ppn,
                                              node_ranks,
                                              hosts,
                                              client_tmpdir);
    if (derr != DRAGON_SUCCESS)
        append_err_return(DRAGON_FAILURE, "Failed to initialize dragonPMIxJob_t data structure");


    // Configure job's namespace
    derr = _dragon_pmix_configure_client_nspace(d_job, dpmix_server);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "PMIx: Failed to configure client namespace");
    }

    // If I'm the job captain initialize the application server
    if (d_job->job_captain) {
        derr = _dragon_pmix_initialize_application_server(d_job);
        if (derr != DRAGON_SUCCESS) {
            append_err_return(derr, "Failed to initialize the PMIx application server on node rank 0");
        }
    }

    // Set up local app support before forking
    derr = _dragon_pmix_configure_local_support(d_job);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to configure PMIx local support as defined by node rank 0");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_pmix_get_client_env(dragonG_UID_t guid,
                           int lrank,
                           char ***env,
                           int *nenv)
{
    /* fork/exec the test for this node*/
    pmix_status_t rc;
    dragonPMIxCBData_t *d_x;
    pmix_proc_t proc;

    // Get our server ref
    dragonPMIxJob_t *d_job = NULL;
    dragonError_t derr = _dragon_get_job_ref(guid, &d_job);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to get PMIx job reference while generating client environment");

    if (d_job == NULL) {
        append_err_return(DRAGON_FAILURE, "Failed to find PMIx job structure for given GUID");
    }


    PMIx_Load_nspace_p(proc.nspace, d_job->client_nspace);

    // Get the rank given the local rank. Start by finding our node in the array:
    dragonPMIxNode_t *mynode = NULL;
    for (int node_count = 0; node_count < d_job->nnodes; node_count++) {
        if ((int) d_job->nodes[node_count].nid == (int) d_job->node_rank) {
            mynode = &d_job->nodes[node_count];
            break;
        }
    }

    if (mynode == NULL) {
        append_err_return(DRAGON_FAILURE, "Failed to find my node in the PMIx server's node list");
    }

    // Confirm the lrank is less than ppn
    if (lrank >= mynode->ppn) {
        append_err_return(DRAGON_FAILURE, "Local rank exceeds the number of processes per node for this node");
    }

    proc.rank = mynode->ranks[lrank];
    if (PMIX_SUCCESS != (rc = PMIx_server_setup_fork_p(&proc, env))) { // n
        PMIx_server_finalize_p();
        append_err_return(DRAGON_FAILURE, "PMIx server fork setup failed");
    }

    // Get the length:
    if (*env) {
        int spot = 0;
        while ((*env)[spot]) spot++;
        *nenv = spot;
    }
    d_x = _dragon_pmix_cbdata_constructor();
    if (d_x == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to allocate space for PMIx callback data");

    if (PMIX_SUCCESS
            != (rc = PMIx_server_register_client_p(&proc, dpmix_server->uid, dpmix_server->gid, NULL, opcbfunc, d_x))) {
        PMIx_server_finalize_p();
        append_err_return(DRAGON_FAILURE, "PMIx server failed to register client");
    }

    /* don't fork/exec the client until we know it is registered
     * so we avoid a potential race condition in the server */
    PMIX_WAIT_FOR_COMPLETION(d_x->active);
    _dragon_pmix_cbdata_deconstructor(d_x);

    return DRAGON_SUCCESS;

}


dragonError_t
dragon_pmix_finalize_server()
{

    // Free the server
    dragonPMIxServer_t *d_server = dpmix_server;

    pmix_status_t rc;
    free(d_server->server_nspace);
    free(d_server->tmpdir);
    free(d_server->hostname);

    /* finalize the server library */
    if (PMIX_SUCCESS != (rc = PMIx_server_finalize_p())) {
        append_err_return(DRAGON_FAILURE, "Unable to finalize PMIx server");
    }
    free(d_server);

    // Destroy the umap and ulists
    dragonError_t derr = dragon_umap_destroy(&job_map);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to destroy Dragon PMIx job umap during server finalize");
    free(job_map);

    derr = dragon_ulist_destroy(&guid_map_keys);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to destroy Dragon PMIx guid ulist during server finalize");
    free(guid_map_keys);

    return DRAGON_SUCCESS;
}


dragonError_t
dragon_pmix_finalize_job(dragonG_UID_t guid)
{

    dragonPMIxJob_t *d_job = NULL;
    dragonError_t derr = _dragon_get_job_ref(guid, &d_job);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to get PMIx job reference while generating client environment");

    if (d_job == NULL) {
        append_err_return(DRAGON_FAILURE, "Failed to find PMIx job structure for given GUID");
    }

    PMIx_Deregister_event_handler_p(0, NULL, NULL);

    free(d_job->client_nspace);
    free(d_job->nsdir);

    for (int32_t nidx = 0; nidx < d_job->nnodes; nidx++) {
        free(d_job->nodes[nidx].ranks);
        free(d_job->nodes[nidx].hostname);
    }

    free(d_job->nodes);
    free(d_job->procs);
    if (d_job->holding_ddict_attach) {
        dragon_ddict_detach(d_job->ddict);
        d_job->holding_ddict_attach = false;
        free(d_job->ddict);
        d_job->ddict = NULL;

        free(d_job->ddict_sdesc);
        d_job->ddict_sdesc = NULL;
    }

    derr = dragon_umap_delitem(&job_map, (uint64_t) guid);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to delete Dragon PMIx job from umap during job's finalize");

    // Delete the guid from the list
    bool locking = false;
    derr = dragon_ulist_lock(&guid_map_keys);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to lock Dragon PMIx guid ulist during job's finalize");

    size_t nguids = dragon_ulist_get_size(&guid_map_keys, locking);
    for (size_t gidx = 0; gidx < nguids; gidx++) {

        dragonG_UID_t *l_guid;
        derr = dragon_ulist_get_by_idx(&guid_map_keys, (int) gidx, (void**) &l_guid, locking);
        if (derr != DRAGON_SUCCESS) {
            dragon_ulist_unlock(&guid_map_keys);
            append_err_return(derr, "Failed to get GUID from PMIx job guid list");
        }

        if (l_guid != NULL) {
            if ((uint64_t) *l_guid == (uint64_t) guid) {
                derr = dragon_ulist_delitem(&guid_map_keys, (void*) l_guid, locking);
                if (derr != DRAGON_SUCCESS) {
                    dragon_ulist_unlock(&guid_map_keys);
                    append_err_return(derr, "Failed to delete GUID from PMIx job guid list during finalize");
                }

                free(l_guid);
                l_guid = NULL;
                break;
            }
        }
    }
    derr = dragon_ulist_unlock(&guid_map_keys);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Failed to unlock Dragon PMIx guid ulist during job's finalize");



    free(d_job);


    no_err_return(DRAGON_SUCCESS);
}

#endif  // HAVE_PMIX_INCLUDE