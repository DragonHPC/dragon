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
#include "_ddict.h"
#include "_pmix.h"

// Several static values to manage internal state
static dragonPMIxServer_t *ref_server = NULL;  // Reference to PMIx server
static dragonDDictDescr_t pmix_ddict;  // DDict used to communicate with all other PMIx servers
static pmix_list_t pubdata;  // Data provided in lookup functions by the server
static volatile int wakeup;  // Number of clients this server is responsible for. At finalize it waits for all to exit

// All the function pointers we need to manage the PMIx dependency for manylinux support
// PMIx standard function pointers
static uint64_t pmix_syms_loaded = 0UL;
static void *lib_pmix_handle = NULL;
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

/*****************************************************************/
/*                                                               */
/*  These typedefs appear here because we want to make           */
/*  sure no pmix structs appear in C++ codes since pmix.h        */
/*  is very incompatible with g++. Thus, they don't appear       */
/*  in header files that may appear in C++ files.                */
/*                                                               */
/*****************************************************************/

typedef struct dragonPMIxCBData_st {
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


void setup_cbfunc(pmix_status_t status, pmix_info_t info[], size_t ninfo,
                         void *provided_cbdata, pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t connected(const pmix_proc_t *proc, void *server_object,
                        pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t finalized(const pmix_proc_t *proc, void *server_object,
                        pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t abort_fn(const pmix_proc_t *proc, void *server_object, int status,
                       const char msg[], pmix_proc_t procs[], size_t nprocs,
                       pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t fence_handler(
    const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
    size_t ninfo, char *data, size_t ndata, pmix_modex_cbfunc_t cbfunc,
    void *cbdata);

pmix_status_t dmodex_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
                        pmix_modex_cbfunc_t cbfunc, void *cbdata);
pmix_status_t publish_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
                         pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t lookup_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
                        size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata);
pmix_status_t unpublish_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
                           size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t spawn_fn(const pmix_proc_t *proc, const pmix_info_t job_info[], size_t ninfo,
                       const pmix_app_t apps[], size_t napps, pmix_spawn_cbfunc_t cbfunc,
                       void *cbdata);
pmix_status_t connect_fn(const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
                         size_t ninfo, pmix_op_cbfunc_t cbfunc, void *cbdata);
pmix_status_t disconnect_fn(const pmix_proc_t procs[], size_t nprocs,
                            const pmix_info_t info[], size_t ninfo, pmix_op_cbfunc_t cbfunc,
                            void *cbdata);
pmix_status_t job_control_handler(const pmix_proc_t *proc, const pmix_proc_t targets[],
                                  size_t ntargets, const pmix_info_t directives[],
                                  size_t ndirs, pmix_info_cbfunc_t cbfunc, void *cbdata);
void errhandler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);
void errhandler_reg_callbk(pmix_status_t status, size_t errhandler_ref, void *cbdata);

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
    .job_control = job_control_handler};



///////////////////////////////////////////////////////////////////////////////
// Regular okay code follows
///////////////////////////////////////////////////////////////////////////////

dragonError_t _dragon_pmix_set_fn_ptrs()
{

    // See if we've already locally loaded everything
    if (pmix_syms_loaded == 1UL)
        no_err_return(DRAGON_SUCCESS);

    // Otherwise, get the pointers loaded
    lib_pmix_handle = dlopen("libpmix.so", RTLD_LAZY | RTLD_GLOBAL);
    if (lib_pmix_handle == NULL)
        append_err_return(DRAGON_FAILURE, "Unable to open libpmix.so for PMIx code execution");
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

dragonPMIxCBData_t* _dragon_pmix_cbdata_constructor()
{
    dragonPMIxCBData_t *p = malloc(sizeof(dragonPMIxCBData_t));
    p->info = NULL;
    p->ninfo = 0;
    p->active = true;
    p->cbfunc = NULL;
    p->spcbfunc = NULL;
    p->cbdata = NULL;
    return p;

}


dragonError_t _dragon_pmix_cbdata_deconstructor(dragonPMIxCBData_t *p)
{
    if (NULL != p->info) {
        PMIx_Info_free_p(p->info, p->ninfo);
    }

    no_err_return(DRAGON_SUCCESS);
}

static void _mkdir(const char *dir) {
    char tmp[256];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp),"%s",dir);
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


void abcbfunc(pmix_status_t status, void *cbdata)
{

    dragonPMIxCBData_t *state = (dragonPMIxCBData_t *) cbdata;

    /* be sure to release the caller */
    if (NULL != state->cbfunc) {
        state->cbfunc(status, state->cbdata);
    }
    PMIX_RELEASE(state);
}

void opcbfunc(pmix_status_t status, void *cbdata)
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

pmix_status_t connect_fn(const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
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

pmix_status_t disconnect_fn(const pmix_proc_t procs[], size_t nprocs,
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

pmix_status_t spawn_fn(const pmix_proc_t *proc, const pmix_info_t job_info[], size_t ninfo,
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


pmix_status_t publish_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
                         pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_locdat_t *p;
    size_t n;

    for (n = 0; n < ninfo; n++) {
        p = malloc(sizeof(pmix_locdat_t));
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


pmix_status_t lookup_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
                        size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    (void) proc;
    (void) info;
    (void) ninfo;

    pmix_locdat_t *p, *p2;
    pmix_list_t *results;
    size_t i, n;
    pmix_pdata_t *pd = NULL;
    pmix_status_t ret = PMIX_ERR_NOT_FOUND;

    results = malloc(sizeof(pmix_list_t));

    for (n = 0; NULL != keys[n]; n++) {
        PMIX_LIST_FOREACH (p, &pubdata, pmix_locdat_t) {
            if (0 == strncmp(keys[n], p->pdata.key, PMIX_MAX_KEYLEN)) {
                p2 = malloc(sizeof(pmix_locdat_t));
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

pmix_status_t unpublish_fn(const pmix_proc_t *proc, char **keys, const pmix_info_t info[],
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
int _send_dmodex_request(const pmix_info_t info[], size_t ninfo,
                         pmix_modex_cbfunc_t cbfunc, void *cbdata,
                         const pmix_proc_t *proc) {


    // Update the request list
    //dragonPMIxRequest_t *req = malloc(sizeof(dragonPMIxRequest_t));

   //// // Update the latest tag:
    //dragonPMIxTracker_t *l_tracker = &d_tracker;

   //// // Set attributes
    //req->tag = l_tracker->latest_tag++;
    ////req->dest = proc->rank;
    //asprintf(&req->id, "%d_%d_%d", (int) REQ_DMODEX, proc->rank, l_tracker->latest_tag);
    //req->msg_type = REQ_DMODEX;
    //req->cbfunc = (void *) cbfunc;
    //req->cbdata = cbdata;

   //// // Add request to tracker list before sending
    //_add_request_to_tracker(req);

   //// // And send
    //char *filename;
    //char *tmp_buf;
    //asprintf(&filename, "%s/pmix_%s.msg", "/tmp", req->id);
    //asprintf(&tmp_buf, "trying to send dmodex request via file %s", filename);
    //_nick_logging(tmp_buf);
    //fprintf(stderr, "NICK: IF YOU SEE THIS, YOU NEED TO SEND A MESSAGE, YOU BIG DUMB DUMB.\n"); fflush(stderr);
    //free(filename);
    //free(tmp_buf);
    return 0;
}

// TODO: Implement dmodex via C implementation of queuue
pmix_status_t dmodex_fn(const pmix_proc_t *proc, const pmix_info_t info[], size_t ninfo,
                        pmix_modex_cbfunc_t cbfunc, void *cbdata)
{

//    for (int i = 0; i < ninfo; i++) {
//        char *tmp_str;
//        asprintf(&tmp_str, "dmodex_func request proc %d | nspace %s\n", proc->rank, proc->nspace);
//        _nick_logging(tmp_str);
//        free(tmp_str);
//    }
//
//    // Send the dmodex request
//    if (_send_dmodex_request(info, ninfo, cbfunc, cbdata, proc) == 0) {
//        // Send operation succeeded. We'll call the cbfunc upon receiving the response
//        return PMIX_SUCCESS;
//    } else {
    return PMIX_ERR_NOT_SUPPORTED;
//    }
}


void setup_cbfunc(pmix_status_t status, pmix_info_t info[], size_t ninfo,
                  void *provided_cbdata, pmix_op_cbfunc_t cbfunc, void *cbdata)
{

    dragonPMIxCBData_t *state = (dragonPMIxCBData_t *) provided_cbdata;
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


void _dragon_pmix_free_fence_request(void *arg)
{

    dragonPMIxRequest_t *req = (dragonPMIxRequest_t*) arg;
    if (req->node_list != NULL)
        free(req->node_list);

    if (req->fence_data != NULL)
        free(req->fence_data);

    free(req);

}

static void *_ddict_allgather(void *arg)
{

    dragonPMIxRequest_t *req = (dragonPMIxRequest_t *) arg;
    dragonDDictDescr_t *ddict = &pmix_ddict;
    dragonError_t derr;

    // My key is the op name and my node id:
    char *allgather_key;
    asprintf(&allgather_key, "fence_ag_%d", req->src);

    // Put my data into the dictionary:
    derr = _dragon_pmix_put_fence_msg(ddict, allgather_key,  req->fence_ndata, req->fence_data);
    free(allgather_key);
    if (derr != DRAGON_SUCCESS) {
        goto leave_thread;
    }

    // Now loop through getting data from all of my friends
    for (int i = 0; i < req->nnodes; i++) {
        // Do the ddict get.
        // TODO: randomize the key selection. I don't want everyone getting the exact same key at the same time leading to
        //       a poorly created DDoS.

        if (i != req->src) {
            char *new_data;
            size_t new_ndata;
            asprintf(&allgather_key, "fence_ag_%d", i);
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
    }

    pmix_modex_cbfunc_t cbfunc = (pmix_modex_cbfunc_t) req->cbfunc;
    cbfunc(PMIX_SUCCESS, req->fence_data, req->fence_ndata, req->cbdata, _dragon_pmix_free_fence_request, req);

    // Checkpoint the dictionary
    dragon_ddict_checkpoint(ddict);

leave_thread:
    return NULL;
}

dragonPMIxRequest_t *_dragon_pmix_init_fence_request(dragonPMIxServer_t *ref_server,
                                                     const pmix_proc_t procs[],
                                                     size_t nprocs,
                                                     char *data,
                                                     size_t ndata,
                                                     pmix_modex_cbfunc_t cbfunc,
                                                     void *cbdata)
{

    // Initialize the request data structure
    dragonPMIxRequest_t *req = malloc(sizeof(dragonPMIxRequest_t));

    req->id = NULL;
    //req->info = NULL;
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

        // If a wildcard, we need a response from everyone
        if (is_wildcard) {
            req->nnodes = ref_server->nnodes; // Exclude myself
            req->node_list = malloc(req->nnodes * sizeof(int));
            for (int i = 0; i < ref_server->nnodes; i++) {
                if (i != ref_server->node_rank) {
                    req->node_list[i] = i;
                }
            }
        }
        // Otherwise, handle it on a process by process basis -- if we're given proc ranks, we may need to map this
        // to nodes hosting the given rank. Leaving it this way for the moment.
        else {
            req->nnodes = (int) nprocs;
            req->node_list = malloc(req->nnodes * sizeof(int));
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
        memcpy(req->fence_data, data, data_size);
    }

    // Lastly, store our node rank for the pthread to use in the dict
    req->src = ref_server->node_rank;

    return req;
}



dragonError_t _dragon_pmix_ddict_allgather(dragonPMIxServer_t *ref_server,
                                           pmix_proc_t *procs,
                                           size_t nprocs,
                                           char *data,
                                           size_t ndata,
                                           pmix_modex_cbfunc_t cbfunc,
                                           void *cbdata)
{

    dragonPMIxRequest_t *req = _dragon_pmix_init_fence_request(ref_server, procs, nprocs, data, ndata, cbfunc, cbdata);


    // Start the pthread that will call the cbfunc when complete
    int err = pthread_create(&(req->req_thread),  NULL, _ddict_allgather, (void *) req);

    if (err != 0) {
        append_err_return(DRAGON_FAILURE, "Unable to start PMIx allgather pthread");
    }
    else {
        no_err_return(DRAGON_SUCCESS);
    }

}

pmix_status_t
fence_handler(
    const pmix_proc_t procs[], size_t nprocs, const pmix_info_t info[],
    size_t ninfo, char *data, size_t ndata, pmix_modex_cbfunc_t cbfunc,
    void *cbdata)
{

    // We only support PMIX_COLLECT_DATA
    int64_t *ranks = (int64_t *) malloc(nprocs * sizeof(uint64_t));
    for (size_t idx = 0; idx < nprocs; idx++) {
        ranks[idx] = (int64_t) procs[idx].rank;
    }
    for (size_t i = 0; i < ninfo; i++) {
        if (!strcmp(info[i].key, PMIX_COLLECT_DATA)) {
            continue;
        }
        if (info[i].flags & PMIX_INFO_REQD) {
            return PMIX_ERR_NOT_SUPPORTED;
        }
    }

    // Prepare the data request going to LS and send it off
    char nspace[PMIX_MAX_NSLEN+1];
    strncpy(nspace, procs[0].nspace, PMIX_MAX_NSLEN + 1);
    dragonError_t derr = _dragon_pmix_ddict_allgather(ref_server, (pmix_proc_t*) procs, nprocs, data, ndata, cbfunc, cbdata);
    if (derr != DRAGON_SUCCESS) {
        return PMIX_ERROR;
    }

    // We'll call their callback once everything is complete
    return PMIX_SUCCESS;
}

pmix_status_t connected(const pmix_proc_t *proc, void *server_object, pmix_op_cbfunc_t cbfunc,
                        void *cbdata)
{
    (void) proc;
    (void) server_object;

    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}


pmix_status_t finalized(const pmix_proc_t *proc, void *server_object,
                               pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    (void) server_object;
    --wakeup;

    /* ensure we call the cbfunc so the proc can exit! */
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
}

pmix_status_t abort_fn(const pmix_proc_t *proc, void *server_object, int status,
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

static int _add_local_peers(dragonPMIxServer_t *d_server,
                            int base_rank,
                            int ppn,
                            char **local_peers)
    {

    const uint nchars = 9; // Assume 7 chars per rank
    size_t len = ppn * (sizeof(char) * nchars);
    char *peers = malloc(len);

    int n = snprintf(peers, len, "%d", base_rank);
    for (int r = 1; r < ppn; r++) {
        n += snprintf(peers + n, len - n, ",%d", base_rank + r);
    }
    *local_peers = peers;
    return 0;
}


void _get_pmix_tmpdir(dragonPMIxServer_t *d_server, char *tmp_base)
{
    struct stat buf;

    /* define and pass a personal tmpdir to protect the system */
    if (0 > asprintf(&d_server->tmpdir, "%s/pmix.%lu/%s", tmp_base, (long unsigned) d_server->uid, d_server->hostname)) {
        exit(1);
    }

    if (0 > asprintf(&d_server->nsdir_global, "%s/pmix.%lu/%s/%s", tmp_base, (long unsigned) d_server->uid, d_server->hostname, "nsdir-global")) {
        exit(1);
    }

    if (0 > asprintf(&d_server->nsdir_local, "%s/pmix.%lu/%s/%s", tmp_base, (long unsigned) d_server->uid, d_server->hostname, "nsdir-local")) {
        exit(1);
    }

    /* create the directory */
    if (0 != stat(d_server->tmpdir, &buf)) {
        _mkdir(d_server->tmpdir);
    }

    if (0 != stat(d_server->nsdir_global, &buf)) {
        _mkdir(d_server->nsdir_global);
    }

    if (0 != stat(d_server->nsdir_local, &buf)) {
        _mkdir(d_server->nsdir_local);
    }


}

dragonError_t _dragon_init_pmix_server(dragonPMIxServer_t **d_server,
                             char *tmp_space,
                             char *server_nspace,
                             char *client_nspace) {


    *d_server = malloc(sizeof(dragonPMIxServer_t));
    if (*d_server == NULL) {
        err_return(DRAGON_FAILURE, "Unable to malloc PMIx data struct");
    }

    (*d_server)->cleanup = NULL;
    (*d_server)->tmpdir = NULL;
    (*d_server)->nsdir_global = NULL;
    (*d_server)->nsdir_local = NULL;

    (*d_server)->uid = getuid();
    (*d_server)->gid = getgid();

    // Fill the hostname
    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, sizeof(hostname)) == -1) {
        err_return(DRAGON_FAILURE, "Unable to use gethostname() to get PMIx server hostname");
    }
    asprintf(&(*d_server)->hostname, "%s", hostname);

    // Fill the tmp dir
    _get_pmix_tmpdir(*d_server, tmp_space);

    // Give some name to the server namespace
    asprintf(&(*d_server)->server_nspace, "%s", server_nspace);
    asprintf(&(*d_server)->client_nspace, "%s", client_nspace);
    (*d_server)->node_rank = 0;


    return 0;
}


dragonError_t _dragon_pmix_load_info(dragonPMIxServer_t *d_server,
                                     int node_rank,
                                     pmix_info_t **info,
                                     size_t *ninfo) {

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
    d_server->node_rank = node_rank;
    PMIx_Info_load_p(&linfo[lninfo], PMIX_SERVER_RANK, &d_server->node_rank, PMIX_PROC_RANK);
    lninfo++;

    *info = linfo;
    *ninfo = lninfo;
    no_err_return(DRAGON_SUCCESS);

}


void errhandler(size_t evhdlr_registration_id, pmix_status_t status,
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

void errhandler_reg_callbk(pmix_status_t status, size_t errhandler_ref, void *cbdata)
{
    (void) status;
    (void) errhandler_ref;
    (void) cbdata;
    return;
}


void _add_hosts(int nhosts,
               char **hosts,
               char **node_map)
{
    size_t space = 0;
    pmix_status_t rc;
    char *nodes = NULL;

    // Concat the lists into a single string. Get total length of concat'ed string
    for (int h = 0; h < nhosts; h++) {
        space += strlen(hosts[h]) + 2; // comma and trailing nul
    }
    nodes = malloc(space * sizeof(char));
    int n = snprintf(nodes, space, "%s", hosts[0]);
    for (int h = 1; h < nhosts; h++) {
        n += snprintf(nodes + n, space - n, ",%s", hosts[h]);
    }

    rc = PMIx_generate_regex_p(nodes, node_map);
    assert(PMIX_SUCCESS == rc);

    free(nodes);
}


void _add_ppn(int nhosts, int nproc, int ippn, char **ppn)
{
    const uint nchars = 12;
    uint32_t *count = calloc(nhosts, sizeof(uint32_t));
    size_t tlen = 0;
    char **arrays, *procs, *str = NULL;
    pmix_status_t rc;

    // Put together an array with how many proccesses are assigned to a node
    // with subscript defined as node rank
    for (int r = 0; r < nhosts; r++) {
        count[r] = ippn;
    }

    // Array for holding a node's ranks represented as a list
    // of comma-separeted ranks
    arrays = calloc(nhosts, sizeof(char *));

    // Create a string for each host of the ranks on it
    //for (int r = 0, n = 0; r < app->num_pes; r++) {
    int lppn_count = 0;
    int node_count = 0;
    for (int r = 0, n = 0; r < nproc; r++) {

        // Reset ppn count if we've filled up a node and
        // go to next node
        if (lppn_count == ippn) {
            lppn_count = 0;
            node_count++;
        }

        // Create or add to string hold ranks on a node
        if (!arrays[node_count]) {
            tlen = count[node_count] * nchars;
            str = arrays[node_count] = malloc(tlen * sizeof(char));
            n = snprintf(str, tlen, "%d", r);
        } else {
            assert(str);
            n += snprintf(str + n, tlen - n, ",%d", r);
        }

        // Increment ppn counter
        lppn_count++;
    }

    // Concatenate all the node ranks into a single string with nodes
    // separated by a semi-colon
    for (int h = 0; h < nhosts; h++) {
        assert(arrays[h]);
        tlen += strlen(arrays[h]) + 1; // Add semicolon or trailing nul
    }
    procs = malloc(tlen * sizeof(char));

    assert(arrays[0]);
    int n = snprintf(procs, tlen, "%s", arrays[0]);
    free(arrays[0]);
    for (int h = 1; h < nhosts; h++) {
        assert(arrays[h]);
        n += snprintf(procs + n, tlen - n, ";%s", arrays[h]);
        free(arrays[h]);
    }

    // Turn it into PMIx's internal format
    rc = PMIx_generate_ppn_p(procs, ppn);
    assert(PMIX_SUCCESS == rc);

    free(arrays);
    free(procs);
}


dragonError_t _dragon_initialize_application_server(dragonPMIxServer_t *d_server,
                                                    int nhosts,
                                                    int nproc,
                                                    int i_ppn,
                                                    char **hosts)
{
    dragonError_t derr;
    size_t ninfo = 2;
    pmix_status_t rc;
    char *node_map, *c_ppn;
    pmix_info_t *info = calloc(ninfo, sizeof(pmix_info_t));

    _add_hosts(nhosts, hosts, &node_map);
    PMIx_Info_load_p(&info[0], PMIX_NODE_MAP, node_map, PMIX_STRING);

    _add_ppn(nhosts, nproc, i_ppn, &c_ppn);
    PMIx_Info_load_p(&info[1], PMIX_PROC_MAP, c_ppn, PMIX_STRING);

    // Set up application server
    dragonPMIxCBData_t *state = _dragon_pmix_cbdata_constructor();
    if (PMIX_SUCCESS
        != (rc = PMIx_server_setup_application_p(d_server->client_nspace, info, ninfo, setup_cbfunc, state))) {

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
    dragonDDictDescr_t *ddict = &pmix_ddict;
    char *local_info_key = LOCAL_INFO_KEY_DEF;
    bool persist = true;
    derr = _dragon_pmix_write_to_ddict(ddict, persist, local_info_key, encoded, strlen(encoded) * sizeof(char));
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

#define INFO_SPOTS 8 // # of fixed attributes in the top level collection
#define RANK_SPOTS 5 // # of attributes for each per-rank collection
#define HOST_SPOTS 8 // # of attributes for each host/node
dragonError_t _configure_nspace(dragonPMIxServer_t *d_server,
                                int nproc,
                                int nhosts,
                                int ppn,
                                char **hostnames)
{

    // Store this info in our server struct
    d_server->nprocs = nproc;
    d_server->nnodes = nhosts;

    // Set how many parameters are going to be in our info array:
    int ninfo = INFO_SPOTS + nproc + nhosts;

    // Define an info array. We'll call it spot and info for reasons
    pmix_info_t *spot, *info;
    spot = info = calloc(ninfo, sizeof(pmix_info_t));

    // Parameters to be used later
    char *node_map, *c_ppn;
    char *local_peers = NULL;

    // Create string of global ranks on this specific node
    int base_rank = ppn * d_server->node_rank;
    _add_local_peers(d_server, base_rank, ppn, &local_peers);


    // Start loading things that's the same for all processes in MPI_COMM_WORLD
    PMIx_Info_load_p(spot, PMIX_UNIV_SIZE, &nproc, PMIX_UINT32);
    spot++;

    uint32_t spawned = 0;
    PMIx_Info_load_p(spot, PMIX_SPAWNED, &spawned, PMIX_UINT32);
    spot++;

    // This can maybe be the same name space at the system server. It's unclear
    PMIx_Info_load_p(spot, PMIX_NSPACE, d_server->server_nspace, PMIX_STRING);
    spot++;


    _add_hosts(nhosts, hostnames, &node_map);
    PMIx_Info_load_p(spot, PMIX_NODE_MAP, node_map, PMIX_STRING);
    free(node_map);
    spot++;

    _add_ppn(nhosts, nproc, ppn, &c_ppn);
    PMIx_Info_load_p(spot, PMIX_PROC_MAP, c_ppn, PMIX_STRING);
    free(c_ppn);
    spot++;

    PMIx_Info_load_p(spot, PMIX_JOB_SIZE, &nproc, PMIX_UINT32);
    spot++;

    PMIx_Info_load_p(spot, PMIX_TMPDIR, d_server->tmpdir, PMIX_STRING);
    spot++;

    PMIx_Info_load_p(spot, PMIX_NSDIR, d_server->nsdir_global, PMIX_STRING);
    spot++;

    // Load array of attributes that are unique to each individual rank in MPI_COMM_WORLD
    // and add them to the spot pointer
    uint16_t node_rank = 0;
    uint16_t local_rank = 0;
    int node_rank_counter = 0;
    for (int32_t global_rank = 0; global_rank < nproc; global_rank++) {

        pmix_info_t *iter;
        pmix_data_array_t *darray = calloc(1, sizeof(pmix_data_array_t));
        darray->type = PMIX_INFO;
        iter = darray->array = calloc(RANK_SPOTS, sizeof(pmix_info_t));

        PMIx_Info_load_p(iter, PMIX_RANK, &global_rank, PMIX_PROC_RANK);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODEID, &node_rank, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_GLOBAL_RANK, &global_rank, PMIX_PROC_RANK);
        iter++;

        // If this proc will be on my node, I need to set stuff local/node ranks up
        if (d_server->node_rank == (int) node_rank) {
            uint16_t l = (uint16_t) local_rank;
            local_rank++;

            PMIx_Info_load_p(iter, PMIX_LOCAL_RANK, &l, PMIX_UINT16);
            iter++;

            PMIx_Info_load_p(iter, PMIX_NODE_RANK, &l, PMIX_UINT16);
            iter++;
        }


        // Point the "spot" pointer to this data array of crap
        strncpy(spot->key, PMIX_PROC_INFO_ARRAY, PMIX_MAX_KEYLEN);
        spot->value.type = PMIX_DATA_ARRAY;
        darray->size = iter - (pmix_info_t *) darray->array;
        spot->value.data.darray = darray;
        spot++;
        assert(RANK_SPOTS >= darray->size);

        // Increment our counters and begin the loop anew
        node_rank_counter++;
        if (node_rank_counter == ppn) {
            node_rank_counter = 0;
            node_rank++;
        }
    }


    for (int32_t host_rank = 0; host_rank < nhosts; host_rank++) {

        pmix_data_array_t *darray = calloc(1, sizeof(pmix_data_array_t));
        darray->array = calloc(HOST_SPOTS, sizeof(pmix_info_t));
        pmix_info_t *iter = darray->array;

        darray->type = PMIX_INFO;

        PMIx_Info_load_p(iter, PMIX_HOSTNAME, hostnames[host_rank], PMIX_STRING);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODEID, &host_rank, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_NODE_SIZE, &ppn, PMIX_UINT32);
        iter++;

        PMIx_Info_load_p(iter, PMIX_LOCAL_SIZE, &ppn, PMIX_UINT32);
        iter++;

        // Allocate extra stuff if this node is my own
        if (d_server->node_rank == host_rank) {
            PMIx_Info_load_p(iter, PMIX_TMPDIR, d_server->tmpdir, PMIX_STRING);
            iter++;

            PMIx_Info_load_p(iter, PMIX_NSDIR, d_server->nsdir_local, PMIX_STRING);
            iter++;

            PMIx_Info_load_p(iter, PMIX_LOCAL_PEERS, local_peers, PMIX_STRING);
            iter++;

            pmix_proc_t *procs = malloc(ppn * sizeof(pmix_proc_t));
            for (int p = 0; p < ppn; p++) {
                strncpy(procs[p].nspace, d_server->client_nspace, PMIX_MAX_NSLEN + 1);
                // This assumes same number of procs per node
                procs[p].rank = base_rank + p;
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
    if (PMIX_SUCCESS
      != (rc = PMIx_server_register_nspace_p(d_server->client_nspace, ppn, info, spot-info, opcbfunc, state))) {
        append_err_return(DRAGON_FAILURE, "PMIx: failed to register nspace!\n");
    }
    PMIX_WAIT_FOR_COMPLETION(state->active);
    _dragon_pmix_cbdata_deconstructor(state);

    free(info);

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t _dragon_configure_local_support(dragonPMIxServer_t *d_server)
{

    dragonDDictDescr_t *ddict = &pmix_ddict;
    dragonError_t derr;
    pmix_info_t *info;
    size_t ninfo;

    // Read the application info from the ddict
    char *local_info_key = LOCAL_INFO_KEY_DEF;
    char *tmp, *b64_data;
    size_t data_len;
    derr = _dragon_pmix_read_from_ddict(ddict, local_info_key, &tmp, &data_len);

    // Make sure there's a null terminator at end of data
    b64_data = malloc((data_len + 1) * sizeof(char));
    memcpy(b64_data, tmp, data_len);
    b64_data[data_len] = '\0';
    free(tmp);

    if (derr != DRAGON_SUCCESS) {
        append_err_return(DRAGON_FAILURE, "Unable to read data from dict for PMIx local app support");
    }

    // Get the data from the json byte array
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

    // Convert from BLOB to an array of Infos
    rc = PMIx_Data_unpack_p(NULL, &pbuf, info, &count, PMIX_INFO);
    if (PMIX_SUCCESS != rc) {
        PMIx_Info_free_p(info, ninfo);
        append_err_return(DRAGON_FAILURE, "Failed to unpack PMIx buffer's info array");
    }


    // And finally use it to set everything up.
    dragonPMIxCBData_t *state = _dragon_pmix_cbdata_constructor();
    if (PMIX_SUCCESS != (rc = PMIx_server_setup_local_support_p(d_server->client_nspace, info,
                                                              ninfo, opcbfunc, state))) {
        PMIx_server_finalize_p();
        append_err_return(DRAGON_FAILURE, "Could not setup local server support for PMIx");
    }
    PMIX_WAIT_FOR_COMPLETION(state->active);
    _dragon_pmix_cbdata_deconstructor(state);

    PMIx_Info_free_p(info, ninfo);
    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_initialize_pmix_server(dragonPMIxServer_t **d_server,
                                            char *pmix_sdesc,
                                            char *local_mgr_sdesc,
                                            dragonChannelSerial_t out_to_ls,
                                            dragonChannelSerial_t in_from_ls,
                                            dragonChannelSerial_t buffered_from_ls,
                                            int node_rank,
                                            int nhosts,
                                            int nprocs,
                                            int ppn,
                                            char **hosts,
                                            char *server_nspace,
                                            char *client_nspace,
                                            char *tmp_space)
{

    dragonError_t derr;
    pmix_status_t rc;
    dragonPMIxServer_t *l_server;

    // First things first, dlopen all the pmix function pointers we need
    derr = _dragon_pmix_set_fn_ptrs();
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to access PMIx function pointers");

    // Attach to the ddict
    timespec_t *timeout = NULL;
    derr = _dragon_ddict_attach(pmix_sdesc,
                                &pmix_ddict,
                                timeout,
                                &out_to_ls,
                                &in_from_ls,
                                &buffered_from_ls,
                                local_mgr_sdesc);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to attach to dictionary");

    derr = _dragon_init_pmix_server(d_server, tmp_space, server_nspace, client_nspace);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "Failed to initialize dragon struct for PMIx server");
    }
    l_server = *d_server;
    ref_server = *d_server;

    pmix_info_t *info = NULL;
    size_t ninfo;
    derr = _dragon_pmix_load_info(l_server, node_rank, &info, &ninfo);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "Failed to load server info into PMIx server namespace");
    }

    /* setup the server library */
    if (PMIX_SUCCESS != (rc = PMIx_server_init_p(&dragon_pmix_cback_module, info, ninfo))) {
        append_err_return(DRAGON_FAILURE, "PMIx_server_init failed");
    }
    PMIx_Info_free_p(info, ninfo);

    /* register the errhandler */
    if (PMIX_SUCCESS != (rc = PMIx_Register_event_handler_p(NULL, 0, NULL, 0, errhandler, errhandler_reg_callbk, NULL))) {
        append_err_return(DRAGON_FAILURE, "PMIx_Register_event_handler failed with error");
    }

    // If I'm the 0th node, set u]p the application server
    if (l_server->node_rank == 0) {
        derr = _dragon_initialize_application_server(l_server, nhosts, nprocs, ppn, hosts);
        if (derr != DRAGON_SUCCESS) {
            append_err_return(derr, "Failed to initialize the PMIx application server on node rank 0");
        }
    }

    info = NULL;
    wakeup = ppn;
    derr = _configure_nspace(l_server, nprocs, nhosts, ppn, hosts);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "PMIx: Failed to configure client namespace");
    }

    // Set up local support before forking
    derr = _dragon_configure_local_support(l_server);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to configure PMIx local support as defined by node rank 0");

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t dragon_pmix_get_client_env(dragonPMIxServer_t *d_server,
                                         int rank,
                                         char ***env,
                                         int *nenv)
{
    /* fork/exec the test for this node*/
    dragonError_t derr;
    pmix_status_t rc;
    dragonPMIxCBData_t *d_x;
    pmix_proc_t proc;
    local_rank_t x;

    // Make sure our function pointers are defined as a just-in-case
    derr = _dragon_pmix_set_fn_ptrs();
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to access PMIx function pointers");


    PMIx_Load_nspace_p(proc.nspace, d_server->client_nspace);
    x.rank = rank;
    proc.rank = rank;
    if (PMIX_SUCCESS != (rc = PMIx_server_setup_fork_p(&proc, env))) { // n
        PMIx_server_finalize_p();
        system(d_server->cleanup);
        append_err_return(DRAGON_FAILURE, "PMIx server fork setup failed");
    }

    // Get the length:
    if (*env) {
        int spot = 0;
        while ((*env)[spot]) spot++;
        *nenv = spot;
    }

    d_x = _dragon_pmix_cbdata_constructor();
    if (PMIX_SUCCESS
        != (rc = PMIx_server_register_client_p(&proc, d_server->uid, d_server->gid, &x, opcbfunc, d_x))) {
        PMIx_server_finalize_p();
        system(d_server->cleanup);
        append_err_return(DRAGON_FAILURE, "PMIx server failed to register client");
    }
    /* don't fork/exec the client until we know it is registered
     * so we avoid a potential race dition in the server */
    PMIX_WAIT_FOR_COMPLETION(d_x->active);
    _dragon_pmix_cbdata_deconstructor(d_x);

    return DRAGON_SUCCESS;

}


dragonError_t dragon_pmix_finalize_server(dragonPMIxServer_t *d_server)
{

    pmix_status_t rc;
    PMIx_Deregister_event_handler_p(0, NULL, NULL);


    /* finalize the server library */
    if (PMIX_SUCCESS != (rc = PMIx_server_finalize_p())) {
        append_err_return(DRAGON_FAILURE, "Unable to finalize PMIx server");
    }

    system(d_server->cleanup);

    return DRAGON_SUCCESS;
}

#endif  // HAVE_PMIX_INCLUDE