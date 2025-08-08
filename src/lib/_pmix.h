#ifndef _DRAGON_PMIX_H_
#define _DRAGON_PMIX_H_

#include "err.h"
#include <dragon/messages_api.h>
#include <dragon/channels.h>
#include <dragon/fli.h>
#include <dragon/ddict.h>

#define LOCAL_INFO_KEY_DEF "local_app_info"
#define DRAGON_PMIX_RANK_WILDCARD UINT32_MAX-2    // Match to be the same as pmix_common.h
#define PMIX_WAIT_FOR_COMPLETION(a) \
do {                            \
while ((a)) {               \
    usleep(10);             \
}                           \
PMIX_ACQUIRE_OBJECT((a));   \
} while (0)


// Definitions for c++ messaging I need to expose to my C API
dragonError_t _dragon_pmix_send_msg();
dragonError_t _dragon_pmix_recv_msg();


typedef enum {
    REQ_COLLECTIVE,     // For handling PMIx_Fence
    REQ_COLLECTIVE_ACK, // For handling PMIx_Fence ACK traffic
    REQ_SETUP,          // Broadcasting global settings
    REQ_SPAWN,          // Request to spawn a new app
    REQ_DMODEX,         // Request the keys from a specific remote rank
    REQ_DMODEX_ACK,     // Return the keys from a remote rank
    REQ_PUBLISH,        // For handling PMIx_Publish
    REQ_LOOKUP,         // For handling PMIx_Lookup
    REQ_UNPUBLISH       // For handling PMIx_Unpublish
} dragonPMIxReqType_t;

typedef struct dragonPMIxRequest_st {
    char *id; // the unique identifier for this request: '{original src}{original destination}{int tag}'
    int tag;  // tag identifying initial request
    int dest;  // Node we need response from for dmodex
    int src;  // Node requesting fence or dmodex op
    int ninfo;  // # info bits for dmodex request
    int nnodes; // Number of procs/nodes we need responses from for a collective
    int *node_list; // List of node IDs we need data from
    int checked_in; // Number of procs/nodes that have checked in
    size_t fence_ndata;  // Data size for collective ops
    char *fence_data; // pointer we concat collective responses to
    pthread_t req_thread;
    dragonPMIxReqType_t msg_type;
    void *cbfunc;  // Response function PMIx needs exec'd when work is done
    void *cbdata;  // Response data to be passed to cbfunc at completion of work
    struct dragonPMIxRequest_st *next;
    struct dragonPMIxRequest_st *prev;
} dragonPMIxRequest_t;

typedef struct dragonPMIxTracker_st {
    int latest_tag;
    int active;
    char *glob_req_pattern, *glob_ack_pattern;
    dragonPMIxRequest_t *head;
    dragonPMIxRequest_t *tail;
} dragonPMIxTracker_t;


typedef struct local_rank {
    int rank;            // Global rank number
    pid_t pid;             // Process ID
    int stdin_fd;          // stdin pipe write fd
    int stdout_fd;         // stdout pipe read fd
    int stderr_fd;         // stderr pipe read fd
    bool barrier;          // Has this rank reached the startup barrier?
} local_rank_t;


typedef struct dragonPMIxServer_st {
    char *tmpdir, *cleanup, *hostname, *nsdir_global, *nsdir_local, *wdir;
    char *server_nspace, *client_nspace;
    uid_t uid;
    gid_t gid;
    pid_t pid;
    int node_rank;
    size_t nprocs;
    size_t nnodes;
    volatile uint64_t tracker_lock;
    pthread_t tracker_thread;
    dragonChannelDescr_t ch_in, ch_out;
    dragonFLIDescr_t fli_in, fli_out;
    dragonPMIxTracker_t tracker;
} dragonPMIxServer_t;

//typedef struct {
//    pmix_list_item_t super;
//    pid_t pid;
//} wait_tracker_t;
//static PMIX_CLASS_INSTANCE(wait_tracker_t, pmix_list_item_t, NULL, NULL);

#ifdef __cplusplus
extern "C" {
#endif


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
                                            char *tmp_space);

dragonError_t dragon_pmix_get_client_env(dragonPMIxServer_t *d_server,
                                         int rank,
                                         char ***env,
                                         int *nenv);
dragonError_t dragon_pmix_finalize_server(dragonPMIxServer_t *d_server);

// Internal functions for using c++ code from PMIx C code
dragonError_t _dragon_pmix_send_hello_msg(dragonPMIxServer_t *l_server);
dragonError_t _dragon_pmix_send_ls_fence(dragonPMIxServer_t *d_server,
                                         uint64_t tag, char *nspace, uint64_t nranks, int64_t *ranks,
                                         size_t ndata, char *data);
dragonError_t _dragon_pmix_write_to_ddict(dragonDDictDescr_t * ddict, bool persist, char key[], char* val, size_t byte_len);
dragonError_t _dragon_pmix_read_from_ddict(dragonDDictDescr_t * ddict, char key[], char **val, size_t *recv_sz);
dragonError_t _dragon_pmix_put_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t ndata, char *data);
dragonError_t _dragon_pmix_get_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t *ndata, char **data);

#ifdef __cplusplus
}
#endif


#endif // _DRAGON_PMIX_H_