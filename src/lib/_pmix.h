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
    dragonDDictDescr_t *ddict;
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

#ifdef __cplusplus
extern "C" {
#endif


dragonError_t
dragon_pmix_initialize_job(dragonG_UID_t guid,
                           char *pmix_sdesc,
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
                           char *server_tmpdir);

dragonError_t
dragon_pmix_get_client_env(dragonG_UID_t guid,
                           int rank,
                           char ***env,
                           int *nenv);

bool
dragon_pmix_is_server_host(dragonG_UID_t guid);

dragonError_t
dragon_pmix_finalize_job(dragonG_UID_t guid);

dragonError_t
dragon_pmix_finalize_server();

#ifdef __cplusplus
}
#endif


#endif // _DRAGON_PMIX_H_