#ifndef HAVE_DRAGON_DDICT_INTERNAL_H
#define HAVE_DRAGON_DDICT_INTERNAL_H

#include <unordered_map>
#include <vector>
#include <string>
#include "umap.h"
#include <dragon/fli.h>
#include <dragon/ddict.h>
#include <dragon/managed_memory.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_DDICT_UMAP_SEED 1776

// Struct to handle buffering keys before sending
typedef struct dragonDDictBufAlloc_st {
    uint8_t * data;
    size_t num_bytes;
    struct dragonDDictBufAlloc_st * next;
} dragonDDictBufAlloc_t;

/**
 * @brief An enum DDict request types
 *
*/
typedef enum dragonDDictReqType_st {
    DRAGON_DDICT_NO_OP,
    DRAGON_DDICT_GET_REQ,
    DRAGON_DDICT_PUT_REQ,
    DRAGON_DDICT_BATCH_PUT_REQ,
    DRAGON_DDICT_B_PUT_BATCH_REQ,
    DRAGON_DDICT_B_PUT_REQ,
    DRAGON_DDICT_CONTAINS_REQ,
    DRAGON_DDICT_POP_REQ,
    DRAGON_DDICT_LENGTH_REQ,
    DRAGON_DDICT_CONNECT_MANAGER_REQ,
    DRAGON_DDICT_FINALIZED
} dragonDDictReqType_t;

class dragonDDict_t {

    public:
    dragonDDict_t(const char * dd_ser, timespec_t * default_timeout);

    std::string ddict_ser; // (serialized_orc)
    dragonFLIDescr_t orchestrator_fli; // FLI handle for orchestrator messages, (orc_connector)

    std::unordered_map<uint64_t, dragonFLIDescr_t> manager_table; // (managers)
    // manager_nodes is a python only object, and is omitted here
    uint64_t tag;
    uint64_t chkpt_id;
    bool detached;
    timespec_t * timeout;
    timespec_t timeout_val;

    dragonMemoryPoolDescr_t pool; // Default pool for memory allocations (for keys)
    dragonChannelDescr_t strm_ch; // Stream channel for send and receive handle. (stream_channel)
    dragonFLIDescr_t respFLI; // This handles non-buffered, streaming responses to requests
    std::string respFLIStr; // Needed for messaging between client and managers/orch.
    dragonFLIDescr_t bufferedRespFLI; // This handles buffered responses to requests
    std::string bufferedRespFLIStr; // Needed for message between client and managers/orc.

    uint64_t clientID;
    bool has_local_manager;
    uint64_t local_manager;
    uint64_t main_manager;
    bool has_chosen_manager;
    uint64_t chosen_manager;
    dragonFLIDescr_t main_manager_fli; // FLI handle for our main manager

    size_t num_managers;
    dragonULInt hostid;

    std::vector<uint64_t> local_managers;

    dragonULInt dd_uid; // UID for umap storage

    // batch put
    bool batch_put_started;
    bool batch_persist;
    std::set<uint64_t> batch_put_msg_tags;
    std::unordered_map<uint64_t, uint64_t> num_batch_puts;
    std::unordered_map<uint64_t, dragonChannelDescr_t*> batch_put_stream_channels;
    std::unordered_map<uint64_t, dragonFLISendHandleDescr_t*> opened_send_handles;

    // bput (broadcast put) with batch
    dragonChannelDescr_t bput_strm;
    bool has_bput_root_manager_sendh;
    dragonFLISendHandleDescr_t bput_root_manager_sendh;
    dragonChannelDescr_t bput_resp_strm;
    dragonFLIDescr_t bput_respFLI;
    std::string bput_respFLIStr;
    uint64_t bput_tag;
    uint64_t num_bputs;
};

// can be a class
typedef struct dragonDDictReq_st {
    dragonDDict_t* ddict;
    dragonULInt dd_uid;
    dragonMemoryDescr_t key_mem; // Key data that was buffered
    dragonULInt key_hash; // Hold onto key hash
    unsigned char* key_data;
    size_t key_size;
    dragonDDictReqType_t op_type; // What operation type for error checking
    dragonFLIDescr_t manager_fli; // Manager this request is tied to
    uint64_t manager_id;
    uint64_t msg_tag;
    dragonFLISendHandleDescr_t sendh; // Manager handle to send messages and data to
    dragonFLIRecvHandleDescr_t recvh; // DDict channel response handle
    dragonFLISendHandleDescr_t key_sendh; // temp send handle used for buffering the key
    size_t num_writes; // Number of nodes in buffered_allocs;
    bool recvh_closed;
    bool free_mem;
    bool free_key_mem;
} dragonDDictReq_t;

#ifdef __cplusplus
}
#endif

#endif