#ifndef HAVE_DRAGON_DDICT_INTERNAL_H
#define HAVE_DRAGON_DDICT_INTERNAL_H

#include "umap.h"
#include <dragon/fli.h>

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
    DRAGON_DDICT_CONTAINS_REQ,
    DRAGON_DDICT_FINALIZED
} dragonDDictReqType_t;

typedef struct dragonDDict_st {
    dragonDDictSerial_t ser; // stored for easy access by serialize call.
    dragonFLIDescr_t orchestrator_fli; // FLI handle for orchestrator messages
    dragonFLIDescr_t * manager_flis; // FLI handles for managers
    dragonFLIDescr_t respFLI; // This handles non-buffered, streaming responses to requests
    dragonFLIDescr_t bufferedRespFLI; // This handles buffered responses to requests
    char* respFLIStr; // Needed for messaging between client and managers/orch.
    char* bufferedRespFLIStr; // Needed for message between client and managers/orch.
    uint64_t clientID;
    size_t num_managers;
    dragonULInt dd_uid; // UID for umap storage
} dragonDDict_t;

typedef struct dragonDDictReq_st {
    dragonDDict_t* ddict;
    dragonULInt dd_uid;
    size_t buffer_size;
    dragonDDictBufAlloc_t * buffered_allocs; // Linked list buffer for key
    uint8_t * key_data; // Hold onto key data (may be useful later, free on finalize)
    dragonULInt key_hash; // Hold onto key hash
    dragonDDictReqType_t op_type; // What operation type for error checking
    dragonFLISendHandleDescr_t sendh; // DDict manager send handle
    dragonFLIRecvHandleDescr_t recvh; // DDict manager receive handle
} dragonDDictReq_t;


#ifdef __cplusplus
}
#endif

#endif