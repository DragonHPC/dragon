#ifndef HAVE_DRAGON_QUEUE
#define HAVE_DRAGON_QUEUE

#include <dragon/return_codes.h>
#include <dragon/global_types.h>
#include <dragon/managed_memory.h>
#include <dragon/bcast.h>

#include <stdint.h>
#include <stdbool.h>
#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_QUEUE_DEFAULT_MAXSIZE 100
#define DRAGON_QUEUE_DEFAULT_BLOCKSIZE 65536

/**
*  Specifies where to locate resources at creation time.
**/
typedef enum dragonAffinity_st {
    DRAGON_AFFINITY_TO_CREATOR,  /**< Locate on the same node as the process requesting the resource */
    DRAGON_AFFINITY_ANYWHERE     /**< Let the Dragon runtime automatically decide where to place a resource */
    // additional placement options
} dragonAffinity_t;

/**
*  Policies to apply to a resource at creation time.
**/
typedef struct dragonPolicy_st {
    dragonAffinity_t placement;   /**< Where to place the resource */
    dragonWaitMode_t wait_type;   /**< The type of operation to use when waiting for an event */
    bool refcount;                /**< Apply global reference counting to the resource and cleanup the resource when the count drops to zero */
} dragonPolicy_t;

/**
 *  A Stream for putting data. Appropriate for sending bytes of data.
 **/

typedef struct dragonQueuePutStream_st {
    int _idx; /**< An opaque handle to the PutStream. */
} dragonQueuePutStream_t;

/**
 *  A Stream for getting data. Appropriate for receiving bytes of data.
 **/

typedef struct dragonQueueGetStream_st {
    int _idx; /**< An opaque handle to the GetStream. */
} dragonQueueGetStream_t;

/**
*  Opaque handle to a Queue resource
**/
typedef struct dragonQueueDescr_st {
    uint64_t _idx;
} dragonQueueDescr_t;


/**
*  Structure with serialized descriptor of Queue that can be shared with other processes
**/
typedef struct dragonQueueSerial_st {
    size_t len;      /**< The number of bytes in the data structure member */
    uint8_t * data;  /**< Pointer to the serialized bytes */
} dragonQueueSerial_t;


/**
*  Customizable attributes for a Queue
**/
typedef struct dragonQueueAttr_st {
    size_t max_blocks; /**< Maximum elements in the queue */
    size_t bytes_per_msg_block;  /**< The size of the message blocks in the underlying Channel */
    // other attributes
} dragonQueueAttr_t;



/* Queue Attributes and Controls */

dragonError_t dragon_policy_init(dragonPolicy_t * policy);

dragonError_t dragon_queue_attr_init(dragonQueueAttr_t * queue_attr);

/* Managed Queue functions */

dragonError_t dragon_managed_queue_create(char * name, size_t maxsize, bool joinable, dragonQueueAttr_t * queue_attr,
                                            dragonPolicy_t * policy, dragonQueueDescr_t * queue);

dragonError_t dragon_managed_queue_destroy(dragonQueueDescr_t * queue);

dragonError_t dragon_managed_queue_attach(char * name, dragonQueueDescr_t * queue);

dragonError_t dragon_managed_queue_detach(dragonQueueDescr_t * queue);


/* Unmanaged Queue functions */

dragonError_t dragon_queue_create(dragonMemoryPoolDescr_t * pool, size_t maxsize, dragonQ_UID_t q_uid, bool joinable,
                                    dragonQueueAttr_t * queue_attr, dragonPolicy_t * policy, dragonQueueDescr_t * queue);

dragonError_t dragon_queue_destroy(dragonQueueDescr_t * queue);

dragonError_t dragon_queue_serialize(dragonQueueDescr_t * queue, dragonQueueSerial_t * queue_serial);

dragonError_t dragon_queue_attach(dragonQueueSerial_t * queue_serial, dragonQueueDescr_t * queue);

dragonError_t dragon_queue_detach(dragonQueueDescr_t * queue);


/* Operational functions */

dragonError_t dragon_queue_put(dragonQueueDescr_t * queue, void * ptr, size_t nbytes, const timespec_t * timeout);


dragonError_t dragon_queue_get(dragonQueueDescr_t * queue, void ** ptr, size_t * nbytes, const timespec_t * timeout);


dragonError_t dragon_queue_put_open(dragonQueueDescr_t * queue, dragonQueuePutStream_t * put_str);


dragonError_t dragon_queue_put_write(dragonQueuePutStream_t * put_str, void * ptr, size_t nbytes, const timespec_t * timeout);


dragonError_t dragon_queue_put_close(dragonQueuePutStream_t * put_str);


dragonError_t dragon_queue_get_open(dragonQueueDescr_t * queue, dragonQueueGetStream_t * get_str);


dragonError_t dragon_queue_get_read(dragonQueueGetStream_t * get_str, void ** ptr, size_t * nbytes, const timespec_t * timeout);


dragonError_t dragon_queue_get_readinto(dragonQueueGetStream_t * get_str, size_t max_bytes, void * ptr, size_t * nbytes, const timespec_t * timeout);


dragonError_t dragon_queue_get_close(dragonQueueGetStream_t * get_str);

#ifdef __cplusplus
}
#endif

#endif
