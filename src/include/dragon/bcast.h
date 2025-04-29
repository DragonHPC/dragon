/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef DRAGON_BCAST_H
#define DRAGON_BCAST_H

#include <dragon/return_codes.h>
#include <dragon/global_types.h>
#include <dragon/managed_memory.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  A BCast handle.
 *
 *  The internals of this structure should not be used directly. It provides an
 *  opaque handle to a BCast object. All manipulation of the BCast
 *  occurs via its API functions. A process gets a valid dragonBCastDescr_t
 *  handle by either creating or attaching to a BCast object using the same API
 *  calls.
 *
 **/
typedef struct dragonBCastDescr_st {
    uint64_t _idx; // internal use only
} dragonBCastDescr_t;


/**
 *  A serialized BCast handle.
 *
 *  This structure should not be used directly. All manipulation of the BCast object
 *  occurs via its API functions. A process gets a valid serialized descriptor
 *  by calling serialize on a valid BCast object.
 *
 *  The data is a serialized representation of a BCast object that can be passed between
 *  processes/thread and used to attach to the same object in another process or
 *  thread.
 *
 **/
typedef struct dragonBCastSerial_st {
    size_t len; /**< The length of the serialized data */
    uint8_t * data; /**< A pointer to the serialized data */
} dragonBCastSerial_t;


/**
 * @brief Constants to specify synchronization between waiters and triggerers
 *
 * These constants are provided as part of the BCast attributes when a BCast
 * is created. The default value of DRAGON_NO_SYNC means it is up to the
 * application to decide on when triggering and waiting occur. When
 * DRAGON_SYNC is specified, then a triggerer will wait until
 * at least one waiter is waiting, providing synchronization between triggerers
 * and waiters. The number of required waiters is provided in the BCast attributes.
 */
typedef enum dragonSyncType_st {
    DRAGON_NO_SYNC, /**< When specified, triggering processes will never block
                         waiting for waiters. This is the default. */
    DRAGON_SYNC /**< When specified, a triggerer will block, waiting for waiters
                     on the BCast object. The required number of waiters is specified
                     in the attributes of the BCast. */
} dragonSyncType_t;

/**
 * BCast Attributes
 *
 * This structure defines user selectable attributes of the BCast Synchronization
 * object. The lock type used internally may be user specified. The sync_type is
 * as described in dragonSyncType_t. If DRAGON_SYNC is
 * specified, then the user is guaranteed that no trigger events will occur
 * without a waiter. This is at the expense of the triggerer blocking should
 * no waiter be presently waiting on the bcast, but provides a convenient
 * synchronization mechanism while eliminating the possibility of a
 * trigger event going unnoticed. The default is NO_SYNC for no synchronization
 * between waiters and triggerers.
 */
typedef struct dragonBCastAttr_st {
    dragonLockKind_t lock_type; /**< The lock type used within the BCast object */
    dragonSyncType_t sync_type; /**< The synchronization requirement */
    dragonULInt sync_num; /**< Only valid when DRAGON_SYNC is specified.
                               This specifies the number of waiters that must be waiting
                               before a trigger can occur. */
} dragonBCastAttr_t;

typedef void
(*dragonBCastCallback)(void* user_def_ptr, void* payload, size_t payload_sz, dragonError_t derr, char* err_str);

dragonError_t
dragon_bcast_size(size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, size_t* size);

dragonError_t
dragon_bcast_attr_init(dragonBCastAttr_t* attr);

dragonError_t
dragon_bcast_create_at(void* loc, size_t alloc_sz, size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, dragonBCastDescr_t* bd);

dragonError_t
dragon_bcast_attach_at(void* loc, dragonBCastDescr_t* bd);

dragonError_t
dragon_bcast_create(dragonMemoryPoolDescr_t* pd, size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, dragonBCastDescr_t* bd);

dragonError_t
dragon_bcast_attach(dragonBCastSerial_t* serial, dragonBCastDescr_t* bd);

dragonError_t
dragon_bcast_destroy(dragonBCastDescr_t* bd);

dragonError_t
dragon_bcast_detach(dragonBCastDescr_t* bd);

size_t
dragon_bcast_max_serialized_len();

dragonError_t
dragon_bcast_serialize(const dragonBCastDescr_t * bd, dragonBCastSerial_t * bd_ser);

dragonError_t
dragon_bcast_serial_free(dragonBCastSerial_t * bd_ser);

dragonError_t
dragon_bcast_wait(dragonBCastDescr_t* bd, dragonWaitMode_t wait_mode, const timespec_t* timer, void** payload, size_t* payload_sz, dragonReleaseFun release_fun, void* release_arg);

dragonError_t
dragon_bcast_notify_callback(dragonBCastDescr_t* bd, void* user_def_ptr, const dragonWaitMode_t wait_mode, const timespec_t* timer, dragonReleaseFun release_fun, void* release_arg, dragonBCastCallback cb);

dragonError_t
dragon_bcast_notify_signal(dragonBCastDescr_t* bd, const dragonWaitMode_t wait_mode, const timespec_t* timer, dragonReleaseFun release_fun, void* release_arg, int sig, void** payload_ptr, size_t* payload_sz, dragonError_t* drc, char** err_string);

dragonError_t
dragon_bcast_trigger_one(dragonBCastDescr_t* bd, const timespec_t* timeout, const void* payload, const size_t payload_sz);

dragonError_t
dragon_bcast_trigger_some(dragonBCastDescr_t* bd, int num_to_trigger, const timespec_t* timer, const void* payload, const size_t payload_sz);

dragonError_t
dragon_bcast_trigger_all(dragonBCastDescr_t* bd, const timespec_t* timeout, const void* payload, const size_t payload_sz);

dragonError_t
dragon_bcast_num_waiting(dragonBCastDescr_t* bd, int* num_waiters);

dragonError_t
dragon_bcast_reset(dragonBCastDescr_t* bd);

char*
dragon_bcast_state(dragonBCastDescr_t* bd);

#ifdef __cplusplus
}
#endif

#endif