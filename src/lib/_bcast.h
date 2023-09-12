#ifndef HAVE_DRAGON_BCAST_INTERNAL_H
#define HAVE_DRAGON_BCAST_INTERNAL_H

#include <dragon/bcast.h>
#include "shared_lock.h"
#include "_managed_memory.h"
#include "umap.h"
#include <dragon/utils.h>
#include <stdbool.h>
#include <stdatomic.h>

#define TRUE 1
#define FALSE 0

#define DRAGON_BCAST_DEFAULT_LOCK_TYPE DRAGON_LOCK_FIFO_LITE
#define DRAGON_BCAST_SERIAL_NULINTS 2UL
#define DRAGON_BCAST_MEM_ORDER_READ memory_order_acquire
#define DRAGON_BCAST_MEM_ORDER memory_order_acq_rel
#define DRAGON_BCAST_UMAP_SEED 487
#define DRAGON_BCAST_MAX_SERIALIZED_LEN (DRAGON_MEMORY_MAX_SERIALIZED_LEN+(DRAGON_BCAST_SERIAL_NULINTS*sizeof(dragonULInt)))
#define DRAGON_BCAST_SPIN_CHECK_TIMEOUT_ITERS 10000UL
#define DRAGON_BCAST_ADAPTIVE_WAIT_TO_IDLE 50
#define DRAGON_BCAST_DESTROY_TIMEOUT_SEC 10

/* attributes and header info embedded into a BCast object NOTE: This must match
the pointers assigned in _map_header and _init_header of bcast.c */

typedef struct dragonBCastHeader_st {
    volatile atomic_uint * num_waiting;
    volatile atomic_uint * num_triggered;
    volatile atomic_uint * triggered;
    volatile atomic_uint * triggering;
    volatile atomic_uint * shutting_down; // set to 1 when shutting down.
    volatile atomic_int * allowable_count;
    atomic_uint * trigger_lock_sz;
    atomic_uint * spin_list_sz; // array size, not bytes
    atomic_uint * spin_list_count; // number of active spinners
    atomic_uint * payload_area_sz;
    volatile atomic_uint * payload_sz;
    atomic_uint * sync_type;
    atomic_uint * sync_num;
    atomic_uint * trigger_lock_type;
    atomic_uint * trigger_lock;
    volatile atomic_uint * spin_list;
    void * payload_area;
} dragonBCastHeader_t;

/* The header (above) maps out the handle to the object which is stored in the
umap for a process when the object is created or attached. The actual object, in
shared memory, has the actual fields (not pointers to fields). The constant
below is used in calculating the space needed for the BCast object. All fields
are the size of dragonULInt values except the three fields: trigger_lock,
spin_list, and payload_area. The lengths of these three are not necessarily the
same size as a dragonULInt, so their sizes are computed separately. The three
below is subtracted so the trigger_lock, spin_list, and payload_area are not
used in computing the object size. All others in the actual BCast object are the
size of dragonULInts and we calculate the number of dragonULInts for the rest of
the object here. */

#define DRAGON_BCAST_NULINTS ((sizeof(dragonBCastHeader_t)/sizeof(dragonULInt*))-3)

/* A seated BCast handle. For internal use only. */

typedef struct dragonBCast_st {
    dragonLock_t trigger_lock;
    void * obj_ptr;
    bool in_managed_memory;
    dragonMemoryPoolDescr_t pool;
    dragonMemoryDescr_t obj_mem;
    dragonBCastHeader_t header;
} dragonBCast_t;

/* Used in the BCast Notify Callback function */

typedef struct dragonBCastCallbackArg_st {
    dragonBCastDescr_t bd;
    void* user_def_ptr;
    dragonBCastCallback fun;
    bool timer_is_null;
    timespec_t timer;
    dragonWaitMode_t wait_mode;
    dragonReleaseFun release_fun;
    void* release_arg;
} dragonBCastCallbackArg_t;

/* Used in the BCast Notify Signal function */

typedef struct dragonBCastSignalArg_st {
    dragonBCastDescr_t bd;
    void** payload_ptr;
    size_t* payload_sz;
    pid_t parent_pid;
    int sig;
    dragonError_t* rc;
    char** err_string;
    bool timer_is_null;
    timespec_t timer;
    dragonWaitMode_t wait_mode;
    dragonReleaseFun release_fun;
    void* release_arg;
} dragonBCastSignalArg_t;

#endif