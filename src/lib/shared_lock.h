#ifndef HAVE_DRAGON_LOCK_H
#define HAVE_DRAGON_LOCK_H

#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <dragon/return_codes.h>
#include <dragon/shared_lock.h>

#ifdef __cplusplus
#include <atomic>
using namespace std;
#else
#include <stdatomic.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef atomic_uint_fast64_t dragonLockType_t;

/* the actual lock structures */
typedef struct dragonFIFOLiteLock_st {
    uint64_t * lock_size;
    dragonLockType_t * initd;
    dragonLockType_t * now_serving;
    dragonLockType_t * ticket_counter;
} dragonFIFOLiteLock_t;

typedef struct dragonFIFOLock_st {
    dragonFIFOLiteLock_t thr_lock;  // this type needs my_node (local node), which we need to protect per process 
                                    // if the same lock struct is used across threads.  for pure FIFO each thread
                                    // should use its own lock struct
    void * thr_lock_dptr;
    uint64_t * lock_size;
    dragonLockType_t * initd;
    dragonLockType_t * now_serving;
    dragonLockType_t * ticket_counter;
    dragonLockType_t * node_counter;
    dragonLockType_t ** nodes_now_serving;
    dragonLockType_t ** nodes_ticket_counter;
    uint32_t my_node;
} dragonFIFOLock_t;

typedef struct dragonGreedyLock_st {
    uint64_t * lock_size;
    dragonLockType_t * initd;
    pthread_mutex_t * mutex;
} dragonGreedyLock_t;

/* Union for lock type pointers */
typedef union {
    dragonFIFOLock_t * fifo;
    dragonFIFOLiteLock_t * fifo_lite;
    dragonGreedyLock_t * greedy;
    void * any; /* Void pointer for free calls */
} dragonLock_u;

typedef struct dragonLock_st {
    dragonLockKind_t kind;
    dragonLock_u ptr;
} dragonLock_t;

typedef enum dragonLockState_st {
    DRAGON_LOCK_STATE_UNDETERMINED,
    DRAGON_LOCK_STATE_LOCKED,
    DRAGON_LOCK_STATE_UNLOCKED
} dragonLockState_t;

/* ----------------------------------------
   Begin high-level API
   ---------------------------------------- */
size_t
dragon_lock_size(dragonLockKind_t kind);

dragonError_t
dragon_lock_init(dragonLock_t * lock, void * ptr, dragonLockKind_t lock_kind);

dragonError_t
dragon_lock_attach(dragonLock_t * lock, void * ptr);

dragonError_t
dragon_lock_detach(dragonLock_t * lock);

dragonError_t
dragon_lock_destroy(dragonLock_t * lock);

dragonError_t
dragon_lock(dragonLock_t * lock);

dragonError_t
dragon_try_lock(dragonLock_t * lock, int * locked);

dragonError_t
dragon_unlock(dragonLock_t * lock);

dragonError_t
dragon_lock_state(dragonLock_t * lock, dragonLockState_t * state);

/* ----------------------------------------
   Begin direct API calls
   ---------------------------------------- */

dragonError_t
dragon_fifo_lock_init(dragonFIFOLock_t * dlock, void * ptr);

dragonError_t
dragon_fifolite_lock_init(dragonFIFOLiteLock_t * dlock, void * ptr);

dragonError_t
dragon_greedy_lock_init(dragonGreedyLock_t * dlock, void * ptr);

dragonError_t
dragon_fifo_lock_attach(dragonFIFOLock_t * dlock, void * ptr);

dragonError_t
dragon_fifolite_lock_attach(dragonFIFOLiteLock_t * dlock, void * ptr);

dragonError_t
dragon_greedy_lock_attach(dragonGreedyLock_t * dlock, void * ptr);

dragonError_t
dragon_fifo_lock_detach(dragonFIFOLock_t * dlock);

dragonError_t
dragon_fifolite_lock_detach(dragonFIFOLiteLock_t * dlock);

dragonError_t
dragon_greedy_lock_detach(dragonGreedyLock_t * dlock);

dragonError_t
dragon_fifo_lock_destroy(dragonFIFOLock_t * dlock);

dragonError_t
dragon_fifolite_lock_destroy(dragonFIFOLiteLock_t * dlock);

dragonError_t
dragon_greedy_lock_destroy(dragonGreedyLock_t * dlock);

dragonError_t
dragon_fifo_lock(dragonFIFOLock_t * dlock);

dragonError_t
dragon_fifolite_lock(dragonFIFOLiteLock_t * dlock);

dragonError_t
dragon_greedy_lock(dragonGreedyLock_t * dlock);

dragonError_t
dragon_fifo_try_lock(dragonFIFOLock_t * dlock, int *locked);

dragonError_t
dragon_fifolite_try_lock(dragonFIFOLiteLock_t * dlock, int *locked);

dragonError_t
dragon_greedy_try_lock(dragonGreedyLock_t * dlock, int *locked);

dragonError_t
dragon_fifo_unlock(dragonFIFOLock_t * dlock);

dragonError_t
dragon_fifolite_unlock(dragonFIFOLiteLock_t * dlock);

dragonError_t
dragon_greedy_unlock(dragonGreedyLock_t * dlock);

dragonError_t
dragon_fifolite_lock_state(dragonFIFOLiteLock_t * dlock, dragonLockState_t * state);

dragonError_t
dragon_fifo_lock_state(dragonFIFOLock_t * dlock, dragonLockState_t * state);

dragonError_t
dragon_greedy_lock_state(dragonGreedyLock_t * dlock, dragonLockState_t * state);

#ifdef __cplusplus
}
#endif

#endif
