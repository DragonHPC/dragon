#include <dragon/return_codes.h>
#include "_shared_lock.h"
#include "_utils.h"
#include "err.h"
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdbool.h>

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

#define LOCK_DESTROYED 0xDEADDEADDEADDEAD
#define LOCK_INITD 0x0101010101010101

/* @MCB: Lock sizes.
   First uint64_t size is to avoid compiler-specific enum sizes.
   Second uint64_t is to avoid architecture-specific size_t sizes.
   Rest of the size is based on the lock type.
*/

#define LITE_LOCK_SIZE (sizeof(uint64_t) + sizeof(uint64_t) + DRAGON_LOCK_CL_PADDING * 4)

const size_t dragonFIFOLiteLockSize = LITE_LOCK_SIZE;
const size_t dragonFIFOLockSize     = LITE_LOCK_SIZE + sizeof(uint64_t) + sizeof(uint64_t) +
                                      DRAGON_LOCK_CL_PADDING * (4 + 2 * DRAGON_LOCK_NODE_FANOUT);
const size_t dragonGreedyLockSize   = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(dragonLockType_t) + sizeof(pthread_mutex_t);

/* obtain the lock on a node */
static inline dragonError_t
_dragon_node_lock(dragonFIFOLock_t * dlock)
{
    if (unlikely(dlock->nodes_now_serving == NULL ||
                 dlock->nodes_ticket_counter == NULL))
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonLockType_t * now_serving = dlock->nodes_now_serving[dlock->my_node];

    dragonLockType_t my_ticket;
    my_ticket = atomic_fetch_add_explicit(dlock->nodes_ticket_counter[dlock->my_node], 1UL,
                                           DRAGON_LOCK_MEM_ORDER);

    uint64_t ntest = 0UL;
    while (my_ticket > *now_serving) {

        if (unlikely(ntest == DRAGON_LOCK_POLL_PRE_SLEEP_ITERS)) {
            usleep(DRAGON_LOCK_POLL_SLEEP_USEC);
            ntest = 0UL;
            PAUSE();
        }
        ntest++;

     }

     no_err_return(DRAGON_SUCCESS);
}

/* try to obtain the lock on a node */
static inline dragonError_t
_dragon_node_try_lock(dragonFIFOLock_t * dlock, int * locked)
{
    if (locked == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    *locked = 0;

    if (unlikely(dlock->nodes_now_serving == NULL ||
                 dlock->nodes_ticket_counter == NULL))
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonLockType_t * ticket_counter, * now_serving;
    ticket_counter = dlock->nodes_ticket_counter[dlock->my_node];
    now_serving = dlock->nodes_now_serving[dlock->my_node];

    dragonLockType_t cticket, now_serve;
    cticket = atomic_load_explicit(ticket_counter,
                                   DRAGON_LOCK_MEM_ORDER_READ);

    now_serve = atomic_load_explicit(now_serving,
                                     DRAGON_LOCK_MEM_ORDER_READ);

    /* if they are equal now, it's more likely we can actually obtain the lock */
    if (cticket == now_serve) {

        dragonLockType_t desired;
        desired = now_serve + 1UL;

        atomic_bool success = atomic_compare_exchange_strong_explicit(ticket_counter,
                                                                      &now_serve, desired,
                                                                      DRAGON_LOCK_MEM_ORDER,
                                                                      DRAGON_LOCK_MEM_ORDER_FAIL);
        if (success == true) {
            *locked = 1;
            no_err_return(DRAGON_SUCCESS);
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

/* determine which node we should use */
static inline dragonError_t
_dragon_get_node(dragonFIFOLock_t * dlock)
{
    dragonLockType_t my_node;
    my_node = atomic_fetch_add_explicit(dlock->node_counter, 1UL,
                                        DRAGON_LOCK_MEM_ORDER);

    dlock->my_node = my_node % DRAGON_LOCK_NODE_FANOUT;
    no_err_return(DRAGON_SUCCESS);
}

/* unlock a node */
static inline dragonError_t
_dragon_node_unlock(dragonFIFOLock_t * dlock)
{
    if (unlikely(dlock->nodes_now_serving == NULL ||
                 dlock->nodes_ticket_counter == NULL))
        err_return(DRAGON_INVALID_ARGUMENT,"");

    atomic_fetch_add_explicit(dlock->nodes_now_serving[dlock->my_node],
                              1UL, DRAGON_LOCK_MEM_ORDER);

    no_err_return(DRAGON_SUCCESS);
}

/******************************
    BEGIN USER API
*******************************/

size_t
dragon_lock_size(dragonLockKind_t kind)
{
    switch(kind) {
    case DRAGON_LOCK_FIFO:
        return dragonFIFOLockSize;

    case DRAGON_LOCK_FIFO_LITE:
        return dragonFIFOLiteLockSize;

    case DRAGON_LOCK_GREEDY:
        return dragonGreedyLockSize;

    default:
        return 0;
    }
}

dragonError_t
dragon_lock_init(dragonLock_t * lock, void * ptr, dragonLockKind_t lock_kind)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    lock->kind = lock_kind;

    switch (lock->kind) {
    case DRAGON_LOCK_FIFO:
        lock->ptr.fifo = malloc(sizeof(dragonFIFOLock_t));
        return dragon_fifo_lock_init(lock->ptr.fifo, ptr);

    case DRAGON_LOCK_FIFO_LITE:
        lock->ptr.fifo_lite = malloc(sizeof(dragonFIFOLiteLock_t));
        return dragon_fifolite_lock_init(lock->ptr.fifo_lite, ptr);

    case DRAGON_LOCK_GREEDY:
        lock->ptr.greedy = malloc(sizeof(dragonGreedyLock_t));
        return dragon_greedy_lock_init(lock->ptr.greedy, ptr);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

dragonError_t
dragon_lock_attach(dragonLock_t * lock, void * ptr)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    lock->kind = *(dragonLockKind_t*)ptr;

    switch (lock->kind) {
    case DRAGON_LOCK_FIFO:
        lock->ptr.fifo = malloc(sizeof(dragonFIFOLock_t));
        return dragon_fifo_lock_attach(lock->ptr.fifo, ptr);

    case DRAGON_LOCK_FIFO_LITE:
        lock->ptr.fifo_lite = malloc(sizeof(dragonFIFOLiteLock_t));
        return dragon_fifolite_lock_attach(lock->ptr.fifo_lite, ptr);

    case DRAGON_LOCK_GREEDY:
        lock->ptr.greedy = malloc(sizeof(dragonGreedyLock_t));
        return dragon_greedy_lock_attach(lock->ptr.greedy, ptr);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

dragonError_t
dragon_lock_detach(dragonLock_t * lock)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonError_t derr = DRAGON_SUCCESS;

    switch (lock->kind) {
    case DRAGON_LOCK_FIFO:
        derr = dragon_fifo_lock_detach(lock->ptr.fifo);
        break;

    case DRAGON_LOCK_FIFO_LITE:
        derr = dragon_fifolite_lock_detach(lock->ptr.fifo_lite);
        break;

    case DRAGON_LOCK_GREEDY:
        derr = dragon_greedy_lock_detach(lock->ptr.greedy);
        break;

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }


    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"Could not detach from lock.");

    free(lock->ptr.any);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_lock_destroy(dragonLock_t * lock)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonError_t derr = DRAGON_SUCCESS;

    switch (lock->kind) {
    case DRAGON_LOCK_FIFO:
        derr = dragon_fifo_lock_destroy(lock->ptr.fifo);
        break;

    case DRAGON_LOCK_FIFO_LITE:
        derr = dragon_fifolite_lock_destroy(lock->ptr.fifo_lite);
        break;

    case DRAGON_LOCK_GREEDY:
        derr = dragon_greedy_lock_destroy(lock->ptr.greedy);
        break;

    default:
        err_return(DRAGON_INVALID_ARGUMENT, "Could not destroy lock.");
    }

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Could not destroy lock.");

    free(lock->ptr.any);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_lock(dragonLock_t * lock)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    switch(lock->kind) {
    case DRAGON_LOCK_FIFO:
        return dragon_fifo_lock(lock->ptr.fifo);

    case DRAGON_LOCK_FIFO_LITE:
        return dragon_fifolite_lock(lock->ptr.fifo_lite);

    case DRAGON_LOCK_GREEDY:
        return dragon_greedy_lock(lock->ptr.greedy);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

dragonError_t
dragon_try_lock(dragonLock_t * lock, int * locked)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    switch(lock->kind) {
    case DRAGON_LOCK_FIFO:
        return dragon_fifo_try_lock(lock->ptr.fifo, locked);

    case DRAGON_LOCK_FIFO_LITE:
        return dragon_fifolite_try_lock(lock->ptr.fifo_lite, locked);

    case DRAGON_LOCK_GREEDY:
        return dragon_greedy_try_lock(lock->ptr.greedy, locked);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

dragonError_t
dragon_unlock(dragonLock_t * lock)
{
    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    switch(lock->kind) {
    case DRAGON_LOCK_FIFO:
        return dragon_fifo_unlock(lock->ptr.fifo);

    case DRAGON_LOCK_FIFO_LITE:
        return dragon_fifolite_unlock(lock->ptr.fifo_lite);

    case DRAGON_LOCK_GREEDY:
        return dragon_greedy_unlock(lock->ptr.greedy);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

dragonError_t
dragon_lock_state(dragonLock_t * lock, dragonLockState_t * state)
{
    *state = DRAGON_LOCK_STATE_UNDETERMINED;

    if (lock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    switch(lock->kind) {
    case DRAGON_LOCK_FIFO:
        return dragon_fifo_lock_state(lock->ptr.fifo, state);

    case DRAGON_LOCK_FIFO_LITE:
        return dragon_fifolite_lock_state(lock->ptr.fifo_lite, state);

    case DRAGON_LOCK_GREEDY:
        return dragon_greedy_lock_state(lock->ptr.greedy, state);

    default:
        err_return(DRAGON_INVALID_ARGUMENT,"");
    }
}

bool
dragon_lock_is_valid(dragonLock_t * lock)
{
    if (lock == NULL)
        return false;

    switch(lock->kind) {
    case DRAGON_LOCK_FIFO:
        return dragon_fifo_lock_is_valid(lock->ptr.fifo);

    case DRAGON_LOCK_FIFO_LITE:
        return dragon_fifolite_lock_is_valid(lock->ptr.fifo_lite);

    case DRAGON_LOCK_GREEDY:
        return dragon_greedy_lock_is_valid(lock->ptr.greedy);

    default:
        return false;
    }
}

/* map a new dragonLock_t to a block of memory and optionally initialize it */
dragonError_t
dragon_fifo_lock_init(dragonFIFOLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    *(uint64_t*)ptr = (uint64_t)DRAGON_LOCK_FIFO;

    dragonError_t derr;
    derr = dragon_fifo_lock_attach(dlock, ptr);
    if (derr != DRAGON_OBJECT_DESTROYED)
        append_err_return(DRAGON_LOCK_ALREADY_INITD,"");

    *(dlock->initd) = LOCK_INITD; // Make note for destroy

    *(dlock->lock_size) = dragonFIFOLockSize;
    *(dlock->now_serving) = 0UL;
    *(dlock->ticket_counter) = 0UL;
    *(dlock->node_counter) = 0UL;

    int i;
    for (i = 0; i < DRAGON_LOCK_NODE_FANOUT; i++) {
        *(dlock->nodes_now_serving[i]) = 0UL;
        *(dlock->nodes_ticket_counter[i]) = 0UL;
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_lock_init(dragonFIFOLiteLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    *(uint64_t*)ptr = (uint64_t)DRAGON_LOCK_FIFO_LITE;

    dragonError_t derr;
    derr = dragon_fifolite_lock_attach(dlock, ptr);
    if (derr != DRAGON_OBJECT_DESTROYED)
        append_err_return(DRAGON_LOCK_ALREADY_INITD,"");

    *(dlock->initd) = LOCK_INITD; // Make note for destroy
    *(dlock->lock_size) = dragonFIFOLiteLockSize;
    *(dlock->now_serving) = 0UL;
    *(dlock->ticket_counter) = 0UL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifo_lock_attach(dragonFIFOLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    ptr += sizeof(uint64_t);
    dlock->lock_size = (uint64_t *)ptr;

    ptr += sizeof(uint64_t);
    dlock->initd = (dragonLockType_t *)ptr;

    ptr += sizeof(dragonLockType_t);
    dlock->now_serving = (dragonLockType_t *)ptr;

    ptr += DRAGON_LOCK_CL_PADDING;
    dlock->ticket_counter = (dragonLockType_t *)ptr;
    ptr += DRAGON_LOCK_CL_PADDING;
    dlock->node_counter = (dragonLockType_t *)ptr;
    ptr += DRAGON_LOCK_CL_PADDING;

    dlock->nodes_now_serving = malloc(sizeof(dragonLockType_t) * DRAGON_LOCK_NODE_FANOUT);
    if (dlock->nodes_now_serving == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,"");
    dlock->nodes_ticket_counter = malloc(sizeof(dragonLockType_t) * DRAGON_LOCK_NODE_FANOUT);
    if (dlock->nodes_ticket_counter == NULL) {
        free(dlock->nodes_now_serving);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,"");
    }

    for (int i = 0; i < DRAGON_LOCK_NODE_FANOUT; i++) {

        dlock->nodes_now_serving[i] = ptr;
        ptr += DRAGON_LOCK_CL_PADDING;

    }
    for (int i = 0; i < DRAGON_LOCK_NODE_FANOUT; i++) {

        dlock->nodes_ticket_counter[i] = ptr;
        ptr += DRAGON_LOCK_CL_PADDING;

    }

    /* create the FIFOLite lock we'll use lock local threads on the object */
    dlock->thr_lock_dptr = calloc(dragon_lock_size(DRAGON_LOCK_FIFO_LITE), 1);
    if (dlock->thr_lock_dptr == NULL) {
        free(dlock->nodes_now_serving);
        free(dlock->nodes_ticket_counter);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,"");
    }
    dragonError_t derr = dragon_fifolite_lock_init(&dlock->thr_lock, dlock->thr_lock_dptr);
    if (derr != DRAGON_SUCCESS) {
        free(dlock->nodes_now_serving);
        free(dlock->nodes_ticket_counter);
        free(dlock->thr_lock_dptr);
        append_err_return(derr,"");
    }

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "The Dragon object was already destroyed and cannot be attached.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_lock_attach(dragonFIFOLiteLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    ptr += sizeof(uint64_t);

    dlock->lock_size = (uint64_t *)ptr;
    ptr += sizeof(uint64_t);

    dlock->initd = (dragonLockType_t *)ptr;
    ptr += sizeof(dragonLockType_t);

    dlock->now_serving = (dragonLockType_t *)ptr;
    ptr += DRAGON_LOCK_CL_PADDING;
    dlock->ticket_counter = (dragonLockType_t *)ptr;
    ptr += DRAGON_LOCK_CL_PADDING;

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "The Dragon object was already destroyed and cannot be attached.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifo_lock_detach(dragonFIFOLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dlock->lock_size = NULL;
    dlock->initd = NULL;
    dlock->now_serving = NULL;
    dlock->ticket_counter = NULL;

    if (dlock->nodes_now_serving != NULL)
        free(dlock->nodes_now_serving);
    if (dlock->nodes_ticket_counter != NULL)
        free(dlock->nodes_ticket_counter);

    dragonError_t derr = dragon_fifolite_lock_destroy(&dlock->thr_lock);
    if (derr != DRAGON_SUCCESS && derr != DRAGON_OBJECT_DESTROYED)
        append_err_return(derr,"");
    if (dlock->thr_lock_dptr != NULL)
        free(dlock->thr_lock_dptr);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_lock_detach(dragonFIFOLiteLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dlock->lock_size = NULL;
    dlock->initd = NULL;
    dlock->now_serving = NULL;
    dlock->ticket_counter = NULL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_lock_init(dragonGreedyLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    *(uint64_t*)ptr = (uint64_t)DRAGON_LOCK_GREEDY;
    dragonError_t derr;
    derr = dragon_greedy_lock_attach(dlock, ptr);
    if (derr != DRAGON_OBJECT_DESTROYED)
        append_err_return(DRAGON_LOCK_ALREADY_INITD,"");

    *(dlock->initd) = LOCK_INITD;

    *(dlock->lock_size) = dragonGreedyLockSize;

    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);

    if (pthread_mutex_init(dlock->mutex, &mattr) != 0)
        err_return(DRAGON_LOCK_PTHREAD_MUTEX_INIT,"");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_lock_attach(dragonGreedyLock_t * dlock, void * ptr)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    ptr += sizeof(uint64_t);
    dlock->lock_size = (uint64_t *)ptr;

    ptr += sizeof(uint64_t);
    dlock->initd = (dragonLockType_t *)ptr;

    ptr += sizeof(dragonLockType_t);
    dlock->mutex = (pthread_mutex_t *)ptr;

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "The Dragon object was already destroyed and cannot be attached.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_lock_detach(dragonGreedyLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dlock->lock_size = NULL;
    dlock->initd = NULL;
    dlock->mutex = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/* unmap a lock from a blob of memory */
dragonError_t
dragon_fifo_lock_destroy(dragonFIFOLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonLockType_t cur_initd, destroy;
    destroy = LOCK_DESTROYED;
    cur_initd = atomic_exchange_explicit(dlock->initd, destroy,
                                         DRAGON_LOCK_MEM_ORDER);

    if (cur_initd != LOCK_INITD && cur_initd != LOCK_DESTROYED)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    return dragon_fifo_lock_detach(dlock);
}

dragonError_t
dragon_fifolite_lock_destroy(dragonFIFOLiteLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonLockType_t cur_initd, destroy;
    destroy = LOCK_DESTROYED;
    cur_initd = atomic_exchange_explicit(dlock->initd, destroy,
                                         DRAGON_LOCK_MEM_ORDER);

    if (cur_initd != LOCK_INITD && cur_initd != LOCK_DESTROYED)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    return dragon_fifolite_lock_detach(dlock);
}

dragonError_t
dragon_greedy_lock_destroy(dragonGreedyLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    dragonLockType_t cur_initd, destroy;
    destroy = LOCK_DESTROYED;
    cur_initd = atomic_exchange_explicit(dlock->initd, destroy,
                                         DRAGON_LOCK_MEM_ORDER);

    if (cur_initd != LOCK_INITD && cur_initd != LOCK_DESTROYED)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (pthread_mutex_destroy(dlock->mutex) != 0)
        err_return(DRAGON_LOCK_PTHREAD_MUTEX_DESTROY,"");

    return dragon_greedy_lock_detach(dlock);
}

/* obtain the lock */
dragonError_t
dragon_fifo_lock(dragonFIFOLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    /* get the local thread lock */
    dragonError_t derr = dragon_fifolite_lock(&dlock->thr_lock);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"");

    /* determine which node we will look at */
    derr = _dragon_get_node(dlock);
    if (unlikely(derr != DRAGON_SUCCESS))
        append_err_return(derr,"");

    /* now obtain the sub-lock on the node */
    derr = _dragon_node_lock(dlock);
    if (unlikely(derr != DRAGON_SUCCESS))
        append_err_return(derr,"");

    /* now contend for the main lock */
    dragonLockType_t my_ticket;
    my_ticket = (dragonLockType_t)dlock->my_node;
    dragonLockType_t next_node = my_ticket + 1UL;
    if (next_node >= DRAGON_LOCK_NODE_FANOUT)
        next_node -= DRAGON_LOCK_NODE_FANOUT;
    atomic_store_explicit(dlock->ticket_counter, next_node, DRAGON_LOCK_MEM_ORDER_FAIL);

    uint64_t ntest = 0UL;
    while (my_ticket != *(dlock->now_serving)) {
        if (unlikely(ntest == DRAGON_LOCK_POLL_PRE_SLEEP_ITERS)) {
            usleep(DRAGON_LOCK_POLL_SLEEP_USEC);
            ntest = 0UL;
            PAUSE();
        }
        ntest++;
    }

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_lock(dragonFIFOLiteLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    /* now contend for the main lock */
    dragonLockType_t my_ticket;
    my_ticket = atomic_fetch_add_explicit(dlock->ticket_counter, 1UL,
                                          DRAGON_LOCK_MEM_ORDER);

    uint64_t ntest = 0UL;
    while (my_ticket > *(dlock->now_serving)) {
        if (unlikely(ntest == DRAGON_LOCK_POLL_PRE_SLEEP_ITERS)) {
            usleep(DRAGON_LOCK_POLL_SLEEP_USEC);
            ntest = 0UL;
            PAUSE();
        }
        ntest++;
    }

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

     no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_lock(dragonGreedyLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    int ierr = pthread_mutex_lock(dlock->mutex);
    if (unlikely(ierr != 0))
        err_return(DRAGON_LOCK_PTHREAD_MUTEX_LOCK,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    no_err_return(DRAGON_SUCCESS);
}

/* try to obtain the lock */
dragonError_t
dragon_fifo_try_lock(dragonFIFOLock_t * dlock, int *locked)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (locked == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    *locked = 0;

    /* try to get the local thread lock */
    int local_locked;
    dragonError_t derr = dragon_fifolite_try_lock(&dlock->thr_lock, &local_locked);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"");

    /* if we didn't get the local lock, return */
    if (local_locked == 0)
        no_err_return(DRAGON_SUCCESS);

    /* determine which node we will look at */
    derr = _dragon_get_node(dlock);
    if (unlikely(derr != DRAGON_SUCCESS))
        append_err_return(derr,"");

    /* try to lock the node */
    int sublocked;
    derr = _dragon_node_try_lock(dlock, &sublocked);

    /* if we got the node, try to obtain the main lock */
    if (sublocked == 1) {

        *locked = 0;

        dragonLockType_t cticket, now_serve;
        cticket = atomic_load_explicit(dlock->ticket_counter,
                                       DRAGON_LOCK_MEM_ORDER_READ);

        now_serve = atomic_load_explicit(dlock->now_serving,
                                         DRAGON_LOCK_MEM_ORDER_READ);

        /* if they are equal now, it's more likely we can actually obtain the lock */
        if (cticket == now_serve) {

            dragonLockType_t desired;
            desired = now_serve + 1UL;

            atomic_bool success;
            success = atomic_compare_exchange_strong_explicit(dlock->ticket_counter,
                                                              &now_serve, desired,
                                                              DRAGON_LOCK_MEM_ORDER,
                                                              DRAGON_LOCK_MEM_ORDER_FAIL);
            if (success == true) {

                if (*dlock->initd != LOCK_INITD)
                    err_return(DRAGON_OBJECT_DESTROYED, "");

                *locked = 1;
                no_err_return(DRAGON_SUCCESS);
            }
        }

        /* if we didn't get the main lock, we need to unlock the sub-lock on the node */
        if (*locked == 0) {

            derr = _dragon_node_unlock(dlock);
            if (unlikely(derr != DRAGON_SUCCESS))
                append_err_return(derr,"");
        }
    }

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_try_lock(dragonFIFOLiteLock_t * dlock, int *locked)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (locked == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    *locked = 0;

    dragonLockType_t cticket, now_serve;
    cticket = atomic_load_explicit(dlock->ticket_counter,
                                   DRAGON_LOCK_MEM_ORDER_READ);

    now_serve = atomic_load_explicit(dlock->now_serving,
                                     DRAGON_LOCK_MEM_ORDER_READ);

    /* if they are equal now, it's more likely we can actually obtain the lock */
    if (cticket == now_serve) {

        dragonLockType_t desired;
        desired = now_serve + 1UL;

        atomic_bool success;
        success = atomic_compare_exchange_strong_explicit(dlock->ticket_counter,
                                                          &now_serve, desired,
                                                          DRAGON_LOCK_MEM_ORDER,
                                                          DRAGON_LOCK_MEM_ORDER_FAIL);
        if (success == true) {

            if (*dlock->initd != LOCK_INITD)
                err_return(DRAGON_OBJECT_DESTROYED, "");

            *locked = 1;
            no_err_return(DRAGON_SUCCESS);
        }
    }

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_try_lock(dragonGreedyLock_t * dlock, int *locked)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (locked == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (dlock->initd == NULL)
        err_return(DRAGON_LOCK_NOT_INITD,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    int ierr = pthread_mutex_trylock(dlock->mutex);

    if (ierr == EBUSY) {
        *locked = 0;
        no_err_return(DRAGON_SUCCESS);
    }

    if (unlikely(ierr != 0))
        err_return(DRAGON_LOCK_PTHREAD_MUTEX_LOCK,"");

    if (*dlock->initd != LOCK_INITD)
        err_return(DRAGON_OBJECT_DESTROYED, "");

    *locked = 1;
    no_err_return(DRAGON_SUCCESS);
}

/* release the lock */
dragonError_t
dragon_fifo_unlock(dragonFIFOLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    dragonLockType_t next_node = dlock->my_node + 1;
    if (next_node >= DRAGON_LOCK_NODE_FANOUT)
        next_node -= DRAGON_LOCK_NODE_FANOUT;
    atomic_store_explicit(dlock->now_serving, next_node, DRAGON_LOCK_MEM_ORDER_FAIL);

    dragonError_t derr = _dragon_node_unlock(dlock);
    if (unlikely(derr != DRAGON_SUCCESS))
        append_err_return(derr,"");

    /* release the local thread lock */
    return dragon_fifolite_unlock(&dlock->thr_lock);
}

dragonError_t
dragon_fifolite_unlock(dragonFIFOLiteLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    atomic_fetch_add_explicit(dlock->now_serving,
                              1UL, DRAGON_LOCK_MEM_ORDER);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_unlock(dragonGreedyLock_t * dlock)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    int ierr = pthread_mutex_unlock(dlock->mutex);
    if (unlikely(ierr != 0))
        err_return(DRAGON_LOCK_PTHREAD_MUTEX_UNLOCK,"");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifolite_lock_state(dragonFIFOLiteLock_t * dlock, dragonLockState_t* state)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (*dlock->now_serving < *dlock->ticket_counter)
        *state = DRAGON_LOCK_STATE_LOCKED;
    else
        *state = DRAGON_LOCK_STATE_UNLOCKED;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fifo_lock_state(dragonFIFOLock_t * dlock, dragonLockState_t* state)
{
    if (dlock == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"");

    if (*dlock->now_serving < *dlock->ticket_counter)
        *state = DRAGON_LOCK_STATE_LOCKED;
    else
        *state = DRAGON_LOCK_STATE_UNLOCKED;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_greedy_lock_state(dragonGreedyLock_t* dlock, dragonLockState_t* state)
{
    int unlocked = pthread_mutex_trylock(dlock->mutex);

    if (unlocked == 0) {
        pthread_mutex_unlock(dlock->mutex);
        *state = DRAGON_LOCK_STATE_UNLOCKED;
    } else
        *state = DRAGON_LOCK_STATE_LOCKED;

    no_err_return(DRAGON_SUCCESS);
}

bool
dragon_fifolite_lock_is_valid(dragonFIFOLiteLock_t * dlock)
{
    if (dlock == NULL)
        return false;

    if (dlock->initd == NULL)
        return false;

    return *dlock->initd == LOCK_INITD;
}

bool
dragon_fifo_lock_is_valid(dragonFIFOLock_t * dlock)
{
    if (dlock == NULL)
        return false;

    if (dlock->initd == NULL)
        return false;

    return *dlock->initd == LOCK_INITD;
}

bool
dragon_greedy_lock_is_valid(dragonGreedyLock_t* dlock)
{
    if (dlock == NULL)
        return false;

    if (dlock->initd == NULL)
        return false;

    return *dlock->initd == LOCK_INITD;
}
