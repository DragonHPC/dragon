#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <unistd.h>

#include "err.h"
#include "hostid.h"
#include "_bcast.h"
#include "_utils.h"

/* dragon globals */
DRAGON_GLOBAL_MAP(bcast);

_Thread_local pid_t my_pid = 0;
static timespec_t min_time_slice = {0, 56000};
static long __detected_num_cores = -1;
static unsigned long nanos_per_second = NANOS_PER_SEC;

static long
_num_cores() {
    if (__detected_num_cores < 0)
        __detected_num_cores = sysconf(_SC_NPROCESSORS_ONLN);

    if (__detected_num_cores < 0) {
        perror("sysconf: number of cores could not be detected in bcast");
        return 1; // default to one core if could not be detected.
    }

    return __detected_num_cores;
}

static int
_core_factor() {
    int factor = _num_cores() / 12;

    if (factor <= 0)
        return 1;

    return factor;
}

static void
_setpid() {
    my_pid = getpid();
}

static int
_getpid() {
    if (my_pid == 0)
        my_pid = getpid();

    return my_pid;
}

static void
_sleep_handler(int signum) {
    // Do nothing. But this must be installed for nanosleepers to wake up.
}

static void
_install_signal_handler(struct sigaction* old_sig_action) {
    // Save the current SIGCONT handler
    sigaction(SIGCONT, NULL, old_sig_action);

    // Install our signal handler
    struct sigaction sa = {0};
    sa.sa_handler = _sleep_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGCONT, &sa, NULL);
}

static void _uninstall_signal_handler(bool handler_installed, struct sigaction* old_sig_action) {
    // Restore the current SIGCONT handler
    if (handler_installed)
        sigaction(SIGCONT, old_sig_action, NULL);
}

// enabling this can prevent core dumps when underlying memory is deleted
// but enabling it incurs an extra cost
// #define CHECK_POINTER

static bool
_is_pointer_valid(void *p) {
    /* get the page size */
    size_t page_size = sysconf(_SC_PAGESIZE);
    /* find the address of the page that contains p */
    void *base = (void *)((((size_t)p) / page_size) * page_size);
    /* call msync, if it returns non-zero, return false */
    int ret = msync(base, page_size, MS_ASYNC) != -1;

    return ret ? ret : errno != ENOMEM;
}

static dragonError_t
_bcast_validate_attrs(dragonBCastAttr_t* attrs) {

    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "attrs cannot be NULL");

    if (attrs->lock_type < DRAGON_LOCK_FIFO || attrs->lock_type > DRAGON_LOCK_GREEDY)
        err_return(DRAGON_INVALID_LOCK_KIND, "Invalid lock type value specified");

    if (attrs->sync_type < DRAGON_NO_SYNC || attrs->sync_type > DRAGON_SYNC)
        err_return(DRAGON_INVALID_SYNC_KIND, "Invalid sync type value specified");

    if (attrs->sync_type == DRAGON_SYNC && attrs->sync_num == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Sync number must be >= 1 when sync kind is DRAGON_SYNC.");

    no_err_return(DRAGON_SUCCESS);
}

/* insert a bcast structure into the unordered map using a generated key */
static dragonError_t
_bcast_add_umap_entry(dragonBCastDescr_t * bd, const dragonBCast_t * handle)
{
    /* register this channel in our umap */
    if (*dg_bcast == NULL) {
        /* this is a process-global variable and has no specific call to be destroyed */
        *dg_bcast = malloc(sizeof(dragonMap_t));
        if (*dg_bcast == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate umap for BCast objects.");

        dragonError_t uerr = dragon_umap_create(dg_bcast, DRAGON_BCAST_UMAP_SEED);
        if (uerr != DRAGON_SUCCESS)
            append_err_return(uerr, "Failed to create umap for BCast objects.");
    }

    dragonError_t uerr = dragon_umap_additem_genkey(dg_bcast, handle, &bd->_idx);
    if (uerr != DRAGON_SUCCESS)
        append_err_return(uerr, "Failed to insert item into BCast umap.");

    no_err_return(DRAGON_SUCCESS);
}

static void
_bcast_map_obj(void* obj_ptr, dragonBCastHeader_t* header)
{
    char* ptr = (char*) obj_ptr;
    header->num_waiting = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->num_triggered = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->triggering = (_Atomic(uint32_t) *) ptr;
    ptr += sizeof(dragonULInt);
    header->shutting_down = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->allowable_count = (atomic_int *) ptr;
    ptr += sizeof(dragonULInt);
    header->num_to_trigger = (atomic_int *) ptr;
    ptr += sizeof(dragonULInt);
    header->state = (dragonULInt *) ptr;
    ptr += sizeof(dragonULInt);
    header->lock_sz = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->spin_list_sz = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->spin_list_count = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->proc_max = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->proc_num = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->payload_area_sz = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->payload_sz = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->sync_type = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->sync_num = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->id = (dragonUUID *) ptr;
    ptr += sizeof(dragonULInt);
    header->reserved = NULL;
    ptr += sizeof(dragonULInt);
    header->lock_type = (atomic_uint *) ptr;
    ptr += sizeof(dragonULInt);
    header->lock = (atomic_uint *) ptr;
}

static dragonError_t
_bcast_init_obj(void* obj_ptr, size_t alloc_sz, size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, dragonBCastHeader_t* header)
{
    _bcast_map_obj(obj_ptr, header);

    *(header->num_waiting) = 0UL;
    *(header->num_triggered) = 0UL;
    *(header->triggering) = 0;
    *(header->shutting_down) = 0UL;
    *(header->allowable_count) = 0UL;
    *(header->num_to_trigger) = 0UL;
    *(header->state) = 0UL;
    *(header->lock_sz) = dragon_lock_size(attr->lock_type);
    *(header->spin_list_sz) = max_spinsig_num;
    *(header->spin_list_count) = 0UL;
    *(header->proc_max) = attr->max_procs_to_track;
    *(header->proc_num) = 0UL;
    *(header->payload_area_sz) = max_payload_sz;
    *(header->payload_sz) = 0UL;
    *(header->sync_type) = attr->sync_type;
    *(header->sync_num) = attr->sync_num;
    dragon_generate_uuid(*header->id);
    *(header->lock_type) = attr->lock_type;
    header->spin_list = ((void*)header->lock) + *(header->lock_sz);
    for (int k=0; k<*(header->spin_list_sz); k++)
        header->spin_list[k] = 0UL;

    header->proc_list = (void*)header->spin_list + (*(header->spin_list_sz)) * sizeof(dragonULInt);

    for (int k=0; k<*(header->proc_max); k++)
        header->proc_list[k] = 0;

    header->payload_area = (void*)header->proc_list + (*(header->proc_max)) * sizeof(pid_t);

    size_t diff = (((void*)header->payload_area) + max_payload_sz) - obj_ptr;
    if (diff > alloc_sz) {
        char err_str[300];
        snprintf(err_str, 299, "The provided size was %lu bytes and the required size was %lu bytes.\nThere is not enough room to allocate the requested bcast object.", alloc_sz, diff);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    _num_cores(); // Call it once to get it initialized if not already. Avoids doing it first time in loop.

    no_err_return(DRAGON_SUCCESS);
}

static void
_bcast_attach_obj(void* obj_ptr, dragonBCastHeader_t* header)
{
    _bcast_map_obj(obj_ptr, header);
    header->spin_list = ((void*)header->lock) + *(header->lock_sz);
    header->proc_list = (void*)header->spin_list + (*(header->spin_list_sz)) * sizeof(dragonULInt);
    header->payload_area = (void*)header->proc_list + (*(header->proc_max)) * sizeof(pid_t);

    _num_cores(); // Call it once to get it initialized if not already. Avoids doing it first time in loop.
}

/* obtain a channel structure from a given channel descriptor */
static dragonError_t
_bcast_handle_from_descr(const dragonBCastDescr_t * bd, dragonBCast_t ** handle)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid BCast handle.");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_bcast, bd->_idx, (void *)handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in BCast umap.");

    no_err_return(DRAGON_SUCCESS);
}

/* given a umap id, check if we already are attached to that BCast object
   and update the descriptor for use */
static dragonError_t
_bcast_descr_from_id(const dragonULInt id, dragonBCastDescr_t * bd)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor pointer is NULL.");

    /* find the entry in our pool map for this descriptor */
    dragonBCast_t * handle;
    dragonError_t err = dragon_umap_getitem(dg_bcast, id, (void *)&handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find BCast object in the umap.");

    /* update the descriptor with the id key */
    /* possibly update a ref count for handles to this object */
    bd->_idx = id;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_bcast_wait_on_triggering(dragonBCast_t * handle, timespec_t * end_time)
{
    _Atomic(uint32_t) *triggering_ptr = handle->header.triggering;
    timespec_t now_time = {0,0};

    /* check_timeout_when_0 is used below when there is a timeout and we spin wait. No need to check it
       on every loop */
    size_t check_timeout_when_0 = 1;

    while (atomic_load(triggering_ptr) != 0) {
        if (end_time != NULL) {
            if (check_timeout_when_0 == 0) {
                clock_gettime(CLOCK_MONOTONIC, &now_time);
                if (dragon_timespec_le(end_time, &now_time))
                    err_return(DRAGON_TIMEOUT, "Timeout while waiting for previous triggering");
            }
            check_timeout_when_0 = (check_timeout_when_0 + 1) % 100;
        }
        sched_yield();
    }

    no_err_return(DRAGON_SUCCESS);
}

static void
_trigger_completion(dragonBCast_t * handle)
{
    /* This is called by either the triggerer or, in the case of a zero-byte payload,
       it is called by the last waiter to wakeup. This is part of an optimization that
       allows the triggerer to trigger and leave immediately when there is no payload
       to transfer to waiters. */

    /* setting triggering to 0 means we are no longer in the process of triggering so
       waiters will wait that were paused while triggering. Using atomic store for the
       final store will insure all earlier ones are flushed too. */

    *handle->header.allowable_count = 0;
    *handle->header.num_to_trigger = 0;
    *handle->header.state = 0;
    atomic_store(handle->header.triggering, 0);

    if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
        dragon_unlock(&handle->lock);
}

static int
_register_pid(dragonBCast_t * handle)
{
    pid_t expected;

    pid_t my_pid = _getpid();

    int k;
    if (atomic_fetch_add(handle->header.proc_num, 1L) < *handle->header.proc_max)
        for (k=0; k<*handle->header.proc_max; k++) {
            expected = 0;
            if (atomic_compare_exchange_strong(&(handle->header.proc_list[k]), &expected, my_pid)) {
                return k;
            }
        }

    atomic_fetch_add(handle->header.proc_num, -1L);
    return -1; /* indicates pid not stored. */
}

/* Use only under a lock. */
static void
_erase_pid(dragonBCast_t * handle, int idx)
{
    pid_t expected = _getpid();

    if (idx < 0)
        return; /* The idx is -1 if there is no room. */

    if (atomic_compare_exchange_strong(&handle->header.proc_list[idx], &expected, 0))
        atomic_fetch_add(handle->header.proc_num, -1L);
}

static dragonError_t
_check_pids(dragonBCast_t * handle)
{
    char msg[200];
    int num_procs = *handle->header.proc_num;
    int found_pids = 0;
    pid_t lost_pid = 0;
    int k;
    int rc;

    for (k=0;k<*handle->header.proc_max; k++) {
        if (handle->header.proc_list[k] != 0) {
            rc = kill(handle->header.proc_list[k], 0);
            if (rc == 0)
                found_pids += 1;

            if (rc == -1) {
                if (errno == ESRCH) {
                    lost_pid = handle->header.proc_list[k];
                    /* attempt to recover from process dying in wait. */
                    atomic_fetch_add(handle->header.num_waiting, -1L);
                    _erase_pid(handle, k);
                }

                if (errno == EPERM)
                    err_return(DRAGON_FAILURE, "Could not send 0 signal to process in bcast! This should not happen.");
            }
        }

        if (found_pids == num_procs)
            no_err_return(DRAGON_SUCCESS);
    }

    if (lost_pid > 0) {
        snprintf(msg, 199, "At least one process, including pid=%d died while waiting in a Dragon BCast object. Trigger failed.", lost_pid);
        err_return(DRAGON_BCAST_DEAD_WAITER, msg);
    }

    no_err_return(DRAGON_SUCCESS);
}

static int
_wake_sleepers(dragonBCast_t * handle, int num_to_wake)
{
    int sleepers = 0;
    int k;
    int rc;
    for (k=0;k<*handle->header.proc_max; k++) {
        if (handle->header.proc_list[k] != 0) {
            /* Any signal, including SIGCONT, will wake up a sleeping proc. */
            rc = kill(handle->header.proc_list[k], SIGCONT);
            if (rc == 0)
                sleepers += 1;
        }

        if (sleepers == num_to_wake)
            break;
    }

    return sleepers;
}

static timespec_t
next_time_slice(int* slice_count, timespec_t* current_slice, int num_waiting, timespec_t* remaining_time) {
    if (dragon_timespec_le(remaining_time, current_slice))
        return *remaining_time;

    timespec_t return_val = {0,0};

    unsigned long nanos = current_slice->tv_sec * nanos_per_second + current_slice->tv_nsec;

    *slice_count += 1;

    int core_factor = _core_factor();
    if (*slice_count >= MAX_SLICE_ITERS) {
        *slice_count = 0;
        nanos = nanos * SLICE_BACKOFF_FACTOR;
        current_slice->tv_sec = nanos / nanos_per_second;
        current_slice->tv_nsec = nanos % nanos_per_second;
    }

    int factor = core_factor / num_waiting;
    if (factor == 0)
        factor = 1;

    // Adjust next slice by waiting factor which factors in the number of CPU cores.
    nanos = nanos / factor;

    if (nanos > nanos_per_second)
        nanos = nanos_per_second;

    nanos += num_waiting * 17;

    return_val.tv_sec = nanos / nanos_per_second;
    return_val.tv_nsec = nanos % nanos_per_second;

    return return_val;
}

static dragonError_t
_idle_wait(dragonBCast_t * handle, void * num_waiting_ptr, timespec_t* end_time, dragonReleaseFun release_fun, void* release_arg, dragonULInt* already_waiter)
{
    timespec_t now_time;
    struct sigaction old_sig_action;
    bool handler_installed = false;
    timespec_t remaining_time = {LONG_MAX,0};
    timespec_t * remaining_timeout = &remaining_time;
    void* shutting_down_ptr = (void*)handle->header.shutting_down;
    dragonULInt my_num_waiting;

    if (already_waiter == NULL) {
        /* this can be called from either the regular wait functions or from the _spin_wait
           function when adaptive wait is chosen. If adaptive waiting, then the process is
           already a waiter and should not execute this code (again). */

        /* increment the num_waiting atomically and get our value. We lock here if it is not a
           synchronized bcast because we want to make sure the triggerer does not get part way
           through triggering while we are incrementing the num_waiting. */
        if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
            dragon_lock(&handle->lock);

        my_num_waiting = atomic_fetch_add(handle->header.num_waiting, 1L) + 1;

        if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
            dragon_unlock(&handle->lock);

        if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting > *(handle->header.sync_num)) {
            atomic_fetch_add(handle->header.num_waiting, -1L);
            err_return(DRAGON_INVALID_OPERATION, "There cannot be more waiters than the specified sync number on a synchronized bcast");
        }

        /* When the process is now officially waiting, the resource release (back in the caller) can occur. The
        resource release is optional. */
        if (release_fun != NULL)
            (release_fun)(release_arg);

        /* When sync between trigger and wait is requested, releasing
        the lock here is necessary to unblock any triggerer
        that is waiting on the specified number of waiters before
        triggering can proceeed.
        */
        if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting == *(handle->header.sync_num))
            dragon_unlock(&handle->lock);
    } else
        my_num_waiting = *already_waiter;

    int rc = 0;
    dragonUUID local_uuid;
    dragon_copy_uuid(local_uuid, *handle->header.id);

    bool waiting = true;
    timespec_t slice = {0, SLICE_NS_START};
    timespec_t sleepy_time = {0,0};
    int slice_count = 0;

    /* the while loop below is needed to handle the spurious wakeup from the futex wait. */
    while (waiting) {

        /* The allowable_count is needed because when triggering there
        is a possiblity that this process and another may both get triggered since
        one might get here while another is waiting on the futex and the futex waiter
        wakes up and this one never waits on the futex. In this way we may have more
        idle waiters than what were woken up proceed through this _idle_wait code. However
        if more than allowable_count make it through this code in this loop, the extra
        processes will see that allowable count is not > 0 (before decrementing) and
        they will continue to wait on here for the next triggering. */

        int ticket = atomic_fetch_add(handle->header.allowable_count, -1);
        if (ticket > 0)
            waiting = false;

        if (waiting) {
            if (end_time != NULL) {
                clock_gettime(CLOCK_MONOTONIC, &now_time);
                if (dragon_timespec_le(end_time, &now_time)) {
                    atomic_fetch_add(handle->header.num_waiting, -1L);
                    /* If we timed out waiting for a triggerer and it is a synchronized BCast and we released
                       the lock, then we need to acquire it again. There may be a window here where we might
                       have allowed a trigger to proceed at same time as timing out. If so, they get what
                       they asked for when using a synchronized bcast and using a timeout. */
                    if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting == *(handle->header.sync_num))
                        dragon_lock(&handle->lock);
                    _uninstall_signal_handler(handler_installed, &old_sig_action);
                    err_return(DRAGON_TIMEOUT, "Timeout while idle waiting on BCast");
                }
                dragon_timespec_diff(remaining_timeout, end_time, &now_time);
            }

            int num_waiting = atomic_load(handle->header.num_waiting);

            sleepy_time = next_time_slice(&slice_count, &slice, num_waiting, remaining_timeout);

            int pid_idx = _register_pid(handle);

            if (!dragon_timespec_le(&sleepy_time, &min_time_slice)) {
                timespec_t less_overhead_of_call;
                dragon_timespec_diff(&less_overhead_of_call, &sleepy_time, &min_time_slice);

                if (!handler_installed) {
                    _install_signal_handler(&old_sig_action);
                    handler_installed = true;
                }
                rc = nanosleep(&less_overhead_of_call, NULL);

            } else {
                sched_yield();
                PAUSE();
            }

            if (dragon_compare_uuid(local_uuid, *handle->header.id)) {
                // not the same uuid. That means this process woke up from another
                // process' bcast. This is a possibility, but would be a HUGE
                // coincidence. In this case we do not do anything more and we leave.
                _uninstall_signal_handler(handler_installed, &old_sig_action);
                err_return(DRAGON_INVALID_OPERATION, "This process woke up from a bcast and found itself inside another bcast.");
            }

#ifdef CHECK_POINTER
            if (!_is_pointer_valid(handle->header.shutting_down)) {
                /* The memory on which this bcast was allocated has been freed while
                    it waited. Get out now! */
                no_err_return(DRAGON_SUCCESS);
            }
#endif

            if (handle->header.shutting_down != shutting_down_ptr) {
                /* This is an indication that the bcast object was deallocated while
                this process was sleeping. */
                _uninstall_signal_handler(handler_installed, &old_sig_action);
                err_return(DRAGON_BCAST_HANDLE_ERROR, "The BCast handle shutdown pointer was corrupted. The BCast or the object it was in was likely deallocated while it slept.");
            }

            _erase_pid(handle, pid_idx);

            if (atomic_load(handle->header.shutting_down)) {
                /* When shutting down the bcast_wait function calling this
                will check shutting_down too and will decrement num_waiting
                and return. This happens when destroy is called while waiters
                are waiting. */
                _uninstall_signal_handler(handler_installed, &old_sig_action);
                no_err_return(DRAGON_SUCCESS);
            }

            /* If the condition below is satisfied, the an error occurred that was not a timeout but some other error. */
            if (rc == -1 && errno != EINTR) {
                char err_str[200];
                atomic_fetch_add(handle->header.num_waiting, -1L);
                snprintf(err_str, 199, "sleep call returned an error %d with ERRNO=%d and ERRNO msg=%s", rc, errno, strerror(errno));
                _uninstall_signal_handler(handler_installed, &old_sig_action);
                err_return(DRAGON_BCAST_IDLE_WAIT_CALL_ERROR, err_str);
            }
        }
    }

    _uninstall_signal_handler(handler_installed, &old_sig_action);
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_spin_wait(dragonBCast_t * handle, void * num_waiting_ptr, timespec_t* end_time, dragonReleaseFun release_fun, void* release_arg, dragonWaitMode_t* wait_mode)
{
    int idx = 0;
    bool found = false;
    atomic_uint expected = 0UL;
    timespec_t now_time;

    /* increment the num_waiting atomically and get our value. We lock here if it is not a
        synchronized bcast because we want to make sure the triggerer does not get part way
        through triggering while we are incrementing the num_waiting. */
    if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
        dragon_lock(&handle->lock);

    /* Assume we will find a spot. This makes sure we don't miss a spinner later. */
    atomic_fetch_add(handle->header.spin_list_count, 1L);

    while ((found == false) && (idx < *(handle->header.spin_list_sz))) {
        if (atomic_compare_exchange_strong(&(handle->header.spin_list[idx]), &expected, 1UL))
            found = true;
        else {
            /* atomic_compare_exchange alters expected when compare fails so reset it */
            expected = 0UL;
            idx += 1;
        }
    }

    if (!found) {
        /* If no space is available for spin waiting, then there are enough spin_waiters to get
           instant performance, and since this process has to idle wait anyway, we'll just
           switch over to idle wait when a process can't get a spot in the spin list. This will
           lead to the process waiting until all other spin waiters are done, but there is no
           guaranteed ordering among spin waiters either. */
        atomic_fetch_add(handle->header.spin_list_count, -1L);
        if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
            dragon_unlock(&handle->lock);
        return _idle_wait(handle, num_waiting_ptr, end_time, release_fun, release_arg, NULL);
    }

    /* increment the num_waiting atomically, but also get the new value for num_waiting */
    dragonULInt my_num_waiting = atomic_fetch_add(handle->header.num_waiting, 1UL) + 1;

    if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
        dragon_unlock(&handle->lock);

    if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting > *(handle->header.sync_num)) {
        atomic_fetch_add(handle->header.num_waiting, -1L);
        atomic_store(&handle->header.spin_list[idx], 0UL);
        atomic_fetch_add(handle->header.spin_list_count, -1L);
        err_return(DRAGON_INVALID_OPERATION, "There cannot be more waiters than the specified sync number on a synchronized bcast");
    }

    /* When the process is now officially waiting, the resource release (back in the caller) can occur. The
    resource release is optional. */
    if (release_fun != NULL)
        (release_fun)(release_arg);

    int proc_idx = _register_pid(handle);

    /* When sync between trigger and wait is requested, releasing
       the lock here is necessary to unblock any triggerer
       that is waiting on the specified number of waiters before
       triggering can proceeed.
    */
    if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting == *(handle->header.sync_num))
        dragon_unlock(&handle->lock);

    /* do_when is used below when there is a timeout and when we spin wait. No need to do it
       on every iteration */
    size_t do_when = 0;
    size_t number_of_yields = 0;

    while ((*handle->header.shutting_down == 0UL) && (handle->header.spin_list[idx] == 1UL)) {
        if (do_when == DRAGON_BCAST_SPIN_CHECK_TIMEOUT_ITERS) {
            if (end_time != NULL) {
                clock_gettime(CLOCK_MONOTONIC, &now_time);
                if (dragon_timespec_le(end_time, &now_time)) {

                    /* There is a very small window where we could set
                    spin_list[idx] to 0 here, but the trigger is happening at the
                    same moment and about to set the entry to 2 for a trigger. So we
                    do a swap here to make sure that does not happen. If we went to
                    set the entry to 0 and it was already 2 then that small window
                    occured where we were going to timeout, but it got triggered at
                    the same moment interleaved with this code and we want to then
                    proceed after the loop. */

                    if (atomic_exchange(&(handle->header.spin_list[idx]), 0UL) == 2UL)
                        break;

                    atomic_fetch_add(handle->header.num_waiting, -1L);
                    atomic_fetch_add(handle->header.spin_list_count, -1L);
                    atomic_store(&handle->header.spin_list[idx], 0UL);
                    _erase_pid(handle, proc_idx);
                    /* If we timed out waiting for a triggerer and it is a synchronized BCast and we released
                       the lock, then we need to acquire it again. There may be a window here where we might
                       have allowed a trigger to proceed at same time as timing out. If so, they get what
                       they asked for when using a synchronized bcast and using a timeout. */
                    if (*(handle->header.sync_type) == DRAGON_SYNC && my_num_waiting == *(handle->header.sync_num))
                        dragon_lock(&handle->lock);
                    err_return(DRAGON_TIMEOUT, "Timeout while spin waiting on BCast.");
                }
            }

            if ((wait_mode != NULL) && (*wait_mode == DRAGON_ADAPTIVE_WAIT) && (number_of_yields == DRAGON_BCAST_ADAPTIVE_WAIT_TO_IDLE)) {
                /* We did a spin wait for a bit, now it's time for idle wait. */
                expected = 1UL;
                if (atomic_compare_exchange_strong(&(handle->header.spin_list[idx]), &expected, 0UL)) {
                    atomic_fetch_add(handle->header.spin_list_count, -1L);
                    _erase_pid(handle, proc_idx);
                    return _idle_wait(handle, num_waiting_ptr, end_time, release_fun, release_arg, &my_num_waiting);
                } else
                    break;
            }

            sched_yield();
            number_of_yields += 1;
            do_when = 0;
        } else
            do_when = do_when + 1;

        if (handle->header.num_waiting != num_waiting_ptr) {
            /* The handle was possibly deleted while spin waiting. This check
               is likely NOT going to catch the error since other code above in the loop
               is using the handle, but we'll try since we do it in idle_wait as well.
               Having the code in this part of the loop is likely best since the usleep
               just completed. */
            err_return(DRAGON_BCAST_HANDLE_ERROR, "The BCast handle was corrupted.");
        }
    }

    _erase_pid(handle, proc_idx);
    atomic_store(&handle->header.spin_list[idx], 0UL);
    atomic_fetch_add(handle->header.spin_list_count, -1L);

    no_err_return(DRAGON_SUCCESS);
}

/* This must be a void* return type on this function because pthread creation requires it. */
static void *
_bcast_notify_callback(void * ptr)
{
    dragonBCastCallbackArg_t * arg = (dragonBCastCallbackArg_t*) ptr;
    void * payload_ptr = NULL;
    size_t payload_sz = 0;
    timespec_t* timer = NULL;

    if (arg->timer_is_null == false)
        timer = &arg->timer;

    dragonError_t err = dragon_bcast_wait(&arg->bd, arg->wait_mode, timer, &payload_ptr, &payload_sz, NULL, NULL);

    if (err != DRAGON_SUCCESS) {
        (arg->fun)(arg->user_def_ptr, NULL, 0, err, dragon_getlasterrstr());
        return NULL;
    }

    /* Call the callback */
    (arg->fun)(arg->user_def_ptr, payload_ptr, payload_sz, DRAGON_SUCCESS, NULL);

    free(arg);

    return NULL;
}

static void *
_bcast_notify_signal(void * ptr)
{
    dragonBCastSignalArg_t * arg = (dragonBCastSignalArg_t*) ptr;
    timespec_t* timer = NULL;

    if (arg->timer_is_null == false)
        timer = &arg->timer;

    if (arg->rc == NULL) {
        fprintf(stderr, "BCast notify rc field of signal handling call cannot be NULL.\n");
        return NULL;
    }

    if (arg->err_string == NULL) {
        fprintf(stderr, "BCast notify err_string field of signal handling call cannot be NULL.\n");
        return NULL;
    }

    if (arg->parent_pid == 0) {
        fprintf(stderr, "BCast notify parent_pid field of signal handling call cannot be 0.\n");
        return NULL;
    }

    *(arg->rc) = dragon_bcast_wait(&arg->bd, arg->wait_mode, timer, arg->payload_ptr, arg->payload_sz, NULL, NULL);

    if (*(arg->rc) != DRAGON_SUCCESS) {
        *(arg->err_string) = dragon_getlasterrstr();
    } else {
        *(arg->err_string) = NULL;
    }

    /* Signal the process */
    kill(arg->parent_pid, arg->sig);

    free(arg);

    return NULL;
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void
_set_thread_local_mode_bcast(bool set_thread_local)
{
    if (set_thread_local) {
        dg_bcast = &_dg_thread_bcast;
    } else {
        dg_bcast = &_dg_proc_bcast;
    }
}

/****************************************************************************
 ************************ USER API ******************************************
 ****************************************************************************/

/** @brief Compute the size of space needed to host a bcast object.
 *
 *  When creating a bcast synchronization object in pre-allocated memory, this
 *  function returns the required size for a given payload maximum size and a
 *  maximum number of spin/async notification waiters. The maximum number of waiters
 *  using either spin wait or asynchronous signal notification must be known
 *  when this function is called to compute the required memory size.
 *
 *  You must call dragon_bcast_attr_init to initialize attr before calling this
 *  function unless you want all the defaults. If you want default behavior, attr
 *  may be NULL.
 *
 *  @param max_payload_sz The maximum number of bytes that will be needed to store
 *  the payload to be broadcast to others.
 *
 *  @param max_spinsig_num The maximum number of spinner waiters and async waiters
 *  that will wait on this BCast object.
 *
 *  @param attr The other attributes of the object. Some of these may impact the size.
 *
 *  @param size A pointer to a size_t variable to hold the required size. This will be
 *  the required number of bytes needed to hold this object in memory.
 *
 *  @return DRAGON_SUCCESS or an error code indicating the problem.
 **/

dragonError_t
dragon_bcast_size(size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, size_t* size)
{
    dragonBCastAttr_t default_attr;

    if (attr == NULL) {
        attr = &default_attr;
        // we don't check for an error since this is handled internally and there
        // won't be an error and we couldn't return an error anyway.
        dragon_bcast_attr_init(attr);
    } else {
        dragonError_t err = _bcast_validate_attrs(attr);

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "BCast attributes validation failed.");

    }

    size_t required_size = 0;

    required_size += sizeof(dragonULInt) * DRAGON_BCAST_NULINTS;
        /* This is the calculated size of all integer fields within the object */

    required_size += dragon_lock_size(attr->lock_type); /* lock */

    required_size += sizeof(pid_t) * attr->max_procs_to_track;

    required_size += sizeof(dragonULInt) * max_spinsig_num;

    required_size += max_payload_sz;

    *size = required_size;

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Initialize a BCast Attributes Structure
 *
 *  When creating a bcast synchronization object certain attributes may be supplied
 *  to affect how the object behaves. This attr_init function initializes the attributes
 *  structure to default values which then may be subsequently overridden before creating
 *  the BCast object.
 *
 *  @param attr A pointer to the attribute structure for a BCast object.
 * *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/

dragonError_t
dragon_bcast_attr_init(dragonBCastAttr_t* attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast attr cannot be NULL");

    attr->lock_type = DRAGON_BCAST_DEFAULT_LOCK_TYPE;
    attr->sync_type = DRAGON_NO_SYNC;
    attr->sync_num = 0;
    attr->max_procs_to_track = DRAGON_BCAST_DEFAULT_TRACK_PROCS;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Create a BCast object in pre-allocated memory for use in sharing a payload.
 *
 *  Create a BCast object in the pre-allocated memory pointed to by loc, with
 *  max_payload_sz and initialize the handle to it in bd. BCast objects are
 *  on-node only objects. The max_payload_sz must be greater than or equal to 0.
 *  The memory in which the BCast object is to be created must start on a word boundary.
 *  Four byte (i.e. word) boundary alignment is required.
 *
 *  @param loc The location in memory where this BCast object will be located.
 *
 *  @param alloc_sz The size of the allocation pointed to by loc.
 *
 *  @param max_payload_sz The maximum size payload to be allowed for this object.
 *
 *  @param max_spinsig_num The maximum number of spin/async notification waiters on this
 *  object.
 *
 *  @param attr Attributes that control the behavior of this object.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/

dragonError_t
dragon_bcast_create_at(void* loc, size_t alloc_sz, size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, dragonBCastDescr_t* bd)
{
    dragonError_t err;
    dragonBCastAttr_t default_attr;

    if (loc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The location for the creation of a BCast object cannot be NULL.");

    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor pointer cannot be NULL.");

    if (((dragonULInt)loc) % sizeof(unsigned int) != 0) {
        char err_str[200];
        snprintf(err_str, 199, "The BCast object must be created on a %ld-byte boundary.",sizeof(int));
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    if (attr == NULL) {
        attr = &default_attr;
        err = dragon_bcast_attr_init(attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create BCast object. Error initializing default attributes on handle.");
    } else {
        err = _bcast_validate_attrs(attr);

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "BCast attributes validation failed.");
    }

    /* this will be freed in the bcast_destroy_at call */
    dragonBCast_t * handle = malloc(sizeof(dragonBCast_t));
    if (handle == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new BCast object handle");


    handle->obj_ptr = loc;

    /* map the header and set values from the attrs */
    err = _bcast_init_obj(handle->obj_ptr, alloc_sz, max_payload_sz, max_spinsig_num, attr, &handle->header);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("The allocated size is too small.");
        goto _bcast_create_at_rollback_chkpnt;
    }
    /* instantiate the dragon lock */
    err = dragon_lock_init(&handle->lock, handle->header.lock, attr->lock_type);

    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not init Dragon Lock in BCast object");
        goto _bcast_create_at_rollback_chkpnt;
    }

    handle->in_managed_memory = FALSE;
        /* this means the pool and mem descriptor are uninitialized and this object is NOT serializable. */

    /* register this BCast object in our umap with a generated key */
    err = _bcast_add_umap_entry(bd, handle);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to insert item into BCast umap.");
        goto _bcast_create_at_rollback_chkpnt;
    }

    /* When used in synchronizing mode, the object starts out locked for triggerers */
    if (*(handle->header.sync_type) == DRAGON_SYNC)
        dragon_lock(&handle->lock);

    no_err_return(DRAGON_SUCCESS);

    /* If we return an error, we rollback any memory allocations on the heap. */

    _bcast_create_at_rollback_chkpnt:

        free(handle);

    return err;
}


/** @brief Create a BCast object in a managed memory pool for use in sharing a payload.
 *
 *  Create a BCast object in the given memory pool, with
 *  max_payload_sz and initialize the handle to it in bd. BCast objects are
 *  on-node only objects. The max_payload_sz must be greater than or equal to 0.
 *
 *  @param pd A pool descriptor where the BCast object will be created.
 *
 *  @param max_payload_sz The maximum size payload to be allowed for this object.
 *
 *  @param max_spinsig_num The maximum number of spin/async notification waiters on this
 *  object.
 *
 *  @param attr Attributes that control the behavior of this object.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/

dragonError_t
dragon_bcast_create(dragonMemoryPoolDescr_t* pd, size_t max_payload_sz, size_t max_spinsig_num, dragonBCastAttr_t* attr, dragonBCastDescr_t* bd)
{
    dragonError_t err;
    dragonBCastAttr_t default_attr;

    if (pd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The Pool Descriptor used in the creation of a BCast object cannot be NULL.");

    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor pointer cannot be NULL.");

    if (attr == NULL) {
        attr = &default_attr;
        err = dragon_bcast_attr_init(attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create BCast object. Error initializing default attributes on handle.");
    } else {
        err = _bcast_validate_attrs(attr);

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "BCast attributes validation failed.");
    }

    /* the memory pool must be locally addressable */
    if (!dragon_memory_pool_is_local(pd))
        append_err_return(err, "cannot directly access memory pool for bcast creation");

    /* this will be freed in the bcast_destroy call */
    dragonBCast_t * handle = malloc(sizeof(dragonBCast_t));
    if (handle == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new BCast object handle");

    /* make a clone of the pool descriptor for use here */
    err = dragon_memory_pool_descr_clone(&handle->pool, pd);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Cannot clone pool descriptor in BCast create.");
        goto _bcast_create_rollback_chkpnt1;
    }

    /* determine the allocation size required for the request channel */
    size_t alloc_sz;

    err = dragon_bcast_size(max_payload_sz, max_spinsig_num, attr, &alloc_sz);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Unable to get size of bcast object.");
        goto _bcast_create_rollback_chkpnt1;
    }

    /* allocate the space using the alloc type interface with a channel type */
    err = dragon_memory_alloc(&handle->obj_mem, &handle->pool, alloc_sz);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Unable to allocate memory for bcast object from memory pool.");
        goto _bcast_create_rollback_chkpnt1;
    }

    err = dragon_memory_get_pointer(&handle->obj_mem, &handle->obj_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Unable to get pointer to memory for BCast object.");
        goto _bcast_create_rollback_chkpnt2;
    }

    if (((dragonULInt)handle->obj_ptr) % sizeof(unsigned int) != 0) {
        char err_str[200];
        snprintf(err_str, 199, "The BCast object must be created on a %ld-byte boundary.",sizeof(int));
        err_return(DRAGON_FAILURE, err_str);
    }

    /* map the header and set values from the attrs */
    err = _bcast_init_obj(handle->obj_ptr, alloc_sz, max_payload_sz, max_spinsig_num, attr, &handle->header);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("The allocated space was too small");
        goto _bcast_create_rollback_chkpnt2;
    }

    /* instantiate the dragon lock */
    err = dragon_lock_init(&handle->lock, handle->header.lock, attr->lock_type);

    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not init Dragon Lock in BCast object");
        goto _bcast_create_rollback_chkpnt2;
    }

    handle->in_managed_memory = TRUE;
        /* this means the pool and mem descriptor are initialized and this object is serializable. */

    /* register this BCast object in our umap with a generated key */
    err = _bcast_add_umap_entry(bd, handle);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to insert item into BCast umap.");
        goto _bcast_create_rollback_chkpnt2;
    }

    /* When used in synchronizing mode, the object starts out locked for triggerers */
    if (*(handle->header.sync_type) == DRAGON_SYNC)
        dragon_lock(&handle->lock);

    no_err_return(DRAGON_SUCCESS);

    /* If we return an error, we rollback any memory allocations on the heap or in managed memory. */

    _bcast_create_rollback_chkpnt2:

        dragon_memory_free(&handle->obj_mem);

        /* We may need to detach from cloned memory pool descriptor if reference
            counting within a process is implemented. */

    _bcast_create_rollback_chkpnt1:

        free(handle);

    return err;
}


/** @brief Attach to a BCast Object.
 *
 *  Given the location of a BCast object, attach to it by initializing a
 *  BCast Descriptor handle to the object. The attach_at function should
 *  only be used if the corresponding create_at function was used to
 *  create it. Otherwise, use the attach function.
 *
 *  @param loc A pointer to the BCast object in the current process' memory.
 *
 *  @param bd The BCast object's descriptor handle to be initialized by this call.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_attach_at(void* loc, dragonBCastDescr_t* bd)
{
    dragonError_t err;

    /* this will be freed in the bcast_destroy_at call */
    dragonBCast_t * handle = malloc(sizeof(dragonBCast_t));
    if (handle == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new BCast object handle");

    handle->obj_ptr = loc;

    /* map the header and set values from the attrs */
    _bcast_attach_obj(handle->obj_ptr, &handle->header);

    /* attach the dragon lock */
    err = dragon_lock_attach(&handle->lock, handle->header.lock);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create BCast object. Lock initialization failed.");
        goto _bcast_attach_at_rollback_chkpnt;
    }

    handle->in_managed_memory = FALSE;
        /* this means the pool and mem descriptor are uninitialized and this object is NOT serializable. */

    /* register this BCast object in our umap with a generated key */
    err = _bcast_add_umap_entry(bd, handle);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to insert item into BCast umap.");
        goto _bcast_attach_at_rollback_chkpnt;
    }

    no_err_return(DRAGON_SUCCESS);

    /* If we return an error, we rollback any memory allocations on the heap. */

    _bcast_attach_at_rollback_chkpnt:

        free(handle);

    return err;
}


/** @brief Attach to a BCast Object.
 *
 *  A serialized descriptor can be shared between processes. A process
 *  may attach to a BCast Object using one of these
 *  serialized descriptors.
 *
 *  @param bd_ser The serialized BCast object descriptor.
 *
 *  @param bd A BCast descriptor handle to be initialized by this call.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_attach(dragonBCastSerial_t* bd_ser, dragonBCastDescr_t* bd)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    if (bd_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized BCast descriptor cannot be NULL.");

    if (bd_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized BCast descriptor appears to be uninitialized. The data cannot be NULL.");

    if (bd_ser->len <= DRAGON_BCAST_SERIAL_NULINTS*sizeof(dragonULInt))
        err_return(DRAGON_INVALID_ARGUMENT, "The size of the BCast serialized descriptor is incorrect.");

    /* pull out the hostid to see if it is a local object or not. */
    dragonULInt * sptr = (dragonULInt *)bd_ser->data;
    dragonULInt hostid = (dragonULInt)*sptr;
    sptr++;

    if (hostid != dragon_host_id())
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast serialized descriptor can only be attached locally, not on a different host node.");

    dragonULInt id = *sptr;
    sptr++;

    /* check if we have already attached to the bcast object. */
    dragonError_t err = _bcast_descr_from_id(id, bd);

    if (err == DRAGON_SUCCESS)
        no_err_return(DRAGON_SUCCESS);

    /* we'll need to construct a new BCast handle (freed in either bcast_destroy or bcast_detach) */
    dragonBCast_t * handle = malloc(sizeof(dragonBCast_t));
    if (handle == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Unable to allocate new BCast handle.");

    /* attach to the memory descriptor */
    dragonMemorySerial_t mem_ser;
    mem_ser.len = bd_ser->len - DRAGON_BCAST_SERIAL_NULINTS*sizeof(dragonULInt);
    mem_ser.data = (uint8_t *)sptr;
    err = dragon_memory_attach(&handle->obj_mem, &mem_ser);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Cannot attach to memory with serialized descriptor.");
        goto _bcast_attach_rollback_chkpnt1;
    }

    /* get the pool descriptor and pointer from the memory descriptor */
    err = dragon_memory_get_pool(&handle->obj_mem, &handle->pool);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Cannot get memory pool from memory descriptor.");
        goto _bcast_attach_rollback_chkpnt2;
    }

    err = dragon_memory_get_pointer(&handle->obj_mem, &handle->obj_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Cannot get pointer from memory descriptor.");
        goto _bcast_attach_rollback_chkpnt2;
    }

    /* map the bcast header */
    _bcast_attach_obj(handle->obj_ptr, &handle->header);

    /* attach the dragon lock */
    err = dragon_lock_attach(&handle->lock, handle->header.lock);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create BCast object. Lock initialization failed.");
        goto _bcast_attach_rollback_chkpnt2;
    }

    handle->in_managed_memory = TRUE;
        /* this means the pool and mem descriptor are initialized and this object is serializable. */

    /* register this channel in our umap using a generated key */
    err = _bcast_add_umap_entry(bd, handle);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to insert item into channels umap");
        goto _bcast_attach_rollback_chkpnt3;
    }

    no_err_return(DRAGON_SUCCESS);

    _bcast_attach_rollback_chkpnt3:
        dragon_lock_detach(&handle->lock);

    _bcast_attach_rollback_chkpnt2:
        dragon_memory_detach(&handle->obj_mem);

    _bcast_attach_rollback_chkpnt1:
        if (handle != NULL)
            free(handle);

    return err;
}


/** @brief Destroy a BCast object.
 *
 *  When done using a BCast object, destroy should be called once to release the
 *  pool resources used by the object.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_destroy(dragonBCastDescr_t* bd)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "BCast descriptor cannot be NULL.");

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not obtain handle from BCast descriptor.");


    timespec_t timeout;
    timeout.tv_nsec = 0;
    timeout.tv_sec = DRAGON_BCAST_DESTROY_TIMEOUT_SEC;
    timespec_t end_time;
    timespec_t now_time;
    clock_gettime(CLOCK_MONOTONIC, &now_time);
    err = dragon_timespec_add(&end_time, &now_time, &timeout);
    atomic_store(handle->header.shutting_down, 1UL);
    int check_pids_iters = 0;

    while (atomic_load(handle->header.num_waiting) > 0 && dragon_timespec_le(&now_time, &end_time)) {
        /* We must repeat the loop here as long as there are waiters */
        if (check_pids_iters == 100) {
            _wake_sleepers(handle, INT_MAX);
            _check_pids(handle);
            check_pids_iters = 0;
            sched_yield();
            clock_gettime(CLOCK_MONOTONIC, &now_time);
        }
        check_pids_iters += 1;
    }

    /* tear down the lock */
    err = dragon_lock_destroy(&handle->lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to destroy Dragon lock in BCast object destroy.");

    if (handle->in_managed_memory) {

        /* release memory back to the pool */
        err = dragon_memory_free(&handle->obj_mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot release BCast memory back to pool.");

        /* When we have reference counting, we'll need to detach from the
           memory pool as well */
        //err = dragon_memory_pool_detach(&handle->pool);
        //if (err != DRAGON_SUCCESS)
        //    append_err_return(err, "Could not detach from BCast memory pool.");
    }

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_bcast, bd->_idx);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Failed to delete BCast object from the umap.");
    }


    bd->_idx = 0; // clean up the index in the descriptor.

    /* free the BCast handle */
    free(handle);

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Detach from a BCast object.
 *
 *  When done using a BCast object, detach can be called to end the use of the
 *  BCast's handle in the current process. Calling detach does not
 *  release any of the shared resources of this object but will release any resources
 *  local to this process.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_detach(dragonBCastDescr_t* bd)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "BCast descriptor cannot be NULL.");

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not obtain handle from BCast descriptor.");

    /* tear down the lock */
    err = dragon_lock_detach(&handle->lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to detach Dragon lock in BCast object detach.");

    if (handle->in_managed_memory) {

        /* release memory back to the pool */
        err = dragon_memory_detach(&handle->obj_mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot detach from BCast memory.");

        /* When we have reference counting, we'll need to detach from the
           memory pool as well */
        //err = dragon_memory_pool_detach(&handle->pool);
        //if (err != DRAGON_SUCCESS)
        //    append_err_return(err, "Could not detach from BCast memory pool.");
    }

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_bcast, bd->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete BCast object from the umap.");

    bd->_idx = 0; // clean up the index in the descriptor.

    /* free the BCast handle */
    free(handle);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Return the number of bytes needed a BCast serialized descriptor.
 *
 *  Return the maximum number of bytes needed for a BCast serialized descriptor.
 *
 *  @param None
 *
 *  @return
 *      The number of bytes needed to hold any bcast serialized descriptor.
 */

size_t
dragon_bcast_max_serialized_len() {
    return DRAGON_BCAST_MAX_SERIALIZED_LEN;
}

/** @brief Serialize a BCast Object.
 *
 *  When sharing a BCast object, a serialied descriptor can be created
 *  by calling this function. This serialized descriptor contains data
 *  that when shared can be used to attach to the BCast object from another
 *  process. This function is only valid to call on a BCast object created
 *  via the create function call. The create_at api call does not support
 *  serialization.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param bd_ser The serialized descriptor created/initialized by this call.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_serialize(const dragonBCastDescr_t * bd, dragonBCastSerial_t * bd_ser)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "BCast descriptor cannot be NULL.");

    if (bd_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "BCast serialized descriptor cannot be NULL");

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid BCast descriptor.");

    if (handle->in_managed_memory == false) {
        err_return(DRAGON_BCAST_NOT_SERIALIZABLE, "The BCast object was created using create_at and therefore is not serializable.");
    }

    /* get a serialized memory descriptor for the memory the channel is in */
    dragonMemorySerial_t mem_ser;
    err = dragon_memory_serialize(&mem_ser, &handle->obj_mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot obtain serialized memory descriptor for BCast object.");

    /*
        the serialized descriptor should contain
            * the serialized memory descriptor for it's allocation
            * the hostid of where it is
    */
    bd_ser->len  = mem_ser.len + DRAGON_BCAST_SERIAL_NULINTS*sizeof(dragonULInt);
    /* This malloc will be freed in dragon_channel_serial_free() */
    bd_ser->data = malloc(bd_ser->len);
    if (bd_ser->data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate space for serialized BCast descriptor data.");

    dragonULInt * sptr = (dragonULInt *)bd_ser->data;

    /* hostid is used to be sure we have the same host/node
       since BCast objects are only valid on the same node. */
    *sptr = dragon_host_id();
    sptr++;

    /* The _idx is used to check if this process
       has already attached. */
    *sptr = bd->_idx;
    sptr++;

    /* finally copy in the memory descriptor */
    memcpy(sptr, mem_ser.data, mem_ser.len);

    /* free our serialized memory descriptor after we memcpy */
    err = dragon_memory_serial_free(&mem_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not release serialized memory descriptor after memcpy.");

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Free a BCast serialized descriptor.
 *
 *  When a BCast serialized descriptor is no longer needed, it should be freed. Calling
 *  this function frees the resources used by this serialized descriptor only
 *  which does not free a serialized descriptor shared with other processes
 *  and does not free the underlying SyncObject's resources.
 *
 *  @param bd_ser A BCast serialized descriptor.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_serial_free(dragonBCastSerial_t * bd_ser)
{
    if (bd_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast serialized descriptor cannot be NULL.");

    if (bd_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast serialized descriptor appears to be uninitialized.");

    free(bd_ser->data);

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Wait for the BCast object to be triggered.
 *
 *  Perform a wait on the BCast object specified by handle bd. The calling
 *  process will block until it is triggered by a call to one of the two
 *  trigger functions or until the timeout
 *  expires. If *timeout* is *NULL* then the process waits indefinitely.
 *
 *  Waiting can fail for a few reasons. If spin wait is specified and there are
 *  more spin waiters than the allowable configured maxium, this call will fail.
 *  If this is a synchronized BCast and sync_num processes are already waiting on
 *  this BCast, then this call will fail. In all cases, if the call fails an
 *  appropriate return code is returned to the caller and should be checked.
 *
 *  @param bd The BCast descriptor handle.
 *
 *  @param wait_mode Either DRAGON_IDLE_WAIT or DRAGON_SPIN_WAIT. Idle waiting is
 *  more resource friendly, while spin waiting has better performance when there
 *  are enough cores to be dedicated to spin waiting. There is a fixed size limit
 *  to the number of spin waiters. If that value is exceeded, the BCast will
 *  revert to idle waiting until more spin wait slots open up. Preference is given
 *  to waking up spin waiters first. See dragon_bcast_trigger_one or trigger_all.
 *
 *  @param timer is the timeout value. If it is *NULL* then spin wait indefinitely.
 *
 *  @param payload is a pointer to where a pointer will be stored for the payload.
 *  This will refer to space that was allocated on the heap for the calling process
 *  and must be freed once the process is done with the payload.
 *
 *  @param payload_sz is a pointer to the variable where the actual payload size will
 *  stored.
 *
 *  @param release_fun If NULL is provided, this argument is ignored. Otherwise, it
 *  is a pointer to a function that takes one argument. Any return value
 *  of the function will be ignored. This function will be called once the waiter
 *  has become an official waiter of the BCast object or if the wait has to return
 *  prematurely because of an error.
 *
 *  @param release_arg If release_fun is not NULL, then this is passed as an
 *  an argument to the release_fun function. The type of this argument is assumed
 *  to be a pointer to something.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_wait(dragonBCastDescr_t* bd, dragonWaitMode_t wait_mode, const timespec_t* timer, void** payload, size_t* payload_sz,
                dragonReleaseFun release_fun, void* release_arg)
{
    size_t local_payload_sz;

    if (bd == NULL) {
        if (release_fun != NULL)
            (release_fun)(release_arg);
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");
    }

    if (wait_mode != DRAGON_IDLE_WAIT && wait_mode != DRAGON_SPIN_WAIT && wait_mode != DRAGON_ADAPTIVE_WAIT)
        err_return(DRAGON_INVALID_ARGUMENT, "wait_mode must be DRAGON_IDLE_WAIT, DRAGON_SPIN_WAIT, or DRAGON_ADAPTIVE_WAIT");

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);

    if (err != DRAGON_SUCCESS) {
        if (release_fun != NULL)
            (release_fun)(release_arg);
        append_err_return(err, "Invalid BCast descriptor.");
    }

    /* Set the process id for this wait. */
    _setpid();

    void* num_waiting_ptr = (void*)handle->header.num_waiting;
    timespec_t end_time = {0,0};
    timespec_t* end_time_ptr = NULL;

    // Compute the timespec timeout from the timespec_t timeout.
    if (timer != NULL) {
        end_time_ptr = &end_time;
        if (timer->tv_sec == 0 && timer->tv_nsec == 0) {
             if (release_fun != NULL)
                (release_fun)(release_arg);
            err_return(DRAGON_TIMEOUT, "A zero timeout was specified, so no waiting occurred.");
        }
        err = dragon_timespec_deadline(timer, end_time_ptr);
        if (err != DRAGON_SUCCESS) {
            if (release_fun != NULL)
                (release_fun)(release_arg);
            append_err_return(err, "Could not compute deadline.");
        }
    }

    err = _bcast_wait_on_triggering(handle, end_time_ptr);
    if (err != DRAGON_SUCCESS) {
        if (release_fun != NULL)
            (release_fun)(release_arg);
        append_err_return(err, "Timeout or unexpected error while waiting for triggering to complete.");
    }

    if (wait_mode == DRAGON_SPIN_WAIT || wait_mode == DRAGON_ADAPTIVE_WAIT) {
        err = _spin_wait(handle, num_waiting_ptr, end_time_ptr, release_fun, release_arg, &wait_mode);
        if (err == DRAGON_TIMEOUT)
            append_err_return(err, "Timed out while waiting on bcast.");

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to spin wait on bcast object");

    } else /* idle wait was selected */ {
        err = _idle_wait(handle, num_waiting_ptr, end_time_ptr, release_fun, release_arg, NULL);
        if (err == DRAGON_TIMEOUT)
            append_err_return(err, "Timed out while waiting on bcast.");

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to idle wait on bcast object");
    }

#ifdef CHECK_POINTER
    if (!_is_pointer_valid(handle->header.shutting_down))
        /* The memory on which this bcast was allocated has been freed while
            it waited. Get out now! */
        no_err_return(DRAGON_SUCCESS);
#endif

    if (atomic_load(handle->header.shutting_down)) {
        /* decrement num_waiting. We are in the midst of
           this bcast being destroyed so we want all threads/processes waiting
           on it to exit gracefully from the bcast code. */
        atomic_fetch_add(handle->header.num_waiting, -1L);
        no_err_return(DRAGON_OBJECT_DESTROYED);
    }

    local_payload_sz = *(handle->header.payload_sz);
    /* must set this even when payload size is 0 if they provided variable pointers. */
    if (payload_sz != NULL)
        *payload_sz = 0;
    if (payload != NULL)
        *payload = NULL;

    if (local_payload_sz > 0) {
        if (payload == NULL) {
            atomic_fetch_add(handle->header.num_waiting, -1L);
            atomic_fetch_add(handle->header.num_triggered, 1UL);
            err_return(DRAGON_INVALID_ARGUMENT, "The BCast payload cannot be NULL when there is a non-zero sized payload.");
        }

        if (payload_sz == NULL) {
            atomic_fetch_add(handle->header.num_waiting, -1L);
            atomic_fetch_add(handle->header.num_triggered, 1UL);
            err_return(DRAGON_INVALID_ARGUMENT, "The BCast payload size variable cannot be NULL when there is a non-zero sized payload.");
        }

        *payload = malloc(local_payload_sz);
        if (*payload == NULL) {
            atomic_fetch_add(handle->header.num_waiting, -1L);
            atomic_fetch_add(handle->header.num_triggered, 1UL);
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not get memory for payload in BCast wait.");
        }

        *payload_sz = local_payload_sz;
        memcpy(*payload, handle->header.payload_area, local_payload_sz);
    }

    atomic_fetch_add(handle->header.num_waiting, -1L);
    atomic_fetch_add(handle->header.num_triggered, 1UL);

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Request an asynchronous signal when one of the BCast object's trigger functions
 *  is called.
 *
 *  The asynchronous signal can come in one of two forms. This function defines a signal
 *  to occur when a trigger event occurs. The payload_ptr pointer
 *  and payload_sz pointer will be used to initialize a pointer to the payload and the size of the payload.
 *  The drc pointer will refer to the return code indicating the success of the asynchronous notification.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param timer is the timeout value. If it is *NULL* then wait indefinitely to signal the caller.
 *
 *  @param payload_ptr The location in which to store a pointer to the payload when signal
 *  notification is requested.
 *
 *  @param payload_sz The location in which to store the payload size when signal notification
 *  is requested.
 *
 *  @param rc The location in which to store the asynchronous wait Dragon return code when signal
 *  notification is requested.
 *
 *  @param err_string A pointer to a string pointer. Upon return it will hold an error string if the
 *  the rc indicates that there was an error. The err_string must be freed once the application is done with it.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_notify_signal(dragonBCastDescr_t* bd, const dragonWaitMode_t wait_mode, const timespec_t* timer, dragonReleaseFun release_fun, void* release_arg, int sig, void** payload_ptr, size_t* payload_sz, dragonError_t* rc, char** err_string)
{
    pthread_t tid;
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    /* signal notification is requested. */
    dragonBCastSignalArg_t* arg = malloc(sizeof(dragonBCastSignalArg_t));
    if (arg == NULL)
        err_return (DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for malloc'ed thread argument.");

    if (payload_sz == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast notify payload_sz argument cannot be NULL when signal notification is requested.");

    if (rc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast notify rc argument cannot be NULL when signal notification is requested.");

    if (err_string == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast notify err_string argument cannot be NULL when signal notification is requested.");

    arg->bd = *bd;
    arg->parent_pid = _getpid();
    arg->payload_ptr = payload_ptr;
    arg->payload_sz = payload_sz;
    arg->rc = rc;
    arg->err_string = err_string;
    arg->sig = sig;
    if (timer == NULL) {
        arg->timer_is_null = true;
    } else {
        arg->timer_is_null = false;
        arg->timer = *timer;
    }
    arg->wait_mode = wait_mode;
    arg->release_fun = release_fun;
    arg->release_arg = release_arg;

    int err = pthread_create(&tid, &attr, _bcast_notify_signal, arg);

    pthread_attr_destroy(&attr);

    if (err != 0) {
        char err_str[80];
        snprintf(err_str, 80, "There was an error on the pthread_create call. ERR=%d", err);
        err_return(DRAGON_BCAST_SIGNAL_ERROR, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Request an asynchronous notification when one of the BCast object's trigger functions
 *  is called via a callback. The two arguments to this function are the BCast descriptor and a
 *  pointer to a callback function. The callback function should take five arguments as follows.
 *
 *      void callback(dragonBCastDescr_t* bd, void* payload, size_t payload_sz, dragonError_t rc, char* err_str)
 *
 *  The BCast descriptor pointer is the first argument to the callback. A payload pointer
 *  is the second argument followed by the size of the payload as the third argument. Finally,
 *  the last two are the return code, indicating success or not, and the error string if not successful
 *  (NULL otherwise).
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param user_def_ptr This is a user defined pointer, presumably to something that the callback
 *  needs to complete its work. It is passed to the callback as the first argument. It can be any
 *  pointer that the user desires including the dragonBCastDescr_t pointer.
 *
 *  @param wait_mode The type of waiting (.e.g SPIN_WAIT or IDLE_WAIT) that should be used.
 *
 *  @param timer is the timeout value. If it is *NULL* then wait indefinitely to call the callback.
 *
 *  @param release_fun is a function to call that will release some lock or resource after the
 *  callback has been registered as a waiter with the bcast. The release fun must take one argument,
 *  a void* pointer.
 *
 *  @param release_arg is an argument to pass to the release_fun when it is called.
 *
 *  @param cb The callback function to be called. The callback function signature must be as
 *  described in this functions description.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_notify_callback(dragonBCastDescr_t* bd, void* user_def_ptr, const dragonWaitMode_t wait_mode, const timespec_t* timer, dragonReleaseFun release_fun, void* release_arg, dragonBCastCallback cb)
{
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    /* callback notification is requested. */
    dragonBCastCallbackArg_t* arg = malloc(sizeof(dragonBCastCallbackArg_t));
    if (arg == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate callback argument.");

    arg->bd = *bd;
    arg->user_def_ptr = user_def_ptr;
    arg->fun = cb;
    if (timer == NULL) {
        arg->timer_is_null = true;
    } else {
        arg->timer_is_null = false;
        arg->timer = *timer;
    }
    arg->wait_mode = wait_mode;
    arg->release_fun = release_fun;
    arg->release_arg = release_arg;

    int err = pthread_create(&tid, &attr, _bcast_notify_callback, arg);

    pthread_attr_destroy(&attr);

    if (err != 0) {
        char err_str[80];
        snprintf(err_str, 80, "There was an error on the pthread_create call. ERR=%d", err);
        err_return(DRAGON_BCAST_CALLBACK_ERROR, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Unblock one blocked process waiting on this BCast bbject.
 *
 *  Triggers one blocked process waiting on this BCast object.
 *  If there is no payload, the payload pointer must be NULL and the payload_sz
 *  must be 0. Otherwise, the payload_sz bytes are copied from the address in
 *  payload to the object's shared payload area so it is available to any
 *  processes waiting on this object.
 *
 *  Preference is given to trigger a spin waiter if one is available. If no
 *  spin waiters are waiting, then an idle waiter is triggered.
 *
 *  If the BCast object is synchronized, this method cannot be called to
 *  trigger just one waiter. On a synchronized BCast, only dragon_bcast_trigger_all
 *  may be called, even if the sync_num is set to 1 for the BCast.
 *
 *  If a process is waiting asynchronously and chosen by this call, a signal
 *  will be initiated or a callback will be called, depending on how the
 *  asynchronous notification was requested.
 *
 *  When DRAGON_SUCCESS is returned, this call successfully blocked until one
 *  waiting processes has picked up its payload.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param timer A timeout value. If the triggered process does not complete its triggering within the timeout
 *  period, this process will get a DRAGON_TIMEOUT return code. A value of NULL means to wait
 *  forever for triggering to complete.
 *
 *  @param payload A pointer to the payload the triggering process wants to
 *  disseminate.
 *
 *  @param payload_sz The size of the payload to be disseminated.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_trigger_one(dragonBCastDescr_t* bd, const timespec_t* timer, const void* payload, const size_t payload_sz)
{
    dragonError_t err;
    err = dragon_bcast_trigger_some(bd, 1, timer, payload, payload_sz);

    if (err == DRAGON_BCAST_NO_WAITERS)
        no_err_return(DRAGON_BCAST_NO_WAITERS);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Call to trigger some of 1 did not succeed.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Unblock some blocked processes waiting on this BCast bbject.
 *
 *  Triggers some blocked processes waiting on this BCast object. Spin waiters
 *  take precedent. The number of processes unblocked is given as an argument.
 *  No more than that number will be unblocked, though if fewer are currently
 *  waiting on the BCast, then that fewer number will be all that is triggered.
 *  This function will not wait for new waiters to come to be triggered. Only
 *  current waiters are unblocked by this call.
 *
 *  If there is no payload, the payload pointer must be NULL and the payload_sz
 *  must be 0. Otherwise, the payload_sz bytes are copied from the address in
 *  payload to the object's shared payload area so it is available to any
 *  processes waiting on this object.
 *
 *  Spin waiters are triggered first. Idle waiters are triggered when no more
 *  spin waiters are available.
 *
 *  If the BCast object is synchronized, this call will block until there
 *  are sync_num number of waiters waiting on the BCast as specifed in the
 *  attributes when the BCast was created.
 *
 *  If waiting processes are waiting asynchronously, a signal will be initiated or a
 *  callback will be called, depending on how the asynchronous notification
 *  was requested.
 *
 *  When DRAGON_SUCCESS is returned, this call successfully blocked until all
 *  waiting processes had picked up their payloads.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param num_to_trigger The number of processes to trigger. Triggering 0 is allowed, but has no effect.
 *
 *  @param timer A timeout value. If all triggered processes do not complete triggering within the timeout
 *  period, this process will get a DRAGON_TIMEOUT return code. A value of NULL means to wait
 *  forever for triggering to complete.
 *
 *  @param payload A pointer to the payload the triggering process wants to
 *  disseminate.
 *
 *  @param payload_sz The size of the payload to be disseminated.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_trigger_some(dragonBCastDescr_t* bd, int num_to_trigger, const timespec_t* timer, const void* payload, const size_t payload_sz)
{
    dragonError_t err;
    char* err_msg = NULL;

    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    if (payload_sz > 0 && payload == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast payload cannot be NULL when payload_sz is greater than 0.");

    //setup the timeout for the trigger
    timespec_t end_time = {0,0};
    timespec_t now_time = {0,0};
    timespec_t* end_time_ptr = NULL;

    // Compute the timespec timeout from the timespec_t timeout.
    if (timer != NULL) {
        end_time_ptr = &end_time;
        err = dragon_timespec_deadline(timer, end_time_ptr);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not compute deadline.");
        }
    }

    dragonBCast_t * handle;
    err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid BCast descriptor.");

    err = dragon_lock(&handle->lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "unable to acquire the BCast lock.");

    /* This first check is so we return quickly if there are no waiters. A second check, under the lock
       is the official check below */
    if (*(handle->header.sync_type) == DRAGON_NO_SYNC && atomic_load(handle->header.num_waiting) == 0) {
        dragon_unlock(&handle->lock);
        no_err_return(DRAGON_BCAST_NO_WAITERS);
    }

    if (payload_sz > *handle->header.payload_area_sz) {
        dragon_unlock(&handle->lock);
        err_return(DRAGON_INVALID_ARGUMENT, "The payload_sz is bigger than the maximum allowable payload size configured for this bcast object.");
    }

    // Compute the timespec timeout from the timespec_t timeout.
    if (timer != NULL) {
        err = dragon_timespec_deadline(timer, &end_time);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&handle->lock);
            append_err_return(err, "Could not compute deadline.");
        }
    }

    _Atomic(uint32_t) * triggering_ptr = handle->header.triggering;
    volatile atomic_uint * num_triggered_ptr = handle->header.num_triggered;

    size_t num_waiting = atomic_load(handle->header.num_waiting);

    if (num_waiting == 0) {

        if (*(handle->header.sync_type) == DRAGON_SYNC) {
            dragon_unlock(&handle->lock);
            err_return(DRAGON_FAILURE, "There were no waiters on a DRAGON_SYNC BCast. This should never happen.");
        }

        dragon_unlock(&handle->lock);
        no_err_return(DRAGON_BCAST_NO_WAITERS);
    }

    /* With lock acquired, no other triggers can be using this object */
    /* Copy in payload and then tell waiters it is available */
    *(handle->header.payload_sz) = payload_sz;
    if (payload_sz > 0)
        memcpy(handle->header.payload_area, payload, payload_sz);

    /* Indicate the BCast object is in the process of triggering with */
    /* possible payload available */
    atomic_store(num_triggered_ptr, 0UL);

    /* If more waiters arrive after this, the allowable_count below will
       only allow this many (i.e. num_waiting) to wake up. */
    if (num_to_trigger == INT_MAX && num_waiting < num_to_trigger)
        num_to_trigger = num_waiting;

    if (num_waiting < num_to_trigger)
        no_err_return(DRAGON_BCAST_INSUFFICIENT_WAITERS);

    atomic_store(handle->header.num_to_trigger, num_to_trigger);
    atomic_store(triggering_ptr, 1);

    size_t num_spinners = 0;
    /* We make a local copy of current_spinner_count because the spin_list_count
       will change in the object as spinners wake up */
    size_t current_spinner_count = (size_t)*handle->header.spin_list_count;
    int idx = 0;
    atomic_uint expected = 0UL;

    while ((num_spinners < current_spinner_count) && (idx < *(handle->header.spin_list_sz)) && (num_spinners < num_to_trigger)) {
        expected = 1UL;
        if (atomic_compare_exchange_strong(&(handle->header.spin_list[idx]), &expected, 2UL))
            num_spinners+=1;
        idx += 1;
    }

    /* the while loop below waits for all the triggered to pick up their payload. */
    size_t check_timeout_when_0 = 1;
    uint32_t num_to_wake = num_to_trigger - num_spinners;

    /* As each process wakes up it will decrement allowable count. */
    atomic_store(handle->header.allowable_count, num_to_wake);
    int iters = 0;
    int when_to_check_procs = DRAGON_BCAST_DEFAULT_ITERS_CHECK_PROCS;

    while ((atomic_load(num_triggered_ptr) < num_to_trigger) && (atomic_load(handle->header.num_waiting) > 0UL)) {

        if (*handle->header.shutting_down != 0UL)
            // Get out, now!
            err_return(DRAGON_OBJECT_DESTROYED, "The object was destroyed");

        iters += 1;

        if (iters == when_to_check_procs) {
            _wake_sleepers(handle, num_to_wake); // Cannot rely on return value.
            iters = 0;
            if (when_to_check_procs < DRAGON_BCAST_BACKOFF_ITERS_CHECK_PROCS)
                when_to_check_procs += when_to_check_procs;

            err = _check_pids(handle);
            if (err != DRAGON_SUCCESS) {
                err_msg = dragon_getlasterrstr();
                if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
                    dragon_unlock(&handle->lock);
                err_noreturn(err_msg);
                free(err_msg);
                append_err_return(err, "Could not complete trigger due to waiters having died while waiting.");
            }
        }

        if (timer != NULL) {
            if (check_timeout_when_0 == 0) {
                clock_gettime(CLOCK_MONOTONIC, &now_time);
                if (dragon_timespec_le(&end_time, &now_time)) {
                    atomic_store(triggering_ptr, 0);
                    if (*(handle->header.sync_type) == DRAGON_NO_SYNC)
                        dragon_unlock(&handle->lock);
                    err_return(DRAGON_TIMEOUT, "BCast trigger_all timed out while waiting for triggered processes to complete payload pickup.");
                }
            }
            check_timeout_when_0 = (check_timeout_when_0 + 1) % 100;
        }
    }

    _trigger_completion(handle);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Unblock all blocked processes waiting on this BCast bbject.
 *
 *  Triggers all blocked processes waiting on this BCast object.
 *  If there is no payload, the payload pointer must be NULL and the payload_sz
 *  must be 0. Otherwise, the payload_sz bytes are copied from the address in
 *  payload to the object's shared payload area so it is available to any
 *  processes waiting on this object.
 *
 *  If the BCast object is synchronized, this call will block until there
 *  are sync_num number of waiters waiting on the BCast as specifed in the
 *  attributes when the BCast was created.
 *
 *  If waiting processes are waiting asynchronously, a signal will be initiated or a
 *  callback will be called, depending on how the asynchronous notification
 *  was requested.
 *
 *  When DRAGON_SUCCESS is returned, this call successfully blocked until all
 *  waiting processes had picked up their payloads.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param timer A timeout value. If all triggered processes do not complete triggering within the timeout
 *  period, this process will get a DRAGON_TIMEOUT return code. A value of NULL means to wait
 *  forever for triggering to complete.
 *
 *  @param payload A pointer to the payload the triggering process wants to
 *  disseminate.
 *
 *  @param payload_sz The size of the payload to be disseminated.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_trigger_all(dragonBCastDescr_t* bd, const timespec_t* timer, const void* payload, const size_t payload_sz)
{
    dragonError_t err;

    err = dragon_bcast_trigger_some(bd, INT_MAX, timer, payload, payload_sz);

    if (err == DRAGON_BCAST_NO_WAITERS)
        no_err_return(DRAGON_BCAST_NO_WAITERS);

    if (err != DRAGON_SUCCESS) {
        char err_str[200];
        snprintf(err_str, 199, "Call to trigger some with INT_MAX failed with %s.", dragon_get_rc_string(err));
        append_err_return(err, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Get the number of waiting processes on this BCast object.
 *
 *  Return the number of waiting processes on this BCast object.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @param num_waiters A pointer to an integer variable where the number waiting
 *  will be stored.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_num_waiting(dragonBCastDescr_t* bd, int* num_waiters)
{
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    if (num_waiters == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The num_waiters argument cannot be NULL.");

    *num_waiters = 0;

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid BCast descriptor.");

    err = _bcast_wait_on_triggering(handle, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to wait on triggering while querying num_waiting in bcast.");

    *num_waiters = atomic_load(handle->header.num_waiting);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Reset the state of the BCast object.
 *
 *  Returns the BCast object to its state just after creating it. This
 *  is only safe to call when there are no waiters and no triggerers using
 *  the object. This reset call takes into account whether this is a synchronizing
 *  or non-synchronizing BCast and puts it into the correct state. For a synchronizing
 *  BCast it is reset to a locked state so that triggering will wait for n waiters. If
 *  the BCast is non-synchronizing, the BCast is reset to an unlocked state so triggering
 *  and waiting are not synchronized via the object.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_bcast_reset(dragonBCastDescr_t* bd) {
    if (bd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The BCast descriptor cannot be NULL.");

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid BCast descriptor.");

    dragonLockState_t state;

    err = dragon_lock_state(&handle->lock, &state);

    /* DRAGON_NO_SYNC and DRAGON_SYNC are mutually exclusive */
    if (*(handle->header.sync_type) == DRAGON_NO_SYNC && state == DRAGON_LOCK_STATE_LOCKED)
        dragon_unlock(&handle->lock);

    if (*(handle->header.sync_type) == DRAGON_SYNC && state == DRAGON_LOCK_STATE_UNLOCKED)
        dragon_lock(&handle->lock);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Return the state of the BCast object as a string.
 *
 *  Returns the BCast object's state as a string for debug purposes.
 *
 *  @param bd The BCast's descriptor handle.
 *
 *  @return The bcast state as a string. The string must be freed
 *  by the caller.
 */
char*
dragon_bcast_state(dragonBCastDescr_t* bd) {
    char state_str[1000];
    char* ret_str;

    if (bd == NULL)
        return NULL;

    dragonBCast_t * handle;
    dragonError_t err = _bcast_handle_from_descr(bd, &handle);
    if (err != DRAGON_SUCCESS)
        return NULL;

    snprintf(state_str, 999, "BCast State:\n   num_waiting %d\n   num_triggered %d\n   triggering %d\n   state %lx\n   shutting_down %d\n   allowable_count %d\n   num_to_trigger %d\n   payload_sz %d\n   sync_type %d\n   sync_num %d\n",
                            *handle->header.num_waiting,
                            *handle->header.num_triggered,
                            *handle->header.triggering,
                            *handle->header.state,
                            *handle->header.shutting_down,
                            *handle->header.allowable_count,
                            *handle->header.num_to_trigger,
                            *handle->header.payload_sz,
                            *handle->header.sync_type,
                            *handle->header.sync_num);

    ret_str = malloc(strlen(state_str)+1);

    strcpy(ret_str, state_str);

    return ret_str;
}