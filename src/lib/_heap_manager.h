/******************************************************************
 * This API provides a heap manager based on a buddy allocator that
 * manages the allocation and deallocation of dynamic memory within
 * a pre-allocated blob of memory. The blob is provided and the
 * state of the heap is maintained completely within the blob.
 *
 * This API depends on a lock for mutually exclusive access and a
 * bitset implementation.
 *
 * This heap manager manages calls to allocate and free by returning
 * the minimum block size that satisfies a request. The size of the
 * block returned will always be a power of 2 size in bytes. So if
 * 33 bytes are requested, a 64 byte block will be returned.
 *
 * When a block is freed, the block will be joined with its buddy
 * if that is also free to eliminate any fragmentation of memory
 * between blocks and their adjacent buddies.
 *
 * No compaction is performed by this buddy allocator. It is a
 * high-perfomance, low-overhead heap manager and limits its
 * reliance on the OS for services to just the lock for mutually
 * exclusive access, meaning it is multi-processing ready.
 *
 * More information can be found at
 * https://dragon.us.cray.com/html/internal/internal.html
 */

#ifndef HAVE_DRAGON_DYN_HEAPMANAGER_H
#define HAVE_DRAGON_DYN_HEAPMANAGER_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <errno.h>
#include "_bitset.h"
#include "shared_lock.h"
#include <dragon/utils.h>
#include <dragon/return_codes.h>
#include <dragon/global_types.h>

struct dragonBCastDescr_st;

#define BLOCK_SIZE_MAX_POWER 62
#define BLOCK_SIZE_MIN_POWER 5

/* This timeout is the amount of time to wait
   when notifying a process that an allocation
   size is available. This won't be hit unless
   a process goes away when it was waiting for an
   allocation. In that case, if it did not clean
   up, then it will have corrupted the bcast object */
#define DRAGON_HEAP_NOTIFY_TIMEOUT 60 /* seconds */

#ifdef __cplusplus
extern "C" {
#endif

/* dyn_mem stats provide statistics for the heap. */

typedef struct dragonHeapStatsAllocationItem_st {
    uint64_t block_size;
    uint64_t num_blocks;
} dragonHeapStatsAllocationItem_t;

typedef struct dragonHeapStats_st {
    uint64_t num_segments;
    uint64_t segment_size;
    uint64_t total_size;
    uint64_t total_free_space;
    double utilization_pct;
    size_t num_block_sizes;
    dragonHeapStatsAllocationItem_t free_blocks[BLOCK_SIZE_MAX_POWER-BLOCK_SIZE_MIN_POWER+1];
} dragonHeapStats_t;

/* Each allocatable block has a minimum size (but may be
   longer). The actual allocation of these is determined by the
   bitsets block_set and free_set.
*/

typedef void dragonDynHeapSegment_t;

/* This defines the handle structure to a heap. A handle is initialized by calling
 * init to initialize a new heap, or attach to attach to an already initalized heap.
 * A user of the API declares their own handle to the heap which is used on subsequent
 * calls to the API.
 */

typedef struct dragonDynHeap_st {
    uint64_t * base_pointer;
    dragonLock_t dlock;
    uint64_t num_segments;
    uint64_t segment_size;
    size_t num_freelists;
    size_t* recovery_needed_ptr;
    size_t* num_waiting;
    size_t biggest_block;
    dragonBitSet_t block_set;
    dragonBitSet_t free_set;
    dragonBitSet_t preallocated_set;
    void* waiter_space;
    size_t* free_lists;
    size_t* preallocated_free_lists;
    dragonDynHeapSegment_t* segments;
    void* end_ptr;
    struct dragonBCastDescr_st* waiters;
} dragonDynHeap_t;

#ifdef __cplusplus
}
#endif

/* heap management functions */
dragonError_t
dragon_heap_size(const size_t max_block_size_power, const size_t min_block_size_power, const size_t alignment,
                 const dragonLockKind_t lock_kind, size_t* size);

dragonError_t
dragon_heap_init(void* ptr, dragonDynHeap_t* heap, const size_t max_block_size_power,
                 const size_t min_block_size_power, const size_t alignment,
                 const dragonLockKind_t lock_kind, const size_t* preallocated);

dragonError_t
dragon_heap_attach(void* ptr, dragonDynHeap_t* heap);

dragonError_t
dragon_heap_destroy(dragonDynHeap_t* heap);

dragonError_t
dragon_heap_detach(dragonDynHeap_t* heap);

dragonError_t
dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** ptr);

dragonError_t
dragon_heap_malloc_blocking(dragonDynHeap_t* heap, const size_t size, void** ptr, const timespec_t * timer);

dragonError_t
dragon_heap_free(dragonDynHeap_t* heap, void* ptr);

dragonError_t
dragon_heap_recover(dragonDynHeap_t* heap);

dragonError_t
dragon_heap_get_stats(dragonDynHeap_t* heap, dragonHeapStats_t* data);

dragonError_t
dragon_heap_dump(const char* title, dragonDynHeap_t* heap);

dragonError_t
dragon_heap_dump_to_fd(FILE* fd, const char* title, dragonDynHeap_t* heap);

#endif
