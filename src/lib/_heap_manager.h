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
#include <dragon/managed_memory.h>
#include <dragon/bcast.h>

struct dragonBCastDescr_st;

#define BLOCK_SIZE_MAX_POWER 62
#define BLOCK_SIZE_MIN_POWER 5
#define DRAGON_HEAP_SPIN_WAITERS 132

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

/** @brief Statistics that can be gathered by a user on a heap's current state.
 *
 * This defines the format of data returned by calling the dragon_heap_get_stats
 * function on a heap.
 */
typedef struct dragonHeapStats_st {
    uint64_t num_segments;
    /*!< The number of segments in the heap. */

    uint64_t segment_size;
    /*!< The segment size of the heap. */

    uint64_t total_size;
    /*!< Total space in bytes that comprises the heap. */

    uint64_t total_free_space;
    /*!< Computed free space given the free blocks in the heap. */

    double utilization_pct;
    /*!< A percentage of the space that is free. It is a value between 0 and 100%. */

    size_t num_block_sizes;
    /*!< The number of block sizes of the heap. */

    dragonHeapStatsAllocationItem_t free_blocks[BLOCK_SIZE_MAX_POWER-BLOCK_SIZE_MIN_POWER+1];
    /*!< This is a list of free blocks that will contain the number of free blocks at each
         of the given powers of 2. free_blocks[0] will be the number of blocks that are
         free where the block is the size of a segment. free_blocks[1] is the number of
         blocks of size 2*segment_size that are free, and so on. The type
         dragonHeapStatsAllocationItem_t is declared in managed_memory.h to make it available
         to other code which may want to pass along this array. */

} dragonHeapStats_t;


/** @brief The handle to a heap manager heap only to be used internally in
 *  some other object.
 *
 * This defines the handle structure to a heap. A handle is initialized by
 * calling init to initialize a new heap, or attach to attach to an already
 * initalized heap. A user of the API declares their own handle to the heap
 * which is used on subsequent calls to the API.
 *
 * All fields within the heap's handle are managed by the heap itself and
 * should not be set by any user. The field descriptions are provided here
 * only as documenation about how these fields are used internally in the heap.
 */
typedef struct dragonDynHeap_st {
    uint64_t * base_pointer;
    /*!< A pointer that points to the start of the heap manager area in shared
       memory. For debug purposes only. */

    dragonLock_t dlock;
    /*!< A handle to the heaps lock. Used internally only. */

    volatile atomic_uint * num_waiting;
    /*!< A pointer into shared memory holding the total number of waiters on the heap. Used
      internally as an optimization but read-only if examined externally. */

    uint64_t segment_size;
    /*!< The size of segments in bytes that are managed by this heap manager. */

    uint64_t num_segments;
    /*!< The number of segments of the heap. */

    uint64_t total_size;
    /*!< The total size of the heap. */

    uint64_t num_block_sizes;
    /*!< The number of block sizes in this heap. */

    uint64_t biggest_block;
    /*!< Records the biggest possible allocation in the heap. Preallocations will
      decrease the maximum sized block possible in the heap. */

    dragonBitSet_t* free;
    /*!< Points to an array of BitSets, one for each block size within the heap. */

    dragonBitSet_t preallocated;
    /*!< The preallocated set which helps the heap manager know when not to merge
      freed data. */

    dragonBCastDescr_t* waiters;
    /*!< The array of BCasts where processes wait for the availability of a block
       allocation that cannot be currently serviced. */

} dragonDynHeap_t;

/* Used internally only during preallocation of blocks. */
typedef struct dragonDynHeapPreAlloc_st {
    void* offset;
    size_t size;
} dragonDynHeapPreAlloc_t;

#ifdef __cplusplus
}
#endif


/** @defgroup heapmanager_lifecycle
 *  Heap Manager Life Cycle Functions
 *  @{
 */

/** @brief Compute the required size of blob needed for the max and min block sizes.
 *
 *  This computes and returns (through a reference parameter) the number of bytes
 *  required for a heap supporting a one block of the max_block_size_power down to
 *  blocks of the min_block_size_power. Any alignment requirement will require that the
 *  blocks will align on that requirement relative to the address provided for the blob.
 *  In other words, if 32 byte alignment is required, then the blob will be assumed to have
 *  a minimum 32 byte alignment itself when it is passed to the init function.
 *
 *  The minimum block size is 32 bytes so the min_block_size_power must be 5 or greater. The maximum
 *  block size is 4 exabytes which is 2^62.
 *
 *  @param max_block_size_power 2**max_block_size power is the biggest allocatable block in the heap.
 *  @param min_block_size_power The base 2 power of the minimum block size. There will be more than
 *  one of these block sizes available. In fact that will be maximum of 2 to the (max_block_size_power -
 *  min_block_size_power) blocks of this size available.
 *  @param lock_kind The type of lock used in the heap.
 *  @param size The returned size required for this heap. The user allocates a blob of this size and
 *  then passes a pointer to it in on the init function call before using any other functions in this
 *  API.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_size(const size_t max_block_size_power, const size_t min_block_size_power,
                const dragonLockKind_t lock_kind, size_t* size);


/** @brief Initialize the heap.
 *
 *  This function initializes the heap meta-data and prepares it for use. This must
 *  be given the required size for a heap with the given max and min block sizes.
 *  The required size is determined by a call to the dragon_heap_size function. After
 *  calling this function any other API call can be made to the heap. A heap handle
 *  is initialized by this call. The same heap handle must be provided when destroying
 *  the heap, which should be the last call for the given heap.
 *
 *  Asking for more pre-allocated blocks than the heap can support will result in an
 *  error on heap creation.
 *
 *  @param ptr A pointer to a location in memory big enough to hold the requested size
 *  heap. The required size should be determined byk calling dragon_heap_size.
 *  @param heap A pointer to a handle for the heap.
 *  @param max_block_size_power The size of the largest block available in the heap. Only
 *  one block of 2^max_block_size_power is available, two of 2^(max_block_size_power-1),
 *  and so on.
 *  @param min_block_size_power The size of the smallest block available will be
 *  2^min_block_size_power. There will be 2^(max_block_size_power-min_block_size_power)
 *  of these minimum sized blocks available when the heap is empty.
 *  @param lock_kind The kind of lock the heap is using.
 *  @param preallocated An array of dragonULInts of size (max_block_size_power -
 *  min_block_size_power). Entry zero corresponds to the number of blocks that should be
 *  preallocated with size 2^min_block_size_power. Each subsequent entry doubles the
 *  allocation size and asks that this many memory allocations be statically available
 *  from the heap up to 2^(max_block_size_power-1). If preallocated is NULL, then no
 *  preallocations will be done.
 *  @return A dragonError_t return code of DRAGON_SUCCESS or an error code if there was
 *  an error that occurs during creation of the heap.
 */
dragonError_t
dragon_heap_init(void* ptr, dragonDynHeap_t* heap, const size_t max_block_size_power,
                const size_t min_block_size_power, const dragonLockKind_t lock_kind, const size_t* preallocated);


/** @brief Attach a handle for an existing heap.
 *
 *  This function attaches to an existing heap by initializing a handle
 *  that can be used to interact with it. After attaching to a heap the
 *  rest of the API is fair game for this heap except destroy. A handle
 *  that was initialized by attach should be detached when it is no longer
 *  needed.
 *
 *  @param ptr A pointer to the blob that contains the heap.
 *  @param heap A pointer to a handle for the heap which will be filled
 *  in by this call.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_attach(void* ptr, dragonDynHeap_t* heap);


/** @brief Called to free resources used by this heap.
 *
 *  Destroy should be called once a heap is completely done being used and can
 *  only be called using the handle created through the call to init. Once
 *  destroyed, the handle is no longer valid and all resources needed by the
 *  heap have been destroyed (deallocated) as well.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_destroy(dragonDynHeap_t* heap);


/** @brief Called to detach from an existing heap.
 *
 *  When a handle created through attach is no longer needed, detach should
 *  be called. Detaching can only be done through an attached handle. The handle
 *  is no longer valid after detaching.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_detach(dragonDynHeap_t* heap);

/** @} */ // end of heapmanager_lifecycle group.

/** @defgroup heapmanager_operation
 *  Heap Manager Operation Functions
 *  @{
 */

/** @brief Dynamically allocate memory.
 *
 *  Allocate a block of at least size bytes, returning the offset to it in the
 *  reference parameter offset. Writing or reading data passed the number of bytes
 *  given in size may result in corruption of user data.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param size The number of bytes requested.
 *  @param offset Returns the offset into the heap's data area. This offset must be added
 *  to the heap's base pointer to get the actual address.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** offset);


/** @brief Dynamically allocate memory and block until available or timeout
 *
 *  Allocate a block of at least size bytes, returning the pointer to it in the
 *  reference parameter ptr. Writing or reading data passed the number of bytes
 *  given in size may result in corruption of user data.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param size The number of bytes to allocate.
 *  @param offset Returns the offset into the heap's data area. This offset must be added
 *  to the heap's base pointer to get the actual address.
 *  @param timeout The timeout value for waiting. If NULL, then it will wait indefinitely.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_malloc_blocking(dragonDynHeap_t* heap, const size_t size, void** offset, const timespec_t * timer);


/** @brief Deallocate heap memory returning it to the heap.
 *
 *  Deallocate the memory pointed to by ptr in this heap. Ptr must be an address
 *  returned by the dynmem_malloc call. After freeing a block it is
 *  no longer permissable to read or write data from/into the block and could
 *  lead to corruption of the heap (see recover).
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param offset An offset into the heap to be freed.
 *  @param size The original size of the allocated block in bytes.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_free(dragonDynHeap_t* heap, void* offset, size_t size);

/** @} */ // end of heapmanager_operation group.

/** @defgroup heapmanager_query
 *  Heap Manager Query Functions
 *  @{
 */

/** @brief Retrieve statistics concerning the present utilization of a heap.
 *
 *  This returns information like the total number of segments and segment size
 *  within a heap and the present free space and utilization percentage of a heap.
 *  The struct containing the stats ends with an array of num_block_sizes entries
 *  each containing the number of free blocks of a given size in the heap when it
 *  was called.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param data A pointer to a dragonHeapStats_t structure that will be initialized
 *  with the current heap statistics.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_get_stats(dragonDynHeap_t* heap, dragonHeapStats_t* data);


/** @brief This prints a memory dump of a dynamic memory heap.
 *
 *  The dump is a hexdump of all memory used within the heap including the meta-data
 *  and user-data. It can be used for debugging should that be necessary.
 *
 *  @param title A null-terminated string to use in identifying the dump output.
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonDynDynHeapErr_t return code.
 */
dragonError_t
dragon_heap_dump(const char* title, dragonDynHeap_t* heap);


/** @brief This prints a memory dump of a dynamic memory heap to a file.
 *
 *  The dump is a hexdump of all memory used within the heap including the meta-data
 *  and user-data. It can be used for debugging should that be necessary.
 *
 *  @param fd A writable file descriptor to which the dump will be written.
 *  @param title A null-terminated string to use in identifying the dump output.
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_heap_dump_to_fd(FILE* fd, const char* title, dragonDynHeap_t* heap);

/** @} */ // end of heapmanager_query group.

#endif
