#include "_hexdump.h"
#include "err.h"
#include <dragon/bcast.h>
#include "_heap_manager.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define MAX(x, y) (((x) > (y)) ? (x) : (y))

/* Python definition - tested, works
def ipow(base, exp):
    if exp == 0:
       return 1

    if exp % 2 == 1:
       return base * ipow(base, exp-1)

    factor = ipow(base, exp//2)
    return factor * factor
*/
static size_t
ipow(size_t base, size_t exp) {
    if (exp == 0)
        return 1;

    if (exp % 2 == 1)
        return base * ipow(base, exp-1);

    size_t factor = ipow(base, exp/2);
    return factor * factor;
}


static size_t
_powerof2(const size_t size, uint64_t segment_size)
{
    // returns the smallest power of 2 of segment_size
    // that can hold size. segment_size should never
    // be 0, but if it were, no sense in looping forever.
    if (segment_size == 0)
        return 0;

    uint64_t seg_size = segment_size;

    size_t power = 0;

    while (size > seg_size) {
        seg_size = seg_size * 2;
        power += 1;
    }

    return power;
}

static dragonError_t
_trigger_waiter(const dragonDynHeap_t* heap, const size_t free_list_index)
{
    int idx = free_list_index;
    dragonError_t err;
    timespec_t timeout = {DRAGON_HEAP_NOTIFY_TIMEOUT,0};

    /* The following while loop favors waiters for the biggest block possible.
    If a large block becomes available and there are multiple waiters, the
    waiter for the largest block will get its allocation first. Other smaller
    block waiters may still follow if there are other smaller blocks made
    available by splitting. Those others will be triggered as soon as the first
    one completes (see the malloc function). */

    while (idx >= 0 && *(heap->num_waiting) > 0) {
        err = dragon_bcast_trigger_one(&heap->waiters[idx], &timeout, NULL, 0);
        if (err == DRAGON_SUCCESS) // We found a waiter. One is enough.
            no_err_return(DRAGON_SUCCESS);

        idx -= 1;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_wait(dragonDynHeap_t* heap, dragonBCastDescr_t* bcast_obj, const timespec_t* timeout)
{
    /* This is done under a lock, so atomic instruction not really necessary. */
    atomic_fetch_add(heap->num_waiting, 1L);

    dragonError_t err = dragon_bcast_wait(bcast_obj, DRAGON_ADAPTIVE_WAIT, timeout, NULL, 0, (dragonReleaseFun)dragon_unlock, &(heap->dlock));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Timeout or could not idle wait in blocking malloc of heap manager.");

    atomic_fetch_add(heap->num_waiting, -1L);

    no_err_return(DRAGON_SUCCESS);
}

static uint64_t buddy(uint64_t idx)
{
    if (idx % 2 == 0)
        return idx+1;

    return idx-1;
}

static dragonError_t
_split(dragonDynHeap_t* heap, size_t index, size_t* free_block)
{
    dragonError_t err;
    uint64_t buddy_idx;

    // split some bigger block than the size found at index
    // and keep splitting until the block size at
    // index has a block in its free list.
    size_t split_idx = index;
    size_t num_block_sizes = heap->num_block_sizes;
    size_t block_idx;

    err = dragon_bitset_first(&heap->free[split_idx], &block_idx);

    while (split_idx < num_block_sizes - 1 && err == DRAGON_BITSET_ITERATION_COMPLETE) {
        split_idx++;
        err = dragon_bitset_first(&heap->free[split_idx], &block_idx);
    }

    size_t free_block_idx = split_idx;

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find first free block.");

    err = dragon_bitset_reset(&heap->free[split_idx], block_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not reset bit during splitting.");

    while (split_idx > index) {
        /* At the next lower block size there are twice as many blocks so the corresponding
           position is at 2*block_idx. */
        split_idx -= 1;
        block_idx = block_idx*2;
        buddy_idx = buddy(block_idx);

        err = dragon_bitset_set(&heap->free[split_idx], buddy_idx);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not make buddy free during splitting.");
    }

    /* When large blocks become available, they are allocated first to
       waiters waiting for them. A large block allocation may
       result in splitting into smaller blocks where there are
       also waiters. We trigger them here in case any waiters were
       waiting for a smaller block. */
    _trigger_waiter(heap, free_block_idx-1);

    *free_block = block_idx;
    return DRAGON_SUCCESS;
}

static dragonError_t
_free_and_merge(dragonDynHeap_t* heap, uint64_t offset, uint64_t size)
{
    dragonError_t err;

    size_t list_index = _powerof2(size, heap->segment_size);

    size_t block_size = heap->segment_size * ipow(2,list_index);
    size_t block_index = offset / block_size;
    size_t segment_index = offset / heap->segment_size; // for preallocated blocks
    bool is_preallocated;
    bool is_free_already;

    err = dragon_bitset_get(&heap->free[list_index], block_index, &is_free_already);
    if (is_free_already)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER, "The supplied pointer was not in use. It cannot be freed.");

    err = dragon_bitset_get(&heap->preallocated, segment_index, &is_preallocated);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get bit to check for preallocated on free operation.");

    if (is_preallocated) {
        err = dragon_bitset_set(&heap->free[list_index], block_index);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free preallocated block");

        _trigger_waiter(heap, list_index);

        no_err_return(DRAGON_SUCCESS);
    }

    if (list_index < heap->num_block_sizes-1) {
        /* There may be some merging that can be done.*/
        size_t buddy_index;
        bool buddy_free;
        bool buddy_is_preallocated;
        size_t buddy_segment_index;

        do {
            buddy_index = buddy(block_index);
            buddy_segment_index = (buddy_index * block_size) / heap->segment_size;

            err = dragon_bitset_get(&heap->preallocated, buddy_segment_index, &buddy_is_preallocated);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get buddy preallocated bit");

            err = dragon_bitset_get(&heap->free[list_index], buddy_index, &buddy_free);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get buddy free bit");

            if (buddy_free && !buddy_is_preallocated) {
                err = dragon_bitset_reset(&heap->free[list_index], buddy_index);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not reset buddy free bit");

                list_index += 1;
                block_size *= 2;
                block_index = block_index / 2;
            }

        } while (buddy_free && !buddy_is_preallocated && list_index < heap->num_block_sizes-1);
    }

    err = dragon_bitset_set(&heap->free[list_index], block_index);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not set the block's free bit");

    _trigger_waiter(heap, list_index);

    return DRAGON_SUCCESS;
}

static dragonError_t _dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** offset, bool lock, bool unlock) {

    dragonError_t err;

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER, "The heap handle was null.");

    if (offset == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER, "The offset argument does not point to a valid address. It is NULL.");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    if (size > heap->biggest_block) {
        // Then this request cannot be satisfied, ever given the preallocated blocks
        // in the heap.
        char err_str[200];
        snprintf(err_str, 199, "Will never be able to satisfy dragon_heap_malloc request of size %lu. This may be because of static pre-allocations. The biggest block allowed is %" PRIu64 "", size, heap->biggest_block);
        err_return(DRAGON_DYNHEAP_REQUESTED_SIZE_TOO_LARGE, err_str);
    }

    // get power of two for size
    size_t index = _powerof2(size, heap->segment_size);

    // make sure the requested allocation is not bigger than the maximum heap size.
    if (index > heap->num_block_sizes)
        err_return(DRAGON_DYNHEAP_REQUESTED_SIZE_TOO_LARGE,"A malloc cannot be satistied. It is larger than the heap itself.");

    if (lock) {
        // get exclusive access to the heap structure.
        dragonError_t err = dragon_lock(&(heap->dlock));
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The heap lock could not be acquired.");
    }

    // splitting may not be required, but _split will determine that and return
    // the free block. It will also make sure it is labelled as not free so no
    // need to reset the free bit.
    size_t free_block;

    err = _split(heap, index, &free_block);
    if (err == DRAGON_BITSET_ITERATION_COMPLETE) {
        if (unlock)
            dragon_unlock(&(heap->dlock));
        return DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE;
    }

    if (err != DRAGON_SUCCESS) {
        if (unlock)
            dragon_unlock(&(heap->dlock));
        append_err_return(err, "Could not split while finding free block.");
    }

    // This calculates the actual offset into the heap given the segment size and
    // the required block_size which is pointed to be index.
    *offset = (void*) (free_block * (heap->segment_size * ipow(2,index)));

    if (unlock) {
        // release the lock.
        err = dragon_unlock(&(heap->dlock));
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The lock could not be unlocked.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/******************************
    BEGIN USER API
*******************************/

dragonError_t dragon_heap_size(const size_t max_block_size_power, const size_t min_block_size_power,
                               const dragonLockKind_t lock_kind, size_t* size) {

    dragonError_t err;

    if (max_block_size_power <= min_block_size_power)
        err_return(DRAGON_INVALID_ARGUMENT, "The max block size must be larger than the minimum block size.");

    *size = 0; // init in case we return with an error.

    // if an invalid block size is passed, then
    if (max_block_size_power > BLOCK_SIZE_MAX_POWER)
        err_return(DRAGON_DYNHEAP_MAX_BLOCK_SIZE_TOO_LARGE,"The requested max block size is too large.");

    if (min_block_size_power < BLOCK_SIZE_MIN_POWER)
        err_return(DRAGON_DYNHEAP_MIN_BLOCK_SIZE_TOO_SMALL,"The requested min block size is too small.");

    size_t num_block_sizes = max_block_size_power - min_block_size_power + 1;
    uint64_t num_segments = 1UL << (num_block_sizes-1);
    size_t bitsets_size = 0;
    size_t preallocated_bitset_size = 0;
    size_t bits = num_segments;

    /* We compute this with a loop since the bit_set sizes may have some padding added into them given
       boundary alignment and odd numbers of bytes (not multiple of 8)*/

    for (int k=0; k<num_block_sizes; k++) {
        bitsets_size += dragon_bitset_size(bits);
        if (k==0)
            preallocated_bitset_size = bitsets_size;
        // A little explanation here: As block power increases, the number of blocks is halved.
        // In this way, the total number of bits ends up being 2*num_segments total for the heap
        // to keep track of free blocks. Another 2*num_segments bits is needed for allocated blocks.
        bits = bits / 2;
    }

    // for the exact layout of the meta-data see the init function below and the very top of this
    // module.

    size_t lock_size = dragon_lock_size(lock_kind);
    size_t waiter_size;
    err = dragon_bcast_size(0, DRAGON_HEAP_SPIN_WAITERS, NULL, &waiter_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get bcast size for heap allocation.");

    *size =
        MAX(lock_size, sizeof(size_t)) + // insure at least size_t boundary alignment.
        sizeof(uint64_t) * 5 + // space for num_waiting, segment_size, num_segments, num_block_sizes, biggest_block
        bitsets_size  + // the free bitsets
        preallocated_bitset_size + // preallocated blocks are recorded here.
        waiter_size * num_block_sizes; // the size of the bcast objects in the heap.

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_init(void* ptr, dragonDynHeap_t* heap, const size_t max_block_size_power,
                               const size_t min_block_size_power, const dragonLockKind_t lock_kind, const size_t* preallocated)
{
    int k;
    dragonError_t err;

    if (ptr == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr argument cannot be NULL.");

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle cannot be NULL.");

    if (max_block_size_power > BLOCK_SIZE_MAX_POWER)
        err_return(DRAGON_DYNHEAP_MAX_BLOCK_SIZE_TOO_LARGE,"The maximum block size exceeds the limit.");

    if (min_block_size_power < BLOCK_SIZE_MIN_POWER)
        err_return(DRAGON_DYNHEAP_MIN_BLOCK_SIZE_TOO_SMALL,"The minimum block size is under the limit.");

    uint64_t num_block_sizes = max_block_size_power - min_block_size_power + 1;

    uint64_t num_segments = 1UL << (num_block_sizes-1);

    // The heap descriptor (dragonDynHeap_t) maps into the shared memory of the heap.
    // The pointers that are computed in this init function are pointers into the meta-data. See
    // the top of this file for the layout of the meta-data. Any change to the meta-data format
    // requires changes in this init function and the size function above.

    heap->base_pointer = (uint64_t*) ptr;
    err = dragon_lock_init(&heap->dlock, ptr, lock_kind);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock initialization failed.");
    }

    uint64_t lock_size = dragon_lock_size(lock_kind);
    ptr += lock_size;

    heap->num_waiting = (atomic_uint*) ptr;
    *heap->num_waiting = 0;
    ptr += sizeof(uint64_t);

    uint64_t segment_size = 1UL << min_block_size_power;

    // The size of segments is next.
    uint64_t* segment_size_ptr = (uint64_t*) ptr;
    ptr += sizeof(uint64_t);

    // Store the segment_size in the meta-data.
    *segment_size_ptr = segment_size;

    // Then copy the segment_size in the user provided struct.
    heap->segment_size = segment_size;

    // The next field is the number of segments.
    uint64_t* num_segments_ptr = (uint64_t*) ptr;
    ptr += sizeof(uint64_t);

    // Store the value of num_segments into the meta-data.
    *num_segments_ptr = num_segments;

    // Copy the num_segments into the user provided struct as well. This doesn't change during execution
    // so a copy is OK to make. It also protects the meta-data from accidentally being changed.
    heap->num_segments = num_segments;
    heap->total_size = heap->num_segments * heap->segment_size; // not stored in shared mem.

    // The next field is the number of block sizes.
    uint64_t* num_block_sizes_ptr = (uint64_t*) ptr;
    ptr += sizeof(uint64_t);

    // Store the value of num_segments into the meta-data.
    *num_block_sizes_ptr = num_block_sizes;

    // Copy the num_block_sizes into the user provided struct as well.
    heap->num_block_sizes = num_block_sizes;

    // The next field is the biggest block.
    uint64_t* biggest_block_ptr = (uint64_t*) ptr;
    ptr += sizeof(uint64_t);

    // Store the value of num_segments into the meta-data.
    *biggest_block_ptr = heap->total_size; // reset below, but set here for preallocations
    heap->biggest_block = heap->total_size;

    heap->free = malloc(sizeof(dragonBitSet_t) * num_block_sizes);
    if (heap->free == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for free lists in heap.");

    uint64_t bits = num_segments;
    uint64_t bitset_size;

    for (int k=0; k<num_block_sizes; k++) {
        bitset_size = dragon_bitset_size(bits);
        dragon_bitset_init(ptr, &heap->free[k], bits);
        ptr += bitset_size;
        bits = bits / 2;
    }

    bitset_size = dragon_bitset_size(num_segments);
    dragon_bitset_init(ptr, &heap->preallocated, num_segments);
    ptr += bitset_size;

    size_t waiter_size;
    dragon_bcast_size(0, DRAGON_HEAP_SPIN_WAITERS, NULL, &waiter_size);

    heap->waiters = malloc(sizeof(dragonBCastDescr_t)*heap->num_block_sizes);
    if (heap->waiters == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for waiter descriptors.");

    // Now initialize all the bcast objects.
    for (k = 0; k < heap->num_block_sizes; k++) {
        err = dragon_bcast_create_at(ptr, waiter_size, 0, DRAGON_HEAP_SPIN_WAITERS, NULL, &heap->waiters[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize the Heap Managers waiter objects.");
        ptr += waiter_size;
    }

    /* Initialize the initial heap*/
    err = dragon_bitset_set(&heap->free[num_block_sizes-1], 0);

    if (preallocated != NULL) {
        /* Now we do any preallocations to improve efficiency of heap allocations.
           Preallocations are not merged when they are released. So they remain
           statically allocated during the lifetime of the heap. This can be used to
           tune the heap for allocation sizes that are continually re-used.
        */

        /* Count the number of preallocations so we can allocate an array to hold the
           allocations so they can be freed before this code is completed here.
        */

        int count = 0;
        int idx = 0;


        for (k=0; k<heap->num_block_sizes; k++)
            count += preallocated[k];

        dragonDynHeapPreAlloc_t* allocations = malloc(sizeof(dragonDynHeapPreAlloc_t)*count);
        if (allocations == NULL)
            err_return(DRAGON_FAILURE, "Could not allocate internal memory for heap manager init.");

        size_t size = 1UL << max_block_size_power;
        for (k=heap->num_block_sizes-1; k>=0; k--) {
            for (int n=0; n<preallocated[k]; n++) {
                allocations[idx].size = size;
                err = dragon_heap_malloc(heap, size, &allocations[idx].offset);
                if (err != DRAGON_SUCCESS) {
                    char err_str[200];
                    snprintf(err_str, 199, "Could not satisfy preallocated block allocation request of size %lu", size);
                    append_err_return(err, err_str);
                }

                size_t alloc_seg = ((uint64_t)allocations[idx].offset) / heap->segment_size;

                // We now set the preallocated bit for this block
                err = dragon_bitset_set(&heap->preallocated, alloc_seg);
                if (err != DRAGON_SUCCESS) {
                    append_err_return(err, "Could not set bit in preallocated set in heap manager.");
                }
                idx++;
            }

            size = size >> 1UL;
        }

        /* Now we free the static allocations so they are available for allocation. The will not merge because
           of the copy of the bitset above. */
        for (k=0; k<idx; k++) {
            err = dragon_heap_free(heap, allocations[k].offset, allocations[k].size);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not free preallocated allocation in dragon_heap_init");
        }

        free(allocations);
    }

    uint64_t block_size = 1UL << min_block_size_power;
    size_t bit_idx;

    for (k=0; k<heap->num_block_sizes; k++) {

        err = dragon_bitset_first(&heap->free[k], &bit_idx);
        if (err == DRAGON_SUCCESS)
            *biggest_block_ptr = block_size;

        block_size = block_size << 1UL;
    }

    heap->biggest_block = *biggest_block_ptr;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_attach(void* ptr, dragonDynHeap_t* heap) {

    if (ptr == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"A NULL pointer was supplied on the attach.");

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    heap->base_pointer = (uint64_t*) ptr;
    dragonError_t err = dragon_lock_attach(&(heap->dlock), heap->base_pointer);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock could not be attached to the heap handle.");
    }

    size_t lock_size = dragon_lock_size(heap->dlock.kind);
    ptr += lock_size;

    heap->num_waiting = (atomic_uint*) ptr;
    ptr += sizeof(uint64_t);

    heap->segment_size = *((uint64_t*)ptr);
    ptr += sizeof(uint64_t);

    heap->num_segments = *((uint64_t*)ptr);
    ptr += sizeof(uint64_t);
    heap->total_size = heap->num_segments * heap->segment_size; // not stored in shared mem.

    heap->num_block_sizes = *((uint64_t*)ptr);
    ptr += sizeof(uint64_t);

    heap->biggest_block = *((uint64_t*)ptr);
    ptr += sizeof(uint64_t);

    heap->free = malloc(sizeof(dragonBitSet_t) * heap->num_block_sizes);
    if (heap->free == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for free lists in heap.");

    uint64_t bits = heap->num_segments;
    size_t bitset_size;

    for (int k=0; k<heap->num_block_sizes; k++) {
        bitset_size = dragon_bitset_size(bits);
        dragon_bitset_attach(ptr, &heap->free[k]);
        ptr += bitset_size;
        bits = bits / 2;
    }

    bitset_size = dragon_bitset_size(heap->num_segments);
    dragon_bitset_attach(ptr, &heap->preallocated);
    ptr += bitset_size;

    size_t waiter_size;
    dragon_bcast_size(0, DRAGON_HEAP_SPIN_WAITERS, NULL, &waiter_size);

    heap->waiters = malloc(sizeof(dragonBCastDescr_t)*heap->num_block_sizes);
    if (heap->waiters == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for waiter descriptors.");

    // Now initialize all the bcast objects.
    for (int k = 0; k < heap->num_block_sizes; k++) {
        err = dragon_bcast_attach_at(ptr, &heap->waiters[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize the Heap Managers waiter objects.");
        ptr += waiter_size;
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_destroy(dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    // release any resources that the mutex holds. It
    // must be unlocked to call this.
    dragonError_t err = dragon_lock_destroy(&(heap->dlock));
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The lock destroy did not succeed.");

    for (unsigned int k=0; k < heap->num_block_sizes; k++) {
        err = dragon_bcast_destroy(&heap->waiters[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The waiter object destroy did not succeed.");

        err = dragon_bitset_destroy(&heap->free[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The free segments destroy did not succeed.");
    }

    err = dragon_bitset_destroy(&heap->preallocated);
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The preallocated set destroy did not succeed.");

    free(heap->waiters);
    free(heap->free);

    heap->base_pointer = NULL;
    heap->segment_size = 0;
    heap->num_segments = 0;
    heap->num_block_sizes = 0;
    heap->biggest_block = 0;
    heap->free = NULL;
    heap->waiters = NULL;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_detach(dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    // release any resources that the mutex holds. It
    // must be unlocked to call this.
    dragonError_t err = dragon_lock_detach(&(heap->dlock));
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The lock detach did not succeed.");

    for (unsigned int k=0; k < heap->num_block_sizes; k++) {
        err = dragon_bcast_detach(&heap->waiters[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The waiter object detach did not succeed.");

        err = dragon_bitset_detach(&heap->free[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err,"The free segments detach did not succeed.");
    }

    err = dragon_bitset_detach(&heap->preallocated);
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The preallocated set detach did not succeed.");

    free(heap->waiters);
    free(heap->free);

    heap->base_pointer = NULL;
    heap->segment_size = 0;
    heap->num_segments = 0;
    heap->num_block_sizes = 0;
    heap->biggest_block = 0;
    heap->free = NULL;
    heap->waiters = NULL;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** offset) {
    return _dragon_heap_malloc(heap, size, offset, true, true);
}


dragonError_t
dragon_heap_malloc_blocking(dragonDynHeap_t* heap, const size_t size, void** offset, const timespec_t* timeout)
{
    dragonError_t err;
    dragonError_t alloc_err;
    timespec_t deadline;
    timespec_t remaining;

    err = dragon_timespec_deadline(timeout, &deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not compute deadline for timeout.");

    alloc_err = _dragon_heap_malloc(heap, size, offset, true, false);

    /* if a zero timeout is supplied, then don't block. This is useful
       for the managed memory code */
    if (deadline.tv_sec == 0 && deadline.tv_nsec == 0) {
        // release the lock. Don't check err and don't return since alloc_err is the
        // more important error to return.
        dragon_unlock(&(heap->dlock));
        /* DON'T change this to an append_err_return, it impacts latency in ch_test.c terribly */
        return alloc_err;
    }

    size_t index = _powerof2(size, heap->segment_size);

    while (alloc_err == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE) {
        err = dragon_timespec_remaining(&deadline, &remaining);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&(heap->dlock));
            append_err_return(err, "Could not compute time remaining.");
        }

        /* The bcast wait will unlock regardless of the return code here
           so no need to call unlock in case of error. */
        err = _wait(heap, &heap->waiters[index], &remaining);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or could not wait in blocking malloc of heap manager.");


        alloc_err = _dragon_heap_malloc(heap, size, offset, true, false);
    }

    if (alloc_err != DRAGON_OBJECT_DESTROYED)
        // release the lock. Don't check err and don't return since alloc_err is the
        // more important error to return.
        dragon_unlock(&(heap->dlock));

    if (alloc_err != DRAGON_SUCCESS)
        append_err_return(alloc_err, "Blocking malloc failed");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_free(dragonDynHeap_t* heap, void* offset, size_t size) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    if ((uint64_t)offset > heap->total_size)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER, "The pointer being freed is not a valid heap pointer.");

    if (size > heap->total_size)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER, "The size of the allocation being freed is not valid.");

    // get exclusive access to the heap structure.
    dragonError_t err = dragon_lock(&(heap->dlock));
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock could not be acquired.");
    }

    err = _free_and_merge(heap, (uint64_t)offset, size);
    if (err != DRAGON_SUCCESS) {
        char info[200];
        char* err_str = dragon_getlasterrstr();
        dragon_unlock(&(heap->dlock));
        err_noreturn(err_str);
        free(err_str);
        snprintf(info, 199, "The pointer/offset %" PRIu64 " with size %lu could not be freed.", (uint64_t)offset, size);
        append_err_return(err, info);
    }

    // release the lock.
    err = dragon_unlock(&(heap->dlock));
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The lock could not be unlocked.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_get_stats(dragonDynHeap_t* heap, dragonHeapStats_t* data) {
    // get exclusive access to the heap structure. Needed to insure we can
    // traverse the linked lists below without it changing underneath us.

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    if (data == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The data pointer was NULL");

    dragonError_t err = dragon_lock(&(heap->dlock));
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock could not be acquired.");
    }

    data->num_segments = heap->num_segments;
    data->segment_size = heap->segment_size;
    data->total_size = heap->num_segments * heap->segment_size;

    uint64_t total = 0;
    uint64_t block_size = heap->segment_size;
    for (size_t k=0; k<heap->num_block_sizes;k++) {
        size_t count = 0;
        dragon_bitset_length(&heap->free[k], &count);
        data->free_blocks[k].block_size = block_size;
        data->free_blocks[k].num_blocks = count;

        total += count * block_size;
        block_size = block_size * 2;

    }

    data->num_block_sizes = heap->num_block_sizes;
    data->total_free_space = total;
    data->utilization_pct = (1-((double)data->total_free_space) / data->total_size)*100;

    // release the lock.
    err = dragon_unlock(&(heap->dlock));
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"The lock could not be unlocked.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_heap_dump(const char* title, dragonDynHeap_t* heap) {
    return dragon_heap_dump_to_fd(stdout, title, heap);
}


dragonError_t dragon_heap_dump_to_fd(FILE* fd, const char* title, dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (heap->segment_size == 0)
        err_return(DRAGON_OBJECT_DESTROYED, "The heap is invalid. Segment size is 0.");

    if (title == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The title was NULL");

    // get exclusive access to the heap structure.
    dragonError_t err = dragon_lock(&(heap->dlock));
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock could not be acquired.");
    }

    size_t k;
    fprintf(fd, "*****************************************************************************************\n");
    fprintf(fd, "* DYNAMIC MEMORY HEAP DUMP\n");
    fprintf(fd, "* %s\n",title);
    fprintf(fd, "*****************************************************************************************\n");

    fprintf(fd, "*  Number of Segments: %" PRIu64 " (0x%" PRIu64 "x)\n", heap->num_segments,heap->num_segments);
    fprintf(fd, "*  Segment Size: %" PRIu64 " (0x%" PRIu64 "x)\n",heap->segment_size, heap->segment_size);
    fprintf(fd, "*  Number of Block Sizes: %" PRIu64 "\n", heap->num_block_sizes);
    fprintf(fd, "*  Total Size of Heap: %" PRIu64 "\n", heap->total_size);

    fprintf(fd, "*  Biggest Possible Block in Heap: %" PRIu64 "\n", heap->biggest_block);
    fprintf(fd, "*  Number of Processes Waiting for Allocations: %u\n", *heap->num_waiting);
    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    fprintf(fd, "*   Free Blocks:\n");
    fprintf(fd, "*  \tindex:\tblock size (# segments):\tfree blocks:\tpreallocated blocks:\n");
    size_t seg_count = 1;
    for (k=0;k<heap->num_block_sizes;k++) {
        size_t count;
        dragon_bitset_length(&heap->free[k], &count);
        size_t idx = 0;
        size_t prealloc_blocks = 0;
        err = dragon_bitset_first(&heap->free[k], &idx);
        while (err == DRAGON_SUCCESS) {
            size_t pre_idx = idx * seg_count;
            bool is_prealloc = false;

            err = dragon_bitset_get(&heap->preallocated, pre_idx, &is_prealloc);
            if (is_prealloc)
                prealloc_blocks += 1;
            err = dragon_bitset_next(&heap->free[k], idx, &idx);
        }

        fprintf(fd, "*  \t%5lu\t%20lu\t%16lu\t%16lu\n",k,seg_count,count,prealloc_blocks);
        seg_count = seg_count * 2;
    }

    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");

    for (int k=0; k<heap->num_block_sizes;k++) {
        fprintf(fd, "*  --- Block Size %8d -------------------------------------------------------------\n", k);
        dragon_bitset_dump_to_fd(fd, "Free Blocks", &heap->free[k],"*  ");
    }

    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    dragon_bitset_dump_to_fd(fd, "Preallocated", &heap->preallocated,"*  ");

    fprintf(fd, "*****************************************************************************************\n");

    err = dragon_unlock(&(heap->dlock));
    if (err != DRAGON_SUCCESS) {
        append_err_return(err,"The lock could not be unlocked.");
    }

    no_err_return(DRAGON_SUCCESS);
}
