#include "_hexdump.h"
#include "err.h"
#include <dragon/bcast.h>
#include "_heap_manager.h"


/* The dragonDynHeap_t structure is owned by a process and contains
   pointers that point into the heap itself where the following layout
   is recorded. These variables aren't named values, but they are
   recorded here to show the format of the heap data (especially in
   case of reading a dump!). NOTE: This is not the handle structure. This
   is the internal heap layout. Handles repeat some of these fields and
   contain pointers to other fields within the actual heap itself.

   - pthread_mutex_t exclusive_access_val;
   - uint64_t num_segments_val;
   - uint64_t segment_size_val;
   - size_t num_freelists_val;
   - size_t recovery_needed_val; // much bigger than needed, but aligns
   - size_t num_waiting; // total number of waiters for any sized block on the heap.
   - dragonDynHeapSegment_t* segments_ptr;
     // segments_ptr is needed because alignment requirement will mean
     // that the start of the segments area is dependent on alignment
   - dragonBitSet_size space needed for the block_set bitset
   - dragonBitSet_size space needed for the free_set bitset
   - dragonBitSet_size space needed for the preallocated_set bitset
   - size_t num_waiters[]; // A count of the number of waiters for a given size allocation
   - size_t num_available[]; // A count of the number of available allocations for a given size
   - Free Lists; // A list of pointers to nodes in the linked lists
   - PreAllocated Free Lists; A list of pointers to preallocated blocks (nodes in linked lists)
   - The Array of Segments; // Each segment is the minimum block size. However,
     // a block may be comprised of one or more segements.
*/

#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define MAX(x, y) (((x) > (y)) ? (x) : (y))

// These defines are the same as the function defintions below but are defined this way
// so that they are expanded inline and avoid the overhead of the function calls. The
// function definitions are left here for reference.

#define set_next(node, val) (((size_t*)node)[1] = val)
#define set_prev(node, val) (((size_t*)node)[0] = val)
#define get_next(node) ((size_t) (((size_t*)node)[1]))
#define get_prev(node) ((size_t) (((size_t*)node)[0]))
#define NULL_OFFSET 0UL
/*
static void set_next(dragonDynHeapSegment_t** node, dragonDynHeapSegment_t* val) {
    // next is at 8 bytes off the node pointer.
    node[1] = val;
}

static void set_prev(dragonDynHeapSegment_t** node, dragonDynHeapSegment_t* val) {
    // previous is at 0 bytes off the node pointer.
    node[0] = val;
}

static dragonDynHeapSegment_t** get_next(dragonDynHeapSegment_t** node) {
    // return the pointer to the next element in the list.
    // We cast it because each node in the list points at an
    // array of pointers (the previous and next items).
    return (dragonDynHeapSegment_t**) node[1];
}

static dragonDynHeapSegment_t** get_prev(dragonDynHeapSegment_t** node) {
    // return the pointer to the previous element in the list.
    // We cast it because each node in the list points at an
    // array of pointers (the previous and next items).
    return (dragonDynHeapSegment_t**) node[0];
}
*/

#define get_freelist_index(node) ((size_t) (((dragonDynHeapSegment_t**)node)[2]))
#define set_freelist_index(node, index) (((dragonDynHeapSegment_t**)node)[2] = (dragonDynHeapSegment_t*)index)

/* These two macros do arithmetic because pointers are not stored in the
   heap because when different processes attach to a heap, the virtual addresses
   are different. So only offsets from the beginning of the heap are stored in the heap.
   These functions do the arithmetic to convert to/from a pointer in the process local storage
   when a pointer is needed/stored.
*/

#define heap_addr(heap, offset) ((offset) == NULL_OFFSET ? NULL : ((dragonDynHeapSegment_t*)(((void*)heap->base_pointer) + (size_t) offset)))
#define heap_offset(heap, heap_ptr) ((heap_ptr) == NULL ? NULL_OFFSET : ((size_t)((void*)heap_ptr-(void*)heap->base_pointer)))


static unsigned char
_is_valid_ptr(const dragonDynHeap_t* heap, const void* ptr)
{

    // Look to see if the pointer falls outside the range of valid pointers for the heap.
    if (ptr < (void*)heap->segments)
        return FALSE;


    size_t offset = ptr - (void*)heap->segments;

    // Look to see if the pointer falls outside the range of valid pointers for the heap.
    if (ptr >= heap->end_ptr)
        return FALSE;

    // Look to see if the pointer falls on a segment boundary.
    if ((offset%heap->segment_size) != 0) {
        return FALSE;
    }

    // All checks pass, return true
    return TRUE;
}

static unsigned char
_is_valid_block_ptr(const dragonDynHeap_t* heap, const void* ptr)
{

    if (_is_valid_ptr(heap,ptr) == FALSE)
        return FALSE;

    size_t offset = ptr - (void*)heap->segments;
    size_t bit_index = offset / heap->segment_size;

    unsigned char block_bit;
    dragon_bitset_get(&heap->block_set,bit_index,&block_bit);

    // If this is not an allocated block, then it is not valid.
    if (block_bit != 1)
        return FALSE;

    // All checks pass, return true
    return TRUE;
}

static unsigned char
_is_valid_free_ptr(const dragonDynHeap_t* heap, const void* ptr)
{
    // When checking pointers, null is a valid pointer value in free lists.
    if (ptr==NULL)
        return TRUE;

    if (_is_valid_block_ptr(heap, ptr) == TRUE) {
        size_t offset = ptr - (void*)heap->segments;
        size_t bit_index = offset / heap->segment_size;

        unsigned char free_bit;
        dragon_bitset_get(&heap->free_set,bit_index,&free_bit);

        // If this is not an allocated block, then it is not valid.
        if (free_bit == 1)
            return TRUE;
    }

    return FALSE;
}

static size_t
_powerof2(const size_t size, const uint64_t segment_size)
{
    // returns the smallest power of 2 of segment_size
    // that can hold size.
    uint64_t seg_size = segment_size;

    size_t power = 0;

    while (size > seg_size) {
        seg_size = seg_size * 2;
        power += 1;
    }

    return power;
}

static dragonError_t
_trigger_all_waiters(const dragonDynHeap_t* heap)
{
    int idx = heap->num_freelists-1;
    timespec_t timeout = {DRAGON_HEAP_NOTIFY_TIMEOUT,0};

    while (idx >= 0) {
        /* We ignore the return code here - but we should log this if there
           were a bad return code without returning that return code to the
           user */
        dragon_bcast_trigger_all(&heap->waiters[idx], &timeout, NULL, 0);
        idx -= 1;
    }

    *(heap->num_waiting) = 0;
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_trigger_waiter(const dragonDynHeap_t* heap, const size_t free_list_index)
{
    if (*(heap->num_waiting) > 0) {
        int idx = free_list_index;
        dragonError_t err;
        timespec_t timeout = {DRAGON_HEAP_NOTIFY_TIMEOUT,0};

        /* The following while loop favors waiters for the biggest block possible.
        If a large block becomes available and there are multiple waiters, the
        waiter for the largest block will get its allocation first. Other smaller
        block waiters may still follow if there are other smaller blocks made
        available by splitting. Those others will be triggered as soon as the first
        one completes (see the malloc function). */

        while (idx >= 0) {

            int num_waiters;

            err = dragon_bcast_num_waiting(&heap->waiters[idx],&num_waiters);

            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not retrieve num waiting from a BCast object");

            if (num_waiters > 0) {
                /* We ignore the return code here - but we should log this if there
                   were a bad return code without returning that return code to the
                   user */
                dragon_bcast_trigger_one(&heap->waiters[idx], &timeout, NULL, 0);
                *(heap->num_waiting) -= 1;
                no_err_return(DRAGON_SUCCESS);
            }

            idx -= 1;
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_wait(dragonDynHeap_t* heap, dragonBCastDescr_t* bcast_obj, const timespec_t* timeout)
{
    *(heap->num_waiting) += 1;

    dragonError_t err = dragon_bcast_wait(bcast_obj, DRAGON_IDLE_WAIT, timeout, NULL, 0, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Timeout or could not idle wait in blocking malloc of heap manager.");

    no_err_return(DRAGON_SUCCESS);
}

static void
__add_to_freelist(dragonDynHeap_t* heap, size_t* free_lists, const size_t free_list_index,
                 dragonDynHeapSegment_t* node)
{
    // Always make the first legitimate node in the freelist
    // have NULL as its previous so the is_valid_free_ptr will
    // function correctly when called on the previous of the
    // first node in the list.
    set_prev(node, NULL_OFFSET);
    set_next(node, free_lists[free_list_index]);

    if (get_next(node) != NULL_OFFSET)
        set_prev(heap_addr(heap, get_next(node)), heap_offset(heap, node)); // store the offset only. Virtual addresses change.

    free_lists[free_list_index] = heap_offset(heap, node);

    set_freelist_index(node,free_list_index);
}

static void
_add_to_preallocated_freelist(dragonDynHeap_t* heap, const size_t free_list_index,
                                         dragonDynHeapSegment_t* node)
{
    __add_to_freelist(heap,heap->preallocated_free_lists,free_list_index, node);
}

static void
_add_to_freelist(dragonDynHeap_t* heap, const size_t free_list_index, dragonDynHeapSegment_t* node)
{
    __add_to_freelist(heap,heap->free_lists,free_list_index, node);
}

static dragonError_t
__remove_from_freelist(dragonDynHeap_t* heap, size_t* free_lists, const size_t free_list_index,
                      dragonDynHeapSegment_t* node)
{
    dragonDynHeapSegment_t* next = heap_addr(heap, get_next(node));
    dragonDynHeapSegment_t* prev = heap_addr(heap, get_prev(node));

    if (_is_valid_free_ptr(heap, next) == FALSE) {
        *heap->recovery_needed_ptr = TRUE;
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED, "Dragon heap corrupted. Recovery required.");
    }

    if (_is_valid_free_ptr(heap, prev) == FALSE) {
        *heap->recovery_needed_ptr = TRUE;
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED, "Dragon heap corrupted. Recovery required.");
    }

    size_t index = get_freelist_index(node);
    if (index >= heap->num_freelists) {
        *heap->recovery_needed_ptr = TRUE;
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED, "Dragon heap corrupted. Recovery required.");
    }

    if (get_next(node) != NULL_OFFSET)
        set_prev(heap_addr(heap, get_next(node)), get_prev(node));

    if (get_prev(node) != NULL_OFFSET)
        set_next(heap_addr(heap, get_prev(node)), get_next(node));

    set_next(node, NULL_OFFSET);
    set_prev(node, NULL_OFFSET);

    if (free_lists[free_list_index] == heap_offset(heap, node)) {
        free_lists[free_list_index] = heap_offset(heap, next);  // store the offset only. Virtual addresses change.
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_remove_from_preallocated_freelist(dragonDynHeap_t* heap, const size_t free_list_index,
                                  dragonDynHeapSegment_t* node)
{
    return __remove_from_freelist(heap, heap->preallocated_free_lists, free_list_index, node);
}

static dragonError_t
_remove_from_freelist(dragonDynHeap_t* heap, const size_t free_list_index, dragonDynHeapSegment_t* node)
{
    return __remove_from_freelist(heap, heap->free_lists, free_list_index, node);
}

static void
_split(dragonDynHeap_t* heap, const size_t index)
{
    // split some bigger block than the size found at index
    // and keep splitting until the block size at
    // index has a block in its free list.
    size_t split_idx = index;
    size_t num_freelists = heap->num_freelists;
    while (split_idx < num_freelists - 1 && heap->free_lists[split_idx] == NULL_OFFSET)
        split_idx++;

    if (heap->free_lists[split_idx] == NULL_OFFSET) {
        // We cannot satisfy the request.
        return;
    }

    while (split_idx > index) {
        // Get a node from the free list. There will be one available
        // because of loop invariant.
        dragonDynHeapSegment_t* node = heap_addr(heap, heap->free_lists[split_idx]);

        if (_is_valid_free_ptr(heap, node) == FALSE) {
            *heap->recovery_needed_ptr = TRUE;
            return;
        }
        // Remove the block to split from the doubly linked list.
        _remove_from_freelist(heap, split_idx, node);

        // Now gather stats about this node.
        void* block_ptr = (void*) node;
        size_t offset = block_ptr - (void*)heap->segments;
        size_t bitset_index = offset / heap->segment_size;

        // compute 2^split_idx to get the right number of
        // segments.
        size_t segment_span = 1 << split_idx;

        // segment_span could be computed this way, but we have the split_index
        // so we use it instead.
        // dragon_bitset_zeroes_to_right(heap->block_set,bitset_index,&segment_span);
        // segment_span = segment_span+1; // add the bit that was a 1.

        // block_size is the number of segments * segment_size.
        size_t block_size = segment_span * heap->segment_size;

        // set up the newly split block in the heap.
        size_t split_seg_span = segment_span >> 1; // divide by 2
        size_t split_block_size = block_size >> 1; // divide by 2
        size_t new_node_bitset_index = bitset_index + split_seg_span;

        // set the bits for the block_set and the free_set. The
        // block_set indicates the start of a block and the free_set
        // says the block is free.
        dragon_bitset_set(&heap->block_set,new_node_bitset_index);
        dragon_bitset_set(&heap->free_set,new_node_bitset_index);

        // get a pointer to the new block which is a new node in the linked
        // list of free nodes.
        dragonDynHeapSegment_t* new_node =
            (dragonDynHeapSegment_t*) (block_ptr + split_block_size);

        // initialize the new spot in the free list with the two split nodes/blocks.
        split_idx--;


        _add_to_freelist(heap,split_idx,new_node);
        _add_to_freelist(heap,split_idx,node);
    }
}

static unsigned char
_free_and_merge(dragonDynHeap_t* heap, void* ptr)
{

    // First, we set the free bit for this block
    size_t offset = ptr - (void*) heap->segments;
    dragonError_t err;

    size_t bit_index = offset / heap->segment_size;

    unsigned char preallocated_bit;
    dragon_bitset_get(&heap->preallocated_set,bit_index,&preallocated_bit);

    size_t segment_span;

    dragon_bitset_zeroes_to_right(&heap->block_set,bit_index,&segment_span);

    segment_span += 1; // add one for the segment itself.

    // Power of 2 of the block_size is the index into the free lists. However, the
    // 0th entry in the free list is the largest segment span so
    // we reverse the index here by subtracting from the number of
    // free lists.
    size_t block_size = heap->segment_size * segment_span;

    size_t free_list_index = _powerof2(block_size, heap->segment_size);

    size_t num_freelists = heap->num_freelists;

    // Link this freed node into the free lists and free the node
    dragonDynHeapSegment_t* node = (dragonDynHeapSegment_t*) ptr;

    unsigned char free_bit;
    dragon_bitset_get(&heap->free_set,bit_index,&free_bit);
    if (free_bit != 1) {
        // This check is made because during recovery we call this
        // method in case any merging can be done. But since the
        // items are already in the free list when this is called,
        // we don't want to add them again.
        dragon_bitset_set(&heap->free_set,bit_index);

        if (preallocated_bit == 1) {
            _add_to_preallocated_freelist(heap, free_list_index, node);
            err = _trigger_waiter(heap,free_list_index);
            if (err != DRAGON_SUCCESS)
                fprintf(stderr, "heap manager trigger_waiter returned err=%s\n", dragon_get_rc_string(err));
            return TRUE;
        }
        else
            _add_to_freelist(heap, free_list_index, node);
    }

    while (free_list_index < num_freelists - 1) {
        // The bit index for the buddy is the exclusive or of the
        // bit_index and the segment_span.
        size_t buddy_index = bit_index ^ segment_span;
        dragonDynHeapSegment_t* buddy_node = (dragonDynHeapSegment_t*)(((void*)heap->segments)+heap->segment_size*buddy_index);

        size_t left_bit_index, right_bit_index;

        if (buddy_index < bit_index) {
            // The buddy is to the left
            left_bit_index = buddy_index;
            right_bit_index = bit_index;
        }
        else {
            // The buddy is to the right
            left_bit_index = bit_index;
            right_bit_index = buddy_index;
        }

        dragon_bitset_get(&heap->free_set,buddy_index,&free_bit);

        if (free_bit == 1) {
            // This block is free. See if it is the same size as this block.
            size_t buddy_segment_span;

            size_t buddy_freelist_index = get_freelist_index(buddy_node);

            if (buddy_freelist_index < num_freelists && buddy_freelist_index == free_list_index) {
                // The first check (buddy_freelist_index < num_freelists) is necessary in case
                // the buddy block had been corrupted by an overrun from a previous block. In that
                // case we'll need to rely on zeroes_to_the_right.
                // The second part of the condition says that if this buddy is in the same free list
                // as the original node being freed, then they have the same segment span.
                buddy_segment_span = segment_span;

            } else {
                // We'll have to get the segment span the slow way for the buddy because the buddy
                // block was corrupted. If it was corrupted we can rely on the meta-data to give
                // us the segment span by counting the 0's to the right of its block_set bit location.
                dragon_bitset_zeroes_to_right(&heap->block_set,buddy_index,&buddy_segment_span);
                // add 1 to include the bit with the 1 in the block_set
                buddy_segment_span += 1;
            }

            if (buddy_segment_span == segment_span) {
                // The buddy segment is free and the same size block as the block
                // being freed. So merge the two blocks into one block. Merging
                // might cascade.
                dragon_bitset_get(&heap->preallocated_set,buddy_index,&preallocated_bit);
                if (preallocated_bit == 1) {
                    // Just get out because we cannot merge and all subsequent
                    // merges are not possible since buddy was preallocated.
                    err = _trigger_waiter(heap,free_list_index);
                    if (err != DRAGON_SUCCESS)
                        fprintf(stderr, "heap manager trigger_waiter returned err=%s\n", dragon_get_rc_string(err));

                    return TRUE;
                }

                dragonDynHeapSegment_t* left = (dragonDynHeapSegment_t*)(((void*)heap->segments)+heap->segment_size*left_bit_index);

                // We perform this check on the free list because if it were corrupted
                // by an overwrite into the segment (by the user) then we can re-construct
                // the free lists.
                if (_is_valid_free_ptr(heap, heap_addr(heap, get_prev(left))) == FALSE)
                    return FALSE;
                if (_is_valid_free_ptr(heap, heap_addr(heap, get_next(left))) == FALSE)
                    return FALSE;

                _remove_from_freelist(heap,free_list_index,left);

                dragonDynHeapSegment_t* right = (dragonDynHeapSegment_t*)(((void*)heap->segments)+heap->segment_size*right_bit_index);

                // We perform this check on the free list because if it were corrupted
                // by an overwrite into the segment (by the user) then we can re-construct
                // the free lists.
                if (_is_valid_free_ptr(heap, heap_addr(heap, get_prev(right))) == FALSE)
                    return FALSE;
                if (_is_valid_free_ptr(heap, heap_addr(heap, get_next(right))) == FALSE)
                    return FALSE;

                _remove_from_freelist(heap,free_list_index,right);

                // Add the node into the next bigger list of free lists
                free_list_index++;

                // Link the larger block into the correct free list
                _add_to_freelist(heap, free_list_index, left);

                // Now set the bits in the free_set and block_set correctly.
                dragon_bitset_reset(&heap->free_set,right_bit_index);
                dragon_bitset_reset(&heap->block_set,right_bit_index);

                // Set up for the cascading merging of blocks in the next
                // iteration.
                block_size = block_size * 2;
                segment_span = segment_span * 2;
                bit_index = left_bit_index;
            } else {
                // blocksize of this block and the buddy are different so we are done.
                err = _trigger_waiter(heap,free_list_index);
                if (err != DRAGON_SUCCESS)
                    fprintf(stderr, "heap manager trigger_waiter returned err=%s\n", dragon_get_rc_string(err));
                return TRUE;
            }

        } else {
            // The buddy is not free so we are done.
            err = _trigger_waiter(heap,free_list_index);
            if (err != DRAGON_SUCCESS)
                fprintf(stderr, "heap manager trigger_waiter returned err=%s\n", dragon_get_rc_string(err));
            return TRUE;
        }
    }

    // We cascaded the merges all the way up the tree so we are done.
    err = _trigger_waiter(heap,free_list_index);
    if (err != DRAGON_SUCCESS)
        fprintf(stderr, "heap manager trigger_waiter returned err=%s\n", dragon_get_rc_string(err));

    return TRUE;
}

/******************************
    BEGIN USER API
*******************************/

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
 *  @param max_block_size_power The base 2 power of the maximum block size. There will be one
 *  block with this maximum block size.
 *  @param min_block_size_power The base 2 power of the minimum block size. There will be more than
 *  one of these block sizes available. In fact that will be maximum of 2 to the (max_block_size_power -
 *  min_block_size_power) blocks of this size available.
 *  @param alignment The byte alignment requirement of blocks in the heap.
 *  @param size The returned size required for this heap. The user allocates a blob of this size and
 *  then passes a pointer to it in on the init function call before using any other functions in this
 *  API.
 *  @return A dragonError_t return code.
 */

dragonError_t dragon_heap_size(const size_t max_block_size_power, const size_t min_block_size_power,
                                    const size_t alignment, const dragonLockKind_t lock_kind, size_t* size) {

    if (max_block_size_power < min_block_size_power)
        err_return(DRAGON_DYNHEAP_MAX_BLOCK_SIZE_TOO_LARGE,"The max block size must be larger than the minimum block size.");

    size_t byte_alignment = alignment;
    *size = 0; // init in case we return with an error.

    // if an invalid block size is passed, then
    if (max_block_size_power > BLOCK_SIZE_MAX_POWER)
        err_return(DRAGON_DYNHEAP_MAX_BLOCK_SIZE_TOO_LARGE,"The requested block size is too large.");

    if (min_block_size_power < BLOCK_SIZE_MIN_POWER)
        err_return(DRAGON_DYNHEAP_MIN_BLOCK_SIZE_TOO_SMALL,"The requested block size is too small.");

    // The lock is the first thing in the meta-data. The lock is located at offset 0 from ptr.
    size_t mask = 0x7;

    if ((byte_alignment & mask) != 0) {
        // The aligment request must be a multiple of 8 bytes.
        err_return(DRAGON_DYNHEAP_BAD_ALIGNMENT_REQUEST,"At least word alignment is required.");
    }

    if (byte_alignment == 0) {
        // If alignment is not not provided, guarantee 8 byte alignment at the beginning of the
        // segments.
        byte_alignment = sizeof(size_t);
    }

    size_t num_freelists = max_block_size_power - min_block_size_power + 1;
    uint64_t num_segments = 1 << (num_freelists-1);
    size_t bitset_size;
    bitset_size = dragon_bitset_size(num_segments);
    uint64_t segment_size = 1 << min_block_size_power;

    // for the exact layout of the meta-data see the init function below and the very top of this
    // module.

    size_t lock_size =  dragon_lock_size(lock_kind);
    size_t waiter_size;
    dragon_bcast_size(0,0,NULL,&waiter_size);

    size_t header_size =
        MAX(lock_size, sizeof(size_t)) + // insure at least size_t boundary alignment.
        sizeof(uint64_t) * 2 + // space for num_segments and segment_size
        sizeof(size_t) * 4 + // num_freelists, recover_needed, and biggest_block, num_waiting
        sizeof(void*) + // a pointer at the segments region of the heap
        bitset_size * 3 + // the three bit sets are block_set, free_set, and preallocated
        waiter_size * num_freelists + // the size of the bcast objects in the heap.
        num_freelists * sizeof(void*) * 2; // Two free lists, freelist and preallocated_freelist

    uint64_t total_segments_size = num_segments * segment_size;
    uint64_t total_size = total_segments_size + header_size;

    // if alignment is passed in then the segments must start on the alignment boundary.
    size_t alignment_units = header_size / byte_alignment;
    size_t alignment_remainder = header_size % byte_alignment;
    if (alignment_remainder != 0)
        alignment_units += 1;

    total_size = alignment_units * byte_alignment + total_segments_size;

    *size = total_size;

    no_err_return(DRAGON_SUCCESS);
}

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
 *  @param alignment The required alignment of blocks within this heap.
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

dragonError_t dragon_heap_init(void* ptr, dragonDynHeap_t* heap, const size_t max_block_size_power,
                   const size_t min_block_size_power, const size_t alignment, const dragonLockKind_t lock_kind, const size_t* preallocated) {

    int k;
    size_t byte_alignment = alignment;
    dragonError_t derr;

    if (ptr == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr argument cannot be NULL.");

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle cannot be NULL.");

    if (max_block_size_power > BLOCK_SIZE_MAX_POWER)
        err_return(DRAGON_DYNHEAP_MAX_BLOCK_SIZE_TOO_LARGE,"The maximum block size exceeds the limit.");

    if (min_block_size_power < BLOCK_SIZE_MIN_POWER)
        err_return(DRAGON_DYNHEAP_MIN_BLOCK_SIZE_TOO_SMALL,"The minimum block size is under the limit.");

    // The heap is a user provided struct with the fields that map onto this heaps meta-data.
    // The pointers that are computed in this init function are pointers into the meta-data. See
    // the top of this file for the layout of the meta-data. Any change to the meta-data format
    // requires changes in this init function and the size function above.

    // The lock is the first thing in the meta-data. The lock is located at offset 0 from ptr.
    size_t mask = 0x7;

    if ((((size_t)ptr) & mask) != 0) {
        // This ptr is not on an 8 byte boundary. This is not allowed.
        err_return(DRAGON_DYNHEAP_INCORRECT_MEMORY_ALIGNMENT,"A minimum of word alignment is required.");
    }

    if ((byte_alignment & mask) != 0) {
        // The aligment request must be a multiple of 8 bytes.
        err_return(DRAGON_DYNHEAP_BAD_ALIGNMENT_REQUEST,"The minimum word alignment was not met.");
    }

    if (byte_alignment == 0) {
        // If alignment is not not provided, guarantee 8 byte alignment at the beginning of the
        // segments.
        byte_alignment = sizeof(size_t);
    }


    heap->base_pointer = (uint64_t*) ptr;
    derr = dragon_lock_init(&heap->dlock, ptr, lock_kind);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock initialization failed.");
    }

    size_t lock_size = dragon_lock_size(lock_kind);

    size_t num_freelists = max_block_size_power - min_block_size_power + 1;

    uint64_t num_segments = 1 << (num_freelists-1);

    // The next field is the number of segements. The address of the segments variable
    // is the address of the lock plus the size the lock.
    uint64_t* num_segments_ptr = (uint64_t*) ((void*)heap->base_pointer + lock_size);

    // Store the value of num_segments into the meta-data.
    *num_segments_ptr = num_segments;

    // Copy the num_segments into the user provided struct as well. This doesn't change during execution
    // so a copy is OK to make. It also protects the meta-data from accidentally being changed.
    heap->num_segments = num_segments;

    uint64_t segment_size = 1 << min_block_size_power;

    // The size of segments is located at 8 bytes offset from the num_segments in the meta-data.
    uint64_t* segment_size_ptr = (uint64_t*) ((void*)num_segments_ptr + sizeof(uint64_t));

    // Store the segment_size in the meta-data.
    *segment_size_ptr = segment_size;

    // Then copy the segment_size in the user provided struct.
    heap->segment_size = segment_size;

    // The number of free lists starts 8 bytes after the segment_size in the meta-data.
    size_t* num_freelists_ptr = (size_t*) ((void*)segment_size_ptr + sizeof(uint64_t));

    // Copy the number of free lists into the meta-data.
    *num_freelists_ptr = num_freelists;

    // Copy the number of free lists into the user provided struct. Again, a copy is OK
    // since this will not change.
    heap->num_freelists = num_freelists;

    // The user provided struct contains a pointer to the recovery needed field because
    // it is NOT ok to make a copy here. We want to refer to the one copy since it may
    // change as execution proceeds. The data is 8 bytes from the address of the
    // number of free lists.
    heap->recovery_needed_ptr = (size_t*) ((void*)num_freelists_ptr + sizeof(size_t));

    // Initially recovery is not required.
    *(heap->recovery_needed_ptr) = FALSE;

    heap->num_waiting = (size_t*) ((void*)heap->recovery_needed_ptr + sizeof(size_t));

    *(heap->num_waiting) = 0;

    size_t* biggest_block_ptr = (size_t*) ((void*)heap->num_waiting + sizeof(size_t));

    // The following biggest_block_size possible value will be adjusted below if there are
    // any static preallocations done.
    *biggest_block_ptr = 1UL << max_block_size_power;
    heap->biggest_block = *biggest_block_ptr;

    // The segments_offset_ptr is a pointer to the offset of the segments from start of the heap.
    // The location of this offset in the meta-data is 8 bytes from the address of the
    // recovery_needed data.
    // The offset to the segments is stored within the heap, not a pointer, because this heap
    // WILL be mapped under different virtual address spaces, so no pointers are stored
    // within the heap itself. Only offsets. The Heap handle only exists on a process basis
    // so pointers can be stored within a handle.
    size_t* segments_offset_ptr = (void*)biggest_block_ptr + sizeof(size_t);
    // The data at segments_offset_ptr will be filled in later - below.

    // The block_set starts 8 bytes after the segments_ptr address computed above.
    void* block_set_ptr = ((void*)segments_offset_ptr) + sizeof(dragonDynHeapSegment_t*);
    dragon_bitset_init(block_set_ptr, &heap->block_set, num_segments);

    // This size is needed to compute the address of the free_set.
    size_t bitset_size;
    bitset_size = dragon_bitset_size(num_segments);

    // The free set is in the meta-data right after the block_set.
    void* free_set_ptr = block_set_ptr + bitset_size;
    dragon_bitset_init(free_set_ptr, &heap->free_set, num_segments);

    void* preallocated_set_ptr = free_set_ptr + bitset_size;
    dragon_bitset_init(preallocated_set_ptr, &heap->preallocated_set, num_segments);

    size_t waiter_size;
    dragon_bcast_size(0,0,NULL,&waiter_size);

    // The waiter_space is an array of bcast objects which are used for waiting for
    // a free block at a specific size (i.e. power of 2).
    heap->waiter_space = preallocated_set_ptr + bitset_size;

    heap->waiters = malloc(sizeof(dragonBCastDescr_t)*heap->num_freelists);

    if (heap->waiters == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for waiter descriptors.");

    // Now initialize all the bcast objects.
    void* bcast_ptr = heap->waiter_space;
    for (k = 0; k < heap->num_freelists; k++) {
        derr = dragon_bcast_create_at(bcast_ptr, waiter_size, 0, 0, NULL, &heap->waiters[k]);
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr, "Could not initialize the Heap Managers waiter objects.");
        bcast_ptr += waiter_size;
    }

    // The free lists are located in the meta-data right after the waiter list.
    heap->free_lists = heap->waiter_space + waiter_size * num_freelists;

    heap->preallocated_free_lists = (void*)heap->free_lists + sizeof(dragonDynHeapSegment_t*) * num_freelists;

    // The segments begin right after the free lists. The free lists are an array of
    // pointers with length num_freelists.
    void* segment_start = (void*)heap->preallocated_free_lists + sizeof(dragonDynHeapSegment_t*) * num_freelists;

    // The following computes the alignment requested by the user. The alignment is all based
    // relative to the start of the allocated space. For instance, if alignment is 4096 the start
    // of the segments will start on a 4K boundary relative to the start of the heap meta data. So
    // if the ptr that is passed starts at address 110, then the segments will start on a
    // 4K multiple+110 bytes.
    size_t diff = segment_start - ptr;
    size_t alignment_units = diff / byte_alignment;
    size_t alignment_remainder = diff % byte_alignment;
    if (alignment_remainder != 0)
        alignment_units += 1;

    *segments_offset_ptr = (alignment_units * byte_alignment);

    heap->segments = (dragonDynHeapSegment_t*) (*(segments_offset_ptr) + ptr);

    // end_ptr points at last byte of heap. One is subtracted just in case this is the last
    // virtual address. Then without subtracting 1 we would have 0 (NULL) as the
    // next available address. So, we point to the last byte, not the next byte after
    // the heap. Not likely to be a problem, but we'll bulletproof it. Since blocks are
    // a minimum of 32 bytes, it points past the beginning of the last byte anyway, and
    // this only used for validating pointers are within the proper range.
    heap->end_ptr = ((void*)heap->segments) + heap->segment_size * heap->num_segments - 1;

    // The following is a sanity check that both the size and init functions are in agreement
    // to the actual size of the heap.
    size_t sz = (void*)heap->end_ptr - (void*)heap->base_pointer + 1;
    size_t size_sz;

    derr = dragon_heap_size(max_block_size_power, min_block_size_power, alignment, lock_kind, &size_sz);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "For some reason unable to check heap size");

    if (sz != size_sz)
        err_return(DRAGON_DYNHEAP_OUT_OF_BOUNDS_INTERNAL_FAILURE, "The created and computed sizes of the heap do not match.");

    /* initialize the free list pointers found in the meta data */
    for (k = 0; k < heap->num_freelists; k++) {
        heap->free_lists[k] = NULL_OFFSET;
        heap->preallocated_free_lists[k] = NULL_OFFSET;
    }

    dragonDynHeapSegment_t* node = heap->segments;
    /* We avoid touching any pages outside the heap state data area
       until malloc is called for the first time. This is due to
       NUMA consderations where paging is controlled by the user
       application. Without these restrictions the following code would
       be executed here. See malloc where this code is executed during the
       first call.
    */
    //set_prev(node,NULL_OFFSET);
    //set_next(node,NULL_OFFSET);
    //set_freelist_index(node,num_freelists-1);

    /* make the free list pointer point to the first node */
    heap->free_lists[heap->num_freelists-1] = heap_offset(heap, node);

    /* set the appropriate bits in the two bit sets */
    dragon_bitset_set(&heap->block_set, 0);
    dragon_bitset_set(&heap->free_set, 0);

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
        int block_size_diff = ((int) max_block_size_power) - min_block_size_power - 1;

        for (k=0; k<block_size_diff; k++)
            count += preallocated[k];

        void** allocations = malloc(sizeof(void*)*count);

        if (allocations == NULL)
            err_return(DRAGON_FAILURE, "Could not allocate internal memory for heap manager init.");

        size_t size = 1UL << max_block_size_power;
        for (k=block_size_diff; k>=0; k--) {
            size = size >> 1;

            for (int n=0; n<preallocated[k]; n++) {
                derr = dragon_heap_malloc(heap, size, &allocations[idx]);
                if (derr != DRAGON_SUCCESS) {
                    char err_str[200];
                    snprintf(err_str, 199, "Could not satisfy preallocated block allocation request of size %lu", size);
                    append_err_return(derr, err_str);
                }

                // We now set the preallocated bit for this block
                size_t offset = allocations[idx] - (void*) heap->segments;
                size_t bit_index = offset / heap->segment_size;
                derr = dragon_bitset_set(&heap->preallocated_set, bit_index);
                if (derr != DRAGON_SUCCESS) {
                    append_err_return(derr, "Could not set bit in preallocated set in heap manager.");
                }
                idx++;
            }
        }

        /* Now we free the static allocations so they are available for allocation. The will not merge because
           of the copy of the bitset above. */
        for (k=0; k<idx; k++) {
            derr = dragon_heap_free(heap, allocations[k]);
            if (derr != DRAGON_SUCCESS)
                append_err_return(derr, "Could not free preallocated allocation in dragon_heap_init");
        }


        free(allocations);
    }

    size_t block_size = 1UL << min_block_size_power;

    for (k=0; k<heap->num_freelists; k++) {
        if (heap->free_lists[k] != NULL_OFFSET) {
            *biggest_block_ptr = block_size;
        }
        block_size = block_size << 1;
    }

    heap->biggest_block = *biggest_block_ptr;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Create a handle for an existing heap.
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

dragonError_t dragon_heap_attach(void* ptr, dragonDynHeap_t* heap) {

    if (ptr == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"A NULL pointer was supplied on the attach.");

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    heap->base_pointer = (uint64_t*) ptr;
    dragonError_t derr = dragon_lock_attach(&(heap->dlock), heap->base_pointer);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be attached to the heap handle.");
    }

    size_t lock_size = dragon_lock_size(heap->dlock.kind);

    uint64_t* num_segments_ptr = (uint64_t*) ((void*)heap->base_pointer + lock_size);
    heap->num_segments = *num_segments_ptr;

    uint64_t* segment_size_ptr = (uint64_t*) ((void*)num_segments_ptr + sizeof(uint64_t));
    heap->segment_size = *segment_size_ptr;

    size_t* num_freelists_ptr = (size_t*) ((void*)segment_size_ptr + sizeof(uint64_t));
    heap->num_freelists = *num_freelists_ptr;

    heap->recovery_needed_ptr = (size_t*) ((void*)num_freelists_ptr + sizeof(size_t));

    heap->num_waiting = (size_t*)((void*)heap->recovery_needed_ptr + sizeof(size_t));

    size_t* biggest_block_ptr = (size_t*)((void*)heap->num_waiting + sizeof(size_t));
    heap->biggest_block = *biggest_block_ptr;

    size_t* segments_offset_ptr = (size_t*)((void*)biggest_block_ptr + sizeof(size_t));
    heap->segments = (*segments_offset_ptr) + ptr;

    // end_ptr points at last byte of heap. One is subtracted just in case this is the last
    // virtual address. Then without subtracting 1 we would have 0 (NULL) as the
    // next available address. So, we point to the last byte, not the next byte after
    // the heap. Not likely to be a problem, but we'll bulletproof it. Since blocks are
    // a minimum of 32 bytes, it points past the beginning of the last byte anyway, and
    // this only used for validating pointers are within the proper range.
    heap->end_ptr = ((void*)heap->segments) + heap->segment_size * heap->num_segments - 1;

    void* block_set_ptr = ((void*)segments_offset_ptr) + sizeof(dragonDynHeapSegment_t*);

    dragon_bitset_attach(block_set_ptr, &heap->block_set);

    size_t bitset_size;
    bitset_size = dragon_bitset_size(heap->num_segments);

    void* free_set_ptr = block_set_ptr + bitset_size;

    dragon_bitset_attach(free_set_ptr, &heap->free_set);

    void* preallocated_set_ptr = free_set_ptr + bitset_size;

    dragon_bitset_attach(preallocated_set_ptr, &heap->preallocated_set);

    size_t waiter_size;
    dragon_bcast_size(0,0,NULL,&waiter_size);

    // The waiter_space is an array of bcast objects which are used for waiting for
    // a free block at a specific size (i.e. power of 2).
    heap->waiter_space = preallocated_set_ptr + bitset_size;

    heap->waiters = malloc(sizeof(dragonBCastDescr_t)*heap->num_freelists);
    if (heap->waiters == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for waiter descriptors.");

    // Now initialize all the bcast objects.
    void* bcast_ptr = heap->waiter_space;
    for (unsigned int k = 0; k < heap->num_freelists; k++) {
        derr = dragon_bcast_attach_at(bcast_ptr, &heap->waiters[k]);
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr, "Could not attach the Heap Managers waiter objects.");
        bcast_ptr += waiter_size;
    }

    // The free lists are located in the meta-data right after the waiter list.
    heap->free_lists = (void*)heap->waiter_space + waiter_size * heap->num_freelists;

    heap->preallocated_free_lists = (void*)heap->free_lists + sizeof(dragonDynHeapSegment_t*) * heap->num_freelists;

    if (heap->segment_size < 32) {
        err_return(DRAGON_DYNHEAP_INVALID_HEAP,"The segment size in the heap is too small.");
    }

    size_t num_bits;

    derr = dragon_bitset_get_num_bits(&heap->block_set, &num_bits);

    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The heap bitset seems to be corrupted.");
    }

    if (heap->num_segments != num_bits) {
        err_return(DRAGON_DYNHEAP_INVALID_HEAP,"The heap number of segments does not align with the size of the heap bitset");
    }

    no_err_return(DRAGON_SUCCESS);
}

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

dragonError_t dragon_heap_destroy(dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL");


    // release any resources that the mutex holds. It
    // must be unlocked to call this.
    dragonError_t derr = dragon_lock_destroy(&(heap->dlock));
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"The lock destroy did not succeed.");

    for (unsigned int k=0; k < heap->num_freelists; k++) {
        derr = dragon_bcast_destroy(&heap->waiters[k]);
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr,"The waiter object destroy did not succeed.");
    }

    free(heap->waiters);

    heap->base_pointer = NULL;
    heap->num_segments = 0;
    heap->segment_size = 0;
    heap->num_freelists = 0;
    heap->biggest_block = 0;
    heap->recovery_needed_ptr = NULL;
    dragon_bitset_destroy(&heap->block_set);
    dragon_bitset_destroy(&heap->free_set);
    heap->free_lists = NULL;
    heap->segments = NULL;
    heap->end_ptr = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Called to detach from an existing heap.
 *
 *  When a handle created through attach is no longer needed, detach should
 *  be called. Detaching can only be done through an attached handle. The handle
 *  is no longer valid after detaching.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonError_t return code.
 */

dragonError_t dragon_heap_detach(dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    dragonError_t derr = dragon_lock_detach(&(heap->dlock));
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"The lock detach did not succeed");

    for (unsigned int k=0; k < heap->num_freelists; k++) {
        derr = dragon_bcast_detach(&heap->waiters[k]);
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr,"The waiter object detach did not succeed.");
    }

    free(heap->waiters);

    heap->base_pointer = NULL;
    heap->num_segments = 0;
    heap->segment_size = 0;
    heap->num_freelists = 0;
    heap->biggest_block = 0;
    heap->recovery_needed_ptr = NULL;
    dragon_bitset_detach(&heap->block_set);
    dragon_bitset_detach(&heap->free_set);
    heap->free_lists = NULL;
    heap->segments = NULL;
    heap->end_ptr = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Dynamically allocate memory.
 *
 *  Allocate a block of at least size bytes, returning the pointer to it in the
 *  reference parameter ptr. Writing or reading data passed the number of bytes
 *  given in size may result in corruption of the heap (see recover).
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param size The number of bytes to allocate.
 *  @param ptr A pointer to a pointer that will be initialized with the address
 *  of the reserved memory.
 *  @return A dragonError_t return code.
 */

dragonError_t dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** ptr) {

    if (heap == NULL) {
        *ptr = NULL;
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was null.");
    }

    if (heap->recovery_needed_ptr == NULL) {
        // The heap has previously been destroyed
        err_return(DRAGON_DYNHEAP_INVALID_HEAP, "The internal heap memory is invalid.");
    }

    if (*heap->recovery_needed_ptr == TRUE) {
        /* If recovery is required, then the user must execute the
           dragon_heap_recover API call. Recovery is required when
           a free block was written into by an errant user process. */
        *ptr = NULL;
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED,"The heap was corrupted and recovery is required.");
    }

    if (ptr == NULL) {
        *ptr = NULL;
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr argument does not point to a valid address. It is NULL.");
    }

    if (size > heap->biggest_block) {
        // Then this request cannot be satisfied, ever given the preallocated blocks
        // in the heap.
        *ptr = NULL;
        char err_str[200];
        snprintf(err_str, 199, "Will never be able to satisfy dragon_heap_malloc request of size %lu. This may be because of static pre-allocations.", size);
        err_return(DRAGON_DYNHEAP_REQUESTED_SIZE_TOO_LARGE, err_str);
    }

    // get power of two for size
    size_t index = _powerof2(size, heap->segment_size);


    // make sure the requested allocation is not bigger than the maximum heap size.
    if (index > heap->num_freelists) {
        *ptr = NULL;
        err_return(DRAGON_DYNHEAP_REQUESTED_SIZE_TOO_LARGE,"A malloc cannot be satistied. It is larger than the heap itself.");
    }

    // get exclusive access to the heap structure.
    dragonError_t derr = dragon_lock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        *ptr = NULL;
        append_err_return(derr,"The lock could not be acquired.");
    }

    if (heap->free_lists[heap->num_freelists-1] != NULL_OFFSET) {
        /* If the largest block size is available as a free block, then
           there is only one block available in the whole heap. In that
           case we initialize the block's prev, next, and size items here.
           We do this here because the block was not initialized during
           the init call due to NUMA considerations. The pages for the
           segments were not touched during initialization due to these
           NUMA considerations and we are safe initializing here. */
           set_prev(heap->segments,NULL_OFFSET);
           set_next(heap->segments,NULL_OFFSET);
           set_freelist_index(heap->segments,heap->num_freelists-1);
    }

    size_t* free_lists;
    unsigned char preallocated = FALSE;

    if (heap->preallocated_free_lists[index] != NULL_OFFSET) {
        free_lists = heap->preallocated_free_lists;
        preallocated = TRUE;
    }
    else  {
        free_lists = heap->free_lists;
        // look in free list for that power of 2
        if (heap->free_lists[index] == NULL_OFFSET) {
            // splitting is required - return NULL if splitting can't be done.
            _split(heap, index);
        }
    }

    if (free_lists[index] == NULL_OFFSET) {
        // We could not satisfy the request
        // release the lock and return NULL
        *ptr = NULL;

        derr = dragon_unlock(&(heap->dlock));
        if (derr != DRAGON_SUCCESS) {
            *ptr = NULL;
            append_err_return(derr,"The lock could not be unlocked");
        }

        *ptr = NULL;
        /* Don't use err_return here. This is in a hot path */
        return DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE;
    }

    // Take it out of the free list and turn off its free bit
    dragonDynHeapSegment_t* node = heap_addr(heap, free_lists[index]);

    if (_is_valid_free_ptr(heap, node) == FALSE) {
        // set the state that recovery is required.
        *ptr = NULL;
        *heap->recovery_needed_ptr = TRUE;

        derr = dragon_unlock(&(heap->dlock));
        if (derr != DRAGON_SUCCESS) {
            *ptr = NULL;
            append_err_return(derr,"The lock could not be unlocked.");
        }

        *ptr = NULL;
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED,"The heap is corrupted. Recovery is needed.");
    }

    dragonError_t ret_code;
    if (preallocated == TRUE)
        ret_code = _remove_from_preallocated_freelist(heap, index, node);
    else
        ret_code = _remove_from_freelist(heap, index, node);

    if (ret_code != DRAGON_SUCCESS) {
        *ptr = NULL;

        derr = dragon_unlock(&(heap->dlock));
        if (derr != DRAGON_SUCCESS) {
            *ptr = NULL;
            append_err_return(derr,"The lock could not be unlocked.");
        }

        *ptr = NULL;
        append_err_return(ret_code,"The allocation could not be removed from its free list.");
    }

    void* seg_ptr = (void*) node;

    if ((void*)node + size - 1 > heap->end_ptr) {
        *ptr = NULL;
        dragon_unlock(&(heap->dlock));
        err_return(DRAGON_DYNHEAP_OUT_OF_BOUNDS_INTERNAL_FAILURE, "There was an internal error in the dragon heap manager.");
    }
    size_t bit_index = ((void*)seg_ptr - (void*)heap->segments)/heap->segment_size;
    dragon_bitset_reset(&heap->free_set, bit_index);

    *ptr = (void*)node;

    /* We wake up any other waiters for this size block. If there were
       other waiters for bigger blocks, they would have already been woken up
       when the bigger block became available, so no need to wake up for bigger
       blocks. */

    _trigger_waiter(heap, index);

    // release the lock.
    derr = dragon_unlock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        *ptr = NULL;
        append_err_return(derr,"The lock could not be unlocked.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Dynamically allocate memory and block until available
 *
 *  Allocate a block of at least size bytes, returning the pointer to it in the
 *  reference parameter ptr. Writing or reading data passed the number of bytes
 *  given in size may result in corruption of the heap (see recover). Wait if
 *  the block is not available.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param size The number of bytes to allocate.
 *  @param ptr A pointer to a pointer that will be initialized with the address
 *  of the reserved memory.
 *  @param timer The timeout value for waiting. If NULL, then it will wait indefinitely.
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_heap_malloc_blocking(dragonDynHeap_t* heap, const size_t size, void** ptr, const timespec_t* timeout)
{
    dragonError_t err;
    dragonError_t alloc_err;

    alloc_err = dragon_heap_malloc(heap, size, ptr);

    /* if the heap's underlying pointer is invalid, nothing more can be done */
    if (alloc_err == DRAGON_DYNHEAP_INVALID_HEAP)
        return alloc_err;

    /* if a zero timeout is supplied, then don't block. This is useful
       for the managed memory code */
    if (timeout!=NULL && timeout->tv_sec == 0 && timeout->tv_nsec == 0)
        /* DON'T change this to an append_err_return, it impacts latency in ch_test.c terribly */
        return alloc_err;

    size_t index = _powerof2(size, heap->segment_size);

    while (alloc_err == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE) {

        err = _wait(heap, &heap->waiters[index], timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or could not wait in blocking malloc of heap manager.");

        alloc_err = dragon_heap_malloc(heap, size, ptr);
    }

    if (alloc_err != DRAGON_SUCCESS)
        append_err_return(alloc_err, "Blocking malloc failed");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Deallocate heap memory returning it to the heap.
 *
 *  Deallocate the memory pointed to by ptr in this heap. Ptr must be an address
 *  returned by the dynmem_malloc call. After freeing a block it is
 *  no longer permissable to read or write data from/into the block and could
 *  lead to corruption of the heap (see recover).
 *
 *  @param heap A pointer to a handle for the heap.
 *  @param ptr A pointer to a block of memory in the heap.
 *  @return A dragonError_t return code.
 */

dragonError_t dragon_heap_free(dragonDynHeap_t* heap, void* ptr) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (*(heap->recovery_needed_ptr) == TRUE) {
        /* If recovery is required, then the user must execute the
           dragon_heap_recover API call. Recovery is required when
           a free block was written into by an errant user process. */
        err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED,"The heap is corrupted. Recovery is required.");
    }

    if (_is_valid_ptr(heap,ptr)==FALSE) {
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr is not a valid heap pointer.");
    }

    if (_is_valid_free_ptr(heap,ptr)==TRUE) {
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr is to an already freed allocation.");
    }

    if (_is_valid_block_ptr(heap, ptr) == FALSE) {
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The ptr is not a valid heap allocation pointer.");
    }

    // get exclusive access to the heap structure.
    dragonError_t derr = dragon_lock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be acquired.");
    }

    // merge blocks and free the block
    if (_free_and_merge(heap, ptr) == FALSE) {
        *heap->recovery_needed_ptr = TRUE;
        /* Mark the block as free and give up for now.
           Return success though subsequent calls will get
           a return code of recovery required. This free worked,
           But before anything else is done, recovery is required. */
        size_t offset = ptr - (void*) heap->segments;
        size_t bit_index = offset / heap->segment_size;
        dragon_bitset_set(&heap->free_set,bit_index);
    }

     // release the lock.
    derr = dragon_unlock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"The lock could not be unlocked.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Recover from corruption of the heap.
 *
 *  If a user-process writes into a freed block or writes past the end of
 *  its allocated space in a block, the heap may become corrupted. In this
 *  case it is possible to recover from this in most circumstances, as long
 *  as meta-data at the very beginning of the heap was not corrupted. Corruption
 *  of the heap is detected in most circumstances. In this case, the heap will
 *  no_err_return(DRAGON_DYNHEAP_RECOVERY_REQUIRED) to most API calls until this
 *  function is called to perform recovery. DRAGON_SUCCESS will
 *  be returned upon successful recovery.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonError_t return code.
 */

dragonError_t dragon_heap_recover(dragonDynHeap_t* heap) {
    // Recovery is necessary when a free list (linked list) got overwritten by a
    // errant user code. A pointer going beyond its allocated block is not
    // caught by this code and if data were written beyond the end of an allocated
    // block it could overwrite a free list node in the free list linked list.

    // The assumption is that the header information meta data about the heap is
    // intact and is NOT corrupted. If the meta data were corrupted then no
    // recovery or detection is possible.

    size_t k;

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    // get exclusive access to the heap structure.
    dragonError_t derr = dragon_lock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be acquired.");
    }

    // This is an optimization for recovery. If the free list with one big
    // block for the whole heap happens to be non-null, then there can be no
    // other free lists. So init the one big block correctly, nullify all other
    // free lists, and return.
    if (heap->free_lists[heap->num_freelists-1] != NULL_OFFSET) {
        for (k=0;k<heap->num_freelists-1;k++)
            heap->free_lists[k] = NULL_OFFSET;

        set_next(heap->segments, NULL_OFFSET);
        set_prev(heap->segments, NULL_OFFSET);
        set_freelist_index(heap->segments,heap->num_freelists-1);
        // Recovery is now done so mark it in the meta-data.
        *heap->recovery_needed_ptr = FALSE;
        _trigger_all_waiters(heap);
        derr = dragon_unlock(&(heap->dlock));
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr,"The lock could not be unlocked.");

        no_err_return(DRAGON_SUCCESS);
    }

    for (k=0;k<heap->num_freelists;k++) {
        heap->free_lists[k] = NULL_OFFSET;
        heap->preallocated_free_lists[k] = NULL_OFFSET;
    }

    k = 0;
    while (k<heap->num_segments) {
        unsigned char free_bit;
        unsigned char preallocated_bit;
        dragon_bitset_get(&heap->free_set,k,&free_bit);
        dragon_bitset_get(&heap->preallocated_set,k,&preallocated_bit);

        if (free_bit == 1) {
            size_t segment_span;
            dragon_bitset_zeroes_to_right(&heap->block_set,k,&segment_span);
            segment_span += 1; // add 1 for the bit==1 at k
            size_t block_size = heap->segment_size * segment_span;
            size_t free_list_index = _powerof2(block_size, heap->segment_size);
            dragonDynHeapSegment_t* node = (dragonDynHeapSegment_t*) ((void*)heap->segments + k*heap->segment_size);
            if (preallocated_bit==FALSE)
                _add_to_freelist(heap, free_list_index, node);
            else
                _add_to_preallocated_freelist(heap, free_list_index, node);

            k = k + segment_span;
        } else {
            k = k + 1;
        }
    }

    for (k=0;k<heap->num_freelists;k++) {
        if (heap->free_lists[k] != NULL_OFFSET)
            _free_and_merge(heap,heap_addr(heap,heap->free_lists[k]));
    }

    // Recovery is now done so mark it in the meta-data.
    *heap->recovery_needed_ptr = FALSE;
    _trigger_all_waiters(heap);

    // release the lock.
    derr = dragon_unlock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be unlocked.");
    }

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Get the base pointer of the heap.
 *
 *  Calling this returns the base pointer of the heap (not including meta-data).
 *  This may be used in making address translations to other address spaces.
 *
 *  @param heap A pointer to a handle for the heap.
 *  @return the base pointer or NULL if there was an error.
 */

void* dragon_heap_base_ptr(dragonDynHeap_t* heap) {

    if (heap == NULL)
        return NULL;

    return (void*)heap->segments;
}


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

dragonError_t dragon_heap_get_stats(dragonDynHeap_t* heap, dragonHeapStats_t* data) {
    // get exclusive access to the heap structure. Needed to insure we can
    // traverse the linked lists below without it changing underneath us.

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (data == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The data pointer was NULL");

    dragonError_t derr = dragon_lock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be acquired.");
    }

    data->num_segments = heap->num_segments;
    data->segment_size = heap->segment_size;
    data->total_size = heap->num_segments * heap->segment_size;

    uint64_t total = 0;
    uint64_t block_size = heap->segment_size;
    for (size_t k=0; k<heap->num_freelists;k++) {
        dragonDynHeapSegment_t* node = heap_addr(heap, heap->free_lists[k]);
        size_t count = 0;
        while (node != NULL) {
            count++;
            node = heap_addr(heap, get_next(node));
        }

        data->free_blocks[k].block_size = block_size;
        data->free_blocks[k].num_blocks = count;

        total += count * block_size;
        block_size = block_size * 2;

    }

    block_size = heap->segment_size;
    for (size_t k=0; k<heap->num_freelists;k++) {
        dragonDynHeapSegment_t* node = heap_addr(heap, heap->preallocated_free_lists[k]);
        size_t count = 0;
        while (node != NULL) {
            count++;
            node = heap_addr(heap, get_next(node));
        }

        data->free_blocks[k].num_blocks = data->free_blocks[k].num_blocks + count;

        total += count * block_size;
        block_size = block_size * 2;
    }

    data->num_block_sizes = heap->num_freelists;
    data->total_free_space = total;
    data->utilization_pct = (1-((double)data->total_free_space) / data->total_size)*100;

    // release the lock.
    derr = dragon_unlock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"The lock could not be unlocked.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief This prints a memory dump of a dynamic memory heap.
 *
 *  The dump is a hexdump of all memory used within the heap including the meta-data
 *  and user-data. It can be used for debugging should that be necessary.
 *
 *  @param title A null-terminated string to use in identifying the dump output.
 *  @param heap A pointer to a handle for the heap.
 *  @return A dragonDynDynHeapErr_t return code.
 */

dragonError_t dragon_heap_dump(const char* title, dragonDynHeap_t* heap) {
    return dragon_heap_dump_to_fd(stdout, title, heap);
}

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

dragonError_t dragon_heap_dump_to_fd(FILE* fd, const char* title, dragonDynHeap_t* heap) {

    if (heap == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The heap handle was NULL.");

    if (title == NULL)
        err_return(DRAGON_DYNHEAP_INVALID_POINTER,"The title was NULL");

    // get exclusive access to the heap structure.
    dragonError_t derr = dragon_lock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be acquired.");
    }

    size_t data_area_size = heap->num_segments*heap->segment_size;
    size_t k;
    fprintf(fd, "*****************************************************************************************\n");
    fprintf(fd, "* DYNAMIC MEMORY HEAP DUMP\n");
    fprintf(fd, "* %s\n",title);
    fprintf(fd, "*****************************************************************************************\n");
    fprintf(fd, "*  Size of Heap: %lu (hex %lx)\n",data_area_size, data_area_size);
    fprintf(fd, "*  Number of Segments: %lu (hex %lx)\n", heap->num_segments,heap->num_segments);
    fprintf(fd, "*  Segment Size: %lu (hex %lx)\n",heap->segment_size, heap->segment_size);
    fprintf(fd, "*  Meta-Data Pointer:  %016llX\n", (unsigned long long int)heap->base_pointer);
    fprintf(fd, "*  Block Set Pointer:  %016llX\n", (unsigned long long int)heap->block_set.data);
    fprintf(fd, "*  Free Set Pointer:   %016llX\n", (unsigned long long int)heap->free_set.data);
    fprintf(fd, "*  Preallocated Set:   %016llX\n", (unsigned long long int)heap->preallocated_set.data);
    fprintf(fd, "*  Waiters Area:       %016llX\n", (unsigned long long int)heap->waiter_space);
    fprintf(fd, "*  Free Lists Pointer: %016llX\n", (unsigned long long int)heap->free_lists);
    fprintf(fd, "*  PreFree Lists Ptr:  %016llX\n", (unsigned long long int)heap->preallocated_free_lists);
    fprintf(fd, "*  Segments Pointer:   %016llX\n", (unsigned long long int)heap->segments);
    fprintf(fd, "*  End of Heap Pointer:%016llX\n", (unsigned long long int)heap->end_ptr);

    if (*heap->recovery_needed_ptr == TRUE) {
        fprintf(fd, "*  Recovery Needed: TRUE\n");
    } else {
        fprintf(fd, "*  Recovery Needed: FALSE\n");
    }
    fprintf(fd, "*  Biggest Possible Block in Heap: %lu\n", heap->biggest_block);

    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    dragon_bitset_dump_to_fd(fd, "Block Set", &heap->block_set,"*  ");
    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    dragon_bitset_dump_to_fd(fd, "Free Set", &heap->free_set,"*  ");
    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    dragon_bitset_dump_to_fd(fd, "Preallocated Set", &heap->preallocated_set,"*  ");
    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    fprintf(fd, "*   Free Lists:\n");
    fprintf(fd, "*  \tindex:\t pointer to 1st node:\tblock size (# segments):\tfree list length:\n");
    size_t seg_count = 1;
    for (k=0;k<heap->num_freelists;k++) {
        dragonDynHeapSegment_t* node = heap_addr(heap, heap->free_lists[k]);
        size_t count = 0;
        unsigned char error = FALSE;
        while (error == FALSE && node != NULL) {
            if (_is_valid_free_ptr(heap, node) == TRUE) {
                count++;
                node = heap_addr(heap, get_next(node));
            } else {
                error = TRUE;
            }
        }

        if (!error)
            fprintf(fd, "*  \t%5lu\t%016llX\t%10lu\t%25lu\n",k,(unsigned long long int)(heap_addr(heap, heap->free_lists[k])),seg_count,count);
        else
            fprintf(fd, "*  \t%5lu\t%016llX\t%10lu\t\t\tcorrupted free list\n",k,(unsigned long long int)(heap_addr(heap, heap->free_lists[k])),seg_count);

        seg_count = seg_count * 2;
    }
    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    fprintf(fd, "*   Preallocated Free Lists:\n");
    fprintf(fd, "*  \tindex:\t pointer to 1st node:\tblock size (# segments):\tfree list length:\n");
    seg_count = 1;
    for (k=0;k<heap->num_freelists;k++) {
        dragonDynHeapSegment_t* node = heap_addr(heap, heap->preallocated_free_lists[k]);
        size_t count = 0;
        unsigned char error = FALSE;
        while (error == FALSE && node != NULL) {
            if (_is_valid_free_ptr(heap, node) == TRUE) {
                count++;
                node = heap_addr(heap, get_next(node));
            } else {
                error = TRUE;
            }
        }

        if (!error)
            fprintf(fd, "*  \t%5lu\t%016llX\t%10lu\t%25lu\n",k,(unsigned long long int)(heap_addr(heap, heap->preallocated_free_lists[k])),seg_count,count);
        else
            fprintf(fd, "*  \t%5lu\t%016llX\t%10lu\t\t\tcorrupted free list\n",k,(unsigned long long int)(heap_addr(heap, heap->preallocated_free_lists[k])),seg_count);

        seg_count = seg_count * 2;
    }

    fprintf(fd, "*  --------------------------------------------------------------------------------------\n");
    fprintf(fd, "*  Free Segment Nodes\n");
    for (k=0;k<heap->num_segments;k++) {
        unsigned char free_bit;
        dragon_bitset_get(&heap->free_set,k,&free_bit);

        if (free_bit == 1) {
            dragonDynHeapSegment_t* node = (dragonDynHeapSegment_t*)(((void*)heap->segments)+heap->segment_size*k);
            size_t segment_span;
            dragon_bitset_zeroes_to_right(&heap->block_set,k,&segment_span);
            segment_span += 1; // add one for the segment itself.
            size_t size = segment_span * heap->segment_size;
            fprintf(fd, "*  -----------\n");
            fprintf(fd, "*  Index: %lu\n",k);
            fprintf(fd, "*  Address: %016llX\n", (unsigned long long int)node);
            fprintf(fd, "*  Size: %lu\n", size);
            fprintf(fd, "*  Number of Segments: %lu\n",segment_span);
            fprintf(fd, "*  Next: %016llX\n",(unsigned long long int)heap_addr(heap, get_next(node)));
            fprintf(fd, "*  Prev: %016llX\n",(unsigned long long int)heap_addr(heap, get_prev(node)));
        }
    }

    fprintf(fd, "*****************************************************************************************\n");

    derr = dragon_unlock(&(heap->dlock));
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr,"The lock could not be unlocked.");
    }

    no_err_return(DRAGON_SUCCESS);
}
