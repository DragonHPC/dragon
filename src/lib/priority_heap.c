#include "priority_heap.h"
#include "err.h"
#include <stdlib.h>

static void
_heap_swap(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t i, dragonPriorityHeapLongUint_t j)
{
    dragonPriorityHeapLongUint_t stepsize = 1UL + *(heap->nvals_per_key);
    dragonPriorityHeapLongUint_t saveitem;

    for (dragonPriorityHeapLongUint_t k = 0; k < stepsize; k++) {
        saveitem = heap->_harr[stepsize*i + k];
        heap->_harr[stepsize*i + k] = heap->_harr[stepsize*j + k];
        heap->_harr[stepsize*j + k] = saveitem;
    }
}

static void
_heap_maxify(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t i)
{
    dragonPriorityHeapLongUint_t stepsize = 1UL + *(heap->nvals_per_key);
    int keep_looping = 1;
    while (keep_looping) {

        dragonPriorityHeapLongUint_t highest, base_child;
        base_child = *(heap->base) * i + 1;
        highest = i;

        for (dragonPriorityHeapLongUint_t cur_child = base_child; cur_child < base_child+*(heap->base); cur_child++) {
            if (cur_child < *(heap->cur_len)) {
                if (heap->_harr[stepsize*cur_child] < heap->_harr[stepsize*highest])
                    highest = cur_child;
            }
        }

        if (i != highest) {
            _heap_swap(heap, i, highest);
            i = highest;
        } else {
            keep_looping = 0;
        }
    }
}

static dragonError_t
_raise_priority(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t i, dragonPriorityHeapLongUint_t priority)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    dragonPriorityHeapLongUint_t stepsize = 1UL + *(heap->nvals_per_key);
    //if (heap->_harr[stepsize*i] > priority)
    //    err_return(DRAGON_PRIORITY_HEAP_INVALID_PRIORITY,"The priority was too large");

    heap->_harr[stepsize*i] = priority;
    dragonPriorityHeapLongUint_t parent, child;

    child  = i;
    parent = (child - 1) / *(heap->base);

    while (child > 0 && heap->_harr[stepsize*parent] > heap->_harr[stepsize*child]) {
        _heap_swap(heap, child, parent);
        child  = parent;
        parent = (child - 1) / *(heap->base);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_insert_item(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals,
                    dragonPriorityHeapLongUint_t priority)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    dragonPriorityHeapLongUint_t stepsize = 1UL + *(heap->nvals_per_key);
    for (dragonPriorityHeapLongUint_t i = 1; i < stepsize; i++) {
        heap->_harr[stepsize * (*(heap->cur_len)) + i] = vals[i-1];
    }

    dragonError_t derr;
    derr = _raise_priority(heap, *(heap->cur_len), priority);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"Could not insert item into priority heap.");

    *(heap->cur_len) += 1UL;

    no_err_return(DRAGON_SUCCESS);
}

/******************************
    BEGIN USER API
*******************************/

/** @brief construct a priority heap from the given memory
 *
 *  This will construct a priority heap from the memory provided.  The
 *  user specifies the overall capacity of the heap and values to carry with
 *  a queued item.  The base determines the fanout, and larger values may be
 *  desirable for deep heaps.
 *
 *  @param base A dragonHeapUint_t value of the tree fanout.
 *  @param capacity A dragonHeapUint_t value for depth of the heap.
 *  @param nvals_per_key A dragonHeapUint_t value for the number of values per queued item.
 *  @param ptr A pointer to dragonHeapLongUint_t array of memory for the heap.
 *  @param heap A pointer to a dragonHeap_t struct to construct the heap on.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_init(dragonPriorityHeap_t * heap, dragonPriorityHeapUint_t base,
                          dragonPriorityHeapLongUint_t capacity, dragonPriorityHeapUint_t nvals_per_key, void * ptr)
{
    dragonError_t derr;
    derr = dragon_priority_heap_attach(heap, ptr);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"Could not attach to priority heap.");

    if (nvals_per_key == 0)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The number of values per key was 0. Has to be bigger than that.");

    if (base == 0)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_BASE,"The base of the priority heap must be greater than 0.");

    *(heap->nvals_per_key) = nvals_per_key;
    *(heap->capacity)      = capacity;
    *(heap->base)          = base;
    *(heap->cur_len)       = 0UL;
    *(heap->cur_last_pri)  = 0UL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_priority_heap_attach(dragonPriorityHeap_t * heap, void * ptr)
{
    if (ptr == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The pointer to the space for the priority heap was NULL.");

    heap->nvals_per_key = ptr;
    ptr += sizeof(dragonPriorityHeapUint_t);
    heap->base          = ptr;
    ptr += sizeof(dragonPriorityHeapUint_t);
    heap->capacity      = ptr;
    ptr += sizeof(dragonPriorityHeapLongUint_t);
    heap->cur_len       = ptr;
    ptr += sizeof(dragonPriorityHeapLongUint_t);
    heap->cur_last_pri  = ptr;
    ptr += sizeof(dragonPriorityHeapLongUint_t);
    heap->_harr         = ptr;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_priority_heap_detach(dragonPriorityHeap_t * heap)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"Could not detach from priority heap. The handle was NULL.");

    heap->nvals_per_key = NULL;
    heap->base          = NULL;
    heap->capacity      = NULL;
    heap->cur_len       = NULL;
    heap->cur_last_pri  = NULL;
    heap->_harr         = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief destroys the given heap
 *
 *  This will cleanup the structure removing mapping from the memory it was
 *  attached to.
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 */
dragonError_t
dragon_priority_heap_destroy(dragonPriorityHeap_t * heap)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"Could not destroy the priority heap. The handle was NULL.");

    dragonError_t derr = dragon_priority_heap_detach(heap);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "In destroy, the call to detach did not succeed.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief insert a new item into the queue with the lowest priority
 *
 *  This will insert the new item into the queue with the lowest priority.
 *  The user provides an array of values to carry along with the item.
 *  A
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 *  @param vals A pointer to a dragonHeapLongUint_t array of values for the item.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_insert_item(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    if (vals == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap key and value pointer/array was NULL.");

    if (*(heap->cur_len) == *(heap->capacity))
        err_return(DRAGON_PRIORITY_HEAP_FULL,"The heap is full.");

    dragonError_t derr;
    dragonPriorityHeapLongUint_t priority = *(heap->cur_last_pri) + 1UL;

    derr = _insert_item(heap, vals, priority);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"Unable to insert item into the heap.");

    *(heap->cur_last_pri) += 1UL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief insert a new item into the queue with the highest priority
 *
 *  This will insert the new item into the queue with the highest priority.
 *  The user provides an array of values to carry along with the item.
 *  A
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 *  @param vals A pointer to a dragonHeapLongUint_t array of values for the item.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_insert_urgent_item(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    if (vals == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap key and value pointer/array was NULL.");

    if (*(heap->cur_len) == *(heap->capacity))
        err_return(DRAGON_PRIORITY_HEAP_FULL,"The heap is full.");

    dragonError_t derr;
    dragonPriorityHeapLongUint_t priority = 0UL;

    derr = _insert_item(heap, vals, priority);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr,"Unable to insert item into the heap.");

    no_err_return(DRAGON_SUCCESS);
}


/** @brief peek the highest priority item from the heap
 *
 *  This will get values from the highest priority item from the queue and return them.
 *  The heap item will not be removed and remain in-place.
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 *  @param vals A pointer to a dragonHeapLongUint_t array of values for the item.
 *  @param priority A dragonHeapLongUint_t value for the priority.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_peek_highest_priority(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals,
                                           dragonPriorityHeapLongUint_t * priority)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    if (*(heap->cur_len) == 0)
        err_return(DRAGON_PRIORITY_HEAP_EMPTY,"The heap is empty so peek is not possible.");

    *priority = heap->_harr[0];
    for (dragonPriorityHeapLongUint_t i = 1; i <= *(heap->nvals_per_key); i++) {
        vals[i-1] = heap->_harr[i];
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief pop the highest priority item from the heap
 *
 *  This will pop the current highest priority item off the heap without returning values.
 *  Intended to be used in combination with a Peek operation for finer logic control flow.
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_pop_highest_priority(dragonPriorityHeap_t * heap)
{
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The heap handle pointer was NULL.");

    if (*(heap->cur_len) == 0)
        err_return(DRAGON_PRIORITY_HEAP_EMPTY,"The heap is empty so popping is not possible.");

    *(heap->cur_len) -= 1UL;
    if (*(heap->cur_len) > 0UL) {
        _heap_swap(heap, 0, *(heap->cur_len));
        _heap_maxify(heap, 0);
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief extract the highest priority item from the heap
 *
 *  This will remove the highest priority item from the queue and return it.
 *  The values for the item and the priority it had are both returned.
 *
 *  @param heap A pointer to a dragonHeap_t struct to operate on.
 *  @param vals A pointer to a dragonHeapLongUint_t array of values for the item.
 *  @param priority A dragonHeapLongUint_t value for the priority.
 *  @return dragonHeapErr_t error code or success.
 */
dragonError_t
dragon_priority_heap_extract_highest_priority(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals,
                                              dragonPriorityHeapLongUint_t * priority)
{
    dragonError_t perr;
    perr = dragon_priority_heap_peek_highest_priority(heap, vals, priority);
    if (perr != DRAGON_SUCCESS)
        append_err_return(perr, "The heap could not be popped for some reason.");

    dragon_priority_heap_pop_highest_priority(heap);

    no_err_return(DRAGON_SUCCESS);

    /*
    if (heap == NULL)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"");

    if (*(heap->cur_len) == 0)
        err_return(DRAGON_PRIORITY_HEAP_EMPTY,"");

    *priority = heap->_harr[0];
    for (dragonPriorityHeapLongUint_t i = 1; i <= *(heap->nvals_per_key); i++) {
        vals[i-1] = heap->_harr[i];
    }

    *(heap->cur_len) -= 1UL;
    if (*(heap->cur_len) > 0UL) {
        _heap_swap(heap, 0, *(heap->cur_len));
        _heap_maxify(heap, 0);
    }

    no_err_return(DRAGON_SUCCESS);
    */
}

/** @brief query the storage size required for a priority heap
 *
 *  This will return the number of bytes a chunk of memory will need to be to
 *  store a priority heap with the given parameters.  Users can use this to
 *  allocate the right amount of memory for a heap.
 *
 *  @param capacity A dragonHeapUint_t value for depth of the heap.
 *  @param nvals_per_key A dragonHeapUint_t value for the number of values per queued item.
 *  @return size_t Number of bytes.
 */
size_t
dragon_priority_heap_size(dragonPriorityHeapLongUint_t capacity, dragonPriorityHeapUint_t nvals_per_key)
{
    if (nvals_per_key == 0)
        err_return(DRAGON_PRIORITY_HEAP_INVALID_ARGUMENT,"The nvals_per_key was 0 and this is not allowed.");

    /* a heap needs space for the key and then however many values per key
        times the capacity.  We need two extra spaces for length and current lowest priority.
        One more space for capacity, those three as long int.  The last two are just int
        for base and vals_per_key */
    size_t req_size = sizeof(dragonPriorityHeapLongUint_t) * (capacity * (1UL + nvals_per_key) + 3UL);
    req_size += sizeof(dragonPriorityHeapUint_t) * 2UL;

    return req_size;
}
