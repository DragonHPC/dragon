#ifndef HAVE_DRAGON_PRIORITY_HEAP_H
#define HAVE_DRAGON_PRIORITY_HEAP_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <dragon/return_codes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* typedefs for the interface */
typedef uint64_t dragonPriorityHeapLongUint_t;
typedef uint32_t dragonPriorityHeapUint_t;

typedef struct dragonPriorityHeap_st {
    dragonPriorityHeapUint_t * nvals_per_key;
    dragonPriorityHeapUint_t * base;
    dragonPriorityHeapLongUint_t * capacity;
    dragonPriorityHeapLongUint_t * cur_len;
    dragonPriorityHeapLongUint_t * cur_last_pri;
    dragonPriorityHeapLongUint_t * _harr;
} dragonPriorityHeap_t;

dragonError_t
dragon_priority_heap_init(dragonPriorityHeap_t * heap, dragonPriorityHeapUint_t base,
                          dragonPriorityHeapLongUint_t capacity, dragonPriorityHeapUint_t nvals_per_key, void * ptr);

dragonError_t
dragon_priority_heap_attach(dragonPriorityHeap_t * heap, void * ptr);

dragonError_t
dragon_priority_heap_detach(dragonPriorityHeap_t * heap);

dragonError_t
dragon_priority_heap_destroy(dragonPriorityHeap_t * heap);

dragonError_t
dragon_priority_heap_insert_item(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals);

dragonError_t
dragon_priority_heap_insert_urgent_item(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals);

dragonError_t
dragon_priority_heap_peek_highest_priority(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals,
                                           dragonPriorityHeapLongUint_t * priority);

dragonError_t
dragon_priority_heap_pop_highest_priority(dragonPriorityHeap_t * heap);

dragonError_t
dragon_priority_heap_extract_highest_priority(dragonPriorityHeap_t * heap, dragonPriorityHeapLongUint_t * vals,
                                dragonPriorityHeapLongUint_t * priority);

size_t
dragon_priority_heap_size(dragonPriorityHeapLongUint_t capacity, dragonPriorityHeapUint_t nvals_per_key);

#ifdef __cplusplus
}
#endif

#endif
