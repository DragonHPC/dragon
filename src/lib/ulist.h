#ifndef HAVE_DRAGON_LIST_H
#define HAVE_DRAGON_LIST_H

#include <stdint.h>
#include <dragon/return_codes.h>
#include "shared_lock.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dragonList_st {
    dragonLock_t _dlock;
    void * _lmem;
    void * _list;
} dragonList_t;

dragonError_t
dragon_ulist_create(dragonList_t * dlist);

dragonError_t
dragon_ulist_destroy(dragonList_t * dlist);

dragonError_t
dragon_ulist_additem(dragonList_t * dlist, const void * item);

dragonError_t
dragon_ulist_delitem(dragonList_t * dlist, const void * item);

dragonError_t
dragon_ulist_get_current_advance(dragonList_t * dlist, void ** item);

dragonError_t
dragon_ulist_get_by_idx(dragonList_t * dlist, int idx, void ** item);

bool
dragon_ulist_contains(dragonList_t * dlist, const void * item);

size_t
dragon_ulist_get_size(dragonList_t * dlist);

#ifdef __cplusplus
}
#endif

#endif
