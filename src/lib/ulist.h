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
dragon_ulist_create(dragonList_t **dlist_in);

dragonError_t
dragon_ulist_destroy(dragonList_t **dlist_in);

dragonError_t
dragon_ulist_lock(dragonList_t **dlist_in);

dragonError_t
dragon_ulist_unlock(dragonList_t **dlist_in);

dragonError_t
dragon_ulist_additem(dragonList_t **dlist_in, const void *item, bool locking);

dragonError_t
dragon_ulist_delitem(dragonList_t **dlist_in, const void *item, bool locking);

dragonError_t
dragon_ulist_get_current_advance(dragonList_t **dlist_in, void **item, bool locking);

dragonError_t
dragon_ulist_get_by_idx(dragonList_t **dlist_in, int idx, void **item, bool locking);

bool
dragon_ulist_contains(dragonList_t **dlist_in, const void *item, bool locking);

size_t
dragon_ulist_get_size(dragonList_t **dlist_in, bool locking);

#ifdef __cplusplus
}
#endif

#endif
