#ifndef HAVE_DRAGON_MAP_H
#define HAVE_DRAGON_MAP_H

#include <stdint.h>
#include <dragon/return_codes.h>
#include "shared_lock.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dragonMap_st {
    dragonLock_t _dlock;
    void * _lmem;
    void * _map;
} dragonMap_t;

dragonError_t
dragon_umap_create(dragonMap_t **dmap_in, uint64_t seed);

dragonError_t
dragon_umap_destroy(dragonMap_t **dmap_in);

dragonError_t
dragon_umap_additem(dragonMap_t **dmap_in, const uint64_t key, const void *data);

dragonError_t
dragon_umap_additem_multikey(dragonMap_t **dmap_in, const uint64_t key0, const uint64_t key1, const void *data);

dragonError_t
dragon_umap_additem_genkey(dragonMap_t **dmap_in, const void *data, uint64_t *new_key);

dragonError_t
dragon_umap_getitem(dragonMap_t **dmap_in, const uint64_t key, void **data);

dragonError_t
dragon_umap_getitem_multikey(dragonMap_t **dmap_in, const uint64_t key0, const uint64_t key1, void **data);

dragonError_t
dragon_umap_delitem(dragonMap_t **dmap_in, const uint64_t key);

dragonError_t
dragon_umap_delitem_multikey(dragonMap_t **dmap_in, const uint64_t key0, const uint64_t key1);

#ifdef __cplusplus
}
#endif

#endif
