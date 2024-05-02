#include <unordered_map>
#include <dragon/return_codes.h>
#include "umap.h"
#include "err.h"
#include <stdexcept>
#include <string>

using namespace std;

#define LOCK_KIND DRAGON_LOCK_FIFO_LITE

#define __lock_map(dmap) ({\
    dragonError_t derr = _lock_map(dmap);\
    if (derr != DRAGON_SUCCESS)\
        append_err_return(derr,"Cannot lock dmap.");\
})

#define __unlock_map(dmap) ({\
    dragonError_t derr = _unlock_map(dmap);\
    if (derr != DRAGON_SUCCESS)\
        append_err_return(derr,"Cannot unlock dmap");\
})

static dragonError_t
_lock_map(dragonMap_t * dmap)
{
    dragonError_t lerr = dragon_lock(&dmap->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Cannot lock dmap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unlock_map(dragonMap_t * dmap)
{
    dragonError_t lerr = dragon_unlock(&dmap->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Cannot unlock dmap");

    no_err_return(DRAGON_SUCCESS);
}

class dragonMap
{
   public:
    dragonMap(uint64_t seed = 4825UL) {
        lkey = seed;
    }

    ~dragonMap() {
        dMap.clear();
        dMap_multikey.clear();
    }

    void addItem(uint64_t key, const void * data) {
        dMap[key] = data;
    }

    void addItem_multikey(std::string keys, const void * data) {
        dMap_multikey[keys] = data;
    }

    const void * getItem(uint64_t key) {
        try
        {
            return dMap.at(key);
        }
        catch (const out_of_range& oor)
        {
            return NULL;
        }
    }

    const void * getItem_multikey(std::string keys) {
        try
        {
            return dMap_multikey.at(keys);
        }
        catch (const out_of_range& oor)
        {
            return NULL;
        }
    }

    void delItem(uint64_t key) {
        dMap.erase(key);
    }

    void delItem_multikey(std::string keys) {
        dMap_multikey.erase(keys);
    }

    uint64_t new_key() {
        do {
            lkey = hash(lkey);
        } while (lkey == 0);

        return lkey;
    }

   private:
    unordered_map<uint64_t, const void *> dMap;
    unordered_map<std::string, const void *> dMap_multikey;
    uint64_t lkey;

    /* this is hash function based on splitmix64 from
    http://xorshift.di.unimi.it/splitmix64.c */
    uint64_t hash(uint64_t x) {
        uint64_t z = (x += 0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }
};



dragonError_t
dragon_umap_create(dragonMap_t * dmap, const uint64_t seed)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"Bad dmap handle.");

    if (seed == 0)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap seed cannot be 0.");

    dragonMap * cpp_map;
    cpp_map = new dragonMap(seed);
    dmap->_map = (void *)cpp_map;

    size_t lock_size = dragon_lock_size(LOCK_KIND);
    dmap->_lmem = calloc(sizeof(char) * lock_size, 1);
    if (dmap->_lmem == NULL) {
        delete static_cast<dragonMap *>(dmap->_map);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,"dmap malloc failed - out of heap space.");
    }

    dragonError_t lerr = dragon_lock_init(&dmap->_dlock, dmap->_lmem, LOCK_KIND);
    if (lerr != DRAGON_SUCCESS) {
        delete static_cast<dragonMap *>(dmap->_map);
        free(dmap->_lmem);
        append_err_return(lerr,"Unable to initialize dmap lock.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_destroy(dragonMap_t * dmap)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot destroy it.");

    delete static_cast<dragonMap *>(dmap->_map);

    dragonError_t lerr = dragon_lock_destroy(&dmap->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Unable to destroy dmap lock.");
    free(dmap->_lmem);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_additem(dragonMap_t * dmap, const uint64_t key, const void * data)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot add item.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    cpp_map->addItem(key, data);
    __unlock_map(dmap);

    no_err_return(DRAGON_SUCCESS);
}

/* TODO: Pass in an array of keys? */
dragonError_t
dragon_umap_additem_multikey(dragonMap_t * dmap, const uint64_t key0, const uint64_t key1, const void * data)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot add item.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    cpp_map->addItem_multikey(std::to_string(key0) + std::to_string(key1), data);
    __unlock_map(dmap);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_additem_genkey(dragonMap_t * dmap, const void * data, uint64_t * new_key)
{
    if (dmap == NULL || data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle or data is NULL. Cannot additem and genkey.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    *new_key = cpp_map->new_key();
    cpp_map->addItem(*new_key, data);
    __unlock_map(dmap);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_getitem(dragonMap_t * dmap, const uint64_t key, void ** data)
{
    if (dmap == NULL || data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot get an item from it.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    *data = (void*)cpp_map->getItem(key);
    __unlock_map(dmap);

    if (*data == NULL) {
        err_return(DRAGON_MAP_KEY_NOT_FOUND,"The dmap item is not found.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_getitem_multikey(dragonMap_t * dmap, const uint64_t key0, const uint64_t key1, void ** data)
{
    if (dmap == NULL || data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot get an item from it.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    *data = (void*)cpp_map->getItem_multikey(std::to_string(key0) + std::to_string(key1));
    __unlock_map(dmap);

    if (*data == NULL) {
        err_return(DRAGON_MAP_KEY_NOT_FOUND,"The dmap item is not found.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_delitem(dragonMap_t * dmap, const uint64_t key)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot delete the key/value pair.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    cpp_map->delItem(key);
    __unlock_map(dmap);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_umap_delitem_multikey(dragonMap_t * dmap, const uint64_t key0, const uint64_t key1)
{
    if (dmap == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dmap handle is NULL. Cannot delete the key/value pair.");

    dragonMap * cpp_map;
    cpp_map = static_cast<dragonMap *>(dmap->_map);

    __lock_map(dmap);
    cpp_map->delItem_multikey(std::to_string(key0) + std::to_string(key1));
    __unlock_map(dmap);

    no_err_return(DRAGON_SUCCESS);
}
