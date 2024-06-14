#ifndef HAVE_DRAGON_HASHTABLE_H
#define HAVE_DRAGON_HASHTABLE_H

#define TRUE 1
#define FALSE 0

#include <dragon/return_codes.h>
#include "_bitset.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/* -----------------------------------------------------------------------------
 * The Hashtable datatype creates a Hashtable. This is a low-level implementation
 * that requires the user provide the space for a fixed size hashtable. The
 * dragon_hashtable_size function returns the required size for a hashtable of
 * max_entries. The init must be called on the space prior to using. The other
 * methods work as expected.
 *
 * In the interest of performance, entries in the hash table are not copied when
 * getting them from the data structure. Instead, a pointer is returned (via a
 * reference parameter) to the actual entry in the hashtable.
 *
 * NOTE: This implementation does NO dynamic memory allocation. The
 * max_entries is enforced as the maximum capacity of the hashtable.
 *
 * NOTE: This implemenation is NOT threadsafe on its own. The user must do
 * appropriate locking for thread safety.
 * ----------------------------------------------------------------------------- */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dragonHashtableHeader_st {
    uint64_t num_slots;
    uint64_t* num_kvs;
    uint64_t* num_placeholders;
    uint64_t key_len;
    uint64_t value_len;
    uint64_t* armor1;
    uint64_t* armor2;
    uint64_t* armor3;
} dragonHashtableHeader_t;

#define DRAGON_HASHTABLE_HEADER_NULINTS sizeof(dragonHashtableHeader_t)/sizeof(uint64_t)

/* This is the Hashtable handle */
typedef struct dragonHashtable_st {
    dragonHashtableHeader_t header;
    dragonBitSet_t allocated;
    dragonBitSet_t placeholder;
    char* slots;
} dragonHashtable_t;

#define DRAGON_HASHTABLE_BITSET_COUNT 2

typedef struct dragonHashtableIterator_st {
    uint64_t index;
} dragonHashtableIterator_t;

typedef struct dragonHashtableStats_st {
    double load_factor;
    uint64_t capacity;
    uint64_t num_items;
    uint64_t key_len;
    uint64_t value_len;
    double avg_chain_length;
    uint64_t max_chain_length;
} dragonHashtableStats_t;

/* Hashtable functions */
dragonError_t
dragon_hashtable_size(const uint64_t max_entries, const uint64_t key_len, const uint64_t value_len, uint64_t* size);

dragonError_t
dragon_hashtable_init(char* ptr, dragonHashtable_t* ht, const uint64_t max_entries, const uint64_t key_len, const uint64_t value_len);

dragonError_t
dragon_hashtable_destroy(dragonHashtable_t* ht);
dragonError_t dragon_hashtable_attach(char* ptr, dragonHashtable_t* ht);

dragonError_t
dragon_hashtable_detach(dragonHashtable_t* ht);

dragonError_t
dragon_hashtable_add(dragonHashtable_t* ht, const char* key, const char* value);
dragonError_t dragon_hashtable_replace(dragonHashtable_t* ht, const char* key, const char* value); // @KDL - adds if it is not in the table. Make sure in docs eventually.

dragonError_t
dragon_hashtable_remove(dragonHashtable_t* ht, const char* key);

dragonError_t
dragon_hashtable_get(const dragonHashtable_t* ht, const char* key, char* value);

dragonError_t
dragon_hashtable_iterator_init(const dragonHashtable_t* ht, dragonHashtableIterator_t* iter);

dragonError_t
dragon_hashtable_iterator_next(const dragonHashtable_t* ht, dragonHashtableIterator_t* iter, char* key, char* value);

dragonError_t
dragon_hashtable_stats(const dragonHashtable_t* ht, dragonHashtableStats_t* stats);

dragonError_t
dragon_hashtable_dump(const char* title, const dragonHashtable_t* ht, const char* indent);

dragonError_t
dragon_hashtable_dump_to_fd(FILE* fd, const char* title, const dragonHashtable_t* ht, const char* indent);

#ifdef __cplusplus
}
#endif

#endif
