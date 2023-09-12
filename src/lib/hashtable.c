#include "_hashtable.h"
#include "_hexdump.h"
#include "err.h"
#include <stddef.h>
#include <math.h>
#include <stdbool.h>
#include <string.h>

#define armorval 0xff01ff02ff03ff04

/* This implementation of a static hashtable consists of an array of key/value entries.
   There is some metadata for the hash table stored at the beginning of the data as
   follows:
        uint64_t num_slots is the number of slots defining the number of slots in the array
            for the hashtable. This is configurable, but is 2*max_entries. The max_entries
            is provided when the hashtable is created.
        uint64_t count is the number of items currently stored in the hashtable.
        uint64_t key_len is the number of 64-bit words for the keys of the hashtable.
        uint64_t value_len is the number of 64-bit words for the values.
        BitSet allocated is a bitset of num_slots indicating that the slot is occupied.
        BitSet placeholder is a bitset of num_slots indicating that an entry is
            part of a chain. Placeholders entries are set when items are deleted from
            a hashtable. They make sure the chain remains unbroken.
        char[] The slots array for the hashtable follows. This is 8 byte boundary aligned
            assuming the memory on which it is mapped is 8 byte boundary aligned.

    A user initializes a handle when a hashtable is created. The user may also attach to the
    hashtable. In either case, the init and attach both initialize a handle for the hashtable.
    The handle for this statically allocated hashtable consists of the fields found in the
    hashtable.h include file and is called dragonHashtable_t.

    NOTE: This hashtable implementation is NOT thread safe. If called from multiple threads, then
    the user must implementat appropriate locking external to this hashtable implementation to
    insure correctness.
*/

//#define HT_DEBUG

const uint64_t max_load_factor = 50; /* 50% load factor */
const uint64_t max_uint64 = 0xffffffffffffffff;
const uint64_t thirty_two_gb = 34359720776;

#define _check_armor(ht) ({\
    if (*(ht->header.armor1) != armorval) {\
        err_return(DRAGON_FAILURE,"Bad write into top of hashtable. Hashtable was corrupted!!!!");\
    }\
    if (*(ht->header.armor2) != armorval) {\
        err_return(DRAGON_FAILURE,"Bad write past end of hashtable. Hashtable was corrupted!!!!");\
    }\
    if (*(ht->header.armor3) != armorval) {\
        err_return(DRAGON_FAILURE,"Bad write past slots and into bitset of hashtable. Hashtable was corrupted!!!!");\
    }\
})

static uint64_t
_hash(const char *str, const uint64_t key_len)
{
    // key_len represents number of 8 byte values

    uint64_t hash = 1610612741;
    int k;
    const uint64_t* key = (uint64_t*)str; // it points to an array of 8 byte values

    for (k=0; k<key_len; k++) {
        hash = (hash << 5) ^ key[k]; /* hash * 33 ^ c, repeated for each char */
    }

    for (k=key_len-1; k>=0; k--) {
        hash = (hash << 5) ^ key[k]; /* hash * 33 ^ c, repeated for each char */
    }

    for (k=0; k<key_len; k++) {
        hash = (hash << 5) ^ key[k]; /* hash * 33 ^ c, repeated for each char */
    }

    return hash;
}

static bool
_keys_equal(const char* key1_ptr, const char* key2_ptr, const uint64_t key_len)
{
    const uint64_t* key1 = (uint64_t*)key1_ptr; // keys are a multiple of 8 bytes
    const uint64_t* key2 = (uint64_t*)key2_ptr;
    uint64_t idx;

    for (idx=0; idx<key_len; idx++) {
        if (key1[idx] != key2[idx])
            return false;
    }

    return true;
}

static dragonError_t
_copy_in(const dragonHashtable_t* ht, char* dest, const char* source, const uint64_t len)
{
    // len is in words (number of 8 byte values)
    uint64_t* d = (uint64_t*) dest;
    const uint64_t* s = (uint64_t*) source;
    uint64_t idx;

    if ((void*)dest < (void*)ht->slots)
        err_return(DRAGON_INVALID_ARGUMENT, "The destination was outside the bounds of the hashtable slots.");

    if ((void*)dest > (void*)ht->header.armor3)
        err_return(DRAGON_INVALID_ARGUMENT, "The destination was outside the bounds of the hashtable slots.");

    for (idx=0;idx<len;idx++)
        d[idx] = s[idx];

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_copy_out(const dragonHashtable_t* ht, char* dest, const char* source, const uint64_t len)
{
    // len is in words (number of 8 byte values)
    uint64_t* d = (uint64_t*) dest;
    const uint64_t* s = (uint64_t*) source;
    uint64_t idx;

    if ((void*)source < (void*)ht->slots)
        err_return(DRAGON_INVALID_ARGUMENT, "The source was outside the bounds of the hashtable slots.");

    if ((void*)source > (void*)ht->header.armor3)
        err_return(DRAGON_INVALID_ARGUMENT, "The source was outside the bounds of the hashtable slots.");

    for (idx=0;idx<len;idx++)
        d[idx] = s[idx];

    no_err_return(DRAGON_SUCCESS);
}

static void
_strcat_key(char* destination, dragonHashtable_t* ht, const char* key) {
    char key_str[80];

    uint64_t* arr = (uint64_t*) key;

    uint64_t idx;

    for (idx = 0; idx < ht->header.key_len; idx++) {
        sprintf(key_str, "%lu ", arr[idx]);
        strcat(destination, key_str);
    }
}

#ifdef HT_DEBUG
static void
_print_key(dragonHashtable_t* ht, char* key) {
    char key_str[80];
    strcpy(key_str, "");
    _strcat_key(key_str, ht, key);
    printf(key_str);
}

static void
_print_chain(dragonHashtable_t* ht, uint64_t idx) {
    uint64_t entry_len = (ht->header.key_len + ht->header.value_len)*sizeof(uint64_t);
    char* key_ptr = ht->slots + entry_len * idx;

    uint64_t start_idx = idx;
    bool searching = true;
    unsigned char allocated;
    unsigned char placeholder;
    dragonError_t rc;
    int count = 0;

    while (searching) {
        rc = dragon_bitset_get(&ht->allocated, idx, &allocated);

        if (rc != DRAGON_SUCCESS) {
            printf("Error on bitset get\n");
        }

        rc = dragon_bitset_get(&ht->placeholder, idx, &placeholder);

        if (rc != DRAGON_SUCCESS) {
            printf("Error on bitset get\n");
        }

        if (allocated == 0 && placeholder == 0)
            searching = false;
        else {
            count += 1;
            _print_key(ht, key_ptr);
            if (allocated)
                printf("A");
            else
                printf("_");

            if (placeholder)
                printf("P\n");
            else
                printf("_\n");
        }

        idx += 1;

        if (idx == ht->header.num_slots)
            idx = 0;

        key_ptr = ht->slots + entry_len * idx;
    }

    printf("Total chain length from %lu is %d\n", start_idx, count);
}
#endif

static dragonError_t
_hashtable_add(dragonHashtable_t* ht, const char* key, const char* value, bool replace)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (key == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The key is NULL.");

    if (value == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The value is NULL.");

    if (!replace && (*ht->header.count_ptr >= ht->header.num_slots/2))
        err_return(DRAGON_HASHTABLE_FULL, "Hashtable is full.");

    _check_armor(ht);

    uint64_t idx = _hash(key, ht->header.key_len) % ht->header.num_slots;

    uint64_t entry_len = (ht->header.key_len + ht->header.value_len)*sizeof(uint64_t);
    uint64_t loc = max_uint64;
    char* key_ptr = NULL;
    char* value_ptr = NULL;
    bool searching = true;
    dragonError_t bit_rc;
    dragonError_t rc;
    key_ptr = ht->slots + entry_len * idx;

    while (searching) {
        unsigned char allocated;
        unsigned char placeholder;

        bit_rc = dragon_bitset_get(&ht->allocated, idx, &allocated);

        if (bit_rc != DRAGON_SUCCESS) {
            append_err_return(bit_rc, "Could not add entry into hashtable.");
        }

        if (!allocated) {
            searching = false;
        } else {
            bit_rc = dragon_bitset_get(&ht->placeholder, idx, &placeholder);

            if (bit_rc != DRAGON_SUCCESS) {
                append_err_return(bit_rc, "Could not add entry into hashtable.");
            }

            if ((placeholder == 1) && (loc == max_uint64)) {
                loc = idx;
            } else {
                // It is not a placeholder and it is allocated so see if keys are equal
                if (_keys_equal(key_ptr, key, ht->header.key_len)) {
                    // already in the hashtable
                    if (replace) {
                        // replace it
                        value_ptr = key_ptr + ht->header.key_len * sizeof(uint64_t);
                        rc = _copy_in(ht, value_ptr, value, ht->header.value_len);
                        if (rc != DRAGON_SUCCESS)
                            append_err_return(rc, "There was an error on copy.");
                        no_err_return(DRAGON_SUCCESS);
                    }

                    // otherwise, replace was false so return an error
                    char err_str[200];
                    char key_str[80];
                    strcpy(key_str,"");
                    _strcat_key(key_str, ht, key);
                    snprintf(err_str, 200, "Duplicate key detected for key %s", key_str);
                    err_return(DRAGON_HASHTABLE_DUPLICATE_KEY, err_str);
                }
            }
        }

        if (searching) {
            // advance idx mod length of the slots array avoiding multiplication in the loop.
            idx = idx + 1;
            if (idx == ht->header.num_slots) {
                idx = 0;
                key_ptr = ht->slots;
            } else {
                key_ptr = key_ptr + entry_len;
            }
        }
    }

    if (loc == max_uint64) {
        loc = idx;
    }

    /* if replacing and it was not found, then allow it to be added as
       long as there is room */
    if (*ht->header.count_ptr >= ht->header.num_slots/2)
        err_return(DRAGON_HASHTABLE_FULL, "Hashtable is full.");

    key_ptr = ht->slots + entry_len * loc;
    value_ptr = key_ptr + ht->header.key_len * sizeof(uint64_t);

    rc = _copy_in(ht, key_ptr, key, ht->header.key_len);
    if (rc != DRAGON_SUCCESS)
        append_err_return(rc, "There was an error on copy.");

    rc = _copy_in(ht, value_ptr, value, ht->header.value_len);
    if (rc != DRAGON_SUCCESS)
        append_err_return(rc, "There was an error on copy.");

    /* mark the location allocated */
    bit_rc = dragon_bitset_set(&ht->allocated, loc);

    if (bit_rc != DRAGON_SUCCESS) {
        append_err_return(bit_rc, "Could not add entry into hashtable.");
    }

    /* location may have been marked a placeholder. If so reset */
    bit_rc = dragon_bitset_reset(&ht->placeholder, loc);

    if (bit_rc != DRAGON_SUCCESS) {
        append_err_return(bit_rc, "Could not add entry into hashtable.");
    }

#ifdef HT_DEBUG
    if (len > 0) {
        printf("<++++++++++++++++++++++ The chain length was %lu during add. Here are keys\n", len);
        _print_chain(ht, start_idx);
    }
#endif

    *ht->header.count_ptr = *ht->header.count_ptr + 1;

    _check_armor(ht);

    no_err_return(DRAGON_SUCCESS);
}

/******************************
    BEGIN USER API
*******************************/

/** @brief Compute the number of bytes needed to hold this hash table.
 *
 *  This API provides a hash table implementation that resides in a pre-allocated blob of memory.
 *  This datatype does not do any dynamic allocation of memory on its own. The hash table is an
 *  array of slots. The hash table uses chaining for collisions. Due to its static requirement
 *  there may be mixing of chains in some cases. However, the load factor will never be more than 50%
 *  so the chains, even though mixed occassionally, will be short.
 *
 *  Keys and values are stored in the hash table. There are no pointers pointing to external entries. It
 *  is completely self-contained and pre-allocated.
 *
 *  @param max_entries The maximum number of entries this hash table will hold.
 *  @param key_len The # bytes of keys for the hash table. Must be a multiple of 8.
 *  @param value_len The # bytes of values for the hash table. Must be a multiple of 8.
 *  @param size The returned required size for this hash table.
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_hashtable_size(const uint64_t max_entries, const uint64_t key_len, const uint64_t value_len, uint64_t* size)
{
    if (size == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The size pointer was NULL.");

    if (key_len%sizeof(uint64_t) != 0) {
        err_return(DRAGON_HASHTABLE_KEY_SIZE_ERROR,"The key length must be a multiple of 8 bytes.");
    }

    if (value_len%sizeof(uint64_t) != 0) {
        err_return(DRAGON_HASHTABLE_VALUE_SIZE_ERROR,"The value length must be a multiple of 8 bytes.");
    }

    size_t bitset_size;

    uint64_t num_slots = max_entries * (100/max_load_factor);

    bitset_size = dragon_bitset_size(num_slots);

    *size = sizeof(uint64_t) * DRAGON_HASHTABLE_HEADER_NULINTS +
            bitset_size * DRAGON_HASHTABLE_BITSET_COUNT +
            (key_len + value_len) * num_slots;

    if (*size > thirty_two_gb) {
        err_return(DRAGON_HASHTABLE_TOO_BIG,"The hashtable would be too big.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Initialize the blob that holds the hash table.
 *
 *  This API provides a hash table implementation that resides in a pre-allocated blob of memory.
 *  This datatype does not do any dynamic allocation of memory on its own. The hash table is an
 *  array of slots. The hash table uses chaining for collisions. Due to its static requirement
 *  there may be mixing of chains in some cases. However, the load factor will never be more than 50%
 *  so the chains, even though mixed occassionally, will be short.
 *
 *  Keys and values are stored in the hash table. There are no pointers pointing to external entries. It
 *  is completely self-contained and pre-allocated.
 *
 *  @param max_entries The maximum number of entries this hash table will hold.
 *  @param key_len The # bytes of keys for the hash table.
 *  @param value_len The # bytes of values for the hash table.
 *  @param size The returned required size for this hash table.
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_hashtable_init(char* ptr, dragonHashtable_t* ht, const uint64_t max_entries, const uint64_t key_len, const uint64_t value_len)
{
    uint64_t size;
    dragonError_t rc = dragon_hashtable_size(max_entries, key_len, value_len, &size);

    if (rc != DRAGON_SUCCESS) {
        append_err_return(rc, "Could not initialize hashtable.");
    }

    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (ptr == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The ptr is NULL");

    char* blob_ptr = ptr;

    // Init the armor1 value
    uint64_t* ui_ptr = (uint64_t*) blob_ptr;

    if (*ui_ptr == armorval)
        err_return(DRAGON_INVALID_ARGUMENT, "The hashtable was already initialized before this call.");

    *ui_ptr = armorval;
    blob_ptr += sizeof(uint64_t);

    // Set the number of slots in the array.
    uint64_t num_slots = max_entries * (100/max_load_factor);
    ui_ptr = (uint64_t*) blob_ptr;
    *ui_ptr = num_slots;
    blob_ptr += sizeof(uint64_t);

    // Set the count of entries in the hashtable.
    ui_ptr = (uint64_t*) blob_ptr;
    *ui_ptr = 0;
    blob_ptr += sizeof(uint64_t);

    // Set the key_len
    ui_ptr = (uint64_t*) blob_ptr;
    *ui_ptr = key_len / sizeof(uint64_t);
    blob_ptr += sizeof(uint64_t);

    // Set the value_len
    ui_ptr = (uint64_t*) blob_ptr;
    *ui_ptr = value_len / sizeof(uint64_t);
    blob_ptr += sizeof(uint64_t);

    // Skip past the slots
    uint64_t entry_len = key_len + value_len;
    blob_ptr += num_slots * entry_len;

    // Init the armor3 value
    ui_ptr = (uint64_t*) blob_ptr;
    *ui_ptr = armorval;
    blob_ptr += sizeof(uint64_t);

    // init the allocation bit set.
    size_t bitset_size;
    bitset_size = dragon_bitset_size(max_entries*2);

    // The handle is inited, but will be inited again with the attach below.
    dragonError_t bitset_err = dragon_bitset_init((void*)blob_ptr, &ht->allocated, num_slots);

    if (bitset_err != DRAGON_SUCCESS)
        append_err_return(bitset_err, "Could not initialize hashtable.");

    blob_ptr = blob_ptr + bitset_size;

    // The handle is inited, but will be inited again with the attach below.
    bitset_err = dragon_bitset_init((void*)blob_ptr, &ht->placeholder, num_slots);

    if (bitset_err != DRAGON_SUCCESS)
        append_err_return(bitset_err, "Could not initialize hashtable.");

    blob_ptr = blob_ptr + bitset_size;
    ui_ptr = (uint64_t*) blob_ptr;

    // Init the armor2 value
    *ui_ptr = armorval;
    blob_ptr += sizeof(uint64_t);

    // attach the handle to the hashtable
    dragon_hashtable_attach(ptr, ht);

    // return success
    no_err_return(DRAGON_SUCCESS);
}

/** @brief Destroy the blob that holds the hash table.
 *
 *  Destroying the hash table has no effect except that the handle is detached.
 *
 *  @param ht A valid handle to a hash table.
 */

dragonError_t
dragon_hashtable_destroy(dragonHashtable_t* ht)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    _check_armor(ht);

    int derr = dragon_hashtable_detach(ht);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Could not destroy hashtable");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Attach a new handle to the blob that holds the hash table.
 *
 *  Attaching initializes a new handle.
 *
 *  @param ptr A valid pointer to a hash table.
 *  @param ht A valid pointer to a hash table handle.
 */

dragonError_t
dragon_hashtable_attach(char* ptr, dragonHashtable_t* ht)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (ptr == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The ptr is NULL.");

    char* blob_ptr = ptr;
    uint64_t* ui_ptr;

    // set the armor1 pointer
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.armor1 = ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // Get the number of slots in the array.
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.num_slots = *ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // Get the pointer to the count of entries in the hashtable.
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.count_ptr = ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // Get the key_len
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.key_len = *ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // Get the value_len
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.value_len = *ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // get the slots pointer
    ht->slots = blob_ptr;
    uint64_t entry_len = (ht->header.key_len + ht->header.value_len) * sizeof(uint64_t);
    blob_ptr += ht->header.num_slots * entry_len;

    // set the armor3 pointer
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.armor3 = ui_ptr;
    blob_ptr += sizeof(uint64_t);

    // get the bitset size
    size_t bitset_size;
    bitset_size = dragon_bitset_size(ht->header.num_slots);

    // get the allocation bit set handle
    dragonError_t bitset_err = dragon_bitset_attach((void*)blob_ptr, &ht->allocated);

    if (bitset_err != DRAGON_SUCCESS)
        err_return(bitset_err, "Could not attach to hashtable.");

    blob_ptr = blob_ptr + bitset_size;

    // The handle is inited, but will be inited again with the attach below.
    bitset_err = dragon_bitset_attach((void*)blob_ptr, &ht->placeholder);

    if (bitset_err != DRAGON_SUCCESS)
        err_return(bitset_err, "Could not attach to hashtable.");

    blob_ptr = blob_ptr + bitset_size;

    // set the armor2 pointer
    ui_ptr = (uint64_t*) blob_ptr;
    ht->header.armor2 = ui_ptr;
    blob_ptr += sizeof(uint64_t);

    _check_armor(ht);

    // return success
    no_err_return(DRAGON_SUCCESS);
}

/** @brief Detach a handle to a hash table.
 *
 *  Detach simply nulls out two pointer fields in the handle. It has no other effect.
 *
 *  @param ht A valid handle to a hash table.
 */

dragonError_t
dragon_hashtable_detach(dragonHashtable_t* ht)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    _check_armor(ht);

    // Not really necessary, but if there were an illegal access after a
    // destroy this will cause a segfault.
    ht->header.count_ptr = NULL;
    ht->slots = NULL;

    // return success
    no_err_return(DRAGON_SUCCESS);
}

/** @brief Add a key, value pair to the hash table.
 *
 *  This will add a key, value pair to the hash table. If the hash table is full
 *  then a new pair cannot be added. If the key is already in the hash table then
 *  this add will be rejected.
 *
 *  @param ht A valid handle to a hash table.
 *  @param key A pointer to a key of length key_len (provided when inited)
 *  @param value A pointer to space that holds the value of length value_len (provided when inited)
 */

dragonError_t
dragon_hashtable_add(dragonHashtable_t* ht, const char* key, const char* value)
{
    // add with no replace
    dragonError_t derr = _hashtable_add(ht, key, value, false);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Could not add key value pair to hashtable.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Replace a key, value pair in the hash table.
 *
 *  This will replace a key, value pair in the hash table. If the key is not in the hash table, then
 *  the replace will fail.
 *
 *  @param ht A valid handle to a hash table.
 *  @param key A pointer to a key of length key_len (provided when inited)
 *  @param value A pointer to space that holds the value of length value_len (provided when inited)
 */

dragonError_t
dragon_hashtable_replace(dragonHashtable_t* ht, const char* key, const char* value)
{
    // add with replace
    dragonError_t derr = _hashtable_add(ht, key, value, true);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Could not add key value pair to hashtable.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Remove a key, value pair from the hash table.
 *
 *  Remove the key, value pair from the hash table whose key matches the specified key. If the key
 *  is not found an error will be returned.
 *
 *  @param ht A valid handle to a hash table.
 *  @param key A pointer to a key of length key_len (provided when inited)
 */

dragonError_t
dragon_hashtable_remove(dragonHashtable_t* ht, const char* key)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (key == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The key is NULL.");

    _check_armor(ht);

    uint64_t idx = _hash(key, ht->header.key_len) % ht->header.num_slots;
    uint64_t entry_len = (ht->header.key_len + ht->header.value_len) * sizeof(uint64_t);
    char* key_ptr = NULL;
    bool searching = true;
    key_ptr = ht->slots + entry_len * idx;

    while (searching) {
        unsigned char allocated;
        unsigned char placeholder;

        dragonError_t bit_rc = dragon_bitset_get(&ht->allocated, idx, &allocated);

        if (bit_rc != DRAGON_SUCCESS) {
           append_err_return(bit_rc, "Unable to remove hashtable entry.");
        }

        if (!allocated) {
            searching = false;
        } else {
            bit_rc = dragon_bitset_get(&ht->placeholder, idx, &placeholder);
            if (bit_rc != DRAGON_SUCCESS) {
                append_err_return(bit_rc, "unable to remove hashtable entry");
            }

            if (!placeholder && _keys_equal(key,key_ptr,ht->header.key_len)) {
                uint64_t next_idx = (idx + 1) % ht->header.num_slots;
                bit_rc = dragon_bitset_get(&ht->allocated, next_idx, &allocated);
                if (bit_rc != DRAGON_SUCCESS) {
                    append_err_return(bit_rc, "Unable to remove hashtable entry.");                }
                if (allocated) {
                    // if entry to the right is allocated, then we don't
                    // want to break the chain so make this entry a placeholder
                    bit_rc = dragon_bitset_set(&ht->placeholder, idx);
                    if (bit_rc != DRAGON_SUCCESS) {
                        append_err_return(bit_rc, "Unable to remove hashtable entry.");
                    }
                } else {
                    // Mark this entry as not allocated because it was removed
                    // and the entry to the right is not allocated so it is
                    // the end of a chain if one exists.
                    bit_rc = dragon_bitset_reset(&ht->allocated, idx);
                        if (bit_rc != DRAGON_SUCCESS) {
                            append_err_return(bit_rc, "Unable to remove hashtable entry.");
                    }

                    // if the entry to the right is not allocated, then
                    // we'll make all placeholders to the left of it
                    // not allocated too since we are at the end of a chain.
                    uint64_t prev_idx = idx;
                    bool moving_left = true;
                    if (prev_idx == 0) {
                        prev_idx = ht->header.num_slots-1;
                    } else {
                        prev_idx -= 1;
                    }
                    while (moving_left) {
                        bit_rc = dragon_bitset_get(&ht->placeholder, prev_idx, &placeholder);
                        if (bit_rc != DRAGON_SUCCESS) {
                            append_err_return(bit_rc, "Unable to remove hashtable entry.");
                        }

                        if (placeholder) {
                            bit_rc = dragon_bitset_reset(&ht->placeholder, prev_idx);
                            if (bit_rc != DRAGON_SUCCESS) {
                                append_err_return(bit_rc, "Unable to remove hashtable entry.");
                            }

                            bit_rc = dragon_bitset_reset(&ht->allocated, prev_idx);
                            if (bit_rc != DRAGON_SUCCESS) {
                                append_err_return(bit_rc, "Unable to remove hashtable entry.");
                            }

                            if (prev_idx == 0) {
                                prev_idx = ht->header.num_slots-1;
                            } else {
                                prev_idx -= 1;
                            }
                        }
                        else {
                            moving_left = false;
                        }
                    }
                }

                *(ht->header.count_ptr) -= 1;

#ifdef HT_DEBUG
                if (idx - start_idx > 0) {
                    printf("deleted ");
                    _print_key(ht,key);
                    printf("from a chain. Here is chain after deleting.\n");
                    _print_chain(ht, start_idx);
                }
#endif
                _check_armor(ht);

                no_err_return(DRAGON_SUCCESS);
            }
        }

        // advance idx mod length of the slots array avoiding multiplication in the loop.
        idx = idx + 1;
        if (idx == ht->header.num_slots) {
            idx = 0;
            key_ptr = ht->slots;
        } else {
            key_ptr = key_ptr + entry_len;
        }
    }

    err_return(DRAGON_HASHTABLE_KEY_NOT_FOUND, "Hashtable key not found.");
}

/** @brief Get a value for the specified key from the hash table.
 *
 *  If a matching key is found, then the space pointed to by value has the
 *  value copied into it.
 *
 *  @param ht A valid handle to a hash table.
 *  @param key A pointer to a key of length key_len (provided when inited)
 *  @param value A pointer to a value of length value_len (provided when inited) to hold the
 *  matching value.
 */

dragonError_t
dragon_hashtable_get(const dragonHashtable_t* ht, const char* key, char* value)
{
    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (key == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The key pointer is NULL.");

    if (value == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The value pointer is NULL.");

    _check_armor(ht);

    uint64_t idx = _hash(key, ht->header.key_len) % ht->header.num_slots;
    uint64_t entry_len = (ht->header.key_len + ht->header.value_len) * sizeof(uint64_t);
    char* key_ptr = NULL;
    char* value_ptr = NULL;
    bool searching = true;
    key_ptr = ht->slots + entry_len * idx;
    dragonError_t rc;

    while (searching) {
        unsigned char allocated;
        unsigned char placeholder;

        dragonError_t bit_rc = dragon_bitset_get(&ht->allocated, idx, &allocated);

        if (bit_rc != DRAGON_SUCCESS) {
            append_err_return(bit_rc, "Unable to look up key.");
        }

        if (!allocated) {
            searching = false;
        } else {
            bit_rc = dragon_bitset_get(&ht->placeholder, idx, &placeholder);
            if (bit_rc != DRAGON_SUCCESS) {
                append_err_return(bit_rc, "Unable to look up key.");
            }

            if (!placeholder && _keys_equal(key,key_ptr,ht->header.key_len)) {
                value_ptr = key_ptr + ht->header.key_len * sizeof(uint64_t);
                rc = _copy_out(ht, value, value_ptr, ht->header.value_len);
                if (rc != DRAGON_SUCCESS)
                    append_err_return(rc, "There was an error on copy.");

                _check_armor(ht);
                no_err_return(DRAGON_SUCCESS);
            }
        }

        // advance idx mod length of the slots array avoiding multiplication in the loop.
        idx = idx + 1;
        if (idx == ht->header.num_slots) {
            idx = 0;
            key_ptr = ht->slots;
        } else {
            key_ptr = key_ptr + entry_len;
        }
    }

    err_return(DRAGON_HASHTABLE_KEY_NOT_FOUND, "Hashtable key not found.");
}

dragonError_t
dragon_hashtable_iterator_init(const dragonHashtable_t* ht, dragonHashtableIterator_t* iter)
{
    iter->index = 0;
    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_hashtable_iterator_next(const dragonHashtable_t* ht, dragonHashtableIterator_t* iter, char* key, char* value)
{
    dragonError_t bit_rc;
    dragonError_t rc;
    unsigned char allocated;
    unsigned char placeholder;

    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (key == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The key pointer is NULL.");

    if (value == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The value pointer is NULL.");

    if (iter == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The iterator pointer is NULL");

    uint64_t entry_len = (ht->header.key_len + ht->header.value_len)*sizeof(uint64_t);
    char* key_ptr = NULL;
    char* value_ptr = NULL;

    while (true) {

        if (iter->index >= ht->header.num_slots) {
            no_err_return(DRAGON_HASHTABLE_ITERATION_COMPLETE);
        }

        bit_rc = dragon_bitset_get(&ht->allocated, iter->index, &allocated);

        if (bit_rc != DRAGON_SUCCESS) {
            append_err_return(bit_rc, "Unable to advance iterator.");
        }

        bit_rc = dragon_bitset_get(&ht->placeholder, iter->index, &placeholder);

        if (bit_rc != DRAGON_SUCCESS) {
            append_err_return(bit_rc, "Unable to advance iterator.");
        }

        uint64_t idx = iter->index;

        iter->index += 1;

        if (allocated && !placeholder) {
            key_ptr = ht->slots + entry_len * idx;
            value_ptr = key_ptr + ht->header.key_len * sizeof(uint64_t);
            rc = _copy_out(ht, value, value_ptr, ht->header.value_len);
            if (rc != DRAGON_SUCCESS)
                append_err_return(rc, "There was an error on copy.");
            rc = _copy_out(ht, key, key_ptr, ht->header.key_len);
            if (rc != DRAGON_SUCCESS)
                append_err_return(rc, "There was an error on copy.");

            no_err_return(DRAGON_SUCCESS);
        }
    }
}

/** @brief Get statistics for the hash table like the number of keys in the hash table, the
 *  maximum and average chain length, the capacity of the hash table, and it's current load factor.
 *
 *  One note: Due to the nature of a statically allocated hash table, the buckets of the hash table
 *  necessarily overlap. This means that the average chain length is an upper bound, worst case
 *  chain length. For example, searching for an item that is not in the hash table might result in searching
 *  the longest chain, but searching for an item that is in the hash table, might never encounter the
 *  longest possible chain because it may be that two buckets overlap.
 *
 *  @param ht A valid handle to a hash table.
 *  @param stats A pointer to a statistics structure to hold the reported statistics.
 */

dragonError_t
dragon_hashtable_stats(const dragonHashtable_t* ht, dragonHashtableStats_t* stats)
{

    if (ht == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The dragonHashtable handle is NULL.");

    if (stats == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The stats structure pointer is NULL.");

    stats->load_factor = *ht->header.count_ptr / ((double)ht->header.num_slots);
    stats->capacity = ht->header.num_slots * (max_load_factor / 100.0);
    stats->num_items = *ht->header.count_ptr;
    stats->key_len = ht->header.key_len * sizeof(uint64_t);
    stats->value_len = ht->header.value_len * sizeof(uint64_t);

    uint64_t k;
    uint64_t idx = 0;
    uint64_t total_chain_length = 0;
    uint64_t max_chain_length = 0;

    for (k=0; k<ht->header.num_slots; k++) {
        bool searching = true;
        idx = k;
        uint64_t chain_length = 0;

        while (searching) {
            unsigned char allocated;
            unsigned char placeholder;

            dragonError_t bit_rc = dragon_bitset_get(&ht->allocated, idx, &allocated);

            if (bit_rc != DRAGON_SUCCESS) {
                append_err_return(bit_rc, "Unable to get hashtable stats.");
            }

            if (!allocated) {
                searching = false;
            } else {
                chain_length = chain_length + 1;

                bit_rc = dragon_bitset_get(&ht->placeholder, idx, &placeholder);
                if (bit_rc != DRAGON_SUCCESS) {
                    append_err_return(bit_rc, "Unable to get hashtable stats.");
                }
            }

            // advance idx mod length of the slots array avoiding multiplication in the loop.
            idx = idx + 1;
            if (idx == ht->header.num_slots) {
                idx = 0;
            }
        }

        total_chain_length += chain_length;

        if (chain_length > max_chain_length) {
            max_chain_length = chain_length;
        }
    }
    if (*ht->header.count_ptr == 0) {
        stats->avg_chain_length = 0.0;
    } else {
        stats->avg_chain_length = ((double)total_chain_length) / (*ht->header.count_ptr);
    }

    stats->max_chain_length = max_chain_length;

    no_err_return(DRAGON_SUCCESS);

}

/** @brief Dump the state of the hash table to stdout.
 *
 *  A hash table dump includes internal state and some statistics on the hash table.
 *
 *  @param title A null-terminated string to be printed as the title.
 *  @param ht A valid handle to a hash table.
 *  @param indent A null-terminated string to be printed before each line of the dump.
 */

dragonError_t
dragon_hashtable_dump(const char* title, const dragonHashtable_t* ht, const char* indent)
{
        dragonError_t derr = dragon_hashtable_dump_to_fd(stdout, title, ht, indent);

        if (derr != DRAGON_SUCCESS)
            append_err_return(derr, "Unable to dump hashtable to stdout.");

        no_err_return(DRAGON_SUCCESS);
}

/** @brief Dump the state of the hash table to a file.
 *
 *  A hash table dump includes internal state and some statistics on the hash table.
 *
 *  @param fd A file descriptor for a file where this dump should be written.
 *  @param title A null-terminated string to be printed as the title.
 *  @param ht A valid handle to a hash table.
 *  @param indent A null-terminated string to be printed before each line of the dump.
 */

dragonError_t
dragon_hashtable_dump_to_fd(FILE* fd, const char* title, const dragonHashtable_t* ht, const char* indent)
{
    dragonError_t rc;

    dragonHashtableStats_t stats;

    rc = dragon_hashtable_stats(ht, &stats);

    if (rc != DRAGON_SUCCESS)
        append_err_return(rc, "Unable to dump hashtable to file descriptor.");

    if (fd == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The file pointer is NULL.");

    if (title == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"THe title is NULL.");

    if (indent == NULL)
        err_return(DRAGON_HASHTABLE_NULL_POINTER,"The indent is NULL.");

    fprintf(fd, "%s%s\n",indent,title);
    fprintf(fd, "%sNumber of slots: %lu\n",indent,ht->header.num_slots);
    fprintf(fd, "%sCapacity: %lu\n", indent, stats.capacity);
    fprintf(fd, "%sFilled slots: %lu\n", indent, *ht->header.count_ptr);
    fprintf(fd, "%sLoad Factor: %f\n", indent, stats.load_factor);
    fprintf(fd, "%sKey length: %lu\n", indent, ht->header.key_len*sizeof(uint64_t));
    fprintf(fd, "%sValue length: %lu\n", indent, ht->header.value_len*sizeof(uint64_t));
    fprintf(fd, "%sAverage Chain Length: %f\n", indent, stats.avg_chain_length);
    fprintf(fd, "%sMaximum Chain Length: %lu\n", indent, stats.max_chain_length);

    dragon_bitset_dump_to_fd(fd, "Allocated Slots", &ht->allocated, indent);
    dragon_bitset_dump_to_fd(fd, "Placeholder Slots", &ht->placeholder, indent);

    no_err_return(DRAGON_SUCCESS);
}
