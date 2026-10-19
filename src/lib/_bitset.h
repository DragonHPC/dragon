#ifndef HAVE_DRAGON_BITSET_H
#define HAVE_DRAGON_BITSET_H

#define TRUE 1
#define FALSE 0

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <dragon/return_codes.h>

/* -----------------------------------------------------------------------------
 * The BitSet datatype creates a bit represented set ADT. This is a low-level
 * implementation that requires the user provide the space. The dragon_bitset_size
 * function returns the required size for a bit set of num_bits. The init must
 * be called on the space prior to using. The set, rest, and get methods work
 * as expected. Note that get is called by passing the address of the result
 * location.
 *
 * The specialty function zeroes_to_right counts the number of zeroes to the right
 * of a bit location. This is useful in the heap management function (see dyn_mem.c).
 * The dump method is useful in debug. The title prints prior to dumping the set and
 * indent is printed before each line in the dump.
 *
 * NOTE: In this implementation the bit order of each byte are reversed to a
 * hex_dump (see below) of a BitSet has bit 0 of the set at the left side of
 * the dump and the bit ordering reads lexicographically from left to right.
 *
 * NOTE: This implementation does NO dynamic memory allocation.
 * ----------------------------------------------------------------------------- */

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  A bitset handle.
 *
 *  This structure should not be used directly. All manipulation of the bitset
 *  occurs via its API functions. This structure is meant to meant to be used
 *  internally within some larger structure and will be self-contained completely
 *  within the larger structure. The bitset implementation does no dynamic
 *  memory allocation. The length field is the number of bits set.
 **/

typedef struct dragonBitSet_st {
    size_t size; /**< For internal use only */
    size_t* length; /**< For internal use only */
    size_t* leading_zeroes; /**< For internal use only */
    char* data;  /**< For internal use only */
} dragonBitSet_t;

#ifdef __cplusplus
}
#endif

/* BitSet functions */
size_t
dragon_bitset_size(size_t num_bits);

dragonError_t
dragon_bitset_get_num_bits(const dragonBitSet_t* set, size_t* num_bits);

dragonError_t
dragon_bitset_init(void* ptr, dragonBitSet_t* set, const size_t num_bits);

dragonError_t
dragon_bitset_destroy(dragonBitSet_t* set);

dragonError_t
dragon_bitset_clear(dragonBitSet_t* set);

dragonError_t
dragon_bitset_attach(void* ptr, dragonBitSet_t* set);

dragonError_t
dragon_bitset_detach(dragonBitSet_t* set);

dragonError_t
dragon_bitset_copy(dragonBitSet_t* destination, dragonBitSet_t* source);

dragonError_t
dragon_bitset_set(dragonBitSet_t* set, const size_t val_index);

dragonError_t
dragon_bitset_reset(dragonBitSet_t* set, const size_t val_index);

dragonError_t
dragon_bitset_get(const dragonBitSet_t* set, const size_t val_index, bool* val);

dragonError_t
dragon_bitset_length(const dragonBitSet_t* set, size_t* length);

dragonError_t
dragon_bitset_zeroes_to_right(const dragonBitSet_t* set, const size_t val_index, size_t* val);

dragonError_t
dragon_bitset_first(const dragonBitSet_t* set, size_t* first);

dragonError_t
dragon_bitset_next(const dragonBitSet_t* set, const size_t current, size_t* next);

dragonError_t
dragon_bitset_dump(const char* title, const dragonBitSet_t* set, const char* indent);

dragonError_t
dragon_bitset_dump_to_fd(FILE* fd, const char* title, const dragonBitSet_t* set, const char* indent);

#endif