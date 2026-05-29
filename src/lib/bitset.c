#include "_bitset.h"
#include "_hexdump.h"
#include "err.h"
#include <stdlib.h>
#include <stddef.h>
#include <stddef.h>
#include <stdio.h>
#include <math.h>

static unsigned char bit_masks[] = {1,2,4,8,16,32,64,128};

/******************************
    BEGIN USER API
*******************************/

/** @brief Compute the number of bytes needed to hold this bitset.
 *
 *  This API provides a bitset implementation that resides in a pre-allocated blob of memory.
 *  This datatype does not do any dynamic allocation of memory on its own. The bitset is a set
 *  of integers ranging from to to num_bits-1. This set is NOT thread or process safe. External
 *  locking must be done to guarantee order of operations.
 *
 *  @param num_bits The number of integers that may be in the set. Potential members of the set range
 *  from 0 to num_bits-1.
 *  @param size The returned size required for this bitset. The user allocates a blob of this size and
 *  then passes a pointer to it in on the init function call before using any other functions in this
 *  API.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 */

size_t
dragon_bitset_size(const size_t num_bits)
{
    size_t size;

    // adding 7 guarantees it will be a number of bytes
    // that is big enough to hold all the bits
    size_t num_bytes = (num_bits + 7) / 8;

    // This guarantees the num_bytes will be a multiple of 8 bytes
    // (for boundary alignment) and big enough to hold all the bits.
    // The two size_t fields are the size and the number of set bits.
    num_bytes = ((num_bytes + 7) / 8) * 8;
    size = 3*sizeof(size_t) + num_bytes;

    return size;
}

/** @brief Return the number of bits contained in this set.
 *
 *  Returns the number of bits (i.e. One more than maximum integer that can be contained in the set)
 *  in the set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param num_bits A pointer to the size_t variable that will hold the number of bits this set
 *  contains.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set or num_bits was a null-pointer.
 */

dragonError_t
dragon_bitset_get_num_bits(const dragonBitSet_t* set, size_t* num_bits)
{
    if (set == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");
    }

    if (num_bits == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The num_bits is NULL");
    }

    *num_bits = set->size;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Initialize a bitset from a blob of memory.
 *
 *  This API provides a bitset implementation that resides in a pre-allocated blob of memory.
 *  This datatype does not do any dynamic allocation of memory on its own. The bitset is a set
 *  of integers ranging from to to num_bits-1.
 *
 *  A bitset must be initialized by calling init before any other API calls can be made except
 *  the size function which should be called first to determine required size of the blob to
 *  hold this bitset.
 *
 *  @param ptr A pointer to the blob of memory where the bitset will reside.
 *  @param set A pointer to a handle to be initialized to use this bitset. The handle is used
 *  on subsequent calls to the API.
 *  @param num_bits The number of bits for this bitset. This must be the same number of bits
 *  that was provided on the previous call to the size function in this API.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set or ptr was a null-pointer.
 */

dragonError_t
dragon_bitset_init(void* ptr, dragonBitSet_t* set, const size_t num_bits)
{
    if (set == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");
    }

    if (ptr == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The ptr cannot be NULL.");

    size_t* size_ptr = (size_t*) ptr;
    *size_ptr = num_bits;
    set->size = num_bits;
    ptr += sizeof(size_t);

    size_t* length_ptr = (size_t*) ptr;
    set->length = length_ptr;
    *set->length = 0;
    ptr += sizeof(size_t);

    size_t* leading_ptr = (size_t*) ptr;
    set->leading_zeroes = leading_ptr;
    *set->leading_zeroes = num_bits;
    ptr += sizeof(size_t);

    set->data = (char*) ptr;

    dragon_bitset_clear(set); // Called internally this will not fail.

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Clear a bitset to all zeroes.
 *
 *  This API provides a bitset implementation that resides in a pre-allocated blob of memory.
 *  This datatype does not do any dynamic allocation of memory on its own. The bitset is a set
 *  of integers ranging from to to num_bits-1.
 *
 *  A bitset must be cleared (i.e. set to zeroes) by calling this. The BitSet should have been
 *  previously initialized.
 *
 *  @param set A pointer to a handle to an initialized bitset.
 *
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 */
dragonError_t
dragon_bitset_clear(dragonBitSet_t* set)
{
    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    size_t max_idx = (set->size + 7) / 8;
    for (size_t k = 0; k<max_idx; k++)
        set->data[k] = 0;

    *set->length = 0;
    *set->leading_zeroes = set->size;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Destroy a bitset.
 *
 *  Once done with a bitset, destroy should be called to free resources use by the bitset.
 *
 *  @param set A pointer to a bitset handle.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 */

dragonError_t
dragon_bitset_destroy(dragonBitSet_t* set)
{
    if (set == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");
    }

    set->size = 0;
    set->data = NULL;
    set->length = NULL;
    set->leading_zeroes = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Copy a BitSet to another Bitet, overlaying the destination's contents.
 *
 *  Copy a BitSet from a source to a destination. They both must be of the same size.
 *
 *  @param destination A pointer to the destiniation bitset handle.
 *  @param source A pointer to a source bitset handle.
 *
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The source or destination was a null-pointer.
 *      * **DRAGON_INVALID_ARGUMENT** The source and destination size must be equal.
 */

dragonError_t
dragon_bitset_copy(dragonBitSet_t* destination, dragonBitSet_t* source)
{
    if (source==NULL)
        err_return(DRAGON_BITSET_NULL_POINTER, "The source cannot be NULL in dragon_bitset_copy.");

    if (destination == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER, "The destination cannot be NULL in dragon_bitset_copy.");

    if (source->data == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER, "The source data was NULL. You must call dragon_bitset_init first");

    if (destination->data == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER, "The destination data was NULL. You must call dragon_bitset_init first");

    if (source->size != destination->size)
        err_return(DRAGON_INVALID_ARGUMENT, "The source and destination must be the same size.");

    size_t bits = source->size;
    size_t bytes;
    bytes = dragon_bitset_size(bits);

    // The actual data for a bitset is contiguous and starts
    // 3*sizeof(size_t) bytes before the data pointer. See
    // init and attach for reference.
    void* dest = (void*)(destination->data) - 3*sizeof(size_t);
    void* src = (void*)(source->data)- 3*sizeof(size_t);
    memcpy(dest, src, bytes);
    // Copy size value into handle because it does not point into
    // header.
    destination->size = source->size;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Attach to a previously initialized bitset.
 *
 *  Once initialized, a previously initialized heap may be attached. However, bitsets are not
 *  multi-threaded/multi-processing safe. Sychronization of API calls must be done separately.
 *
 *  @param ptr The pointer to the blob of previously initialized bitset.
 *  @param set A pointer to a handle that will be initialized by this call.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set or ptr was a null-pointer.
 */

dragonError_t
dragon_bitset_attach(void* ptr, dragonBitSet_t* set)
{
    if (ptr == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The ptr is NULL.");
    }

    if (set == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");
    }


    size_t* size_ptr = (size_t*) ptr;
    set->size = *size_ptr;
    ptr += sizeof(size_t);

    size_t* length_ptr = (size_t*) ptr;
    set->length = length_ptr;
    ptr += sizeof(size_t);

    size_t* leading_ptr = (size_t*) ptr;
    set->leading_zeroes = leading_ptr;
    ptr += sizeof(size_t);

    set->data = (char*) ptr;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Detach from an attached bitset.
 *
 *  If attached was called to attach a bitset, then detach should be called when access to the
 *  bitset is no longer required.
 *
 *  @param set A pointer to a bitset handle.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 */

dragonError_t
dragon_bitset_detach(dragonBitSet_t* set)
{
     if (set == NULL) {
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");
    }

    set->size = 0;
    set->data = NULL;
    set->length = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Set a bit to 1 in the bitset.
 *
 *  Turn on a bit in the bitset regardless of its previous value. This makes the integer given
 *  by val_index a member of the set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param val_index The integer to add to the set. The val_index value must be between 0 and
 *  the number of bits in the bitset minus one.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 *      * **DRAGON_BITSET_BOUNDS_ERROR** The val_index was too big for the given set.
 */

dragonError_t
dragon_bitset_set(dragonBitSet_t* set, const size_t val_index)
{
    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (val_index >= set->size)
        err_return(DRAGON_BITSET_BOUNDS_ERROR,"The index is bigger than the set size.");

    size_t byte_index = val_index >> 3;
    // The following computes the index into the byte but
    // inverts the bit so the left-most bit (in a dump) is bit 0. This
    // reads lexicographically for a bit set from left to right.
    size_t bit_index = 7-(val_index & 0x7);
    unsigned char mask = bit_masks[bit_index];
    char previous = set->data[byte_index];
    set->data[byte_index] = set->data[byte_index] | mask;

    if (previous != set->data[byte_index])
        *set->length +=1;

    if (val_index < *set->leading_zeroes) {
        *set->leading_zeroes = val_index;
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Reset a bit to 0 in the bitset.
 *
 *  Turn off a bit in the bitset regardless of its previous value. This means the integer given
 *  by val_index is no longer a member of the bitset.
 *
 *  @param set A pointer to a bitset handle.
 *  @param val_index The integer to remove from the set. The val_index value must be between 0 and
 *  the number of bits in the bitset minus one.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** The set was a null-pointer.
 *      * **DRAGON_BITSET_BOUNDS_ERROR** The val_index was too big for the given set.
 */

dragonError_t
dragon_bitset_reset(dragonBitSet_t* set, const size_t val_index)
{
    dragonError_t err;

    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (val_index >= set->size)
        err_return(DRAGON_BITSET_BOUNDS_ERROR,"The index is bigger than the set size.");

    size_t byte_index = val_index >> 3;
    // The following computes the index into the byte but
    // inverts the bit so the left-most bit (in a dump) is bit 0. This
    // reads lexicographically for a bit set from left to right.
    size_t bit_index = 7-(val_index & 0x7);
    unsigned char mask = ~(bit_masks[bit_index]);
    char previous = set->data[byte_index];
    set->data[byte_index] = set->data[byte_index] & mask;

    if (previous != set->data[byte_index])
        *set->length -=1;

    if (val_index == *set->leading_zeroes) {
        if (*set->length == 0)
            *set->leading_zeroes = set->size;
        else {
            size_t zeroes_to_right;
            err = dragon_bitset_zeroes_to_right(set, val_index, &zeroes_to_right);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Internal Failure while resetting leading zeroes.");

            *set->leading_zeroes = val_index + zeroes_to_right + 1;
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Get a bit from the bitset.
 *
 *  Retrieve a bit from the bitset. This is a membership test for the set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param val_index The integer to test for membership. The integer must be between 0 and
 *  the number of bits in the bitset minus one.
 *  @param val A pointer to an bool that will hold the result. It will be 0 or 1.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** Either the set or the val was a null-pointer.
 *      * **DRAGON_BITSET_BOUNDS_ERROR** The val_index was too big for the given set.
 */

dragonError_t
dragon_bitset_get(const dragonBitSet_t* set, const size_t val_index, bool* val)
{
    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (val == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The value is NULL.");

    if (val_index >= set->size) {
        *val = 0;
        err_return(DRAGON_BITSET_BOUNDS_ERROR,"The index is bigger than the set size.");
    }

    size_t byte_index = val_index >> 3;
    // The following computes the index into the byte but
    // inverts the bit so the left-most bit (in a dump) is bit 0. This
    // reads lexicographically for a bit set from left to right.
    size_t bit_index = 7-(val_index & 0x7);
    unsigned char mask = bit_masks[bit_index];
    // We shift it back to the right side to return 0 or 1.
    *val = (set->data[byte_index] & mask) >> bit_index;
    no_err_return(DRAGON_SUCCESS);
}

static size_t size_t_mask = (size_t)sizeof(size_t)-1;

/** @brief Return the length (i.e. number of set bits) in the bit set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param length A pointer to an unsigned integer that will hold the number of set bits in the set.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** If either the set or the length pointer were passed in as NULL.
 *
 */
dragonError_t
dragon_bitset_length(const dragonBitSet_t* set, size_t* length) {
    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (length == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The length argument must point to a valid size_t variable.");

    if (set->length == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet is not initialized. Call dragon_bitset_init or dragon_bitset_attach first.");

    *length = *set->length;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Compute the number of zero bits to the right of a member of the bitset.
 *
 *  This is a special purpose function (primarily for the heap manager) that counts
 *  the zeroes to the right of a member of the set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param val_index The integer member of the set. Whether this integer is a member of the
 *  set or not is irrelevant to this function call. It will count the zeroes to the right of
 *  any integer in the acceptable range of integers for the bitset.
 *  @param val A pointer to an unsigned integer that will hold the number of zeroes to the right
 *  of val_index.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** If either the set or the val pointer were passed in as NULL.
 *      * **DRAGON_BITSET_BOUNDS_ERROR** If val_index was too large for the given set.
 *
 */

dragonError_t
dragon_bitset_zeroes_to_right(const dragonBitSet_t* set, const size_t val_index, size_t* val)
{
    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (val == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The value is NULL.");

    if (val_index >= set->size) {
        *val = 0;
        err_return(DRAGON_BITSET_BOUNDS_ERROR,"The index is bigger than the set size.");
    }

    if (set->length == 0) {
        // length is the number of set bits. If there are none, then we can return quickly.
        *val = set->size - val_index;
        no_err_return(DRAGON_SUCCESS);
    }

    size_t idx = val_index+1;
    size_t lval = 0;
    bool bit_val;
    while (idx < set->size) {
        size_t byte_index = idx >> 3;
        size_t addr = (size_t)&set->data[byte_index];
        if ((idx + sizeof(size_t)*8 < set->size) &&
            ((addr & size_t_mask) == 0) &&
            (*((size_t*)addr) == 0)) {
            // if there are at least 64 bits left to check and
            // if the address of the byte is on a
            // word boundary, check the whole word
            // for 0. If the whole word is zero, then add
            // the appropriate number of zero bits to lval
            // and increment idx the appropriate number of bits.
            idx += sizeof(size_t) * 8;
            lval = lval + sizeof(size_t) * 8;
        }  else if (((idx + sizeof(char)*8) < set->size) &&
                    ((idx & 0x7) == 0) &&
                    (set->data[byte_index] == 0)) {
                // if there are at least 8 bits left to check and
                // if the idx is on a byte boundary, the check the
                // whole byte for 0. if the byte is zero then add
                // the 8 bits to idx and to lval.
                idx += sizeof(char) * 8;
                lval = lval + sizeof(char) * 8;
        } else {
            dragon_bitset_get(set,idx,&bit_val);
            if (bit_val == 1) {
                *val = lval;
                no_err_return(DRAGON_SUCCESS);
            }

            lval++;
            idx++;
        }
    }

    *val = lval;
    no_err_return(DRAGON_SUCCESS);
}

/** @brief Return the first bit found in the set that is a 1.
 *
 *  This finds the first 1 that is set in the bit set.
 *
 *  @param set A pointer to a bitset handle.
 *  @param bit_index The index of the least significant bit with value 1 in the set.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** If either the set or the val pointer were passed in as NULL.
 *      * **DRAGON_NOT_FOUND** If there is no bit with value 1 in the set.
 */

dragonError_t
dragon_bitset_first(const dragonBitSet_t* set, size_t* first)
{
    dragonError_t err;
    char err_msg[200];
    bool val;

    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (first == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The first parameter cannot be NULL.");

    /* for the first element in the iteration, the return value would be the
       number of zeroes to the right of index 0 but add 1 more for the first
       non-zero value. */
    *first = *set->leading_zeroes;

    if (*set->length == 0)
        no_err_return(DRAGON_BITSET_ITERATION_COMPLETE);

    if (*first >= set->size)
        no_err_return(DRAGON_BITSET_ITERATION_COMPLETE);

    err = dragon_bitset_get(set, *first, &val);

    if (err != DRAGON_SUCCESS) {
        snprintf(err_msg, 199, "Could not get first element from set with index %lu. Size of set is %lu.", *first, set->size);
        append_err_return(err, err_msg);
    }

    if (!val) {
        snprintf(err_msg, 199, "Called dragon_bitset_first with index %lu and got an unset bit.", *first);
        err_return(DRAGON_FAILURE, err_msg);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_bitset_next(const dragonBitSet_t* set, const size_t current, size_t* next)
{
    dragonError_t err;
    size_t count;

    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (next == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The next parameter cannot be NULL.");

    if (current >= set->size)
        no_err_return(DRAGON_BITSET_ITERATION_COMPLETE);

    err = dragon_bitset_zeroes_to_right(set, current, &count);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get next element of bitset.");

    /* for the next element in the iteration, the return value would be the
       number of zeroes to the right of current but add 1 more for the first
       non-zero value and add that to current. */
    *next = current + count + 1;

    if (*next >= set->size)
        no_err_return(DRAGON_BITSET_ITERATION_COMPLETE);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Dump a bitset to standard output for debug purposes.
 *
 *  This prints the contents of a bitset as a hex dump to standard output.
 *
 *  @param title A null-terminated string to identify the dump in the output.
 *  @param set A pointer to a bitset handle.
 *  @param indent A null-terminated string (usually spaces) that prefixes each line of output in the dump.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER**
 */

dragonError_t
dragon_bitset_dump(const char* title, const dragonBitSet_t* set, const char* indent)
{
    return dragon_bitset_dump_to_fd(stdout, title, set, indent);
}

/** @brief Dump a bitset to a file descriptor for debug purposes.
 *
 *  Print a dump to the file *fd*. Exactly the same of :c:func:`dragon_bitset_dump` except you can specify an open, writable file.
 *
 *  @param fd An open, writable file descriptor.
 *  @param title A null-terminated string to identify the dump in the output.
 *  @param set A pointer to a bitset handle.
 *  @param indent A null-terminated string (usually spaces) that prefixes each line of output in the dump.
 *  @return
 *      * **DRAGON_SUCCESS** It did its job.
 *      * **DRAGON_BITSET_NULL_POINTER** Indicates an incorrect call. The fd, title, set, and indent must all
 *        be non-null. A bad, non-null, pointer would result in a segfault.
 */

dragonError_t
dragon_bitset_dump_to_fd(FILE* fd, const char* title, const dragonBitSet_t* set, const char* indent)
{
    if (fd == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The file pointer is NULL.");

    if (title == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The title is NULL.");

    if (set == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The dragonBitSet handle pointer is NULL.");

    if (indent == NULL)
        err_return(DRAGON_BITSET_NULL_POINTER,"The indent is NULL.");

    fprintf(fd, "%s%s\n",indent,title);
    size_t num_bytes;
    num_bytes = dragon_bitset_size(set->size);
    num_bytes = num_bytes - sizeof(size_t);
    fprintf(fd, "%sSize of bitset bytes in memory is %lu\n",indent,num_bytes);
    fprintf(fd, "%sThe number of bits in set is %lu\n", indent, set->size);
    fprintf(fd, "%sThe number of items currently in set is %lu\n", indent, *set->length);
    hex_dump_to_fd(fd, "BITS",(void*)set->data,(set->size+7)/8,indent);

    no_err_return(DRAGON_SUCCESS);
}