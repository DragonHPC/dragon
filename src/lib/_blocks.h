#ifndef HAVE_DRAGON_BLOCKS_H
#define HAVE_DRAGON_BLOCKS_H

#include <dragon/return_codes.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/* -----------------------------------------------------------------------------
 * The blocks datatype creates a table of fixed sized blocks that can be
 * allocated and deallocated. The blocks data structure provides a unique id
 * back to the caller on an allocation that identifies the location where data
 * is stored within the blocks structure. This is a low-level implementation
 * that requires the user provide the space for the blocks structure. The
 * dragon_blocks_size function returns the required size for max_entries blocks.
 * The init must be called on the space prior to using. The other methods work as
 * expected.
 *
 * In the interest of performance, entries in the blocks table are not copied when
 * getting them from the data structure. Instead, a pointer is returned (via a
 * reference parameter) to the actual entry in the blocks structure.
 *
 * NOTE: This implementation does NO dynamic memory allocation. The
 * max_entries is enforced as the maximum capacity of the blocks.
 *
 * NOTE: This implemenation is NOT threadsafe on its own. The user must do
 * appropriate locking for thread safety.
 * ----------------------------------------------------------------------------- */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dragonBlocksStats_st {
    uint64_t num_blocks; /* number of slots that could be used. */
    uint64_t value_len; /* length of the values stored in blocks. */
    uint64_t current_count; /* number of in use blocks at this moment. */
    uint64_t max_count; /* maximum that were in use in lifetime of blocks structure */
} dragonBlocksStats_t;

typedef struct dragonBlocks_st {
    void* space;
    uint64_t num_slots;
    uint64_t* current;
    uint64_t* free_space_head;
    uint64_t value_len;
    uint64_t* num_used;
    uint64_t* num_inited;
    uint64_t* armor1;
    uint64_t* armor2;
    uint64_t* slots;
} dragonBlocks_t;

/* Blocks functions */

/**
 * @brief Compute required size for a Blocks structure.
 *
 * See init for complete description of a Blocks structure. This function computes the required size
 * for a Blocks structure with the given number of blocks and value length. If DRAGON_SUCCESS is returned
 * then size contains the required size.
 *
 * @param num_blocks The maximum number of blocks needed in the Blocks structure.
 * @param value_len Each block holds a value and this is the required size for the block not including its
 * identifier.
 * @param size This is a pointer to a uint64 to hold the required size.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_size(const uint64_t num_blocks, const uint64_t value_len, uint64_t* size);

/**
 * @brief Initialize a space to be used as a Blocks structure.
 *
 * A Blocks structure manages a collection of fixed size blocks that may be dynamically allocated and
 * freed as a program executes. When a block is allocated, it is assigned a unique identifier that
 * identifies the block for future lookup.
 *
 * @param space The space for the Blocks structure whose size is consistent with what was provided by calling
 * dragon_blocks_size with the same args.
 * @param blocks A blocks structure handle to be used to interact with it. This argument must point at a valid
 * struct of the correct type and will be initialized by this call.
 * @param num_blocks The maximum number of blocks needed in the Blocks structure.
 * @param value_len The size of each value stored in a block.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_init(void* space, dragonBlocks_t* blocks, const uint64_t num_slots, const uint64_t value_len);

/**
 * @brief Destroy a Blocks structure.
 *
 * Calling this destroys the blocks structure by resetting values within it so it may not be used as a Blocks structure
 * after the call.
 *
 * @param blocks A Blocks structure handle.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_destroy(dragonBlocks_t* blocks);

/**
 * @brief Attach to a pre-existing Blocks structure.
 *
 * Calling this attaches to an already initialized blocks structure.
 *
 * @param space The pre-initialized space for the Blocks structure.
 * @param blocks A Blocks structure handle.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_attach(void* space, dragonBlocks_t* blocks);

/**
 * @brief Detach a Blocks structure.
 *
 * Calling this detaches the Blocks structure handle by resetting values within it so it may not be used to
 * further call functions on the Blocks structure. The Blocks structure still exists and is initialized. It
 * just isn't connected to this handle.
 *
 * @param blocks A Blocks structure handle.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_detach(dragonBlocks_t* blocks);

/**
 * @brief Allocate a block from the Blocks structure.
 *
 * Calling this reserves a block in the block structure and copies the data into it pointed to by value
 * for the number of bytes specified in value_len with the Blocks structure was initialized. The value argument
 * must point to a valid value of value_len size. The id is initialized by this call to hold the identifier
 * for this block to be used on subsequent operations like get and free.
 *
 * @param blocks A pointer to a Blocks handle.
 * @param value A pointer to a value of value_len bytes (see init) to be stored in the block.
 * @param id A pointer to an identifier to be used to refer to the block later.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_alloc(dragonBlocks_t* blocks, const void* value, uint64_t* id);

/**
 * @brief Free a block
 *
 * Calling this frees a previously allocated block.
 *
 * @param blocks A pointer to a Blocks handle.
 * @param id A valid identifier of a block that was previously allocated.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_free(dragonBlocks_t* blocks, uint64_t id);

/**
 * @brief Access a block
 *
 * Calling this with a valid id fills in the space pointed to by value with the data
 * associated with the identified block.
 *
 * @param blocks a pointer to a Blocks handle
 * @param id A valid identifier of a block that was previously allocated.
 * @param value A pointer to space that must be value_len in size (see init) where the resulting
 * value will be stored assuming there was a sucessful lookup.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_get(const dragonBlocks_t* blocks, uint64_t id, void* value);

/**
 * @brief Count the number of blocks that match a value.
 *
 * The blocks structure contains values in blocks. This function counts the number of blocks that contain a given value. The
 * offset is used as an offset into the block to start comparing for len bytes. This provides a substring comparison for the
 * bytes in the block. To count all of the blocks regardless of their content (i.e. to get a count of all used blocks) you
 * can pass NULL for the value.
 *
 * @param blocks a pointer to a Blocks handle
 * @param value a pointer to space that should be used in the substring comparison
 * @param offset an offset into the blocks space where the comparison should start
 * @param len the number of bytes to be compared
 * @param num_blocks a pointer to a uint64 that will hold the result on successful completion.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 * */
dragonError_t dragon_blocks_count(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* num_blocks);

/**
 * @brief Get the first block to start an iteration over the blocks
 *
 * This function is used to start iterating over the blocks, looking for blocks which contain a substring that matches the given
 * value provided to the function. The given value and the substring both are assumed to have len bytes to be compared. The id
 * will be set to the first block (not chronological or otherwise ordered) with matching substring. If len and offset are 0, then
 * this returns the first allocated block of the Blocks structure (again unordered).
 *
 * @param blocks a pointer to a Blocks handle
 * @param value a pointer to space that should be used in the substring comparison
 * @param offset an offset into the blocks space where the comparison should start
 * @param len the number of bytes to be compared
 * @param id a pointer to an uint64 that will contain the id of the first found block that matches upon successful
 * completion of the call.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_first(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* id);

/**
 * @brief  Get the next block in an iteration of blocks
 *
 * This function uses the id that is provided to continue an iteration of blocks that contain a substring that matches the given
 * value provided to the function. The given value and the substring both are assumed to have len bytes to be compared. The id
 * will be set to the next block (not chronological or otherwise ordered) with matching substring after the value provided on
 * in the id at the beginning of the call. If len and offset are 0, then this returns the next allocated block of the Blocks
 * structure (again unordered) without regard to any substring matching.
 *
 * @param blocks a pointer to a Blocks handle
 * @param value a pointer to space that should be used in the substring comparison
 * @param offset an offset into the blocks space where the comparison should start
 * @param len the number of bytes to be compared
 * @param id a pointer to an uint64 that contains the id of the previous found block that matches. Upon successful
 * completion of the call it will be updated to contain the next id in the iteration.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_next(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* id);

/**
 * @brief Get statistics from the Blocks structure.
 *
 * @param blocks a pointer to a Blocks handle
 * @param stats a pointer to a Blocks Statistics struct that will contain the stats upon successful completion.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_stats(const dragonBlocks_t* blocks, dragonBlocksStats_t* stats);

/**
 * @brief Dump a hex dump and related meta data for debugging
 *
 * Calling this will dump a hex dump of the Blocks structure for debugging. The indent can be used to indent
 * inside a larger dump for formatting purposes. The title is printed just before the dump. The dump is printed
 * to stdout.
 *
 * @param title a character string title
 * @param blocks a pointer to a Blocks handle
 * @param indent a character string to be prepended to each line in the dump.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_dump(const char* title, const dragonBlocks_t* blocks, const char* indent);

/**
 * @brief Dump a hex dump and related meta data to a file descriptor for debugging
 *
 * This does exactly the same thing as the dump function except that you can direct it to an open file descriptor.
 *
 * @param fd an open file descriptor
 * @param title a character string title
 * @param blocks a pointer to a Blocks handle
 * @param indent a character string to be prepended to each line in the dump.
 * @return DRAGON_SUCCESS if called correctly. Otherwise an error code to indicate the problem. You may call
 * dragon_get_rc_string to convert the return code to a printable string and dragon_getlasterrstr to get a
 * traceback that provides more information about the problem.
 */
dragonError_t dragon_blocks_dump_to_fd(FILE* fd, const char* title, const dragonBlocks_t* blocks, const char* indent);

#endif