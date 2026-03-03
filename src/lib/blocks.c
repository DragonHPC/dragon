#include "_blocks.h"
#include "_hexdump.h"
#include "err.h"
#include <stddef.h>
#include <math.h>
#include <stdbool.h>
#include <string.h>
#include <stdatomic.h>

#define armorval 0xff01ff02ff03ff04
#define EOLL 0xffffffffffffffff

typedef struct dragonBlocksEntry_st {
    uint64_t id;
    char data;
} dragonBlocksEntry_t;

#define _check_armor(blocks) ({\
    if (*(blocks->armor1) != armorval) {\
        err_return(DRAGON_INVALID_ARGUMENT,"This is not a valid Blocks structure (1).");\
    }\
    if (*(blocks->armor2) != armorval) {\
        err_return(DRAGON_INVALID_ARGUMENT,"This is not a valid Blocks structure (2).");\
    }\
})

void _map_header(dragonBlocks_t* blocks, uint64_t* map) {
    blocks->space = map;
    blocks->num_slots = map[0];
    blocks->current = (_Atomic(uint64_t)*)&map[1];
    blocks->free_space_head = &map[2];
    blocks->value_len = map[3];
    blocks->num_used = (_Atomic(uint64_t)*)&map[4];
    blocks->num_inited = (_Atomic(uint64_t)*)&map[5];
    blocks->armor1 = &map[6];
    blocks->armor2 = &map[7];
    blocks->slots = &map[8];
}

void _unmap_header(dragonBlocks_t* blocks) {
    blocks->space = NULL;
    blocks->num_slots = 0UL;
    blocks->current = NULL;
    blocks->free_space_head = NULL;
    blocks->value_len = 0UL;
    blocks->num_used = NULL;
    blocks->num_inited = NULL;
    blocks->armor1 = NULL;
    blocks->armor2 = NULL;
    blocks->slots = NULL;
}

dragonError_t dragon_blocks_size(const uint64_t num_blocks, const uint64_t value_len, size_t* size) {
    if (size == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The size variable cannot be NULL");

    *size = 0;

    if (value_len % 8 != 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The value_len must be a multiple of 8 bytes.");

    // size of two pointers is subtracted because slots and space are not part of header and comes immediately
    // after it, but is included in data structure for convenience. The rest of the data is all stored
    // in shared mem in the header.
    *size = num_blocks * (value_len + sizeof(uint64_t)) + sizeof(dragonBlocks_t) - 2*sizeof(uint64_t*);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_init(void* space, dragonBlocks_t* blocks, const uint64_t num_blocks, const uint64_t value_len) {
    size_t size;
    dragonError_t err;
    uint64_t* map;
    uint64_t computed_space;

    err = dragon_blocks_size(num_blocks, value_len, &size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not compute required size for blocks structure.");

    map = (uint64_t*) space;

    map[0] = num_blocks;
    map[1] = 1UL; // current
    map[2] = EOLL; // free_space_head - EOLL (end of linked list, technically not needed)
    map[3] = value_len;
    map[4] = 0UL; // num_used
    map[5] = 0UL; // num_inited
    map[6] = armorval; // alignment
    map[7] = armorval; // alignment

    _map_header(blocks, map);

    computed_space = (uint64_t)blocks->slots + ((sizeof(uint64_t)+value_len) * num_blocks) - (uint64_t)space;

    if (computed_space != size)
        err_return(DRAGON_FAILURE, "The computed space for the blocks structure did not match the required size.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_destroy(dragonBlocks_t* blocks) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks argument cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    uint64_t* map = (uint64_t*)blocks->space;
    map[0] = 0UL;
    map[1] = 0UL;
    map[2] = 0UL;
    map[3] = 0UL;
    map[4] = 0UL;
    map[5] = 0UL;
    map[6] = 0UL;
    map[7] = 0UL;

    _unmap_header(blocks);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_attach(void* space, dragonBlocks_t* blocks) {
    if (space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The space pointer for blocks cannot be null.");

    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    _map_header(blocks, (uint64_t*)space);

    _check_armor(blocks);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_detach(dragonBlocks_t* blocks) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    _unmap_header(blocks);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_alloc(dragonBlocks_t* blocks, const void* value, uint64_t* id) {
    char err_msg[200];

    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (value == NULL && blocks->value_len > 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The value pointer cannot be null.");

    if (id == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The id pointer cannot be null.");

    _check_armor(blocks);

    uint64_t idx;

    if (atomic_load(blocks->num_used) == blocks->num_slots) {
        snprintf(err_msg, 199, "Out of space in the blocks structure.\nThere are %" PRIu64 " blocks and all of them are in use.", blocks->num_slots);
        err_return(DRAGON_OUT_OF_SPACE, err_msg);
    }

    dragonBlocksEntry_t* entry = NULL;

    if (*blocks->num_used == *blocks->num_inited) {
        idx = atomic_fetch_add(blocks->num_inited, 1UL);
        entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));
    } else {
        idx = *blocks->free_space_head;
        entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));
        *blocks->free_space_head = entry->id;
    }

    if (idx > blocks->num_slots)
        err_return(DRAGON_FAILURE, "The computed index was too large.");

    uint64_t current = atomic_fetch_add(blocks->current, 1UL);

    /* Check if current is too large. Very unlikely to reach this point. */
    uint64_t end_of_range = EOLL; // -1 if it were signed. max uint64_t value
    if (end_of_range - current * blocks->num_slots <= blocks->num_slots) {
        /* This rotates current when current reaches the limit of what it can
           be before we would have an overflow of the id assignment below. */
        *blocks->current = 2;
        current = 1;
    }

    entry->id = current * blocks->num_slots + idx;
    *id = entry->id;
    memcpy(&entry->data, value, blocks->value_len);
    atomic_fetch_add(blocks->num_used, 1UL);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_free(dragonBlocks_t* blocks, uint64_t id) {
    char err_str[200];

    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    uint64_t idx = id % blocks->num_slots;
    dragonBlocksEntry_t* entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));

    if (entry->id < blocks->num_slots)
        err_return(DRAGON_INVALID_ARGUMENT, "The block being freed is not currently in use.");

    if (entry->id != id) {
        snprintf(err_str, 199, "The block being freed is not owned by this identifier. Owner is %" PRIu64 " and id is %" PRIu64 ",\n", entry->id, id);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    entry->id = *blocks->free_space_head;
    *blocks->free_space_head = idx;
    memset(&entry->data, 0, blocks->value_len); // wouldn't need to be done, but zeroing out here.
    *blocks->num_used -= 1;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_get(const dragonBlocks_t* blocks, uint64_t id, void* value) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument must point at valid space for returning the id's value.");

    _check_armor(blocks);

    uint64_t idx = id % blocks->num_slots;
    dragonBlocksEntry_t* entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));

    if (entry->id < blocks->num_slots)
        err_return(DRAGON_INVALID_ARGUMENT, "The block being addressed is not currently in use.");

    if (entry->id != id)
        err_return(DRAGON_KEY_NOT_FOUND, "The id is not a key in the blocks structure.");

    memcpy(value, &entry->data, blocks->value_len);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_count(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* num_blocks) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    if (num_blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The num_blocks argument must point at valid space for returning the number of occurrences of the value.");

    _check_armor(blocks);

    if (value == NULL) {
        // Asking for a count of all entries if no value provided.
        *num_blocks = *blocks->num_used;
        no_err_return(DRAGON_SUCCESS);
    }

    if (len == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The len argument cannot be zero unless the value argument is also NULL.");

    if (offset+len > blocks->value_len)
        err_return(DRAGON_INVALID_ARGUMENT, "The offset plus the length is greater than the length of the stored value.");

    *num_blocks = 0;

    for (uint64_t idx=0; idx<*blocks->num_inited; idx++) {
        dragonBlocksEntry_t* entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));
        void* substring = &entry->data + offset;

        if (entry->id >= blocks->num_slots && entry->id != EOLL && !memcmp(value, substring, len))
            *num_blocks += 1;
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_first(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* id) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (id == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The id pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    if (offset+len > blocks->value_len)
        err_return(DRAGON_INVALID_ARGUMENT, "The offset plus the length is greater than the length of the stored value.");

    for (uint64_t idx=0; idx<*blocks->num_inited; idx++) {
        dragonBlocksEntry_t* entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));
        void* substring = &entry->data + offset;

        if (entry->id >= blocks->num_slots && entry->id != EOLL && !memcmp(value, substring, len)) {
            *id = entry->id;
            no_err_return(DRAGON_SUCCESS);
        }
    }

    err_return(DRAGON_BLOCKS_ITERATION_COMPLETE, "There were no occurrences of the value.");
}

dragonError_t dragon_blocks_next(const dragonBlocks_t* blocks, void* value, uint64_t offset, uint64_t len, uint64_t* id) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (id == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The id pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    if (offset+len > blocks->value_len)
        err_return(DRAGON_INVALID_ARGUMENT, "The offset plus the length is greater than the length of the stored value.");

    uint64_t start_idx = *id % blocks->num_slots;
    dragonBlocksEntry_t* entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + start_idx * (blocks->value_len + sizeof(uint64_t)));

    if (entry->id < blocks->num_slots)
        err_return(DRAGON_INVALID_ARGUMENT, "The start block being addressed is not currently in use.");

    if (entry->id != *id)
        err_return(DRAGON_INVALID_ARGUMENT, "The start block being addressed is not valid for this identifier.");

    for (uint64_t idx=start_idx+1; idx<*blocks->num_inited; idx++) {
        entry = (dragonBlocksEntry_t*)((uint64_t)blocks->slots + idx * (blocks->value_len + sizeof(uint64_t)));
        void* substring = &entry->data + offset;

        if (entry->id >= blocks->num_slots && entry->id != EOLL && !memcmp(value, substring, len)) {
            *id = entry->id;
            no_err_return(DRAGON_SUCCESS);
        }
    }

    err_return(DRAGON_BLOCKS_ITERATION_COMPLETE, "No more occurrences of value were found.");
}

dragonError_t dragon_blocks_stats(const dragonBlocks_t* blocks, dragonBlocksStats_t* stats) {
    if (blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure pointer cannot be null.");

    if (stats == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stats structure pointer cannot be null.");

    if (blocks->space == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The blocks structure does not appear to be attached.");

    _check_armor(blocks);

    stats->num_blocks = blocks->num_slots;
    stats->value_len = blocks->value_len;
    stats->current_count = *blocks->num_used;
    stats->max_count = *blocks->num_inited;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_dump(const char* title, const dragonBlocks_t* blocks, const char* indent) {
    dragonError_t derr = dragon_blocks_dump_to_fd(stdout, title, blocks, indent);

    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "Unable to dump blocks to stdout.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_blocks_dump_to_fd(FILE* fd, const char* title, const dragonBlocks_t* blocks, const char* indent) {
    dragonError_t rc;
    dragonBlocksStats_t stats;

    if (fd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The file pointer is NULL.");

    if (title == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The title is NULL.");

    if (indent == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The indent is NULL.");

    rc = dragon_blocks_stats(blocks, &stats);
    if (rc != DRAGON_SUCCESS)
        append_err_return(rc, "Unable to dump blocks to file descriptor.");

    fprintf(fd, "%s%s\n",indent,title);
    fprintf(fd, "%sNumber of blocks: %" PRIu64 "\n",indent,stats.num_blocks);
    fprintf(fd, "%sOccupied Blocks: %" PRIu64 "\n", indent, stats.current_count);
    fprintf(fd, "%sLifetime Maximum Occupied Blocks: %" PRIu64 "\n", indent, stats.max_count);
    fprintf(fd, "%sValue length: %" PRIu64 "\n", indent, stats.value_len);

    uint64_t total_space = ((void*)(blocks->slots) - blocks->space) + stats.max_count * (stats.value_len + sizeof(uint64_t));

    hex_dump_to_fd(fd, "BLOCKS", blocks->space, total_space, indent);

    no_err_return(DRAGON_SUCCESS);
}
