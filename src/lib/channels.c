#include "_channels.h"
#include "_utils.h"
#include "err.h"
#include "hostid.h"
#include <assert.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <time.h>

static dragonMap_t* dg_channels = NULL;
static dragonList_t* dg_gateways = NULL;

/* used to verify header assignment in channels */
static bool _header_checked = false;

static const int dg_num_gateway_types = 3;

/* below are macros that cover locking and unlocking the UT and OT locks with
 * error handling */
#define _obtain_ut_lock(channel)                                                                             \
    ({                                                                                                       \
        dragonError_t err = dragon_lock(&channel->ut_lock);                                                  \
        if (err != DRAGON_SUCCESS)                                                                           \
            append_err_return(err, "unable to obtain UT lock");                                              \
    })

#define _release_ut_lock(channel)                                                                            \
    ({                                                                                                       \
        dragonError_t err = dragon_unlock(&channel->ut_lock);                                                \
        if (err != DRAGON_SUCCESS)                                                                           \
            append_err_return(err, "unable to release UT lock");                                             \
    })

#define _obtain_ot_lock(channel)                                                                             \
    ({                                                                                                       \
        dragonError_t err = dragon_lock(&channel->ot_lock);                                                  \
        if (err != DRAGON_SUCCESS)                                                                           \
            append_err_return(err, "unable to obtain OT lock");                                              \
    })

#define _release_ot_lock(channel)                                                                            \
    ({                                                                                                       \
        dragonError_t err = dragon_unlock(&channel->ot_lock);                                                \
        if (err != DRAGON_SUCCESS)                                                                           \
            append_err_return(err, "unable to release OT lock");                                             \
    })

#define _obtain_channel_locks(channel)                                                                       \
    ({                                                                                                       \
        dragonError_t err = dragon_lock(&channel->ut_lock);                                                  \
        if (err != DRAGON_SUCCESS)                                                                           \
            append_err_return(err, "unable to obtain UT lock");                                              \
        err = dragon_lock(&channel->ot_lock);                                                                \
        if (err != DRAGON_SUCCESS) {                                                                         \
            dragon_unlock(&channel->ut_lock);                                                                \
            append_err_return(err, "unable to obtain OT lock");                                              \
        }                                                                                                    \
    })

static void
_release_channel_locks(dragonChannel_t* channel)
{
    dragon_unlock(&channel->ut_lock);
    dragon_unlock(&channel->ot_lock);
}

static dragonError_t
_fast_copy(const size_t bytes, const void* src, void* dest)
{
    // TODO, down the road we can contemplate fancy things like multi-threaded
    // copies

    /* this call assumes the pointers have been validated and the length is safe
     */
    memcpy(dest, src, bytes);

    no_err_return(DRAGON_SUCCESS);
}

/* obtain a channel structure from a given channel descriptor */
static dragonError_t
_channel_from_descr(const dragonChannelDescr_t* ch_descr, dragonChannel_t** ch)
{
    if (ch_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem_multikey(dg_channels, ch_descr->_rt_idx, ch_descr->_idx, (void*)ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in channels umap");

    no_err_return(DRAGON_SUCCESS);
}

/* given an rt_uid and c_uid, check if we already are attached to that channel and
 *  update the descriptor for use */
static dragonError_t
_channel_descr_from_uids(const dragonRT_UID_t rt_uid, const dragonC_UID_t c_uid, dragonChannelDescr_t* ch_descr)
{
    if (ch_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonChannel_t* channel;
    dragonError_t err = dragon_umap_getitem_multikey(dg_channels, rt_uid, c_uid, (void*)&channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in channels umap");

    /* update the descriptor with the m_uid key and note this cannot be original
     */
    // ch_descr->_original = 0; /* @MCB: Not used yet */
    ch_descr->_rt_idx = rt_uid;
    ch_descr->_idx = c_uid;

    no_err_return(DRAGON_SUCCESS);
}

/* insert a channel structure into the unordered map using the ch->_idx (i.e.
 * cuid) as the key */
static dragonError_t
_add_umap_channel_entry(const dragonChannelDescr_t* ch, const dragonChannel_t* newch)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (dg_channels == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        dg_channels = malloc(sizeof(dragonMap_t));
        if (dg_channels == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for channels");

        err = dragon_umap_create(dg_channels, DRAGON_CHANNEL_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for channels");
    }

    err = dragon_umap_additem_multikey(dg_channels, ch->_rt_idx, ch->_idx, newch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into channels umap");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Register a channel as a Gateway Channel
 *
 * This can be used to register a channel as a gateway channel
 * for the channels library. It maps the channel as a gateway channel
 * for the current process only by mapping it into a linked list
 * which can be used to quickly look up the next available gateway
 * channel when sending to a operating on a remote channel.
 *
 * @param ch is a pointer to the internal channel object.
 *
 * @returns DRAGON_SUCCESS or an error indicating the problem.
*/
static dragonError_t
_register_gateway(const dragonChannel_t* ch, dragonList_t** gateways)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (*gateways == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        *gateways = malloc(sizeof(dragonList_t));
        if (*gateways == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate ulist for gateway channels.");

        err = dragon_ulist_create(*gateways);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create ulist for gateway channels");
    }

    err = dragon_ulist_additem(*gateways, ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into gateway channels list");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unregister_gateway(const dragonChannel_t* ch, dragonList_t *gateways)
{
    if (gateways == NULL)
        err_return(DRAGON_CHANNEL_NO_GATEWAYS, "no gateways have been registered");

    dragonError_t err = dragon_ulist_delitem(gateways, ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item from gateway channels list");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_is_cuid_a_gateway(dragonC_UID_t c_uid, bool* is_gw)
{
    dragonError_t err;
    dragonChannel_t* channel;

    if (dg_gateways == NULL) {
        *is_gw = false;
        no_err_return(DRAGON_SUCCESS);
    }

    size_t size = dragon_ulist_get_size(dg_gateways);

    for (size_t k = 0; k < size; k++) {
        err = dragon_ulist_get_current_advance(dg_gateways, (void**)&channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not advance and get with gateway channel iterator.");

        if (*channel->header.c_uid == c_uid) {
            *is_gw = true;
            no_err_return(DRAGON_SUCCESS);
        }
    }

    *is_gw = false;
    no_err_return(DRAGON_SUCCESS);
}

/* compute the required storage size for the requested channel */
static size_t
_channel_allocation_size(const dragonChannelAttr_t* attr)
{
    /* the channel requires:
        2 priority heaps
        2 locks
        3 bcast objects
        space for the header region (including serialized pool descriptor)
        space for the event bcasts which includes:
            The event mask for the channel set events for this bcast object.
            The index into the channel set it corresponds to. Used to report
       events. The length of the serialized descriptor for the bcast object. The
       serialized bcast object itself. the message blocks

    */
    size_t alloc_size = 0UL;
    size_t bcast_size;
    dragon_bcast_size(0, attr->max_spinners, NULL, &bcast_size);

    alloc_size += dragon_priority_heap_size(attr->capacity, DRAGON_CHANNEL_OT_PHEAP_NVALS);
    alloc_size += dragon_priority_heap_size(attr->capacity, DRAGON_CHANNEL_UT_PHEAP_NVALS);
    alloc_size += (2UL * dragon_lock_size(attr->lock_type));
    alloc_size += ((2UL + DRAGON_CHANNEL_NUM_POLL_BCASTS) * bcast_size);
    alloc_size += attr->max_event_bcasts * sizeof(dragonEventRec_t);

    alloc_size += DRAGON_CHANNEL_HEADER_NULINTS * sizeof(dragonULInt);
    // the serialized memory pool length is set to the max size because the
    // serialized pool descriptor can be later modified via the
    // dragon_channel_setattr function. So we can't use the current length and
    // know we have enough room to modify it later.
    alloc_size += dragon_memory_pool_max_serialized_len();
    alloc_size += (attr->capacity * attr->bytes_per_msg_block);

    return alloc_size;
}

/* assign all of the pointers in the header structure into the allocated memory
 * for the channel */
static void
_map_header(dragonChannel_t* ch)
{
    dragonULInt* hptr = (dragonULInt*)ch->local_main_ptr;
    // clang-format off
    ch->header.c_uid                      = &hptr[0];
    ch->header.bytes_per_msg_block        = &hptr[1];
    ch->header.capacity                   = &hptr[2];
    ch->header.lock_type                  = &hptr[3];
    ch->header.oflag                      = &hptr[4];
    ch->header.fc_type                    = &hptr[5];
    ch->header.max_spinners               = &hptr[6];
    ch->header.available_msgs             = &hptr[7];
    ch->header.available_blocks           = &hptr[8];
    ch->header.max_event_bcasts           = &hptr[9];
    ch->header.num_event_bcasts           = &hptr[10];
    ch->header.next_bcast_token           = &hptr[11];
    ch->header.barrier_count              = &hptr[12];
    ch->header.barrier_broken             = &hptr[13];
    ch->header.barrier_reset_in_progress  = &hptr[14];
    ch->header.ot_offset                  = &hptr[15];
    ch->header.ut_offset                  = &hptr[16];
    ch->header.ot_lock_offset             = &hptr[17];
    ch->header.ut_lock_offset             = &hptr[18];
    ch->header.recv_bcast_offset          = &hptr[19];
    ch->header.send_bcast_offset          = &hptr[20];
    ch->header.poll_bcasts_offset         = &hptr[21];
    ch->header.event_records_offset       = &hptr[22];
    ch->header.msg_blks_offset            = &hptr[23];
    ch->header.buffer_pool_descr_ser_len  = &hptr[24];
    ch->header.buffer_pool_descr_ser_data = (uint8_t*)&hptr[25];
    // clang-format on

    if (!_header_checked) {
        dragonULInt number_of_channel_header_entries =
          ((dragonULInt*)ch->header.buffer_pool_descr_ser_data) - hptr;
        assert(number_of_channel_header_entries == DRAGON_CHANNEL_HEADER_NULINTS);
        _header_checked = true;
    }
}

/* set the values in the header */
static dragonError_t
_assign_header(const dragonC_UID_t c_uid, const dragonChannelAttr_t* attr, dragonChannel_t* ch)
{
    dragonError_t err;

    // clang-format off
    *(ch->header.c_uid)                     = (dragonC_UID_t)c_uid;
    *(ch->header.bytes_per_msg_block)       = (dragonULInt)attr->bytes_per_msg_block;
    *(ch->header.capacity)                  = (dragonULInt)attr->capacity;
    *(ch->header.lock_type)                 = (dragonULInt)attr->lock_type;
    *(ch->header.oflag)                     = (dragonULInt)attr->oflag;
    *(ch->header.fc_type)                   = (dragonULInt)attr->fc_type;
    *(ch->header.max_spinners)              = (dragonULInt)attr->max_spinners;
    *(ch->header.available_msgs)            = 0;
    *(ch->header.available_blocks)          = attr->capacity;
    *(ch->header.max_event_bcasts)          = attr->max_event_bcasts;
    *(ch->header.num_event_bcasts)          = 0;
    *(ch->header.next_bcast_token)          = 1;
    *(ch->header.barrier_count)             = 0;
    *(ch->header.barrier_broken)            = 0;
    *(ch->header.barrier_reset_in_progress) = 0;
    // clang-format on

    if (attr->buffer_pool != NULL) {
        dragonMemoryPoolSerial_t pool_ser;

        err = dragon_memory_pool_serialize(&pool_ser, attr->buffer_pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot serialize pool descriptor");

        *(ch->header.buffer_pool_descr_ser_len) = pool_ser.len;
        _fast_copy(pool_ser.len, pool_ser.data, ch->header.buffer_pool_descr_ser_data);

        err = dragon_memory_pool_serial_free(&pool_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot free serialized pool descriptor");
    } else {
        // 0 length indicates no side buffer pool.
        *(ch->header.buffer_pool_descr_ser_len) = 0;
    }

    uint64_t header_len =
      (DRAGON_CHANNEL_HEADER_NULINTS * sizeof(dragonULInt)) + dragon_memory_pool_max_serialized_len();

    size_t lock_size = dragon_lock_size(attr->lock_type);
    size_t bcast_size;
    dragon_bcast_size(0, attr->max_spinners, NULL, &bcast_size);

    dragonULInt offset = header_len;
    *(ch->header.ot_offset) = offset;
    offset += dragon_priority_heap_size(attr->capacity, DRAGON_CHANNEL_OT_PHEAP_NVALS);

    *(ch->header.ut_offset) = offset;
    offset += dragon_priority_heap_size(attr->capacity, DRAGON_CHANNEL_UT_PHEAP_NVALS);

    *(ch->header.ot_lock_offset) = offset;
    offset += lock_size;

    *(ch->header.ut_lock_offset) = offset;
    offset += lock_size;

    *(ch->header.recv_bcast_offset) = offset;
    offset += bcast_size;

    *(ch->header.send_bcast_offset) = offset;
    offset += bcast_size;

    *(ch->header.poll_bcasts_offset) = offset;
    offset += bcast_size * DRAGON_CHANNEL_NUM_POLL_BCASTS;

    *(ch->header.event_records_offset) = offset;
    offset += attr->max_event_bcasts * sizeof(dragonEventRec_t);

    *(ch->header.msg_blks_offset) = offset;

    // Final offset not needed. Nothing follows the message blocks. But we use it
    // for some internal verification of function synchronicity.
    offset += (attr->capacity * attr->bytes_per_msg_block);

    if (offset != _channel_allocation_size(attr))
        err_return(DRAGON_FAILURE, "The _channel_allocation_size and "
                                   "_assign_header functions are not in sync.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_attrs_from_header(const dragonChannel_t* ch, dragonChannelAttr_t* attr)
{
    dragonError_t err;

    // TODO: Need a RW lock, which does not yet exist, to protect this
    // lock the channel so we don't get any partially written values if they
    // change

    attr->c_uid = *(ch->header.c_uid);
    attr->bytes_per_msg_block = *(ch->header.bytes_per_msg_block);
    attr->capacity = *(ch->header.capacity);
    attr->lock_type = *(ch->header.lock_type);
    attr->oflag = *(ch->header.oflag);
    attr->fc_type = *(ch->header.fc_type);
    attr->max_spinners = *(ch->header.max_spinners);
    attr->max_event_bcasts = *(ch->header.max_event_bcasts);
    attr->num_msgs = *(ch->header.available_msgs);
    attr->num_avail_blocks = *(ch->header.available_blocks);
    attr->broken_barrier = *(ch->header.barrier_broken) != 0;
    attr->barrier_count = *(ch->header.barrier_count);

    if (*(ch->header.buffer_pool_descr_ser_len) != 0) {
        // There is a side buffer pool so we need to put it in the attributes

        dragonMemoryPoolSerial_t side_buffer_ser;
        side_buffer_ser.data = ch->header.buffer_pool_descr_ser_data;
        side_buffer_ser.len = *(ch->header.buffer_pool_descr_ser_len);

        if (attr->buffer_pool == NULL) {
            // we must allocate space for the buffer pool descriptor since the
            // attributes currently don't have space for it allocated.
            attr->buffer_pool = (dragonMemoryPoolDescr_t*)malloc(sizeof(dragonMemoryPoolDescr_t));
        }

        err = dragon_memory_pool_attach(attr->buffer_pool, &side_buffer_ser);

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot attach to side buffer pool");
    } else {
        attr->buffer_pool = NULL;
    }

    dragonBCastDescr_t* recv_bcast = &((dragonChannel_t*)ch)->recv_bcast;
    dragonBCastDescr_t* send_bcast = &((dragonChannel_t*)ch)->send_bcast;
    err = dragon_bcast_num_waiting(recv_bcast, &attr->blocked_receivers);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get receivers waiting attribute from channel.");

    err = dragon_bcast_num_waiting(send_bcast, &attr->blocked_senders);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get sends waiting attribute from channel.");

    // release the lock

    no_err_return(DRAGON_SUCCESS);
}

/* assign the pointer to the location of the serialized bcast event objects in
   the channel */
static void
_map_event_records(dragonChannel_t* ch)
{
    ch->event_records = ch->local_main_ptr + *ch->header.event_records_offset;
}

/* assign the pointers for the message blocks into the required locations in
 * the channel */
static dragonError_t
_map_message_blocks(dragonChannel_t* ch)
{
    /* this will be freed in the channel_destroy call */
    ch->msg_blks_ptrs = malloc(sizeof(void*) * *(ch->header.capacity));
    if (ch->msg_blks_ptrs == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate message block pointers");

    uint64_t base = *(ch->header.msg_blks_offset);
    uint64_t step = *(ch->header.bytes_per_msg_block);
    for (uint64_t i = 0; i < *(ch->header.capacity); i++) {
        ch->msg_blks_ptrs[i] = (void*)&((char*)ch->local_main_ptr)[base + i * step];
    }

    no_err_return(DRAGON_SUCCESS);
}

/* initialize the UT and OT priority heaps */
static dragonError_t
_instantiate_priority_heaps(dragonChannel_t* ch)
{
    void* ot_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ot_offset)];
    void* ut_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ut_offset)];

    dragonPriorityHeapUint_t base = 2;
    // TODO, can make this smarter based on capacity

    dragonError_t err =
      dragon_priority_heap_init(&ch->ot, base, (dragonPriorityHeapLongUint_t) * (ch->header.capacity),
                                DRAGON_CHANNEL_OT_PHEAP_NVALS, ot_ptr);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate OT priority heap");

    err = dragon_priority_heap_init(&ch->ut, base, (dragonPriorityHeapLongUint_t) * (ch->header.capacity),
                                    DRAGON_CHANNEL_UT_PHEAP_NVALS, ut_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate UT priority heap");

    /* populate the UT with all of the message blocks.  we only need to track the
     * block index */
    for (dragonPriorityHeapLongUint_t i = 0; i < *(ch->header.capacity); i++) {
        err = dragon_priority_heap_insert_item(&ch->ut, &i);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "unable to add message block to UT");
    }

    *(ch->header.available_blocks) = *(ch->header.capacity);
    *(ch->header.available_msgs) = 0UL;

    no_err_return(DRAGON_SUCCESS);
}

/* destroy UT and OT priority heaps */
static dragonError_t
_destroy_priority_heaps(dragonChannel_t* ch)
{
    dragonError_t err = dragon_priority_heap_destroy(&ch->ot);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy OT priority heap");

    err = dragon_priority_heap_destroy(&ch->ut);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy UT priority heap");

    no_err_return(DRAGON_SUCCESS);
}

/* attach to the UT and OT priority heaps */
static dragonError_t
_attach_priority_heaps(dragonChannel_t* ch)
{
    void* ot_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ot_offset)];
    void* ut_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ut_offset)];

    dragonError_t err = dragon_priority_heap_attach(&ch->ot, ot_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach OT priority heap");

    err = dragon_priority_heap_attach(&ch->ut, ut_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach UT priority heap");

    no_err_return(DRAGON_SUCCESS);
}

/* detach the UT and OT priority heaps */
static dragonError_t
_detach_priority_heaps(dragonChannel_t* ch)
{
    dragonError_t err = dragon_priority_heap_detach(&ch->ot);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach OT priority heap");

    err = dragon_priority_heap_detach(&ch->ut);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach UT priority heap");

    no_err_return(DRAGON_SUCCESS);
}

/* initialize the OT and UT dragon locks */
static dragonError_t
_instantiate_channel_locks(const dragonChannelAttr_t* attr, dragonChannel_t* ch)
{
    void* ot_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ot_lock_offset)];
    void* ut_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ut_lock_offset)];

    dragonError_t err = dragon_lock_init(&ch->ot_lock, ot_ptr, attr->lock_type);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate OT lock");

    err = dragon_lock_init(&ch->ut_lock, ut_ptr, attr->lock_type);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate UT lock");

    no_err_return(DRAGON_SUCCESS);
}

/* destroy the OT and UT dragon locks */
static dragonError_t
_destroy_channel_locks(dragonChannel_t* ch)
{
    dragonError_t err = dragon_lock_destroy(&ch->ot_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy OT lock");

    err = dragon_lock_destroy(&ch->ut_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy UT lock");

    no_err_return(DRAGON_SUCCESS);
}

/* attach the OT and UT dragon locks */
static dragonError_t
_attach_channel_locks(dragonChannel_t* ch)
{
    void* ot_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ot_lock_offset)];
    void* ut_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.ut_lock_offset)];

    dragonError_t err = dragon_lock_attach(&ch->ot_lock, ot_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach OT lock");

    err = dragon_lock_attach(&ch->ut_lock, ut_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach UT lock");

    no_err_return(DRAGON_SUCCESS);
}

/* detach the OT and UT dragon locks */
static dragonError_t
_detach_channel_locks(dragonChannel_t* ch)
{
    dragonError_t err = dragon_lock_detach(&ch->ot_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach OT lock");

    err = dragon_lock_detach(&ch->ut_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach UT lock");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_instantiate_bcast_objects(dragonChannel_t* ch)
{
    void* recv_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.recv_bcast_offset)];
    void* send_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.send_bcast_offset)];
    void* poll_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.poll_bcasts_offset)];
    size_t bcast_size;
    dragon_bcast_size(0, *ch->header.max_spinners, NULL, &bcast_size);

    dragonError_t err;

    err = dragon_bcast_create_at(recv_ptr, bcast_size, 0, *(ch->header.max_spinners), NULL, &ch->recv_bcast);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not instantiate the receiver bcast object.");

    err = dragon_bcast_create_at(send_ptr, bcast_size, 0, *(ch->header.max_spinners), NULL, &ch->send_bcast);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not instantiate the sender bcast object.");

    for (int k = 0; k < DRAGON_CHANNEL_NUM_POLL_BCASTS; k++) {
        err = dragon_bcast_create_at(poll_ptr, bcast_size, 0, *(ch->header.max_spinners), NULL,
                                     &ch->poll_bcasts[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not instantiate the poll bcast object.");
        poll_ptr = poll_ptr + bcast_size;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_destroy_bcast_objects(dragonChannel_t* ch)
{
    dragonError_t err = dragon_bcast_destroy(&ch->recv_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy receiver bcast");

    err = dragon_bcast_destroy(&ch->send_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot destroy sender bcast");

    for (int k = 0; k < DRAGON_CHANNEL_NUM_POLL_BCASTS; k++) {
        err = dragon_bcast_destroy(&ch->poll_bcasts[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot destroy poll bcast");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_attach_bcast_objects(dragonChannel_t* ch)
{
    void* recv_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.recv_bcast_offset)];
    void* send_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.send_bcast_offset)];
    void* poll_ptr = (void*)&((char*)ch->local_main_ptr)[*(ch->header.poll_bcasts_offset)];
    size_t bcast_size;
    dragon_bcast_size(0, *ch->header.max_spinners, NULL, &bcast_size);

    dragonError_t err = dragon_bcast_attach_at(recv_ptr, &ch->recv_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach the receiver bcast");

    err = dragon_bcast_attach_at(send_ptr, &ch->send_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot attach the sender bcast");

    for (int k = 0; k < DRAGON_CHANNEL_NUM_POLL_BCASTS; k++) {
        err = dragon_bcast_attach_at(poll_ptr, &ch->poll_bcasts[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot attach the poll bcast");
        poll_ptr = poll_ptr + bcast_size;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_detach_bcast_objects(dragonChannel_t* ch)
{
    dragonError_t err = dragon_bcast_detach(&ch->recv_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach the receiver bcast");

    err = dragon_bcast_detach(&ch->send_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot detach sender bcast");

    for (int k = 0; k < DRAGON_CHANNEL_NUM_POLL_BCASTS; k++) {
        err = dragon_bcast_detach(&ch->poll_bcasts[k]);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot detach poll bcast");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_validate_attr(const dragonChannelAttr_t* attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel attribute");

    if (attr->bytes_per_msg_block < DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK)
        err_return(DRAGON_CHANNEL_BLOCKSIZE_BELOW_REQUIRED_MINIMUM, "Channel block size is too small");

    if (attr->capacity < DRAGON_CHANNEL_MINIMUM_CAPACITY)
        err_return(DRAGON_CHANNEL_CAPACITY_BELOW_REQUIRED_MINIMUM, "Channel capacity is too small");

    if (attr->fc_type < DRAGON_CHANNEL_FC_NONE || attr->fc_type > DRAGON_CHANNEL_FC_MSGS)
        err_return(DRAGON_CHANNEL_INVALID_FC_TYPE, "Invalid channel flow control value specified");

    if (attr->lock_type < DRAGON_LOCK_FIFO || attr->lock_type > DRAGON_LOCK_GREEDY)
        err_return(DRAGON_INVALID_LOCK_KIND, "Invalid lock type value specified");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_validate_and_copy_send_attrs(const dragonChannelSendAttr_t* sattrs, dragonChannelSendAttr_t* attr_cpy)
{
    if (sattrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel send attribute");

    if (attr_cpy == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel output send attribute");

    if (sattrs->return_mode < DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY ||
        sattrs->return_mode > DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid send return mode specified.");

    if (sattrs->wait_mode != DRAGON_IDLE_WAIT && sattrs->wait_mode != DRAGON_SPIN_WAIT && sattrs->wait_mode != DRAGON_ADAPTIVE_WAIT)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid send wait type specified.");

    attr_cpy->return_mode = sattrs->return_mode;
    attr_cpy->wait_mode = sattrs->wait_mode;
    attr_cpy->default_timeout = sattrs->default_timeout;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_validate_and_copy_recv_attrs(const dragonChannelRecvAttr_t* rattrs, dragonChannelRecvAttr_t* attr_cpy)
{
    if (rattrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel receive attributes");

    if (attr_cpy == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel output recv attribute");

    if (rattrs->default_notif_type != DRAGON_RECV_SYNC_SIGNAL &&
        rattrs->default_notif_type != DRAGON_RECV_SYNC_MANUAL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid recv default notification type specified.");

    if (rattrs->wait_mode != DRAGON_IDLE_WAIT && rattrs->wait_mode != DRAGON_SPIN_WAIT && rattrs->wait_mode != DRAGON_ADAPTIVE_WAIT)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid recv wait type specified.");

    attr_cpy->default_notif_type = rattrs->default_notif_type;
    attr_cpy->wait_mode = rattrs->wait_mode;
    attr_cpy->default_timeout = rattrs->default_timeout;
    attr_cpy->signal = rattrs->signal;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_copy_payload(dragonMemoryDescr_t* dest_mem_descr, const void* src_ptr, const size_t src_bytes)
{
    void* msg_ptr;
    dragonError_t err = dragon_memory_get_pointer(dest_mem_descr, &msg_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    /* Source is destination, do nothing */
    if (msg_ptr == src_ptr)
        no_err_return(DRAGON_SUCCESS);

    /* Assert our destination is sufficiently large to copy into */
    size_t dst_bytes;
    err = dragon_memory_get_size(dest_mem_descr, &dst_bytes);
    if (dst_bytes < src_bytes)
        err_return(DRAGON_CHANNEL_BUFFER_ERROR, "destination memory is too small for payload");

    /* copy the data into the final landing pad */
    err = _fast_copy(src_bytes, src_ptr, msg_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "unable to copy payload data into managed memory");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_pack_ot_item(dragonPriorityHeapLongUint_t* ot_item, const dragonPriorityHeapLongUint_t mblk,
              const dragonPriorityHeapLongUint_t nbytes,
              const dragonPriorityHeapLongUint_t is_serialized_descr, const dragonUUID sendhid,
              const dragonULInt clientid, const dragonULInt msg_hints)
{
    ot_item[0] = mblk;
    ot_item[1] = nbytes;
    ot_item[2] = is_serialized_descr;
    ot_item[3] = clientid;
    ot_item[4] = msg_hints;

    dragonError_t err = dragon_encode_uuid(sendhid, (void*)&ot_item[5]);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to encode uuid into OT");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unpack_ot_item(const dragonPriorityHeapLongUint_t* ot_item, dragonPriorityHeapLongUint_t* mblk,
                dragonPriorityHeapLongUint_t* nbytes, dragonPriorityHeapLongUint_t* is_serialized_descr,
                dragonUUID sendhid, dragonULInt* clientid, dragonULInt* msg_hints)
{
    *mblk = ot_item[0];
    *nbytes = ot_item[1];
    *is_serialized_descr = ot_item[2];
    *clientid = ot_item[3];
    *msg_hints = ot_item[4];

    dragonError_t err = dragon_decode_uuid((void*)&ot_item[5], sendhid);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to decode uuid from OT");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_put_serialized_desc_in_msg_blk(dragonMemoryDescr_t* payload, dragonPriorityHeapLongUint_t mblk,
                                dragonChannel_t* channel, size_t* serialized_desc_len)
{
    /* get a serialized descriptor for the destination */
    dragonMemorySerial_t dest_ser;
    dragonError_t err;
    err = dragon_memory_serialize(&dest_ser, payload);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot serialize original message memory descriptor");

    /* encode that into the message block */
    err = _fast_copy(dest_ser.len, dest_ser.data, channel->msg_blks_ptrs[mblk]);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "unable to buffer serialized message memory "
                               "descriptor into message block");

    /* We must return the size of the serialized descriptor */
    *serialized_desc_len = dest_ser.len;

    err = dragon_memory_serial_free(&dest_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot free serialized message memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_return_msgblk_to_ut(dragonChannel_t* channel, dragonPriorityHeapLongUint_t* mblk)
{
    dragonError_t err;

    /* obtain the UT lock */
    _obtain_ut_lock(channel);

    /* add the message block back onto the UT */
    err = dragon_priority_heap_insert_item(&channel->ut, mblk);
    if (err != DRAGON_SUCCESS) {
        _release_ut_lock(channel);
        append_err_return(err, "A message block could not be returned to usage "
                               "table of the channel.");
    }

    /* increment number of available message blocks */
    *(channel->header.available_blocks) += 1;

    /* release the UT lock */
    _release_ut_lock(channel);

    return DRAGON_SUCCESS;
}

static dragonError_t
_release_barrier_waiters(dragonChannel_t* channel, dragonULInt* num_waiters)
{
    dragonError_t err;
    int broken_val = -1;
    int k;
    int* val_ptr = &k;
    dragonPriorityHeapLongUint_t mblk;
    dragonPriorityHeapLongUint_t pri;
    dragonMessageAttr_t mattr;
    dragonUUID sendhid;

    *num_waiters = *(channel->header.barrier_count);

    if (*(channel->header.barrier_broken) && *num_waiters == 0)
        no_err_return(DRAGON_SUCCESS);

    if (*(channel->header.barrier_broken) || (*num_waiters < *(channel->header.capacity)))
        val_ptr = &broken_val; // abort() or reset(), return -1

    dragon_generate_uuid(sendhid);
    dragon_channel_message_attr_init(&mattr);

    for (k = 0; k < *num_waiters; k++) {
        /* pop a free message block. There will be one so we ignore error code.
         */
        err = dragon_priority_heap_extract_highest_priority(&channel->ut, &mblk, &pri);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Popping free message block from ut heap failed.");

        /* decrement the number of available message blocks */
        *(channel->header.available_blocks) -= 1;

        err = _fast_copy(sizeof(int), val_ptr, channel->msg_blks_ptrs[mblk]);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Copy of message value into channel failed.");

        /* add entry for the message block */
        dragonPriorityHeapLongUint_t ot_item[DRAGON_CHANNEL_OT_PHEAP_NVALS];
        err = _pack_ot_item(ot_item, mblk, sizeof(int), DRAGON_CHANNEL_MSGBLK_IS_NOT_SERDESCR, sendhid,
                            mattr.clientid, mattr.hints);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Packing of ot item failed.");

        err = dragon_priority_heap_insert_item(&channel->ot, ot_item);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Inserting item in ot failed.");

        /* increment number of available messages and decrement the available
         * blocks */
        *(channel->header.available_msgs) += 1;
    }

    /* At this point the channel is full, so signal. This would allow a process
       that just wanted to know when the barrier is triggered to learn that it
       was triggered by doing a channel poll on POLLFULL */
    dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLFULL - 1], NULL, NULL, 0);

    /* Seems like overkill here, but if there are channel sets monitoring
       POLLFULL we'll trigger them too. */
    for (int i = 0; i < *channel->header.num_event_bcasts; i++) {
        if (channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLFULL) {
            dragonBCastDescr_t bcast;
            dragonBCastSerial_t serialb;
            serialb.data = (void*)&channel->event_records[i].serialized_bcast;
            serialb.len = channel->event_records[i].serialized_bcast_len;
            err = dragon_bcast_attach(&serialb, &bcast);

            if (err == DRAGON_SUCCESS) {
                dragonChannelEventNotification_t event;
                event.user_token = channel->event_records[i].user_token;
                event.revent = DRAGON_CHANNEL_POLLFULL;
                err =
                  dragon_bcast_trigger_all(&bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
            }
        }
    }

    /* Now tell all barrier waiters to go wild */
    dragon_bcast_trigger_all(&channel->recv_bcast, NULL, NULL, 0);
    *(channel->header.barrier_count) = 0UL;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_release_message_block_and_trigger_bcasts(dragonChannel_t* channel, dragonPriorityHeapLongUint_t mblk)
{
    dragonError_t err;
    bool channel_empty = false; /* this is only here to remember if it was empty
                                   in a check below */

    /* obtain the UT lock */
    _obtain_ut_lock(channel);

    /* add the message block back onto the UT */
    err = dragon_priority_heap_insert_item(&channel->ut, &mblk);
    if (err != DRAGON_SUCCESS) {
        _release_ut_lock(channel);
        append_err_return(err, "A message block could not be returned to usage "
                               "table of the channel.");
    }

    /* increment number of available message blocks */
    *(channel->header.available_blocks) += 1;

    /* This is used in implementing the blocking send and must be done here
       to trigger a blocked sender if one exists */
    dragon_bcast_trigger_one(&channel->send_bcast, NULL, NULL, 0);

    /* This check below has been verified to work while checking available_msgs
       will not. Very subtle, but necessary. */
    if (*(channel->header.available_blocks) == *(channel->header.capacity))
        channel_empty = true; /* remember this for when we release the lock below */

    /* This must be done here to prevent the event bcast list from changing
       while we are iterating over it. The use of triggered_since_last_send
       below is an optimization that says that if trigger_all was already
       called on this event bcast and no intervening send has been done,
       then don't keep calling trigger. The poller has already been notified
       once of the availability of a block, so don't bother repeating it
       if no send on the channel has occurred. That speeds up the emptying
       of messages from the channel once the poll operation has reported that
       some messages have been sent */
    for (int i = 0; i < *channel->header.num_event_bcasts; i++) {
        if (((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLOUT) ||
             (channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLINOUT)) &&
            ((channel->event_records[i].triggered_since_last_send) == false)) {
            dragonBCastDescr_t bcast;
            dragonBCastSerial_t serialb;
            serialb.data = (void*)&channel->event_records[i].serialized_bcast;
            serialb.len = channel->event_records[i].serialized_bcast_len;
            err = dragon_bcast_attach(&serialb, &bcast);

            if (err == DRAGON_SUCCESS) {
                dragonChannelEventNotification_t event;
                event.user_token = channel->event_records[i].user_token;
                event.revent = DRAGON_CHANNEL_POLLOUT;
                err =
                  dragon_bcast_trigger_all(&bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
                /* if err == DRAGON_SUCCESS then there was a waiter that was
                 * notified */
                if (err == DRAGON_SUCCESS)
                    channel->event_records[i].triggered_since_last_send = true;
            } // else log it?
        }
        if ((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLEMPTY) && channel_empty) {
            dragonBCastDescr_t bcast;
            dragonBCastSerial_t serialb;
            serialb.data = (void*)&channel->event_records[i].serialized_bcast;
            serialb.len = channel->event_records[i].serialized_bcast_len;
            err = dragon_bcast_attach(&serialb, &bcast);

            if (err == DRAGON_SUCCESS) {
                dragonChannelEventNotification_t event;
                event.user_token = channel->event_records[i].user_token;
                event.revent = DRAGON_CHANNEL_POLLEMPTY;
                err =
                  dragon_bcast_trigger_all(&bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
            }
        }
        /* If this bcast is tracking POLLIN, then since a receive has been done,
           reset this bit */
        if ((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLIN) ||
            (channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLINOUT))
            channel->event_records[i].triggered_since_last_recv = true;
    }

    /* release the UT lock */
    _release_ut_lock(channel);

    /* doing this here means the available message slot could be gone before the
       event takes place, but there are windows anyway where this could happen
       with multiple senders, so by doing it here we require less synchronization
       within the channel. */

    dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLOUT - 1], NULL, NULL, 0);
    dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLINOUT - 1], NULL, NULL, 0);
    if (channel_empty)
        dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLEMPTY - 1], NULL, NULL, 0);

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_send_msg(dragonChannel_t* channel, const dragonUUID sendhid, const dragonMessage_t* msg_send, void* msg_ptr,
          size_t msg_bytes, dragonMemoryDescr_t* dest_mem_descr, const timespec_t* end_time_ptr,
          bool blocking)
{
    dragonError_t err;
    dragonError_t uterr;
    timespec_t remaining_time;
    timespec_t* remaining_time_ptr = NULL;
    timespec_t no_wait = { 0, 0 };
    bool channel_full = false; /* this is only here to remember if it was full in
                                  a check below */

    if (blocking) {
        /* if end_time_ptr is NULL, leave remaining_time_ptr pointing to NULL for
         * no time out */
        if (end_time_ptr != NULL)
            remaining_time_ptr = &remaining_time;
    } else
        remaining_time_ptr = &no_wait;

    /* If we are given an end_time_ptr of 0,0 then this also indicates no
       waiting/blocking */
    if (end_time_ptr != NULL && end_time_ptr->tv_nsec == 0 && end_time_ptr->tv_sec == 0) {
        blocking = false;
        remaining_time_ptr = &no_wait;
    }

    /* This is an optimization. It will be checked again below under the lock,
       but if it is full, then no sense in acquiring the lock */
    if ((blocking == false) && (atomic_load(channel->header.available_blocks) == 0))
        no_err_return(DRAGON_CHANNEL_FULL);

    /* obtain the UT lock */
    _obtain_ut_lock(channel);

    /* pop a free message block */
    dragonPriorityHeapLongUint_t mblk;
    dragonPriorityHeapLongUint_t pri;
    err = dragon_priority_heap_extract_highest_priority(&channel->ut, &mblk, &pri);
    if (err == DRAGON_PRIORITY_HEAP_EMPTY) {
        /* Under the ut lock, record that no message blocks are available
           by setting the available_blocks to 0 - not necessary, but just in
           case...*/
        *(channel->header.available_blocks) = 0;
        _release_ut_lock(channel);
        no_err_return(DRAGON_CHANNEL_FULL);
    }

    if (err != DRAGON_SUCCESS) {
        _release_ut_lock(channel);
        append_err_return(err, "unable to get item from UT");
    }

    /* decrement the number of available message blocks */
    *(channel->header.available_blocks) -= 1;

    /* release the UT lock */
    _release_ut_lock(channel);

    /* allow another blocked process to proceed if there are message blocks */
    if (*(channel->header.available_blocks) > 0) {
        /* Here we need to log if there were an error in calling this. However,
           it should not affect this path through the code. */
        dragon_bcast_trigger_one(&channel->send_bcast, NULL, NULL, 0);
    }

    dragonPriorityHeapLongUint_t is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_NOT_SERDESCR;

    if (dest_mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP) {
        is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_SERDESCR;

        err = _put_serialized_desc_in_msg_blk(msg_send->_mem_descr, mblk, channel, &msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer serialized original message "
                                "memory descriptor into message block");
            goto ch_send_fail;
        }
    } else if (dest_mem_descr != NULL) {
        // This still might be a no-copy/transfer of ownership path if the
        // dest_mem_descr and the message's memory descriptor are actually
        // pointing to the same memory. In that case, the _copy_payload
        // call below will return without actually making a copy.
        is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_SERDESCR;

        err = _copy_payload(dest_mem_descr, msg_ptr, msg_bytes);
        // if it is successful (even if source is destination), then the
        // _copy_payload worked.
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer message into destination memory");
            goto ch_send_fail;
        }

        err = _put_serialized_desc_in_msg_blk(dest_mem_descr, mblk, channel, &msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer serialized destination "
                                "message memory descriptor into message block");
            goto ch_send_fail;
        }
    } else if (msg_bytes <= *(channel->header.bytes_per_msg_block)) {
        is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_NOT_SERDESCR;

        /* if the caller did not specify a no-copy path, then we'll buffer
           through a message block if it fits. Write the buffer from the message
           into that block */
        err = _fast_copy(msg_bytes, msg_ptr, channel->msg_blks_ptrs[mblk]);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer message into message block");
            goto ch_send_fail;
        }
    } else if (*(channel->header.buffer_pool_descr_ser_len) != 0) {
        is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_SERDESCR;

        /* The message won't fit into the message block and a side buffer pool
           was provided to the channel, so use it */
        dragonMemoryPoolDescr_t pool;
        dragonMemoryDescr_t side_buffer;
        dragonMemoryPoolSerial_t ser_pool;
        ser_pool.len = *(channel->header.buffer_pool_descr_ser_len);
        ser_pool.data = channel->header.buffer_pool_descr_ser_data;

        err = dragon_memory_pool_attach(&pool, &ser_pool);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot attach to side-buffer pool");
            goto ch_send_fail;
        }

        // TODO, we need a concrete way to generate a unique ID for side-buffers
        // probably with help from managed memory.  For now, we'll use the
        // generic allocator err = dragon_memory_alloc_type(dest_mem_descr,
        // &channel->pool, msg_bytes,
        //                                                    DRAGON_MEMORY_ALLOC_CHANNEL_BUFFER,
        //                                                    c_uid);

        if (blocking && end_time_ptr != NULL) {
            err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
            if (err != DRAGON_SUCCESS) {
                append_err_noreturn("Timeout or unexpected error.");
                goto ch_send_fail;
            }
        }

        err = dragon_memory_alloc_blocking(&side_buffer, &pool, msg_bytes, remaining_time_ptr);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Unable to allocate memory for side-buffer from memory pool");
            goto ch_send_fail;
        }

        err = _copy_payload(&side_buffer, msg_ptr, msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer message into destination memory");
            goto ch_send_fail;
        }

        err = _put_serialized_desc_in_msg_blk(&side_buffer, mblk, channel, &msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer serialized destination "
                                "message memory descriptor into message block");
            goto ch_send_fail;
        }

        err = dragon_memory_pool_detach(&pool);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot detach from side-buffer pool");
            goto ch_send_fail;
        }
    } else {
        is_serialized_descr = DRAGON_CHANNEL_MSGBLK_IS_SERDESCR;

        /* if they did not specify a destination managed memory location and the
           message is bigger than a message block, we'll need to allocate a new
           managed memory destination location from the channel's pool. */

        dragonMemoryDescr_t side_buffer;
        // TODO, we need a concrete way to generate a unique ID for side-buffers
        // probably with help from managed memory.  For now, we'll use the
        // generic allocator err = dragon_memory_alloc_type(dest_mem_descr,
        // &channel->pool, msg_bytes,
        //  DRAGON_MEMORY_ALLOC_CHANNEL_BUFFER, c_uid);

        if (blocking && end_time_ptr != NULL) {
            err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
            if (err != DRAGON_SUCCESS) {
                append_err_noreturn("Timeout or unexpected error.");
                goto ch_send_fail;
            }
        }

        /* If remaining_time_ptr is not NULL and points to {0,0} then it is a non-blocking call.*/
        err = dragon_memory_alloc_blocking(&side_buffer, &channel->pool, msg_bytes, remaining_time_ptr);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not get memory for the message copy.");
            goto ch_send_fail;
        }

        err = _copy_payload(&side_buffer, msg_ptr, msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer message into destination memory");
            goto ch_send_fail;
        }

        err = _put_serialized_desc_in_msg_blk(&side_buffer, mblk, channel, &msg_bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("unable to buffer serialized message memory "
                                "descriptor into message block");
            goto ch_send_fail;
        }
    }

    dragonMessageAttr_t mattr;
    err = dragon_channel_message_getattr(msg_send, &mattr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to extract attributes from send message");

    /* obtain the OT lock */
    _obtain_ot_lock(channel);

    /* add entry for the message block */
    dragonPriorityHeapLongUint_t ot_item[DRAGON_CHANNEL_OT_PHEAP_NVALS];
    err = _pack_ot_item(ot_item, mblk, msg_bytes, is_serialized_descr, sendhid, mattr.clientid, mattr.hints);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_noreturn("Unable to pack item into OT.");
        goto ch_send_fail;
    }

    err = dragon_priority_heap_insert_item(&channel->ot, ot_item);
    if (err == DRAGON_PRIORITY_HEAP_FULL) {
        /* the error path should not be possible unless there is a bug and we
         * somehow leak blocks */
        _release_ot_lock(channel);
        err = DRAGON_CHANNEL_FULL;
        err_noreturn("the channel is full");
        goto ch_send_fail;
    }
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_noreturn("unable to add item to OT");
        goto ch_send_fail;
    }

    /* increment number of available messages */
    *(channel->header.available_msgs) += 1;

    /* This check below has been verified to work while checking available_blocks
       will not. Very subtle, but necessary. */
    if (*(channel->header.available_msgs) == *(channel->header.capacity))
        channel_full = true;

    /* This is used in implementing the blocking receive and must be done here
       to trigger a blocked receiver if one exists */
    dragon_bcast_trigger_one(&channel->recv_bcast, NULL, NULL, 0);

    /* This must be done here to prevent the event bcast list from changing
       while we are iterating over it. The use of triggered_since_last_recv
       below is an optimization that says that if trigger_all was already
       called on this event bcast and no intervening receive has been done,
       then don't keep calling trigger. The poller has already been notified
       once of the availability of a message, so don't bother repeating it
       if no receive on the channel has occurred. That speeds up the filling
       of messages into the channel once the poll operation has reported that
       some messages have been received */
    for (int i = 0; i < *channel->header.num_event_bcasts; i++) {
        if (((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLIN) ||
             (channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLINOUT)) &&
            (channel->event_records[i].triggered_since_last_recv == false)) {
            dragonBCastDescr_t bcast;
            dragonBCastSerial_t serialb;
            serialb.data = (void*)&channel->event_records[i].serialized_bcast;
            serialb.len = channel->event_records[i].serialized_bcast_len;
            err = dragon_bcast_attach(&serialb, &bcast);

            if (err == DRAGON_SUCCESS) {
                dragonChannelEventNotification_t event;
                event.user_token = channel->event_records[i].user_token;
                event.revent = DRAGON_CHANNEL_POLLIN;
                err =
                  dragon_bcast_trigger_all(&bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
                /* if err == DRAGON_SUCCESS then there was a waiter that was
                 * notified */
                if (err == DRAGON_SUCCESS)
                    channel->event_records[i].triggered_since_last_recv = true;
            } // else log it
        }
        if ((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLFULL) && channel_full) {
            dragonBCastDescr_t bcast;
            dragonBCastSerial_t serialb;
            serialb.data = (void*)&channel->event_records[i].serialized_bcast;
            serialb.len = channel->event_records[i].serialized_bcast_len;
            err = dragon_bcast_attach(&serialb, &bcast);

            if (err == DRAGON_SUCCESS) {
                dragonChannelEventNotification_t event;
                event.user_token = channel->event_records[i].user_token;
                event.revent = DRAGON_CHANNEL_POLLFULL;
                err =
                  dragon_bcast_trigger_all(&bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
            }
        }
        /* If this bcast is tracking POLLOUT, then since a send has been done,
           reset this bit */
        if ((channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLOUT) ||
            (channel->event_records[i].event_mask == DRAGON_CHANNEL_POLLINOUT))
            channel->event_records[i].triggered_since_last_send = false;
    }

    /* release the OT lock */
    _release_ot_lock(channel);

    /* doing this here means the available message could be gone before the event
       takes place, but there are windows anyway where this could happen with
       multiple readers, so by doing it here we require less synchronization
       within the channel. */
    dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLIN - 1], NULL, NULL, 0);
    dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLINOUT - 1], NULL, NULL, 0);
    if (channel_full)
        dragon_bcast_trigger_all(&channel->poll_bcasts[DRAGON_CHANNEL_POLLFULL - 1], NULL, NULL, 0);

    no_err_return(DRAGON_SUCCESS);

ch_send_fail:
    uterr = _return_msgblk_to_ut(channel, (dragonPriorityHeapLongUint_t*)&mblk);
    if (uterr != DRAGON_SUCCESS)
        append_err_return(uterr, "Multiple problems in channel_send_msg.");

    no_err_return(err);
}

static dragonError_t
_get_msg(dragonChannel_t* channel, dragonMessage_t* msg_recv, timespec_t* end_time_ptr, bool blocking)
{
    dragonMemoryDescr_t* msg_mem = NULL;
    dragonMemoryDescr_t* cached_mem = NULL;
    dragonError_t err;
    timespec_t remaining_time;
    timespec_t* remaining_time_ptr = NULL;
    timespec_t no_wait = { 0, 0 };

    if (blocking) {
        /* if end_time_ptr is NULL, leave remaining_time_ptr pointing to NULL for
         * no time out */
        if (end_time_ptr != NULL)
            remaining_time_ptr = &remaining_time;
    } else
        remaining_time_ptr = &no_wait;

    /* If we are given an end_time_ptr of 0,0 then this also indicates no
       waiting/blocking */
    if (end_time_ptr != NULL && end_time_ptr->tv_nsec == 0 && end_time_ptr->tv_sec == 0) {
        blocking = false;
        remaining_time_ptr = &no_wait;
    }

    /* This is an optimization. It will be checked again below under the lock,
       but if it is empty, then no sense in acquiring the lock */
    if ((blocking == false) && (atomic_load(channel->header.available_msgs) == 0))
        no_err_return(DRAGON_CHANNEL_EMPTY);

    /* obtain the OT lock */
    _obtain_ot_lock(channel);

    /* pull the highest priority message off of the OT */
    dragonPriorityHeapLongUint_t ot_item[DRAGON_CHANNEL_OT_PHEAP_NVALS];
    dragonPriorityHeapLongUint_t pri;
    err = dragon_priority_heap_peek_highest_priority(&channel->ot, ot_item, &pri);
    if (err == DRAGON_PRIORITY_HEAP_EMPTY) {
        /* Under the ot lock, record that no messages are available
           by setting the available_msgs to 0 - not necessary, but just in
           case...*/
        *(channel->header.available_msgs) = 0;
        _release_ot_lock(channel);
        no_err_return(DRAGON_CHANNEL_EMPTY);
    }

    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to get message due to unexpected error.");
    }

    dragonPriorityHeapLongUint_t mblk, src_bytes, is_serialized_descr;
    dragonMessageAttr_t mattr;
    err = _unpack_ot_item(ot_item, &mblk, &src_bytes, &is_serialized_descr, mattr.sendhid, &mattr.clientid,
                          &mattr.hints);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to unpack item from OT.");
    }

    if (msg_recv->_mem_descr != NULL) {
        /* If we have a pre-allocated landing pad, assert the message can fit
         * into it */
        /* If our landing pad is too small, leave the message on the heap and
         * fail out */
        size_t dest_mem_size;
        size_t src_mem_size = src_bytes;
        err = dragon_memory_get_size(msg_recv->_mem_descr, &dest_mem_size);
        if (err != DRAGON_SUCCESS) {
            _release_ot_lock(channel);
            append_err_return(err, "cannot obtain size from destination message memory descriptor");
        }

        if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {
            dragonMemoryDescr_t mem_descr;
            dragonMemorySerial_t mem_ser;
            mem_ser.data = (uint8_t*)channel->msg_blks_ptrs[mblk];
            mem_ser.len = src_bytes;
            err = dragon_memory_attach(&mem_descr, &mem_ser);
            if (err != DRAGON_SUCCESS) {
                _release_ot_lock(channel);
                err_return(DRAGON_CHANNEL_RECV_INVALID_SERIALIZED_MSG, "Cannot attach to serialized message");
            }
            err = dragon_memory_get_size(&mem_descr, &src_mem_size);
            if (err != DRAGON_SUCCESS) {
                _release_ot_lock(channel);
                err_return(DRAGON_CHANNEL_RECV_INVALID_SERIALIZED_MSG,
                           "Cannot get size of serialized message");
            }
        }

        if (dest_mem_size < src_mem_size) {
            _release_ot_lock(channel);
            char err_str[200];
            snprintf(err_str, 199,
                     "Destination memory size is %lu and source memory size is "
                     "%lu. The destination memory size is "
                     "too small.",
                     dest_mem_size, src_mem_size);
            err_return(DRAGON_CHANNEL_RECV_INVALID_DESTINATION_MEMORY, err_str);
        }
    } else {
        if (is_serialized_descr != DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {
            cached_mem = malloc(sizeof(dragonMemoryDescr_t));
            if (cached_mem == NULL) {
                _release_ot_lock(channel);
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory descriptor");
            }
            if (blocking && end_time_ptr != NULL) {
                err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
                if (err != DRAGON_SUCCESS) {
                    free(cached_mem);
                    _release_ot_lock(channel);
                    append_err_return(err, "Timer expired before getting "
                                           "memory to receive message.");
                }
            }

            /* If remaining_time_ptr points to {0,0} then this is a non-blocking call. */
            err = dragon_memory_alloc_blocking(cached_mem, &channel->pool, src_bytes, remaining_time_ptr);
            if (err != DRAGON_SUCCESS) {
                free(cached_mem);
                _release_ot_lock(channel);
                append_err_return(err, "unable to allocate new buffer from pool");
            }
        }
    }

    /* Pop the item off the heap */
    err = dragon_priority_heap_pop_highest_priority(&channel->ot);
    if (err != DRAGON_SUCCESS) {
        if (cached_mem != NULL)
            free(cached_mem);
        _release_ot_lock(channel);
        append_err_return(err, "Could not pop item from heap");
    }

    /* decrement the number of available messages */
    *(channel->header.available_msgs) -= 1;

    /* release the OT lock */
    _release_ot_lock(channel);

    /* allow another blocked process to proceed if there are messages */
    if (*(channel->header.available_msgs) > 0) {
        /* Here we need to log if there were an error in calling this. However,
           it should not affect this path through the code. */
        dragon_bcast_trigger_one(&channel->recv_bcast, NULL, NULL, 0);
    }

    /* based on the message size, determine what the source pointer is for the
     * payload data */
    void* src_ptr = channel->msg_blks_ptrs[mblk];

    if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {

        /* this can be freed in the message_destroy call */
        msg_mem = malloc(sizeof(dragonMemoryDescr_t));
        if (msg_mem == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory descriptor");

        dragonMemorySerial_t mem_ser;
        mem_ser.len = src_bytes;
        mem_ser.data = (uint8_t*)channel->msg_blks_ptrs[mblk];
        err = dragon_memory_attach(msg_mem, &mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot attach to payload memory");

        err = dragon_memory_get_pointer(msg_mem, &src_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "invalid memory descriptor for payload");
    }

    /* do any unpacking work */
    if (msg_recv->_mem_descr == NULL) {
        /* if the payload is already in a managed memory buffer just use that
         * directly */
        if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {
            msg_recv->_mem_descr = msg_mem;
        } else {
            /* We insured there was an allocation to place the data into above,
            so get it from the cached mem and then use it here */
            err = _copy_payload(cached_mem, src_ptr, (size_t)src_bytes);
            if (err != DRAGON_SUCCESS) {
                free(cached_mem);
                append_err_return(err, "unable to copy payload data from message into new message");
            }

            msg_recv->_mem_descr = cached_mem;
        }

        /* if the caller did provide destination memory, we must copy into that */
    } else {
        void* dest_ptr = NULL;
        err = dragon_memory_get_pointer(msg_recv->_mem_descr, &dest_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot get pointer to message destination.");

        if (dest_ptr == src_ptr) {
            /* There is nothing to copy since the data is already in the
               destination. However, the memory we malloc'ed to hold the memory
               descriptor of the source needs to be freed. */
            free(msg_mem);
        } else {
            /* We have some copying to do. */
            err = _fast_copy((size_t)src_bytes, src_ptr, dest_ptr);

            if (err != DRAGON_SUCCESS) {
                if (msg_mem != NULL)
                    free(msg_mem);

                append_err_return(err, "unable to copy payload data from "
                                       "message into provided message");
            }

            dragonMemoryDescr_t* clone = malloc(sizeof(dragonMemoryDescr_t));
            if (clone == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate landing pad memory descriptor");

            err = dragon_memory_descr_clone(clone, msg_recv->_mem_descr, 0, &src_bytes);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not complete clone of landing pad descriptor.");

            msg_recv->_mem_descr = clone;

            /* if the payload was in a serialized descriptor, we need to clean up
            that memory descriptor that we used to attach to the serialized
            descriptor's memory. */
            if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {
                err = dragon_memory_free(msg_mem);
                free(msg_mem);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not free attached serialized descriptor.");
            }
        }
    }

    err = dragon_channel_message_setattr(msg_recv, &mattr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to set attributes on received message");

    err = _release_message_block_and_trigger_bcasts(channel, mblk);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to release message block");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_peek_msg(dragonChannel_t* channel, dragonMessage_t* msg_peek)
{
    dragonMemoryDescr_t* cached_mem = NULL;
    dragonError_t err;

    /* This is an optimization. It will be checked again below under the lock,
       but if it is empty, then no sense in acquiring the lock */
    if (atomic_load(channel->header.available_msgs) == 0)
        no_err_return(DRAGON_CHANNEL_EMPTY);

    /* obtain the OT lock */
    _obtain_ot_lock(channel);

    /* pull the highest priority message off of the OT */
    dragonPriorityHeapLongUint_t ot_item[DRAGON_CHANNEL_OT_PHEAP_NVALS];
    dragonPriorityHeapLongUint_t pri;
    err = dragon_priority_heap_peek_highest_priority(&channel->ot, ot_item, &pri);
    if (err == DRAGON_PRIORITY_HEAP_EMPTY) {
        /* Under the ot lock, record that no messages are available
           by setting the available_msgs to 0 - not necessary, but just in
           case...*/
        *(channel->header.available_msgs) = 0;
        _release_ot_lock(channel);
        no_err_return(DRAGON_CHANNEL_EMPTY);
    }

    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to get message due to unexpected error.");
    }

    dragonPriorityHeapLongUint_t mblk, src_bytes, is_serialized_descr;
    dragonMessageAttr_t mattr;
    err = _unpack_ot_item(ot_item, &mblk, &src_bytes, &is_serialized_descr, mattr.sendhid, &mattr.clientid,
                          &mattr.hints);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to unpack item from OT.");
    }

    /* based on the message size, determine the source pointer for the payload
     * data, and the size if the message block had a serialized descriptor */
    void* src_ptr = channel->msg_blks_ptrs[mblk];
    dragonMemoryDescr_t msg_mem_descr;

    if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {

        dragonMemorySerial_t mem_ser;
        mem_ser.len = src_bytes;
        mem_ser.data = (uint8_t*)channel->msg_blks_ptrs[mblk];
        err = dragon_memory_attach(&msg_mem_descr, &mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot attach to payload memory");

        err = dragon_memory_get_pointer(&msg_mem_descr, &src_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "invalid memory descriptor for payload");

        err = dragon_memory_get_size(&msg_mem_descr, &src_bytes);
        if (err != DRAGON_SUCCESS) {
            _release_ot_lock(channel);
            err_return(DRAGON_CHANNEL_RECV_INVALID_SERIALIZED_MSG,
                       "cannot get size of serialized message");
        }
    }

    if (msg_peek->_mem_descr != NULL) {
        /* If we have a pre-allocated landing pad, assert the message can fit
         * into it */
        /* If our landing pad is too small, leave the message on the heap and
         * fail out */
        size_t dst_bytes;
        err = dragon_memory_get_size(msg_peek->_mem_descr, &dst_bytes);
        if (err != DRAGON_SUCCESS) {
            _release_ot_lock(channel);
            append_err_return(err, "cannot obtain size from destination message memory descriptor");
        }

        if (dst_bytes < src_bytes) {
            _release_ot_lock(channel);
            char err_str[200];
            snprintf(err_str, 199,
                     "Destination memory size is %lu and source memory size is "
                     "%lu. The destination memory size is "
                     "too small.",
                     dst_bytes, src_bytes);
            err_return(DRAGON_CHANNEL_RECV_INVALID_DESTINATION_MEMORY, err_str);
        }
    } else {
        cached_mem = malloc(sizeof(dragonMemoryDescr_t));
        if (cached_mem == NULL) {
            _release_ot_lock(channel);
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory descriptor");
        }

        timespec_t no_wait = { 0, 0 };

        err = dragon_memory_alloc_blocking(cached_mem, &channel->pool, src_bytes, &no_wait);
        if (err != DRAGON_SUCCESS) {
            free(cached_mem);
            _release_ot_lock(channel);
            append_err_return(err, "unable to allocate new buffer from pool");
        }
    }

    /* do any unpacking work */
    if (msg_peek->_mem_descr == NULL) {
        /* We insured there was an allocation to place the data into above,
        so get it from the cached mem and then use it here */
        err = _copy_payload(cached_mem, src_ptr, (size_t)src_bytes);
        if (err != DRAGON_SUCCESS) {
            free(cached_mem);
            append_err_return(err, "unable to copy payload data from message into new message");
        }

        msg_peek->_mem_descr = cached_mem;
    } else {
        /* if the caller did provide destination memory, we must copy into that */
        void* dest_ptr = NULL;
        err = dragon_memory_get_pointer(msg_peek->_mem_descr, &dest_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot get pointer to message destination");

        if (dest_ptr != src_ptr) {
            /* We have some copying to do. */
            err = _fast_copy((size_t)src_bytes, src_ptr, dest_ptr);

            if (err != DRAGON_SUCCESS)
                append_err_return(err, "unable to copy payload data from "
                                       "message into provided message");

            err = dragon_memory_modify_size(msg_peek->_mem_descr, src_bytes);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "failed to modify size of memory descriptor");
        } else {
            err_return(DRAGON_CHANNEL_BUFFER_ERROR, "user-provided destination buffer is the same as buffer from message block");
        }
    }

    /* release the OT lock */
    _release_ot_lock(channel);

    err = dragon_channel_message_setattr(msg_peek, &mattr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to set attributes on received message");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_pop(dragonChannel_t* channel)
{
    dragonError_t err;

    /* obtain the OT lock */
    _obtain_ot_lock(channel);

    dragonPriorityHeapLongUint_t ot_item[DRAGON_CHANNEL_OT_PHEAP_NVALS];
    dragonPriorityHeapLongUint_t pri;
    err = dragon_priority_heap_peek_highest_priority(&channel->ot, ot_item, &pri);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to pop message due to unexpected error.");
    }

    dragonPriorityHeapLongUint_t mblk, src_bytes, is_serialized_descr;
    dragonMessageAttr_t mattr;
    err = _unpack_ot_item(ot_item, &mblk, &src_bytes, &is_serialized_descr, mattr.sendhid, &mattr.clientid,
                          &mattr.hints);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Unable to unpack item from OT.");
    }

    /* Pop the item off the heap */
    err = dragon_priority_heap_pop_highest_priority(&channel->ot);
    if (err != DRAGON_SUCCESS) {
        _release_ot_lock(channel);
        append_err_return(err, "Could not pop item from heap");
    }

    /* decrement the number of available messages */
    *(channel->header.available_msgs) -= 1;

    /* release the OT lock */
    _release_ot_lock(channel);

    /* allow another blocked process to proceed if there are messages */
    if (*(channel->header.available_msgs) > 0) {
        /* Here we need to log if there were an error in calling this. However,
           it should not affect this path through the code. */
        dragon_bcast_trigger_one(&channel->recv_bcast, NULL, NULL, 0);
    }

    /* free the serialized descriptor that was in the message block */
    if (is_serialized_descr == DRAGON_CHANNEL_MSGBLK_IS_SERDESCR) {
        dragonMemoryDescr_t mem_descr;
        dragonMemorySerial_t mem_ser;

        mem_ser.len = src_bytes;
        mem_ser.data = (uint8_t*)channel->msg_blks_ptrs[mblk];

        err = dragon_memory_attach(&mem_descr, &mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot attach to payload memory");

        err = dragon_memory_free(&mem_descr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free message memory descriptor.");
    }

    err = _release_message_block_and_trigger_bcasts(channel, mblk);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to release message block");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_channel_serialize(const dragonChannelDescr_t* ch, dragonChannelSerial_t* ch_ser)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel serialized descriptor cannot be NULL.");

    /* We'll initialize variables in serialized descriptor here. They will be
       reset below when serialization is successful */
    ch_ser->len = 0;
    ch_ser->data = NULL;

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    /* get a serialized memory descriptor for the memory the channel is in */
    dragonMemorySerial_t mem_ser;
    err = dragon_memory_serialize(&mem_ser, &channel->main_mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain serialized memory descriptor for channel");

    /*
        the serialized descriptor should contain
            * the serialized memory descriptor for it's allocation
            * the c_uid
            * the hostid of where it is
    */
    ch_ser->len = mem_ser.len + DRAGON_CHANNEL_CHSER_NULINTS * sizeof(dragonULInt);
    /* This malloc will be freed in dragon_channel_serial_free() */
    ch_ser->data = malloc(ch_ser->len);
    if (ch_ser->data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,
                   "cannot allocate space for serialized channel descriptor data");

    dragonULInt* sptr = (dragonULInt*)ch_ser->data;

    *sptr = (dragonULInt) * (channel->header.c_uid);
    sptr++;

    /* finally copy in the memory descriptor */
    memcpy(sptr, mem_ser.data, mem_ser.len);

    /* free our serialized memory descriptor after we memcpy */
    err = dragon_memory_serial_free(&mem_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not release serialized memory descriptor after memcpy");

    no_err_return(DRAGON_SUCCESS);
}

static bool
_channel_is_masquerading(const dragonChannelDescr_t* ch)
{

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        return false;

    if (channel->proc_flags & DRAGON_CHANNEL_FLAGS_MASQUERADE_AS_REMOTE)
        return true;

    return false;
}

static dragonError_t
_attach_to_gateway(char *ip_addrs_key, dragonChannelDescr_t *gw_ch)
{
    dragonError_t err;
    char err_str[400];

    char *gw_str = getenv(ip_addrs_key);

    if (gw_str == NULL) {
        snprintf(err_str, 399, "NULL gateway descriptor for key=%s", ip_addrs_key);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    dragonChannelSerial_t gw_ser;
    gw_ser.data = dragon_base64_decode(gw_str, &gw_ser.len);
    if (gw_ser.data == NULL) {
        err_return(DRAGON_INVALID_ARGUMENT, "failed to decode string specifying gateway descriptor");
    }

    err = dragon_channel_attach(&gw_ser, gw_ch);
    if (err != DRAGON_SUCCESS) {
        append_err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_get_gw_idx(const dragonChannelDescr_t *ch, dragonChannelOpType_t op_type, int *gw_idx)
{
    dragonULInt target_hostid;

    dragonError_t err = dragon_channel_get_hostid(ch, &target_hostid);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to obtain hostid for target channel.");

    /* For tcp agents, there is always 1 gateway and the index is hence always 0.
     * For hsta agents, there will be a multiple of dg_num_gateway_types gateways,
     * and a group of dg_num_gateway_types for each nic in the multi-nic case.
     *
     * Example with 4 nics
     * -------------------
     *
     * send_msg get_msg poll  send_msg get_msg poll  send_msg get_msg poll  send_msg get_msg poll
     * \______/ \_____/ \__/  \______/ \_____/ \__/  \______/ \_____/ \__/  \______/ \_____/ \__/
     *   gw 0     gw 1  gw 2    gw 3     gw 4  gw 5    gw 6     gw 7  gw 8    gw 9    gw 10  gw 11
     * \___________________/  \___________________/  \___________________/  \___________________/
     *         nic 0                  nic 1                  nic 2                  nic 3
     */
    size_t num_gws = dragon_ulist_get_size(dg_gateways);

    if (num_gws == 1) {
        *gw_idx = 0;
    } else {
        int num_gw_groups = num_gws / dg_num_gateway_types;
        int my_gw_group = dragon_hash_ulint(target_hostid) % num_gw_groups;
        *gw_idx = (dg_num_gateway_types * my_gw_group) + op_type;
    }

    if (*gw_idx < 0 || num_gws <= *gw_idx) {
        char err_str[100];
        snprintf(err_str, 99,
                "Invalid gateway index: gateway idx=%d, num gateways=%lu.",
                *gw_idx, num_gws);
        append_err_return(err, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_get_gateway(const dragonChannelDescr_t *ch_descr, dragonChannelOpType_t op_type, dragonChannel_t** gw_channel)
{
    dragonError_t err;
    dragonChannel_t *channel = NULL;
    bool runtime_is_local;

    err = _channel_from_descr(ch_descr, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get channel from descriptor.");

    err = dragon_memory_pool_runtime_is_local(&channel->pool, &runtime_is_local);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not determine if channel is hosted by local runtime.");

    if (runtime_is_local) {
        int gw_idx;

        if (dg_gateways == NULL) {
            char err_str[400];
            dragonULInt rt_uid;

            err = dragon_memory_pool_get_rt_uid(&channel->pool, &rt_uid);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get pool's rt_uid.");

            snprintf(err_str, 399,
                     "There are no registered gateway channels and the channel is not local, "
                     "local and remote runtime ip addrs: %lu and %lu",
                     dragon_get_local_rt_uid(),
                     rt_uid);
            err_return(DRAGON_CHANNEL_NO_GATEWAYS, err_str);
        }

        err = _get_gw_idx(ch_descr, op_type, &gw_idx);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get a gateway index.");

        err = dragon_ulist_get_by_idx(dg_gateways, gw_idx, (void **) gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get gateway channel.");
    } else {
        dragonULInt rt_uid;
        dragonChannelDescr_t gw_ch;
        char ip_addrs_key[64];

        err = dragon_memory_pool_get_rt_uid(&channel->pool, &rt_uid);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pool's rt_uid.");

        sprintf(ip_addrs_key, "DRAGON_RT_UID__%lu", rt_uid);

        err = _attach_to_gateway(ip_addrs_key, &gw_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to gateway channel.");

        err = _channel_from_descr(&gw_ch, gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get gateway channel from descriptor.");
    }

    no_err_return(DRAGON_SUCCESS);
}

// BEGIN USER API

/** @defgroup channels_lifecycle Channels Lifecycle Functions
 *  @{
 */

/**
 * @brief Initialize a send attributes structure.
 *
 * When custom send attributes are desired, this function should be called first,
 * to initialize the to default values. Then the user may override desired
 * attributes before using it in creating a send handle.
 *
 * @param send_attr is a pointer to the send attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/

dragonError_t
dragon_channel_send_attr_init(dragonChannelSendAttr_t* send_attr)
{
    if (send_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "send_attrs cannot be NULL");

    send_attr->return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED;
    send_attr->default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;
    send_attr->wait_mode = DRAGON_DEFAULT_WAIT_MODE;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Initialize a receive attributes structure.
 *
 * When custom receive attributes are desired, this function should be called
 * first to initialize them to default values. Then the user may override
 * desired attributes before using it in creating a receive handle.
 *
 * @param recv_attr is a pointer to the receive attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channel_recv_attr_init(dragonChannelRecvAttr_t* recv_attr)
{
    if (recv_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "recv_attrs cannot be NULL");

    recv_attr->default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    recv_attr->default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;
    recv_attr->signal = 0;
    recv_attr->wait_mode = DRAGON_DEFAULT_WAIT_MODE;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Initialize a channel attributes structure.
 *
 * When custom channel attributes are desired, this function should be called
 * first to initialize them to default values. Then the user may override
 * desired attributes before using it in creating a channel.
 *
 * @param attr is a pointer to the channel attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channel_attr_init(dragonChannelAttr_t* attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "attr cannot be NULL");

    // cannot set a default dragonC_UID_t c_uid;
    attr->bytes_per_msg_block = DRAGON_CHANNEL_DEFAULT_BYTES_PER_BLOCK;
    attr->capacity = DRAGON_CHANNEL_DEFAULT_CAPACITY;
    attr->lock_type = DRAGON_CHANNEL_DEFAULT_LOCK_TYPE;
    attr->oflag = DRAGON_CHANNEL_DEFAULT_OMODE;
    attr->fc_type = DRAGON_CHANNEL_DEFAULT_FC_TYPE;
    attr->flags = DRAGON_CHANNEL_FLAGS_NONE;
    attr->buffer_pool = NULL;
    attr->max_spinners = DRAGON_CHANNEL_DEFAULT_SENDRECV_SPIN_MAX;
    attr->max_event_bcasts = DRAGON_CHANNEL_DEFAULT_MAX_EVENT_BCASTS;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy a channel send handle attributes structure.
 *
 * Release any allocations or resources associated with a send handle
 * attributes structure. This does not destroy the underlying channel.
 *
 * @param send_attr is a pointer to the channel send handle attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_send_attr_destroy(dragonChannelSendAttr_t* send_attr)
{
    if (send_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "send_attr cannot be NULL");

    // no allocations to free up for now

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy a channel recv handle attributes structure.
 *
 * Release any allocations or resources associated with a recv handle
 * attributes structure. This does not destroy the underlying channel.
 *
 * @param recv_attr is a pointer to the channel send handle attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_recv_attr_destroy(dragonChannelRecvAttr_t* recv_attr)
{
    if (recv_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "recv_attr cannot be NULL");

    // no allocations to free up for now

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy a channel attributes structure.
 *
 * Release any allocations in the attributes structure. This does not
 * destroy the underlying channel.
 *
 * @param attr is a pointer to the attributes structure that was previously
 * inited.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_attr_destroy(dragonChannelAttr_t* attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel attribute");

    // if there was a side buffer pool, then the descriptor must be freed.
    if (attr->buffer_pool != NULL) {
        free(attr->buffer_pool);
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Create a channel in a memory pool
 *
 * Create a channel in a memory pool with the given c_uid. While the c_uid
 * is not enforced to be unique across the Dragon run-time services by this
 * api, that is desirable and is left to the user of this API call. Unique c_uid
 * values are enforced by this API call at the process level. If you need
 * globally unique c_uids, then use the managed channel API instead which coordinates
 * with the Dragon run-time services to insure global uniqueness. For transparent
 * multi-node communication between channels, c_uids do not have to be unique across
 * all nodes.
 *
 * @param ch is the channel descriptor which will be initialzed from this call.
 *
 * @param c_uid is a channel identifier. It must be unique on a process level
 * and should be unique across all nodes. Read about uniqueness in the general
 * description.
 *
 * @param pool_descr is the pool in which to allocate this channel.
 *
 * @param attr are the attributes to be used in creating this channel. If
 * providing attributes, make sure you call dragon_channel_attr_init first.
 * Otherwise, NULL can be provided to get default attributes.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_create(dragonChannelDescr_t* ch, const dragonC_UID_t c_uid,
                      dragonMemoryPoolDescr_t* pool_descr, const dragonChannelAttr_t* attr)
{
    dragonError_t err;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel descriptor");

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory pool descriptor");

    /* the memory pool must be locally addressable */
    if (!dragon_memory_pool_is_local(pool_descr))
        err_return(DRAGON_INVALID_ARGUMENT, "cannot directly access memory pool for channel creation");

    /* if the attrs are NULL populate a default one */
    dragonChannelAttr_t def_attr;
    if (attr == NULL) {

        err = dragon_channel_attr_init(&def_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize channel attributes.");

        attr = &def_attr;
    } else {

        err = _validate_attr(attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Channel Attribute(s) are invalid.");
    }

    /* this will be freed in the channel_destroy call */
    dragonChannel_t* newch = malloc(sizeof(dragonChannel_t));
    if (newch == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new channel object");

    /* make a clone of the pool descriptor for use here */
    err = dragon_memory_pool_descr_clone(&newch->pool, pool_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot clone pool descriptor");
        goto ch_fail;
    }

    /* determine the allocation size required for the request channel */
    size_t alloc_size = _channel_allocation_size(attr);

    /* allocate the space using the alloc type interface with a channel type */
    err = dragon_memory_alloc_type(&newch->main_mem, &newch->pool, alloc_size, DRAGON_MEMORY_ALLOC_CHANNEL, c_uid);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("unable to allocate memory for channel from memory pool");
        goto ch_fail;
    }
    err = dragon_memory_get_pointer(&newch->main_mem, &newch->local_main_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("unable to get pointer to memory for channel");
        goto ch_mem_alloc_fail;
    }

    /* map the header and set values from the attrs */
    _map_header(newch);
    err = _assign_header(c_uid, attr, newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("There is a problem initializing the channel header.");
        goto ch_mem_alloc_fail;
    }

    /* These are process local flags */
    newch->proc_flags = attr->flags;

    /* instantiate the locks and priority heaps */
    err = _instantiate_priority_heaps(newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot instantiate priority heaps");
        goto ch_mem_alloc_fail;
    }

    /* instantiate the dragon locks */
    err = _instantiate_channel_locks(attr, newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot instantiate channel dragon locks");
        goto ch_mem_alloc_fail;
    }

    err = _instantiate_bcast_objects(newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot instantiate channel dragon bcast objects");
        goto ch_mem_alloc_fail;
    }

    _map_event_records(newch);

    /* map the message blocks */
    err = _map_message_blocks(newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot create channel");
        goto ch_mem_alloc_fail;
    }

    /* register this channel in our umap using the c_uid as the key */
    ch->_idx = c_uid;
    ch->_rt_idx = dragon_get_local_rt_uid();

    err = _add_umap_channel_entry(ch, newch);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to insert item into channels umap");
        goto ch_mem_alloc_fail;
    }

    /* If serialized later, we'll retrieve it from this pre-inited
       serialized descriptor so we only have to serialize it once. */

    err = _channel_serialize(ch, &newch->ch_ser);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to serialize the masquerading channel.");
        goto ch_mem_alloc_fail;
    }

    atomic_store(&(newch->ref_cnt), 1);

    no_err_return(DRAGON_SUCCESS);

ch_mem_alloc_fail:
    dragon_memory_free(&newch->main_mem);
ch_fail:
    if (newch != NULL)
        free(newch);

    no_err_return(err);
}

/**
 * @brief Destroy a channel
 *
 * Destroying a channel can only be done on the node where the channel is
 * located. Destroying a channel frees the allocated memory in the memory pool
 * and invalidates any use of the channel from this or other processes.
 *
 * @param ch is the channel descriptor to destroy.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_destroy(dragonChannelDescr_t* ch)
{
    /* @MCB: What happens if we destroy a channel with open send/recv handles? */
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    /* You cannot destroy a non-local channel (at least not without help from a
       transport service - which is not implemented) BUT we will allow a
       channel that is masquerading as a remote channel to be destroyed. */
    if (!dragon_channel_is_local(ch) && !_channel_is_masquerading(ch))
        err_return(DRAGON_CHANNEL_OPERATION_UNSUPPORTED_REMOTELY, "Cannot destroy non-local channel.");

    int allocation_exists;

    err = dragon_memory_pool_allocation_exists(&channel->pool, DRAGON_MEMORY_ALLOC_CHANNEL,
                                   (dragonULInt) *channel->header.c_uid, &allocation_exists);

    if (allocation_exists == 0) {
        err_return(DRAGON_CHANNEL_ALREADY_DESTROYED, "This channel allocation does not exist and was likely already destroyed.");
    }

    /* tear down the locks */
    err = _destroy_channel_locks(channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy locks in channel teardown");

    /* tear down priority heaps */
    err = _destroy_priority_heaps(channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy priority heap in channel teardown");

    err = _destroy_bcast_objects(channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy bcast objects in channel teardown");

    /* release memory back to the pool */
    err = dragon_memory_free(&channel->main_mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot release channel memory back to pool");

    err = dragon_channel_serial_free(&channel->ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot free the serialized descriptor");

    /* remove the item from the umap */
    err = dragon_umap_delitem_multikey(dg_channels, ch->_rt_idx, ch->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item from channels umap");

    /* free the channel structure */
    free(channel->msg_blks_ptrs);
    free(channel);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Serialize a channel to be shared with another process
 *
 * @param ch is a channel descriptor for a channel.
 *
 * @param ch_ser is a serialized channel descriptor that may be shared with
 * another process.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_serialize(const dragonChannelDescr_t* ch, dragonChannelSerial_t* ch_ser)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel serialized descriptor cannot be NULL.");

    ch_ser->len = 0;
    ch_ser->data = NULL;

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    ch_ser->data = malloc(channel->ch_ser.len);
    if (ch_ser->data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,
                   "cannot allocate space for serialized channel descriptor data");

    ch_ser->len = channel->ch_ser.len;
    memcpy(ch_ser->data, channel->ch_ser.data, ch_ser->len);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Free the internal resources of a serialized channel descriptor
 *
 * This frees internal structures of a serialized channel descriptor. It does not
 * destroy the channel itself.
 *
 * @param ch_ser is a serialized channel descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_serial_free(dragonChannelSerial_t* ch_ser)
{
    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "channel serial is NULL");

    if (ch_ser->data != NULL) {
        free(ch_ser->data);
        ch_ser->data = NULL; // Set NULL explicitly to prevent double-free
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Attach to a channel
 *
 * Calling this attaches to a channel by using a serialized channel descriptor
 * that was passed to this process. The serialized channel descriptor must have
 * been created using the dragon_channel_serialize function.
 *
 * @param ch_ser is a pointer to the serialized channel descriptor.
 *
 * @param ch is a pointer to a channel descriptor that will be initialized by
 * this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_attach(const dragonChannelSerial_t* ch_ser, dragonChannelDescr_t* ch)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel descriptor");

    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid serialized channel descriptor");

    if (ch_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid data in serialized channel descriptor");

    if (ch_ser->len <= DRAGON_CHANNEL_CHSER_NULINTS * sizeof(dragonULInt))
        err_return(DRAGON_INVALID_ARGUMENT, "invalid data length in serialized channel descriptor");

    /* pull out the c_uid */
    dragonULInt* sptr = (dragonULInt*)ch_ser->data;
    dragonC_UID_t c_uid = (dragonC_UID_t)*sptr;
    sptr++;

    /* we'll need to construct a new channel structure (freed in either
     * channel_destroy or channel_detach) */
    dragonChannel_t* channel = malloc(sizeof(dragonChannel_t));
    if (channel == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "unable to allocate new channel structure");

    /* These are create attributes only. So attaching in a different process
       should not inherit the flags specified when the channel was created. */
    channel->proc_flags = DRAGON_CHANNEL_FLAGS_NONE;

    /* attach to the memory descriptor */
    dragonError_t err;
    dragonMemorySerial_t mem_ser;

    mem_ser.len = ch_ser->len - DRAGON_CHANNEL_CHSER_NULINTS * sizeof(dragonULInt);
    mem_ser.data = (uint8_t*)sptr;

    err = dragon_memory_attach(&channel->main_mem, &mem_ser);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot attach to memory with serialized descriptor");
        goto ch_attach_fail;
    }

    /* get the pool descriptor and pointer from the memory descriptor */
    err = dragon_memory_get_pool(&channel->main_mem, &channel->pool);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot get memory pool from memory descriptor");
        goto ch_attach_mem_fail;
    }

    dragonRT_UID_t rt_uid;

    err = dragon_memory_pool_get_rt_uid(&channel->pool, &rt_uid);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot get rt_uid from memory pool");
        goto ch_attach_mem_fail;
    }

    /* check if we have already attached to the rt_uid/c_uid pair */
    err = _channel_descr_from_uids(rt_uid, c_uid, ch);
    if (err == DRAGON_SUCCESS) {
        dragonChannel_t* channel;
        _channel_from_descr(ch, &channel);
        /* The proc_flags are creation attributes. If the same process created
           the channel, and then attached to it, the attached instance should not
           inherit the flags specified when the channel was created. */
        channel->proc_flags = DRAGON_CHANNEL_FLAGS_NONE;
        atomic_fetch_add_explicit(&(channel->ref_cnt), 1, memory_order_acq_rel);
        no_err_return(DRAGON_SUCCESS);
    }

    if (dragon_memory_pool_is_local(&channel->pool)) {

        err = dragon_memory_get_pointer(&channel->main_mem, &channel->local_main_ptr);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot get pointer from memory descriptor");
            goto ch_attach_mem_fail;
        }

        /* map the channel header on */
        _map_header(channel);

        /* attach the channel locks */
        err = _attach_channel_locks(channel);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot attach locks");
            goto ch_attach_mem_fail;
        }

        /* attach the OT and UT priority heaps */
        err = _attach_priority_heaps(channel);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot attach OT and UT priority heaps");
            goto ch_attach_lock_fail;
        }

        err = _attach_bcast_objects(channel);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot attach bcast objects");
            goto ch_attach_pheap_fail;
        }

        _map_event_records(channel);

        /* map the message blocks into out channel structure */
        err = _map_message_blocks(channel);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("cannot map message blocks");
            goto ch_attach_bcast_fail;
        }
    } else {
        /* This is not a local channel, so set the local_main_ptr to NULL */
        channel->local_main_ptr = NULL;
    }

    // First attach, explicitly set counter to 1
    atomic_store(&(channel->ref_cnt), 1);

    /* register this channel in our umap using the c_uid as the key */
    ch->_rt_idx = rt_uid;
    ch->_idx = c_uid;

    err = _add_umap_channel_entry(ch, channel);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to insert item into channels umap");
        goto ch_attach_mblock_fail;
    }

    /* If serialized later, we'll retrieve it from this pre-inited
    serialized descriptor so we only have to serialize it once. */

    channel->ch_ser.data = malloc(ch_ser->len);
    if (channel->ch_ser.data == NULL) {
        append_err_noreturn("Could not allocate space for serialized "
                            "descriptor in channel structure.");
        goto ch_attach_fail;
    }

    channel->ch_ser.len = ch_ser->len;
    memcpy(channel->ch_ser.data, ch_ser->data, ch_ser->len);

    no_err_return(DRAGON_SUCCESS);

ch_attach_mblock_fail:
    if (channel->msg_blks_ptrs != NULL)
        free(channel->msg_blks_ptrs);
ch_attach_bcast_fail:
    _detach_bcast_objects(channel);
ch_attach_pheap_fail:
    _detach_priority_heaps(channel);
ch_attach_lock_fail:
    _detach_channel_locks(channel);
ch_attach_mem_fail:
    dragon_memory_detach(&channel->main_mem);
ch_attach_fail:
    if (channel != NULL)
        free(channel);

    return err;
}

/**
 * @brief Detach from a channel
 *
 * Calling this will clean up any local references to a channel and release any
 * local resources needed for communicating with the channel. It does not destroy
 * the channel itself.
 *
 * @param ch is the channel descriptor from which to detach.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_detach(dragonChannelDescr_t* ch)
{
    /* @MCB: What happens if we detach from a channel with open send/recv
     * handles? */

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    long int ref_cnt = atomic_fetch_sub_explicit(&(channel->ref_cnt), 1, memory_order_acq_rel) - 1;
    if (ref_cnt > 0)
        no_err_return(DRAGON_SUCCESS);

    if (channel->local_main_ptr != NULL) {
        /* This is a local channel, not remote so more work to do */
        // detach from locks
        err = _detach_channel_locks(channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from locks.");

        // detach from priority heaps
        err = _detach_priority_heaps(channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from priority heap.");

        err = _detach_bcast_objects(channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from bcast objects.");

        // detach from memory
        err = dragon_memory_detach(&channel->main_mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from underlying memory");

        // detach from base pool
        err = dragon_memory_pool_detach(&channel->pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from base pool");

        // free base channel structure
        free(channel->msg_blks_ptrs);
    }

    // remove channel from umap
    err = dragon_umap_delitem_multikey(dg_channels, ch->_rt_idx, ch->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not remove channel from umap");

    /* We may have stored the serialized descriptor in the channel structure so
     * free it */
    err = dragon_channel_serial_free(&channel->ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot free the serialized descriptor");

    free(channel);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Clone a channel descriptor
 *
 * Calling this will copy a channel descriptor from one location to another. This does not
 * copy the channel. It is used only for making a copy of a channel descriptor inside a process.
 *
 * @param newch_descr is the channel descriptor space to copy into.
 *
 * @param oldch_descr is the existing descriptor to clone.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_descr_clone(dragonChannelDescr_t * newch_descr, const dragonChannelDescr_t * oldch_descr)
{
    if (oldch_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot clone from NULL descriptor.");

    if (newch_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot clone to NULL descriptor.");

    *newch_descr = *oldch_descr;

    no_err_return(DRAGON_SUCCESS);
}

/** @} */ // end of group.

/** @defgroup channels_functionality Channels Functions
 *  @{
 */

/**
 * @brief Get pool from channel
 *
 * Given a channel, get the pool from the channel where it resides.
 *
 * @param ch is a pointer to the channel descriptor.
 * @param pool_descr is a pointer the the pool descriptor that will be
 * initialized by this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_get_pool(const dragonChannelDescr_t* ch, dragonMemoryPoolDescr_t* pool_descr)
{
    dragonChannel_t* channel;

    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    err = dragon_memory_pool_descr_clone(pool_descr, &channel->pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot clone memory pool descriptor from channel");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the host identifier for a channel
 *
 * The host identifier is used to decide on which node the channel is hosted.
 * This determines if it is a local channel or a remote channel. Sends and receives
 * to/from remote channels are handled through gateway channels that must be
 * registered prior to sending and or receiving. Gateway channels are shared
 * with a process via an environment variable or variables. See register_gateways_from_env
 * for more information on registering gateway channels. Host ids are arbitrary
 * unsigned long integers and cannot not be interpreted or inferred from any other data.
 *
 * @param ch is an initialized channel descriptor
 * @param hostid is a pointer to space to provide the hostid.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_get_hostid(const dragonChannelDescr_t* ch, dragonULInt* hostid)
{
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (hostid == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The hostid pointer cannot be NULL.");

    err = dragon_memory_pool_get_hostid(&channel->pool, hostid);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error retrieving hostid from channel's pool.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the channel's cuid and/or type
 *
 * From a serialized channel descriptor, this function will return the
 * cuid of the channel and the type of the channel. The channel is not
 * attached while doing this.
 *
 * @param ch_ser is a pointer to a serialied channel descriptor.
 * @param cuid is a pointer to a location there the cuid will be stored. If
 * NULL is provided, the cuid is not copied from the channel descriptor.
 * @param type is a pointer to a location where the channel's type will be stored.
 * If NULL is provided, the type is not copied from the channel descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_get_uid_type(const dragonChannelSerial_t* ch_ser, dragonULInt* cuid, dragonULInt* type)
{
    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel serializer is NULL");

    if (cuid != NULL) {
        *cuid = *(dragonULInt*)ch_ser->data;
    }

    if (type != NULL) {
        dragonULInt* ptr = (dragonULInt*)ch_ser->data;
        ptr++; // Skip cuid
        size_t pool_len = *(size_t*)ptr;
        uint8_t* t_ptr = (uint8_t*)ptr; // Recast to single 8bit pointer
        t_ptr += sizeof(size_t);        // Move past length
        t_ptr += pool_len;              // Skip pool serializer
        ptr = (dragonULInt*)t_ptr;      // Cast back
        *type = *ptr;
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the muid and filename of a channel's pool.
 *
 * From a serialized channel descriptor, this function returns information
 * about the pool the channel is allocated in, including the muid of the pool
 * and the pool's filename. The channel is not attached while doing this.
 *
 * @param ch_ser is a pointer to a serialized channel descriptor.
 * @param muid is a pointer to a location where the muid of the pool will be
 * stored. If NULL is provided, then the muid is not copied from the channel.
 * @param pool_fname is the filename associated with the shared memory of the
 * channel's pool.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_pool_get_uid_fname(const dragonChannelSerial_t* ch_ser, dragonULInt* muid,
                                  char** pool_fname)
{
    if (ch_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel serializer is NULL");

    dragonULInt* ptr = (dragonULInt*)ch_ser->data;
    ptr++; // skip c_uid
    uint8_t* t_ptr = (uint8_t*)ptr;
    t_ptr += sizeof(size_t); // skip stored pool serializer length

    dragonMemoryPoolSerial_t pool_ser;
    pool_ser.data = t_ptr;
    return dragon_memory_pool_get_uid_fname(&pool_ser, muid, pool_fname);
}

/**
 * @brief Check to see is a channel is local or not.
 *
 * Returns true if a channel is local and false if not.
 *
 * @param ch is an intialized channel descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
bool
dragon_channel_is_local(const dragonChannelDescr_t* ch)
{
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        return false;

    if ((channel->local_main_ptr == NULL) ||
        (channel->proc_flags & DRAGON_CHANNEL_FLAGS_MASQUERADE_AS_REMOTE))
        return false;

    return true;
}

/**
 * @brief Initialize a send handle on a channel
 *
 * Calling this initializes a send handle on a channel. To use the send handle it
 * must also be opened using the dragon_chsend_open function. Messages sent
 * with a single send handle are guaranteed to be received from the channel in
 * the same order. Order is maintained by both send and receive handles. If
 * customized send handle attributes are to be supplied,
 * dragon_channel_send_attr_init should be called first to initialize all
 * attributes to default values before customizing the desired values.
 *
 * @param ch is an initialized channel descriptor.
 * @param ch_sh is a pointer to a send handle descriptor that will be initialized
 * by this call.
 * @param attr are send handle attributes that may be supplied when initializing
 * the send handle. Providing NULL will result in the default send handle attributes
 * being applied.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_sendh(const dragonChannelDescr_t* ch, dragonChannelSendh_t* ch_sh,
                     const dragonChannelSendAttr_t* attr)
{
    if (ch_sh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel send handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    err = dragon_channel_send_attr_init(&ch_sh->_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize send attributes.");
    if (attr != NULL) {
        err = _validate_and_copy_send_attrs(attr, &ch_sh->_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Channel Send Attribute(s) are invalid.");
    }

    dragon_generate_uuid(ch_sh->_attrs.sendhid);
    ch_sh->_ch = *ch;
    ch_sh->_opened = 0;

    if (dragon_channel_is_local(ch)) {
        ch_sh->_gw._idx = 0;
        ch_sh->_gw._rt_idx = 0;
    } else {
        dragonChannel_t* gw_channel;

        err = _get_gateway(&ch_sh->_ch, DRAGON_OP_TYPE_SEND_MSG, &gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get a gateway channel.");

        ch_sh->_gw._idx = *((dragonC_UID_t*)gw_channel->header.c_uid);
        ch_sh->_gw._rt_idx = dragon_get_local_rt_uid();
    }

    /* TODO: at the moment, there are no data structures associated with the
       handle that require freeing or other cleanup.  One would be tempted to do
       this on handle close, but the handle could be reopened.  If that was the
       pattern, it is possible important state that was given at handle
       construction could be lost.  There is no concern with the current design,
       but if we did need to allocate/free resources for the handle with state
       that needs to survive open/close, we could utilize a umap as part of the
       channel itself.  That could hold any needed state.  This is not needed
       now, so I am just leaving this note.
    */

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the attributes of a send handle.
 *
 * Copy the attributes from a send handle into a send attributes structure.
 * The handle does not need to be open in order to get the attributes.
 *
 * @param ch_sh A pointer to the channel send handle.
 *
 * @param attr A pointer to the channel send handle attributes structure to
 * copy into.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chsend_get_attr(const dragonChannelSendh_t* ch_sh, dragonChannelSendAttr_t* attr)
{
    if (ch_sh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel send handle cannot be NULL.");

    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Send handle attributes to copy into cannot be NULL.");

    *attr = ch_sh->_attrs;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Open a channel send handle
 *
 * Open a send handle on a channel. Once opened, the send handle can be used for
 * sending messages.
 *
 * @param ch_sh is a pointer to an initialized send handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chsend_open(dragonChannelSendh_t* ch_sh)
{
    /* TODO: If we discover the channel is remote, then call the
       dragon_ulist_get_current_advance function on dg_gateways
       to get the next gateway channel to be used as a proxy for this
       channel and set the gateway channel in the ch_sh->_ch field */

    if (ch_sh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel send handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(&ch_sh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (ch_sh->_opened == 1)
        err_return(DRAGON_CHANNEL_SEND_ALREADY_OPENED, "cannot open handle that is already opened");

    ch_sh->_opened = 1;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Close a send handle
 *
 * Once sending is done on a particular send handle, it should be closed.
 *
 * @param ch_sh is a pointer to an open send handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chsend_close(dragonChannelSendh_t* ch_sh)
{
    if (ch_sh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel send handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(&ch_sh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    /* check if the channel is actually opened */
    if (ch_sh->_opened == 0)
        err_return(DRAGON_CHANNEL_SEND_NOT_OPENED, "cannot close handle that is not opened");

    ch_sh->_opened = 0;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Initialized a channel receive handle.
 *
 * Initialize a receive handle structure given a channel. To use the receive
 * handle it must also be opened using the dragon_chrecv_open function.
 * Messages received with a single receive handle are guaranteed to be
 * received from the channel in the same order they were sent. Order is
 * maintained by both send and receive handles. If customized receive handle
 * attributes are to be supplied, dragon_channel_recv_attr_init should be
 * called first to initialize all attributes to default values before
 * customizing the desired values.
 *
 * @param ch is an initialized channel descriptor
 * @param ch_rh is a pointer to a structure that will be initalized by this call.
 * @param rattrs is a pointer to receive handle attributes. If NULL is provided,
 * default receive handle attributes will be used.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_recvh(const dragonChannelDescr_t* ch, dragonChannelRecvh_t* ch_rh,
                     const dragonChannelRecvAttr_t* rattrs)
{
    /* TODO: If we discover the channel is remote, then call the
       dragon_ulist_get_current_advance function on dg_gateways
       to get the next gateway channel to be used as a proxy for this
       channel and set the gateway channel in the ch_rh->_ch field */

    if (ch_rh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel receive handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    err = dragon_channel_recv_attr_init(&ch_rh->_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize receive attributes.");
    if (rattrs != NULL) {
        err = _validate_and_copy_recv_attrs(rattrs, &ch_rh->_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Channel Recv Attribute(s) are invalid.");
    }

    ch_rh->_ch = *ch;
    ch_rh->_opened = 0;

    if (dragon_channel_is_local(ch)) {
        ch_rh->_gw._idx = 0;
        ch_rh->_gw._rt_idx = 0;
    } else {
        dragonChannel_t* gw_channel;

        err = _get_gateway(&ch_rh->_ch, DRAGON_OP_TYPE_GET_MSG, &gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get a gateway channel.");

        ch_rh->_gw._idx = *((dragonC_UID_t*)gw_channel->header.c_uid);
        ch_rh->_gw._rt_idx = dragon_get_local_rt_uid();
    }

    /* TODO: at the moment, there are no data structures associated with the
       handle that require freeing or other cleanup.  One would be tempted to do
       this on handle close, but the handle could be reopened.  If that was the
       pattern, it is possible important state that was given at handle
       construction could be lost.  There is no concern with the current design,
       but if we did need to allocate/free resources for the handle with state
       that needs to survive open/close, we could utilize a umap as part of the
       channel itself.  That could hold any needed state.  This is not needed
       now, so I am just leaving this note.
    */

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the attributes for a receive handle.
 *
 * Copy the attributes from a receive handle into a receive attributes
 * structure. The handle does not need to be open in order to get the
 * attributes.
 *
 * @param ch_rh A pointer to the channel receive handle.
 *
 * @param attr A pointer to the channel receive handle attributes structure to
 * copy into.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_get_attr(const dragonChannelRecvh_t* ch_rh, dragonChannelRecvAttr_t* attr)
{
    if (ch_rh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel receive handle cannot be NULL.");

    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Receive handle attributes to copy into cannot be NULL.");

    *attr = ch_rh->_attrs;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Open a channel receive handle
 *
 * Once opened, a receive handle can be used to receive messages from a channel.
 * Messages received from a receive handle are guaranteed to be received in the
 * order they were sent.
 *
 * @param ch_rh is the receive handle to be opened.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_open(dragonChannelRecvh_t* ch_rh)
{
    if (ch_rh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel receive handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(&ch_rh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (ch_rh->_opened == 1)
        err_return(DRAGON_CHANNEL_RECV_ALREADY_OPENED, "cannot open handle that is already opened");

    ch_rh->_opened = 1;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Close a channel receive handle.
 *
 * Once a receive handle is no longer to be used for receiving, the handle
 * should be closed.
 *
 * @param ch_rh is a pointer to an open channel receive handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_close(dragonChannelRecvh_t* ch_rh)
{
    if (ch_rh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel receive handle");

    /* make sure the channel from this descriptor is valid */
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(&ch_rh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    /* check if the channel is actually opened */
    if (ch_rh->_opened == 0)
        err_return(DRAGON_CHANNEL_RECV_NOT_OPENED, "cannot close handle that is not opened");

    ch_rh->_opened = 0;

    // TODO, down the road we'll actually do something more here

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Send a message into a channel.
 *
 * Much like putting data in a queue, messages can be sent through channels.
 * Sending a message into a channel can be done in several ways. The
 * destination memory descriptor (i.e. dest_mem_descr) may be NULL in which
 * case the memory associated with the message to send will be copied upon
 * sending. In that way the caller retains ownwership of the messages original
 * memory allocation. If dest_mem_descr is non-NULL, then the message is
 * copied into the dest_mem_descr before sending it. However, if
 * dest_mem_descr is the special value DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
 * then this indicates the caller wishes to transfer the ownership of the
 * memory allocation in msg_send to the receiver of this message. In this way,
 * a zero-copy send operation is possible when sending on-node between
 * processes. Using transfer of ownership also has performance advantages when
 * communicating between nodes since the path through the transport agent then
 * has fewer copies as well.
 *
 * If a NULL dest_mem_descr is provided, the memory allocation for the message
 * copy is made from either the channel's pool (if the channel is local) or from
 * the default memory pool for the node where the message is being sent from.
 *
 * When a memory allocation is required because the dest_mem_descr is NULL, calling
 * this function may block the sender until the memory allocation is available. This
 * call will also block while waiting for exclusive access to the channel. If a timeout
 * occurs on sending, then it was because the required memory was not available.
 *
 * @param ch_sh is a pointer to an initialized and open send handle.
 *
 * @param msg_send is a pointer to a valid channel message descriptor.
 *
 * @param dest_mem_descr is a pointer to a destination memory descriptor. The
 * detailed description above has more details on the valid values for this
 * argument.
 *
 * @param timeout_override is a pointer to a timeout structure that may be used
 * to override the default send timeout as provided in the send handle
 * attributes. A timeout of zero seconds and zero nanoseconds will result in a
 * try-once call of send. A value of NULL will result in using the default timeout
 * from the send handle attributes.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chsend_send_msg(const dragonChannelSendh_t* ch_sh, const dragonMessage_t* msg_send,
                       dragonMemoryDescr_t* dest_mem_descr, const timespec_t* timeout_override)
{
    dragonError_t err;
    size_t msg_bytes;

    if (ch_sh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel send handle");

    if (msg_send == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    /* check if the channel is actually opened */
    if (ch_sh->_opened == 0)
        err_return(DRAGON_CHANNEL_SEND_NOT_OPENED, "handle is not opened");

    /* check if the message structure has memory associated with it */
    if (msg_send->_mem_descr == NULL)
        err_return(DRAGON_CHANNEL_SEND_MESSAGE_MEMORY_NULL, "message has no memory associated with it");

    err = dragon_memory_get_size(msg_send->_mem_descr, &msg_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size of message to send.");

    /* get a pointer to the memory in the memory descriptor */
    void* msg_ptr;
    err = dragon_memory_get_pointer(msg_send->_mem_descr, &msg_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor in message");

    if (dest_mem_descr != NULL && dest_mem_descr != DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP) {
        /* If we have a valid memory descriptor, then it should work to get it's
         * size */
        size_t dest_bytes;
        err = dragon_memory_get_size(dest_mem_descr, &dest_bytes);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot obtain size from destination memory descriptor");
        if (dest_bytes < msg_bytes)
            err_return(DRAGON_INVALID_ARGUMENT, "Cannot specify destination memory that is smaller than "
                                                "message on dragon_ch_send_msg operation.");
    }

    /* get the channel from the send handle */
    dragonChannel_t* channel;
    err = _channel_from_descr(&ch_sh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    const timespec_t* timer = NULL;

    if (timeout_override != NULL) {
        timer = timeout_override;
    } else {
        timer = &ch_sh->_attrs.default_timeout;
    }

    if (dragon_channel_is_local(&ch_sh->_ch)) {
        // Initially, we try to send a few times with no wait.
        timespec_t no_wait = { 0, 0 };

        for (int i = 0; i < DRAGON_CHANNEL_PRE_SPINS; i++) {
            err = _send_msg(channel, ch_sh->_attrs.sendhid, msg_send, msg_ptr, msg_bytes, dest_mem_descr,
                            &no_wait, false);

            if (err == DRAGON_SUCCESS)
                no_err_return(DRAGON_SUCCESS);
            if (err != DRAGON_CHANNEL_FULL && err != DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE)
                append_err_return(err, "failed to do initial spin to send a message");
        }

        // If a zero timeout is provided, then return immediately
        // without blocking and return the return code we got when
        // trying to send (i.e. not a timeout error code).
        if (timer != NULL && timer->tv_sec == 0 && timer->tv_nsec == 0)
            err_return(err, "Attempted to send message without blocking, and it failed.");
    }

    // providing the NOTIMEOUT constant means providing a NULL pointer to the
    // bcast object in the remaining_timeout pointer below.
    if (timer->tv_nsec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_nsec &&
        timer->tv_sec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_sec)
        timer = NULL;

    timespec_t* end_time_ptr = NULL;
    timespec_t* remaining_time_ptr = NULL;
    timespec_t start_time = { 0, 0 }, end_time = { 0, 0 }, remaining_time = { 0, 0 };

    if (timer != NULL) {
        if (timer->tv_nsec == 0 && timer->tv_sec == 0) {
            remaining_time_ptr = &remaining_time;
            end_time_ptr = &end_time;
        } else {
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            err = dragon_timespec_add(&end_time, &start_time, timer);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "This shouldn't happen.");

            end_time_ptr = &end_time;
            remaining_time_ptr = &remaining_time;
        }
    }

    if (dragon_channel_is_local(&ch_sh->_ch)) {

        err = DRAGON_CHANNEL_FULL;

        while (err == DRAGON_CHANNEL_FULL) {
            if (end_time_ptr != NULL) {
                err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Timeout or unexpected error.");
            }

            _obtain_ut_lock(channel);

            if ((*(channel->header.available_blocks)) == 0) {
                err = dragon_bcast_wait(&channel->send_bcast, ch_sh->_attrs.wait_mode, remaining_time_ptr,
                                        NULL, 0, (dragonReleaseFun)dragon_unlock, &channel->ut_lock);
                if (err != DRAGON_SUCCESS)
                    // A little ambiguous if ut lock is still locked or not. If it
                    // is a timeout, it is unlocked for sure. But anything else
                    // would be a bad error anyway. So assume unlocked in all
                    // cases.
                    append_err_return(err, "Timeout or unexpected error.");
            } else
                _release_ut_lock(channel);

            err = _send_msg(channel, ch_sh->_attrs.sendhid, msg_send, msg_ptr, msg_bytes, dest_mem_descr,
                            end_time_ptr, true);
        }

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or unexpected error.");
    } else {
        /* This is a non-local channel and sending must go through a gateway */
        dragonChannel_t* gw_channel;
        dragonChannelSendh_t gw_sh;
        dragonGatewayMessage_t gw_msg;
        dragonGatewayMessageSerial_t gw_ser_msg;
        dragonMessage_t req_msg;
        dragonMemoryDescr_t req_mem;

        err = _channel_from_descr(&ch_sh->_gw, &gw_channel);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not resolve gateway channel descriptor while sending a message.");
        }

        /* it is not a local channel so we interact with the gateway channel
         * instead */
        dragonError_t err = dragon_channel_gatewaymessage_send_create(
          &gw_channel->pool, msg_send, dest_mem_descr, &ch_sh->_ch, &ch_sh->_attrs, end_time_ptr, &gw_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway message for send request.");

        err = dragon_channel_gatewaymessage_serialize(&gw_msg, &gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize gateway message for send request.");

        err = dragon_memory_alloc_blocking(&req_mem, &gw_channel->pool, gw_ser_msg.len, remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not allocate memory for send request message.");

        err = dragon_memory_get_pointer(&req_mem, &msg_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pointer to space for serialized descriptor.");

        memcpy(msg_ptr, gw_ser_msg.data, gw_ser_msg.len);

        err = dragon_channel_gatewaymessage_serial_free(&gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for gateway message.");

        err = dragon_channel_message_init(&req_msg, &req_mem, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message to send to "
                                   "transport service via gateway channel.");

        err = dragon_channel_sendh(&ch_sh->_gw, &gw_sh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway send handle.");

        err = dragon_chsend_open(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open gateway send handle.");

        if (end_time_ptr != NULL) {
            err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Timeout or unexpected error.");
        }

        err = dragon_chsend_send_msg(&gw_sh, &req_msg, NULL, remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send gateway message.");

        err = dragon_chsend_close(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close gateway send handle.");

        err = dragon_channel_message_destroy(&req_msg, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unexpected error on destroying the send "
                                   "request after sending to the gateway.");

        err = dragon_channel_gatewaymessage_client_send_cmplt(&gw_msg, ch_sh->_attrs.wait_mode);
        if (err != DRAGON_SUCCESS)
            err_return(err, "non-zero completion of remote send operation.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Receive a message from a channel.
 *
 * Calling this receives the next available message from a channel without blocking.
 *
 * @param ch_rh is a pointer to an intialized and open receive handle structure.
 *
 * @param msg_recv is a pointer to a message structure that will be initialized with the
 * received message.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred. If no message
 * is immediately available it will return DRAGON_CHANNEL_EMPTY.
 */
dragonError_t
dragon_chrecv_get_msg(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv)
{
    dragonError_t err;
    timespec_t no_wait = { 0, 0 };

    err = dragon_chrecv_get_msg_blocking(ch_rh, msg_recv, &no_wait);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get message");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Asynchronously receive a message from a channel.
 *
 * This function is not currently implemented.
 *
 * @param ch_rh is a pointer to an initialized, open receive handle.
 *
 * @param msg_recv is a pointer to a message descriptor to be initialzed
 * aynchronously.
 *
 * @param sync is a pointer to a BCast descriptor which points to a valid
 * BCast object. The calling process can then wait on this BCast for a message
 * to receive. This can be safely done by a thread of the calling process or by
 * the process itself.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_get_msg_notify(dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv, dragonBCastDescr_t* sync)
{
    err_return(DRAGON_NOT_IMPLEMENTED, "dragon_chrecv_get_msg_notify is not yet implemented.");
}

/**
 * @brief Receive a message from a channel.
 *
 * This receives a message from a channel into the supplied message structure. A
 * timeout override can be supplied to override the default timeout found in the
 * channel receive handle. If the timeout_override is NULL, the default receive handle
 * timeout is used. A timeout of zero seconds and zero nanoseconds will result in
 * try once attempt which will return DRAGON_CHANNEL_EMPTY if no message is available.
 *
 * @param ch_rh is a pointer to an initialized, open receive handle.
 *
 * @param msg_recv is a pointer to a message descriptor which will be initialized by this
 * call upon successful completion should a message be received.
 *
 * @param timeout_override is a pointer to a structure that contains the timeout
 * to use in place of the default handle timeout. If NULL, the default handle
 * timeout is used. If zero seconds and nanoseconds, the call is non-blocking
 * and returns the next message if availble or DRAGON_CHANNEL_EMPTY if no
 * message is currently available.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_get_msg_blocking(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv,
                               const timespec_t* timeout_override)
{
    dragonError_t err; /* used for all other errors */

    if (ch_rh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel recv handle");

    if (msg_recv == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    /* check if the channel is actually opened */
    if (ch_rh->_opened == 0)
        err_return(DRAGON_CHANNEL_RECV_NOT_OPENED, "handle is not opened");

    /* get the channel from the receive handle */
    dragonChannel_t* channel;
    err = _channel_from_descr(&ch_rh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    const timespec_t* timer = NULL;

    if (timeout_override != NULL) {
        timer = timeout_override;
    } else
        timer = &ch_rh->_attrs.default_timeout;

    // providing the NOTIMEOUT constant means providing a NULL pointer to the
    // bcast in the end_time_ptr pointer below.
    if (timer->tv_nsec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_nsec &&
        timer->tv_sec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_sec) {
        timer = NULL;
    }

    timespec_t* end_time_ptr = NULL;
    timespec_t* remaining_time_ptr = NULL;
    timespec_t start_time = { 0, 0 }, end_time = { 0, 0 }, remaining_time = { 0, 0 };

    if (timer != NULL) {
        if (timer->tv_nsec == 0 && timer->tv_sec == 0) {
            remaining_time_ptr = &remaining_time;
            end_time_ptr = &end_time;
        } else {
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            err = dragon_timespec_add(&end_time, &start_time, timer);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "This shouldn't happen.");

            end_time_ptr = &end_time;
            remaining_time_ptr = &remaining_time;
        }
    }

    if (dragon_channel_is_local(&ch_rh->_ch)) {
        timespec_t no_wait = { 0, 0 };
        int num_spins = DRAGON_CHANNEL_PRE_SPINS;

        if (timeout_override != NULL && timeout_override->tv_nsec == 0 && timeout_override->tv_sec == 0)
            /* This is really a non-blocking call so set spins to 1 */
            num_spins = 1;

        // Initially, we try to get a few times with no wait.
        for (int i = 0; i < num_spins; i++) {
            err = _get_msg(channel, msg_recv, &no_wait, false);

            if (err == DRAGON_SUCCESS)
                no_err_return(DRAGON_SUCCESS);

            if (err != DRAGON_CHANNEL_EMPTY && err != DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE)
                append_err_return(err, "The recv operation failed for an undetermined reason.");
        }

        // If a zero timeout is provided, then return immediately
        // without blocking and return the return code we got when
        // trying to recv (i.e. not a timeout error code).
        if (timer != NULL && timer->tv_sec == 0 && timer->tv_nsec == 0 && err == DRAGON_CHANNEL_EMPTY)
            no_err_return(DRAGON_CHANNEL_EMPTY);

        if (timer != NULL && timer->tv_sec == 0 && timer->tv_nsec == 0) {
            char err_str[200];
            snprintf(err_str, 199,
                     "Attempted to receive a message without blocking, and it "
                     "failed (ERR=%s).",
                     dragon_get_rc_string(err));
            append_err_return(err, err_str);
        }

        err = DRAGON_CHANNEL_EMPTY;

        while (err == DRAGON_CHANNEL_EMPTY) {
            if (timer != NULL) {
                err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Timeout or unexpected error.");
            }

            _obtain_ot_lock(channel);

            if ((*(channel->header.available_msgs)) == 0) {
                err = dragon_bcast_wait(&channel->recv_bcast, ch_rh->_attrs.wait_mode, remaining_time_ptr,
                                        NULL, 0, (dragonReleaseFun)dragon_unlock, &channel->ot_lock);
                if (err != DRAGON_SUCCESS)
                    // A little ambiguous if ot lock is still locked or not. If it
                    // is a timeout, it is unlocked for sure. But anything else
                    // would be a bad error anyway. So assume unlocked in all
                    // cases.
                    append_err_return(err, "Timeout or unexpected error.");
            } else
                _release_ot_lock(channel);

            err = _get_msg(channel, msg_recv, end_time_ptr, true);
        }

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or unexpected error.");
    } else {
        /* This is a non-local channel and receiving must go through a gateway */
        dragonChannel_t* gw_channel;
        dragonChannelSendh_t gw_sh;
        dragonGatewayMessage_t gw_msg;
        dragonGatewayMessageSerial_t gw_ser_msg;
        dragonMessage_t req_msg;
        dragonMemoryDescr_t req_mem;

        err = _channel_from_descr(&ch_rh->_gw, &gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not resolved gateway channel "
                                   "descriptor while receiving a message.");

        /* it is not a local channel so we interact with the gateway channel
         * instead */
        dragonError_t err = dragon_channel_gatewaymessage_get_create(&gw_channel->pool, msg_recv->_mem_descr,
                                                                     &ch_rh->_ch, end_time_ptr, &gw_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway message for recv request.");

        err = dragon_channel_gatewaymessage_serialize(&gw_msg, &gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize gateway message for recv request.");

        err = dragon_memory_alloc_blocking(&req_mem, &gw_channel->pool, gw_ser_msg.len, remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not allocate memory for send request message.");

        void* msg_ptr;

        err = dragon_memory_get_pointer(&req_mem, &msg_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pointer to space for serialized descriptor.");

        memcpy(msg_ptr, gw_ser_msg.data, gw_ser_msg.len);

        err = dragon_channel_gatewaymessage_serial_free(&gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for gateway message.");

        err = dragon_channel_message_init(&req_msg, &req_mem, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message to send to "
                                   "transport service via gateway channel.");

        err = dragon_channel_sendh(&ch_rh->_gw, &gw_sh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway send handle.");

        err = dragon_chsend_open(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open gateway send handle.");

        if (end_time_ptr != NULL) {
            err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Timeout or unexpected error.");
        }

        /* TBD: Might be faster not to use
         * DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP. Might be faster. Investigate.
         */
        err = dragon_chsend_send_msg(&gw_sh, &req_msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                     remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "The gateway message could not be sent.");

        err = dragon_chsend_close(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close gateway send handle.");

        err = dragon_channel_message_destroy(&req_msg, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unexpected error on destroying the recv "
                                   "request after sending to the gateway.");

        err = dragon_channel_gatewaymessage_client_get_cmplt(&gw_msg, msg_recv, ch_rh->_attrs.wait_mode);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "non-zero completion of remote get_msg");
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Peek at a message from a channel.
 *
 * This copies a message from a channel into the supplied message structure without
 * removing the message from the channel.
 *
 * @param ch_rh is a pointer to an initialized, open receive handle.
 *
 * @param msg_recv is a pointer to a message descriptor which will be initialized by this
 * call upon successful completion should a message be received.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_peek_msg(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_peek)
{
    dragonError_t err; /* used for all other errors */

    /* assuming that the operation is local for now */
    if (ch_rh == NULL || !dragon_channel_is_local(&ch_rh->_ch))
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel recv handle");

    if (msg_peek == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    /* check if the channel is actually opened */
    if (ch_rh->_opened == 0)
        err_return(DRAGON_CHANNEL_RECV_NOT_OPENED, "handle is not opened");

    /* get the channel from the receive handle */
    dragonChannel_t* channel;
    err = _channel_from_descr(&ch_rh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    err = _peek_msg(channel, msg_peek);
    if (err != DRAGON_SUCCESS && err != DRAGON_CHANNEL_EMPTY && err != DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE)
        append_err_return(err, "The peek operation failed for an undetermined reason.");

    no_err_return(err);
}

/**
 * @brief Pop a message from a channel.
 *
 * This pairs with @ref dragon_chrecv_peek_msg, removing a message from the channel.
 * Calling this after @ref dragon_chrecv_pop_msg will have the same effect as having
 * called @ref dragon_chrecv_get_msg with the same parameters. It is not required to
 * call @ref dragon_chrecv_pop_msg after calling dragon_chrecv_peek_msg().
 *
 * @param ch_rh is a pointer to an initialized, open receive handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_chrecv_pop_msg(const dragonChannelRecvh_t* ch_rh)
{
    dragonError_t err; /* used for all other errors */

    /* assuming that the operation is local for now */
    if (ch_rh == NULL || !dragon_channel_is_local(&ch_rh->_ch))
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel recv handle");

    /* check if the channel is actually opened */
    if (ch_rh->_opened == 0)
        err_return(DRAGON_CHANNEL_RECV_NOT_OPENED, "handle is not opened");

    /* get the channel from the receive handle */
    dragonChannel_t* channel;
    err = _channel_from_descr(&ch_rh->_ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    err = _pop(channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "The pop operation failed for an undetermined reason.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Poll a channel for status or the occurrence of an event.
 *
 * Polling may be done on any of the poll values found on dragonChannelEvent_t.
 * The polling values POLLIN and POLLOUT may be done together with the value
 * DRAGON_CHANNEL_POLLINOUT. All other polling event_masks must be done
 * separately. They are not supported in combination with each other.
 *
 * @param ch A channel descriptor for a channel either on-node or off-node.
 *
 * @param wait_mode A choice between IDLE waiting or SPIN waiting for events
 * polling. It is only relevant when polling for events.
 *
 * @param event_mask This specifies one of the dragonChannelEvent_t constants.
 *
 * @param timeout NULL indicates blocking with no timeout. Otherwise, blocks
 * for the specified amount of time. If (0,0) is provided, then it is a
 * non-blocking call.
 *
 * @param result For all but the DRAGON_CHANNEL_POLLSIZE the result will be a
 * 64-bit cast of the event_mask field. With DRAGON_CHANNEL_POLLSIZE it is
 * the number of messages currently in the channel.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_poll(const dragonChannelDescr_t* ch, dragonWaitMode_t wait_mode, const short event_mask,
                    const timespec_t* timeout, dragonULInt* result)
{
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    dragonULInt num_waiters = 0;

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (event_mask > DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS) {
        err_return(DRAGON_INVALID_ARGUMENT, "The event mask must be one of the DragonChannelEvent_t values.");
    }

    if (result == NULL &&
        ((event_mask == DRAGON_CHANNEL_POLLSIZE) || (event_mask == DRAGON_CHANNEL_POLLBARRIER_ISBROKEN) ||
         (event_mask == DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS) || (event_mask == DRAGON_CHANNEL_POLLBARRIER_WAITERS))) {
        err_return(DRAGON_INVALID_ARGUMENT,
                   "If polling for size, barrier_isbroken, blocked_receivers, or barrier_waiters you must provide a non-null result argument.");
    }

    /* This initializes result for all but a few poll values which set it below. */
    if (result != NULL)
        *result = (dragonULInt)event_mask;

    if (dragon_channel_is_local(ch)) {
        /* There are currently 5 poll bcasts (see number constant in _channels.h)
           and for the first 4 the event_mask-1 provides the index. The
           DRAGON_CHANNEL_POLLFULL==8 and is at index 4 so that adjustment is
           made below. */
        int poll_bcast_index = event_mask - 1;
        _obtain_channel_locks(channel);

        if ((event_mask == DRAGON_CHANNEL_POLLIN) || (event_mask == DRAGON_CHANNEL_POLLINOUT))
            if (*(channel->header.available_msgs) != 0) {
                _release_channel_locks(channel);
                no_err_return(DRAGON_SUCCESS);
            }

        if ((event_mask == DRAGON_CHANNEL_POLLOUT) || (event_mask == DRAGON_CHANNEL_POLLINOUT))
            if (*(channel->header.available_blocks) != 0) {
                _release_channel_locks(channel);
                no_err_return(DRAGON_SUCCESS);
            }

        if ((event_mask == DRAGON_CHANNEL_POLLEMPTY) && (*(channel->header.available_msgs) == 0)) {
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if ((event_mask == DRAGON_CHANNEL_POLLFULL) && (*(channel->header.available_blocks) == 0)) {
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLSIZE) {
            *result = *(channel->header.available_msgs);
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBARRIER_ISBROKEN) {
            *result = *(channel->header.barrier_broken);
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBARRIER_WAITERS) {
            *result = *(channel->header.barrier_count);
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS) {
            int num_waiters;

            err = dragon_bcast_num_waiting(&channel->recv_bcast, &num_waiters);
            if (err != DRAGON_SUCCESS) {
                _release_channel_locks(channel);
                append_err_return(err, "Could not get the number of waiters on the channel.");
            }

            *result = num_waiters;
            _release_channel_locks(channel);
            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLRESET) {

            if (*(channel->header.barrier_reset_in_progress) != 0UL)
                err_return(DRAGON_INVALID_OPERATION, "Reset already in progress.");

            *(channel->header.barrier_reset_in_progress) = 1UL;

            err = _release_barrier_waiters(channel, &num_waiters);
            if (err != DRAGON_SUCCESS) {
                _release_channel_locks(channel);
                append_err_return(err, "Could not release waiters for Reset");
            }

            *(channel->header.barrier_broken) = 0UL;
            *(channel->header.barrier_count) = 0UL;

            *(channel->header.barrier_reset_in_progress) = 0UL;

            _release_channel_locks(channel);

            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBARRIER_WAIT) {
            /* do not enter wait while a RESET is going on, instead poll until
             * channel is empty */
            while (*(channel->header.barrier_reset_in_progress) != 0) {
                _release_channel_locks(channel);
                err = dragon_channel_poll(ch, wait_mode, DRAGON_CHANNEL_POLLEMPTY, timeout, NULL);
                if (err != DRAGON_SUCCESS) {
                    _release_channel_locks(channel);
                    err_return(err, "Timeout while waiting for a barrier reset.");
                }
                _obtain_channel_locks(channel);
            }

            if (*(channel->header.barrier_broken) != 0) {
                _release_channel_locks(channel);
                err_return(DRAGON_BARRIER_BROKEN, "The Barrier Channel must be reset via a "
                                                  "DRAGON_CHANNEL_RESET poll operation.");
            }

            /* This check below has been verified to work while checking
               available_msgs will not. Very subtle, but necessary. If available
               blocks does not equal capacity, then there is a message, perhaps
               not in the ordering table that is in the channel. This indicates
               that a barrier trigger is in progress. We have to check it this
               way because when the message is received, but processing is not
               done, the message will not be in the usage table and
               available_msgs will be zero, while available blocks is still not
               incremented.

               The second check below is because there is a window where the
               barrier messages have not yet been added to the channel, but the
               barrier has all its waiters. So we cannot allow more to go
               through, otherwise the barrier count would be incorrect and the
               next firing of the barrier would never reach its intended parties
               value. */
            while ((*(channel->header.available_blocks) != *(channel->header.capacity)) ||
                   (*(channel->header.barrier_count) == *(channel->header.capacity))) {
                _release_channel_locks(channel);
                err = dragon_channel_poll(ch, wait_mode, DRAGON_CHANNEL_POLLEMPTY, timeout, NULL);
                _obtain_channel_locks(channel);

                if (err == DRAGON_TIMEOUT) {
                    if (timeout != NULL && timeout->tv_nsec == 0 && timeout->tv_sec == 0) {
                        /* if barrier wait was called with no timeout, it was to
                           try once and return with a non-blocking wait. This is
                           needed by a transport agent that is trying to do the
                           wait on behalf of a remote process. So just tell the
                           caller to try again in this case without blocking. */
                        _release_channel_locks(channel);
                        no_err_return(DRAGON_BARRIER_WAIT_TRY_AGAIN);
                    }
                    *(channel->header.barrier_broken) = true;

                    err = _release_barrier_waiters(channel, &num_waiters);
                    _release_channel_locks(channel);
                    if (err != DRAGON_SUCCESS)
                        append_err_return(err, "Could not release waiters for Timeout");

                    err_return(DRAGON_BARRIER_BROKEN, "The operation timed out and the barrier must "
                                                      "be reset via a "
                                                      "DRAGON_CHANNEL_RESET poll operation.");
                } else if (err != DRAGON_SUCCESS) {
                    _release_channel_locks(channel);
                    err_return(DRAGON_BARRIER_BROKEN, "Poll in Barrier wait did not succeed.");
                }
            }

            if (*(channel->header.barrier_broken) != 0) {
                _release_channel_locks(channel);
                err_return(DRAGON_BARRIER_BROKEN, "The Barrier Channel must be reset via a "
                                                  "DRAGON_CHANNEL_RESET poll operation.");
            }

            *(channel->header.barrier_count) += 1;

            if (*(channel->header.barrier_count) == *(channel->header.capacity))
                /* signal that we are ready to release the kraken! */
                err = DRAGON_BARRIER_READY_TO_RELEASE;
            else
                err = DRAGON_SUCCESS;

            _release_channel_locks(channel);

            no_err_return(err);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBARRIER_ABORT) {
            if (*(channel->header.barrier_broken) != 0) {
                _release_channel_locks(channel);
            } else {
                *(channel->header.barrier_broken) = true;

                err = _release_barrier_waiters(channel, &num_waiters);

                _release_channel_locks(channel);

                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not release waiters for Abort.");
            }

            no_err_return(DRAGON_SUCCESS);
        }

        if (event_mask == DRAGON_CHANNEL_POLLBARRIER_RELEASE) {

            if (*(channel->header.barrier_broken) == 0) {

                err = _release_barrier_waiters(channel, &num_waiters);

                if (err != DRAGON_SUCCESS) {
                    _release_channel_locks(channel);
                    append_err_return(err, "Could not release waiters for Release.");
                }
            }

            _release_channel_locks(channel);

            no_err_return(DRAGON_SUCCESS);
        }

        err = dragon_bcast_wait(&channel->poll_bcasts[poll_bcast_index], wait_mode, timeout, NULL, 0,
                                (dragonReleaseFun)_release_channel_locks, channel);

        if (err == DRAGON_TIMEOUT)
            err_return(DRAGON_TIMEOUT, "The poll operation timed out.");

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unexpected error.");
    } else {
        /* This is a non-local channel and polling must go through a gateway */
        dragonChannelDescr_t gw_descr;
        dragonChannel_t* gw_channel;
        dragonChannelSendh_t gw_sh;
        dragonGatewayMessage_t gw_msg;
        dragonGatewayMessageSerial_t gw_ser_msg;
        dragonMessage_t req_msg;
        dragonMemoryDescr_t req_mem;

        err = _get_gateway(ch, DRAGON_OP_TYPE_POLL, &gw_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get a gateway channel.");

        timespec_t* end_time_ptr = NULL;
        timespec_t* remaining_time_ptr = NULL;
        timespec_t start_time = { 0, 0 }, end_time = { 0, 0 }, remaining_time = { 0, 0 };

        if (timeout != NULL) {
            if (timeout->tv_nsec == 0 && timeout->tv_sec == 0) {
                remaining_time_ptr = &remaining_time;
                end_time_ptr = &end_time;
            } else {
                clock_gettime(CLOCK_MONOTONIC, &start_time);

                err = dragon_timespec_add(&end_time, &start_time, timeout);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "This shouldn't happen.");

                end_time_ptr = &end_time;
                remaining_time_ptr = &remaining_time;
            }
        }

        dragonError_t err = dragon_channel_gatewaymessage_event_create(&gw_channel->pool, event_mask, ch,
                                                                       end_time_ptr, &gw_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway message for poll request.");

        err = dragon_channel_gatewaymessage_serialize(&gw_msg, &gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize gateway message for poll request.");

        err = dragon_memory_alloc_blocking(&req_mem, &gw_channel->pool, gw_ser_msg.len, remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not allocate memory for send request message.");

        void* msg_ptr;

        err = dragon_memory_get_pointer(&req_mem, &msg_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pointer to space for serialized descriptor.");

        memcpy(msg_ptr, gw_ser_msg.data, gw_ser_msg.len);

        err = dragon_channel_gatewaymessage_serial_free(&gw_ser_msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for gateway message.");

        err = dragon_channel_message_init(&req_msg, &req_mem, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message to send to "
                                   "transport service via gateway channel.");

        dragonRT_UID_t rt_uid = dragon_get_local_rt_uid();

        err = _channel_descr_from_uids(rt_uid, *((dragonC_UID_t*)gw_channel->header.c_uid), &gw_descr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get gateway channel descriptor.");

        err = dragon_channel_sendh(&gw_descr, &gw_sh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create gateway send handle.");

        err = dragon_chsend_open(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open gateway send handle.");

        if (end_time_ptr != NULL) {
            err = dragon_timespec_remaining(end_time_ptr, remaining_time_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Timeout or unexpected error.");
        }

        err = dragon_chsend_send_msg(&gw_sh, &req_msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                     remaining_time_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or unexpected error while sending "
                                   "gateway poll/event request.");

        err = dragon_chsend_close(&gw_sh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close gateway send handle.");

        err = dragon_channel_message_destroy(&req_msg, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unexpected error on destroying the recv "
                                   "request after sending to the gateway.");

        dragonULInt captured_event_result;
        err = dragon_channel_gatewaymessage_client_event_cmplt(&gw_msg, &captured_event_result, wait_mode);
        if (result != NULL)
            *result = captured_event_result;

        if (err != DRAGON_SUCCESS)
            err_return(err, "non-zero return code on remote poll operation.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the number of messages in a channel.
 *
 * This will return the number of messages found in a channel. The number of messages
 * will be correct when queried, but may change before this function returns to the caller
 * if other processes are interacting with the channel by sending or receiving messages.
 *
 * @param ch is a pointer to an initialized channel descriptor.
 *
 * @param count is a pointer to space for a 64 bit unsigned integer that will
 * hold the number of messages found in the channel.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_count(const dragonChannelDescr_t* ch, uint64_t* count)
{
    dragonError_t err;

    err = dragon_channel_poll(ch, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLSIZE, NULL, count);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get available messages via the size poll on the channel.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Return the number of waiters on a barrier
 *
 * This function returns the number of waiters on a barrier for either
 * local or remote channels that are configured to support a barrier.
 * This returns the number of processes that have called the barrier wait
 * functionality via the proper poll argument.
 *
 * @param ch a pointer to a channel descriptor
 * @param count a pointer to an integer that will hold the result.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_barrier_waiters(const dragonChannelDescr_t* ch, uint64_t* count)
{
    dragonError_t err;

    /* timeout is ignored on this poll operation. */
    err = dragon_channel_poll(ch, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLBARRIER_WAITERS, NULL, count);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get number of barrier waiters via the poll on the channel.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Return the number of blocked receivers
 *
 * This function returns the number of blocked receivers on either a local
 * or remote channel.
 *
 * @param ch a pointer to a channel descriptor
 * @param count a pointer to an integer that will hold the result.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_blocked_receivers(const dragonChannelDescr_t* ch, uint64_t* count)
{
    dragonError_t err;

    /* timeout is ignored on this poll operation. */
    err = dragon_channel_poll(ch, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS, NULL, count);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get number of blocked receivers via the poll on the channel.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Return whether the barrier is broken or not.
 *
 * Returns whether the barrier is broken or not.
 *
 * @param ch is a pointer to a valid channel descriptor.
 *
 * @return true or false
 */
bool
dragon_channel_barrier_is_broken(const dragonChannelDescr_t* ch)
{
    dragonULInt result;
    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        return false;

    /* timeout is ignored on this poll operation. */
    err = dragon_channel_poll(ch, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLBARRIER_ISBROKEN, NULL, &result);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get is barrier broken state via the poll on the channel.");

    if (result != 0)
        return true;

    return false;
}

/** @brief Get the attributes from a channel
 *
 * Calling this will initialize a channel attributes structure with read-only
 * and user-specified attributes of the channel. Read-only attributes are supplied
 * on this call so a caller may inspect the current state of the channel.
 *
 * @param ch is a pointer to an initialized channel descriptor.
 *
 * @param attr is a pointer to a channel attributes structure that will be
 * initialized by this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_get_attr(const dragonChannelDescr_t* ch, dragonChannelAttr_t* attr)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "channel descriptor is NULL");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve channel from descriptor");

    if (!dragon_channel_is_local(ch))
        err_return(DRAGON_CHANNEL_OPERATION_UNSUPPORTED_REMOTELY,
                   "Cannot get attributes from non-local channel.");

    // TODO: note that this is NOT multi-process/thread safe.  A RW lock is
    // needed to properly protect the attributes during access.

    _attrs_from_header(channel, attr);

    /* These are process local attributes */
    attr->flags = channel->proc_flags;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Register gateway channels from the environment.
 *
 * When communicating off-node by using a transport agent, the channels
 * library will handle packaging send and receive requests and sending them
 * to the transport agent via gateway channels. To enable this, the serialized
 * descriptors of gateway channels are provided to processes via environment
 * variables. This function should be called by any C code that wishes to commmunicate
 * off-node to other nodes within the allocation served by the Dragon run-time services.
 * Any process started by the Dragon run-time services will have the appropriate
 * gateway channels present in the environment. Calling this function, then registers
 * those gateway channels with the channels library.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_register_gateways_from_env()
{
    dragonError_t err;
    char* gw_str;
    char name[DRAGON_CHANNEL_DEFAULT_MAX_GW_ENV_NAME_LENGTH];
    char err_str[400];
    int num_gateways = 0;
    char* num_gw_str;
    bool using_gw_num = false;

    num_gw_str = getenv(DRAGON_NUM_GW_ENV_VAR);
    if (num_gw_str == NULL)
        num_gateways = 9999; /* We'll default to trying until done in this case. */
    else {
        num_gateways = atoi(num_gw_str);
        using_gw_num = true;
    }

    for (int id=1; id<=num_gateways; id++) {
        int nchars = snprintf(name, DRAGON_CHANNEL_DEFAULT_MAX_GW_ENV_NAME_LENGTH, "%s%d",
                              DRAGON_CHANNEL_DEFAULT_GW_ENV_PREFIX, id);

        if (nchars >= 200)
            err_return(DRAGON_FAILURE, "The gateway channel environment variable length was too long.");

        gw_str = getenv(name);

        if (gw_str == NULL) {
            if (using_gw_num) {
                snprintf(err_str, 400,
                            "Gateway channel not found in environment. "
                            "The number of gateways specified was %d and "
                            "only %d were found.",
                            num_gateways, id-1);
                err_return(DRAGON_INVALID_ARGUMENT, err_str);
            } else
                no_err_return(DRAGON_SUCCESS);
        }

        dragonChannelSerial_t gw_ser;
        gw_ser.data = dragon_base64_decode(gw_str, &gw_ser.len);
        if (gw_ser.data == NULL) {
            snprintf(err_str, 400,
                        "The environment variable %s was not a valid "
                        "serialized descriptor.",
                        name);
            err_return(DRAGON_INVALID_ARGUMENT, err_str);
        }

        dragonChannelDescr_t gw_ch;
        err = dragon_channel_attach(&gw_ser, &gw_ch);
        if (err != DRAGON_SUCCESS) {
            snprintf(err_str, 400,
                        "Could not attach to the gateway channel associated "
                        "with environment variable %s.",
                        name);
            append_err_return(DRAGON_INVALID_ARGUMENT, err_str);
        }

        dragon_channel_serial_free(&gw_ser);

        err = dragon_channel_register_gateway(&gw_ch);
        if (err != DRAGON_SUCCESS) {
            snprintf(err_str, 400,
                        "Could not register the gateway channel associated "
                        "with environment variable %s and cuid %lu.",
                        name, gw_ch._idx);
            append_err_return(DRAGON_INVALID_ARGUMENT, err_str);
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Discard the registered gateway channels.
 *
 * Any register gateways may be discarded as part of a process' tear down.
 * This is generally not required, but for completeness is available.
 * *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_discard_gateways()
{

    dragonError_t err;
    dragon_ulist_destroy(dg_gateways);

    err = dragon_ulist_create(dg_gateways);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not discard the gateway definitions.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Register a channel as a gateway channel.
 *
 * A local channel can be promoted to be a gateway channel so that messaging
 * through non-local Channels can be passed through and serviced by a transport
 * agent.  This operation will take a local channel and register it as a
 * Gateway. A particular channel can only be registered once for a given
 * process or its threads.
 *
 * This function is not thread-safe in that registering the same channel from
 * multiple threads at the same time could allow a channel to be registered
 * more than once.
 *
 * @param ch is a pointer to a channel descriptor for the channel to register.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_register_gateway(dragonChannelDescr_t* ch)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "channel descriptor is NULL");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve channel from descriptor");

    if (!dragon_channel_is_local(ch))
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot register non-local channel as gateway.");

    bool is_gw;
    err = _is_cuid_a_gateway(ch->_idx, &is_gw);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "There was an error determining whether this "
                               "channel was already registered as a gateway.");

    // This would be one way to handle this, but it appears that sometimes we don't know
    // not to call it again.
    // if (is_gw)
    //     err_return(DRAGON_CHANNEL_GATEWAY_ALREADY_REGISTERED,
    //                "This channel was already registered as a gateway.");
    if (is_gw)
        no_err_return(DRAGON_SUCCESS);

    dragon_generate_uuid(channel->proc_gw_sendhid);

    err = _register_gateway(channel, &dg_gateways);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "There was an error registering this channel as a gateway.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Unregister a Channel as a Gateway Channel.
 *
 * Remove a previously registered Channel from the internally managed list of
 * Gateway Channels. This removes it as a gateway for this process and any
 * associated threads. It does nothing to deregister it for other processes and
 * does nothing to the underlying channel.
 *
 * Unregistering a gateway channel can only be safely done if all send and
 * receive handles are closed for the current process and its threads,
 * particularly if those send or receive handles refer to a a remote channel.
 * If they are not closed, a send or receive handle could end up referring to a
 * non-existent gateway channel.
 *
 * @param ch is a pointer to a Channel descriptor for the Channel to
 * deregister.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_unregister_gateway(dragonChannelDescr_t* ch)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "channel descriptor is NULL");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve channel from descriptor.");

    if (!dragon_channel_is_local(ch))
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot unregister non-local channel as gateway. This "
                                            "shouldn't have happened, ever.");

    err = _unregister_gateway(channel, dg_gateways);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot unregister channel as a gateway due to some unknown error.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Create a channel set from a list of channels with possible
 * attributes.
 *
 * You create a channel set when you wish to poll across a set of channels.
 * The details of how this is accomplished is handled by the channel set API.
 * In this call you provide a list of the channels you wish to poll across. To
 * get the most efficient implementation of this multi-channel poll, if it is
 * in your control, specify the same default bcast event descriptor in the
 * channel attributes for each channel when it is created.
 *
 * @param ch is a pointer to an initialized channel descriptor.
 *
 * @param ser_bcast is a serialized BCast decriptor to be added to the channel.
 *
 * @param event_mask is the event mask to be monitored in the channel and signaled
 * via the given BCast object.
 *
 * @param user_token is a user-supplied token to be provided when the BCast is triggered.
 *
 * @param channel_token is a pointer to space for an unsigned integer that is returned
 * as a channel identifier when the event is triggered for this channel. This is used by
 * the channel itself, not by the caller, but is provided for subsequent event bcast calls
 * on this channel.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_add_event_bcast(dragonChannelDescr_t* ch, dragonBCastSerial_t* ser_bcast,
                               const short event_mask, int user_token, dragonULInt* channel_token)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    if (ser_bcast == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized bcast cannot be NULL.");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (!dragon_channel_is_local(ch))
        err_return(DRAGON_CHANNEL_OPERATION_UNSUPPORTED_REMOTELY,
                   "Cannot add event BCasts on non-local channel.");

    _obtain_channel_locks(channel);

    if (*channel->header.num_event_bcasts == *channel->header.max_event_bcasts) {
        _release_channel_locks(channel);
        err_return(DRAGON_CHANNEL_EVENT_CAPACITY_EXCEEDED,
                   "The channel is at maximum capacity of channel sets.");
    }

    *channel_token = *channel->header.next_bcast_token;
    *channel->header.next_bcast_token += 1;

    size_t idx = *channel->header.num_event_bcasts;

    channel->event_records[idx].event_mask = event_mask;
    channel->event_records[idx].triggered_since_last_recv = false;
    channel->event_records[idx].triggered_since_last_send = false;
    channel->event_records[idx].user_token = user_token;
    channel->event_records[idx].channel_token = *channel_token;
    channel->event_records[idx].serialized_bcast_len = ser_bcast->len;
    memcpy(&channel->event_records[idx].serialized_bcast, ser_bcast->data, ser_bcast->len);
    *channel->header.num_event_bcasts += 1;

    _release_channel_locks(channel);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Remove an event BCast from this channel.
 *
 * The channel_token returned when a process adds an event bcast to a channel
 * can be used to subsequently delete the even bcast from the channel.
 *
 * @param ch is a pointer to an initialized channel descriptor.
 *
 * @param channel_token is the channel token that was provided when the event
 * bcast was registered. It can be used to remove the event bcast from the channel.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_remove_event_bcast(dragonChannelDescr_t* ch, dragonULInt channel_token)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    if (!dragon_channel_is_local(ch))
        err_return(DRAGON_CHANNEL_OPERATION_UNSUPPORTED_REMOTELY,
                   "Cannot modify event bcasts on non-local channel.");

    int allocation_exists;

    err = dragon_memory_pool_allocation_exists(&channel->pool, DRAGON_MEMORY_ALLOC_CHANNEL,
                                   *channel->header.c_uid, &allocation_exists);

    /* If this happens, the channel was destroyed before removing from the channelset. The
       channelset is being destroyed, so just return and move on. */
    if (allocation_exists == 0) {
        no_err_return(DRAGON_SUCCESS);
    }

    _obtain_channel_locks(channel);

    size_t idx = 0;
    size_t last_idx = *channel->header.num_event_bcasts - 1;
    bool found = false;

    while (!found && idx < *channel->header.num_event_bcasts) {
        if (channel_token == channel->event_records[idx].channel_token) {
            found = true;
            if (idx < last_idx) {
                memcpy(&channel->event_records[idx], &channel->event_records[last_idx],
                       sizeof(dragonEventRec_t));
            }
            *channel->header.num_event_bcasts -= 1;
        } else
            idx += 1;
    }

    _release_channel_locks(channel);

    if (!found)
        err_return(DRAGON_NOT_FOUND, "Could not find the event monitor to be removed");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Update the event mask of an event BCast.
 *
 * If desired, this function can be called to update the event mask of a BCast object
 * that is stored within a channel. This only updates the triggering event for the given channel.
 *
 * @param ch is a pointer to an initialized channel descriptor.
 *
 * @param channel_token is an unsigned integer identifier that was provided when
 * the event bcast was registered.
 *
 * @param event_mask is the new event mask to be applied to the event bcast.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_update_event_mask(dragonChannelDescr_t* ch, dragonULInt channel_token, const short event_mask)
{
    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Channel descriptor cannot be NULL.");

    dragonChannel_t* channel;
    dragonError_t err = _channel_from_descr(ch, &channel);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid channel descriptor");

    _obtain_channel_locks(channel);

    size_t idx = 0;
    bool found = false;

    while (!found && idx < *channel->header.num_event_bcasts) {
        if (channel_token == channel->event_records[idx].channel_token) {
            found = true;
            channel->event_records[idx].event_mask = event_mask;
            channel->event_records[idx].triggered_since_last_recv = false;
            channel->event_records[idx].triggered_since_last_send = false;
        } else
            idx += 1;
    }

    _release_channel_locks(channel);

    if (!found)
        err_return(DRAGON_NOT_FOUND, "Could not find the event monitor to be updated.");

    no_err_return(DRAGON_SUCCESS);
}

/** @} */ // end of group.
