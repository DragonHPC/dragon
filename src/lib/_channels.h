#ifndef HAVE_DRAGON_CHANNELS_INTERNAL_H
#define HAVE_DRAGON_CHANNELS_INTERNAL_H

#include "_bcast.h"
#include "priority_heap.h"
#include "ulist.h"
#include "umap.h"
#include <dragon/bcast.h>
#include <dragon/channels.h>
#include <dragon/utils.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_CHANNEL_DEFAULT_BYTES_PER_BLOCK 1024
#define DRAGON_CHANNEL_MINIMUM_CAPACITY 1
#define DRAGON_CHANNEL_DEFAULT_LOCK_TYPE DRAGON_LOCK_FIFO_LITE
#define DRAGON_CHANNEL_DEFAULT_OMODE DRAGON_CHANNEL_EXCLUSIVE
#define DRAGON_CHANNEL_DEFAULT_FC_TYPE DRAGON_CHANNEL_FC_NONE
#define DRAGON_CHANNEL_UT_PHEAP_NVALS 1
#define DRAGON_CHANNEL_OT_PHEAP_NVALS 7 // 5 + 2 to cover a UUID at 16 bytes
#define DRAGON_CHANNEL_CHSER_NULINTS 1UL
#define DRAGON_CHANNEL_MSGBLK_IS_SERDESCR 1UL
#define DRAGON_CHANNEL_MSGBLK_IS_NOT_SERDESCR 0UL
#define DRAGON_CHANNEL_UMAP_SEED 1877
#define DRAGON_CHANNEL_PRE_SPINS 10
#define DRAGON_CHANNEL_DEFAULT_SENDRECV_SPIN_MAX 32
#define DRAGON_CHANNEL_NUM_POLL_BCASTS 5
#define DRAGON_CHANNEL_DEFAULT_MAX_EVENT_BCASTS 8
#define DRAGON_CHANNEL_DEFAULT_MAX_GW_ENV_NAME_LENGTH 200

/* attributes and header info embedded into a Channel */
/* NOTE: This must match the pointers assigned
   in _map_header of channels.c */
typedef struct dragonChannelHeader_st {
    dragonC_UID_t* c_uid;
    dragonULInt* bytes_per_msg_block;
    dragonULInt* capacity;
    dragonULInt* lock_type;
    dragonULInt* oflag;
    dragonULInt* fc_type;
    bool* semaphore;
    bool* bounded;
    dragonULInt* initial_sem_value;
    dragonULInt* max_spinners;
    dragonULInt* available_msgs;
    dragonULInt* available_blocks;
    dragonULInt* max_event_bcasts;
    dragonULInt* num_event_bcasts;
    dragonULInt* next_bcast_token;
    dragonULInt* barrier_count;
    dragonULInt* barrier_broken;            /*< When "broken", POLL_RESET must be called */
    dragonULInt* barrier_reset_in_progress; /*< prevent waiting during POLL_RESET */
    dragonULInt* ot_offset;
    dragonULInt* ut_offset;
    dragonULInt* ot_lock_offset;
    dragonULInt* ut_lock_offset;
    dragonULInt* recv_bcast_offset;
    dragonULInt* send_bcast_offset;
    // poll bcast offset is offset of first the poll bcast objects.
    dragonULInt* poll_bcasts_offset;
    dragonULInt* event_records_offset;
    dragonULInt* msg_blks_offset;
    dragonULInt* buffer_pool_descr_ser_len;
    uint8_t* buffer_pool_descr_ser_data;
} dragonChannelHeader_t;

/* The 1 is subtracted because the final buffer_pool_descr_ser_data field in the header is a
   pointer to where it begins in the channel. This field is past the header. */

#define DRAGON_CHANNEL_HEADER_NULINTS ((sizeof(dragonChannelHeader_t) / sizeof(dragonULInt*)) - 1)

typedef struct dragonEventRec_st {
    short event_mask;
    bool triggered_since_last_recv;
    bool triggered_since_last_send;
    char serialized_bcast[DRAGON_BCAST_MAX_SERIALIZED_LEN];
    size_t serialized_bcast_len;
    dragonULInt user_token;
    dragonULInt channel_token;
} dragonEventRec_t;

/* seated channel structure */
typedef struct dragonChannel_st {
    dragonLock_t ot_lock;
    dragonLock_t ut_lock;
    dragonUUID proc_gw_sendhid; /*< Process local send handle id equivalent if Channel is a gateway */
    dragonBCastDescr_t send_bcast;
    dragonBCastDescr_t recv_bcast;
    dragonBCastDescr_t poll_bcasts[DRAGON_CHANNEL_NUM_POLL_BCASTS];
    dragonULInt proc_flags; /*< These are process local flags */
    void* local_main_ptr;
    void** msg_blks_ptrs;
    dragonPriorityHeap_t ot;
    dragonPriorityHeap_t ut;
    dragonMemoryPoolDescr_t pool;
    dragonMemoryDescr_t main_mem;
    dragonChannelHeader_t header;
    atomic_int_fast64_t ref_cnt;
    dragonEventRec_t* event_records;
    dragonChannelSerial_t ch_ser;
    dragonC_UID_t c_uid;
} dragonChannel_t;

#ifdef __cplusplus
}
#endif

#endif
