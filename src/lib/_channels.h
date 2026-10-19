#ifndef HAVE_DRAGON_CHANNELS_INTERNAL_H
#define HAVE_DRAGON_CHANNELS_INTERNAL_H

#include "_bcast.h"
#include "priority_heap.h"
#include "ulist.h"
#include "umap.h"
#include <arpa/inet.h>
#include <dragon/bcast.h>
#include <dragon/channels.h>
#include <dragon/utils.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
#define DRAGON_CHANNEL_MAX_SERIALIZED_LEN (DRAGON_MEMORY_MAX_SERIALIZED_LEN + (DRAGON_CHANNEL_CHSER_NULINTS * sizeof(dragonULInt)))
#define DRAGON_CHANNEL_MSGBLK_IS_SERDESCR 1UL
#define DRAGON_CHANNEL_MSGBLK_IS_NOT_SERDESCR 0UL
#define DRAGON_CHANNEL_UMAP_SEED 1877
#define DRAGON_CHANNEL_PRE_SPINS 10
#define DRAGON_CHANNEL_DEFAULT_SENDRECV_SPIN_MAX 32
#define DRAGON_CHANNEL_NUM_POLL_BCASTS 5
#define DRAGON_CHANNEL_DEFAULT_MAX_EVENT_BCASTS 8
#define DRAGON_CHANNEL_DEFAULT_MAX_GW_ENV_NAME_LENGTH 200

/* Cross-language contract: these key formats (DRAGON_RT_UID__<rt> and the
   _BOOTSTRAP_ADDR suffix) must stay identical to the Python helpers in
   src/dragon/infrastructure/runtime_env.py (runtime_route_env_key /
   runtime_bootstrap_env_key), which write these same variables.

   Portable cross-runtime route metadata lives in the environment: for a target
   runtime's rt_uid, DRAGON_RT_UID__<rt> holds a serialized OOB gateway channel
   (only used during single-node/OOB bootstrap) and DRAGON_RT_UID__<rt>_BOOTSTRAP_ADDR
   holds "ip:port" for that runtime's transport ingress. The helpers below build
   those key names and parse the bootstrap address. */

/* Build the DRAGON_RT_UID__<rt_uid> env-var name into env_key. */
static inline bool
dragon_channel_runtime_route_env_key(dragonRT_UID_t rt_uid, char *env_key, size_t env_key_len)
{
    int nchars = snprintf(env_key, env_key_len, "DRAGON_RT_UID__%" PRIu64, rt_uid);
    return nchars > 0 && (size_t) nchars < env_key_len;
}

/* Build the DRAGON_RT_UID__<rt_uid>_BOOTSTRAP_ADDR env-var name into env_key. */
static inline bool
dragon_channel_runtime_bootstrap_env_key(dragonRT_UID_t rt_uid, char *env_key, size_t env_key_len)
{
    int nchars = snprintf(env_key, env_key_len, "DRAGON_RT_UID__%" PRIu64 "_BOOTSTRAP_ADDR", rt_uid);
    return nchars > 0 && (size_t) nchars < env_key_len;
}

/* Return the serialized OOB gateway descriptor published for rt_uid, or NULL if
   none. env_key receives the resolved variable name for error reporting. */
static inline char *
dragon_channel_runtime_gateway_env_value(dragonRT_UID_t rt_uid, char *env_key, size_t env_key_len)
{
    if (!dragon_channel_runtime_route_env_key(rt_uid, env_key, env_key_len)) {
        return NULL;
    }

    return getenv(env_key);
}

/* Parse the target runtime's bootstrap ingress "ip:port" into a host-order IPv4
   address and port. Returns false (and zeros the outputs) when no bootstrap
   address is published for rt_uid or it is malformed. The gateway message layer
   copies these into the message header so the transport agent can reach the
   target runtime's ingress without attaching any runtime-local descriptor. */
static inline bool
dragon_channel_runtime_bootstrap_addr(dragonRT_UID_t rt_uid, dragonUInt *ip_addr, dragonULInt *port)
{
    char env_key[96];

    if (ip_addr == NULL || port == NULL) {
        return false;
    }

    *ip_addr = 0U;
    *port = 0UL;

    if (!dragon_channel_runtime_bootstrap_env_key(rt_uid, env_key, sizeof(env_key))) {
        return false;
    }

    char *bootstrap_addr = getenv(env_key);
    if (bootstrap_addr == NULL) {
        return false;
    }

    char *sep = strrchr(bootstrap_addr, ':');
    if (sep == NULL) {
        return false;
    }

    char *port_str = sep + 1;
    char *endptr;
    if (*port_str < '0' || *port_str > '9') {
        return false;
    }
    errno = 0;
    unsigned long long parsed_port = strtoull(port_str, &endptr, 10);
    if (errno != 0 || endptr == port_str || *endptr != '\0' || parsed_port == 0 || parsed_port > 65535) {
        return false;
    }

    char host_buf[64];
    size_t host_len = (size_t) (sep - bootstrap_addr);
    if (host_len == 0UL || host_len >= sizeof(host_buf)) {
        *port = 0UL;
        return false;
    }

    memcpy(host_buf, bootstrap_addr, host_len);
    host_buf[host_len] = '\0';

    struct in_addr addr;
    if (inet_aton(host_buf, &addr) == 0) {
        *port = 0UL;
        return false;
    }

    *ip_addr = (dragonUInt) ntohl(addr.s_addr);
    *port = (dragonULInt) parsed_port;

    return true;
}

/* These FLAGS are used in the internal flags structure to
   keep track of some shared state in the channel. */
#define DRAGON_CHANNEL_FLAGS_NONE 0
#define DRAGON_CHANNEL_FLAGS_HSTA_CLEANUP_REQUESTED 1


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
    dragonULInt* flags;
    bool* semaphore;
    bool* bounded;
    dragonULInt* initial_sem_value;
    dragonULInt* max_spinners;
    _Atomic(dragonULInt)* available_msgs;
    _Atomic(dragonULInt)* available_blocks;
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
    dragonRT_UID_t owner_rt_uid;
} dragonChannel_t;

#ifdef __cplusplus
}
#endif

#endif
