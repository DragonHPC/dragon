#ifndef HAVE_DRAGON_FLI_INTERNAL_H
#define HAVE_DRAGON_FLI_INTERNAL_H

#include <pthread.h>

#include <dragon/channels.h>
#include <dragon/managed_memory.h>

#define DRAGON_FLI_UMAP_SEED 1605
#define FLI_HAS_MAIN_CHANNEL 1
#define FLI_HAS_MANAGER_CHANNEL 2
#define FLI_USING_BUFFERED_PROTOCOL 4
#define FLI_EOT 0xFFFFFFFFFFFFFFFF
#define FLI_TERMINATOR 0xFFFFFFFFFFFFFFFE

#ifdef __cplusplus
extern "C" {
#endif

/* seated fli structure */
typedef struct dragonFLI_st {
    dragonChannelDescr_t main_ch;
    dragonChannelDescr_t mgr_ch;
    dragonMemoryPoolDescr_t pool;
    dragonULInt num_strm_chs;
    dragonFLIAttr_t attrs;
    bool has_main_ch; /* true if main_ch is initialized and used. */
    bool has_mgr_ch; /* true if mgr_ch is initialized and used. */
    bool use_buffered_protocol; /* true if not using stream channels */
    bool was_attached; /* true if attach is used */
} dragonFLI_t;

/* buffered allocation used for the buffered protocol on these
   adapters. */
typedef struct dragonFLISendBufAlloc_st {
    uint8_t* data;
    size_t num_bytes;
    size_t offset; /* used only on received buffered bytes */
    uint64_t arg; /* used only on received buffered bytes */
    bool free_data;
    struct dragonFLISendBufAlloc_st* next;
} dragonFLISendBufAlloc_t;

/* buffered allocation used for buffered received bytes. */
typedef struct dragonFLIRecvBufAlloc_st {
    dragonMemoryDescr_t mem;
    size_t num_bytes;
    size_t offset; /* used only on received buffered bytes */
    uint64_t arg; /* used only on received buffered bytes */
    struct dragonFLIRecvBufAlloc_st* next;
} dragonFLIRecvBufAlloc_t;

/**
 * @brief An FLI Send Handle
 *
 * When an adapter is open for sending, a send handle is provided which
 * is initialized and used until closed.
 *
*/
typedef struct dragonFLISendHandle_st {
    dragonFLI_t* adapter;
    dragonChannelDescr_t strm_channel;
    dragonChannelSendh_t chan_sendh;
    dragonMemoryPoolDescr_t dest_pool;
    dragonFLISendBufAlloc_t* buffered_allocations;
    dragonChannelDescr_t terminate_stream_channel;
    uint64_t buffered_arg;
    size_t total_bytes;
    bool has_term_channel;
    bool has_dest_pool;
    bool user_supplied;
    bool close_required;
    pthread_t tid; /* used to keep track of send or receive file descriptors */
    int pipe[2];
} dragonFLISendHandle_t;

/**
 * @brief An FLI Receive Handle
 *
 * When an adapter is open for receiving, a recv handle is provided which
 * is initialized and used until closed. The has_term_channel is set to
 * true when there is a terminate stream channel that has been received
 * and attached. For buffered streams there is no terminate stream channel.
 * For non-buffered streams it is possible the terminate channel was
 * destroyed by the sender (by closing the send handle) prior to the
 * receiver receiving the serialized term channel descriptor and
 * attaching to it. In this case, that is not an error. It just means the
 * stream has been completed and cannot (does not need to be) terminated.
 *
*/
typedef struct dragonFLIRecvHandle_st {
    dragonFLI_t* adapter;
    dragonChannelDescr_t strm_channel;
    dragonChannelRecvh_t chan_recvh;
    dragonMemoryPoolDescr_t dest_pool;
    dragonChannelDescr_t terminate_stream_channel;
    bool recv_called;
    bool has_term_channel;
    bool has_dest_pool;
    bool user_supplied;
    bool stream_received;
    bool EOT_received;
    size_t num_bytes_received;
    size_t buffered_bytes;
    dragonFLIRecvBufAlloc_t* buffered_data;
    dragonFLIRecvBufAlloc_t* tail;
    pthread_t tid; /* used to keep track of send or receive file descriptors */
    int pipe[2];
} dragonFLIRecvHandle_t;

#ifdef __cplusplus
}
#endif

#endif