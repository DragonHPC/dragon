/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_FLI_H
#define HAVE_DRAGON_FLI_H

#include <dragon/channels.h>
#include <dragon/global_types.h>
#include <dragon/managed_memory.h>
#include <dragon/return_codes.h>

#ifdef __cplusplus
extern "C" {
#endif

/** @defgroup fli_consts FLI Constants
 *
 *  The FLI constants.
 *  @{
 */

 /**
  * @brief Constant to be used when opening a send and a receive handle as the
  * stream channel argument when wishing to use the main channel as a stream
  * channel. Both sender and receiver must have been designed to use this
  * protocol since no run-time negotiation of this is provided.
  *
  */

static dragonChannelDescr_t* const STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION = (dragonChannelDescr_t*)0x0000000000001111;

 /**
  * @brief Constant to be used when opening a send handle as the
  * stream channel argument when wishing to send one message. Several sends may be done in the
  * fli, but they will all be buffered (ignoring any buffer arguments on sends). The buffered
  * data will be sent when the send handle is closed. The fli receiver code automatically handles any
  * buffered data that was sent on the main channel and the client receiver code will have no
  * idea that the data was sent this way. The client opens a receive handle as normal and
  * receives the data.
  */

static dragonChannelDescr_t* const STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND = (dragonChannelDescr_t*)0x0000000000001112;

/** @} */ // end of fli_consts group.

/** @defgroup fli_structs
 *
 *  The FLI API structures.
 *  @{
 */

/**
 * @brief The attributes structure of an fli adapter.
 *
 * This structure contains members that can tune the file-like interface (fli)
 * adapter.
 **/

typedef struct dragonFLIAttr_st {
    int _placeholder;
} dragonFLIAttr_t;

/**
 * @brief The send handle attributes of an fli send handle.
 *
 * This structure contains members that can tune the send handle of a file-like
 * interface (fli) adapter.
 **/
 typedef struct dragonFLISendAttr_st {
    dragonMemoryPoolDescr_t* dest_pool;

    /*!<  dest_pool is a pool descriptor that can be used to indicate which pool
     will be used as the destination for sending data through a channel. This
     pool descriptor can be either a local or remote channel but must exist
     on the node where the data is sent. The default, NULL, will select the
     default pool on the destination node. */

    bool allow_strm_term;

    /*!< If true, then when the receiver closes the receive handle
     the sender will be notified via a DRAGON_EOT return code. The FLI will
     return this on any subsequent send operations once the receiver has
     canceled the stream. After stream cancelation, the sender should close
     the send handle since no more data will be sent. For short streams, one
     or two sent messages, this should be set to false. For longer streams
     of multiple message sends this should be set to true if there is some
     situation where the receiver may wish to terminate the stream
     prematurely. The sender controls this though, not the receiver. If the
     receiver closes a receive handle prematurely on a non-terminating
     stream, the rest of the stream will be sent by the sender and discarded
     automatically the FLI code when the receiver closes the receive handle.
     The default is false. Note: Stream termination is unavailable if the
     FLI is a buffered FLI. */

    bool turbo_mode;

    /*!< This may be set as an attribute on the FLI too. If set
     on the FLI, this argument is ignored. Otherwise, true indicates that data
     sent with transfer of ownership will be sent and sends will return
     immediately. This means that the sender may not know of a failure in
     sending and a receiver should likely have a timeout on receiving should
     something go wrong. The default is false. */

    bool flush;

    /*!< Flushing will insure a write into a send handle has been deposited
     even off-node when the send handle is closed. This will guarantee that the
     stream channel or buffered channel will contain the written data before
     the send handle close completes. Normally, this value can be false and
     perform faster if it is. On-node all data will be deposited, but when
     writing to off-node streams/channels, using flush will guarantee that
     it is deposited off-node as well before the send handle close
     completes. The default is false. */

    dragonULInt debug;

    /*!< This is a debug constant which has internal use only. If set by a
    user it is ignored. */

} dragonFLISendAttr_t;

/**
 * @brief The receive handle attributes of an fli receive handle.
 *
 * This structure contains members that can tune the receive handle of a
 * file-like interface (fli) adapter.
 **/
typedef struct dragonFLIRecvAttr_st {
    dragonMemoryPoolDescr_t* dest_pool;

    /*!< dest_pool is a pool descriptor that is used to copy the received
     message into when messages are received from the fli. While useful when
     using recv_mem, other receiving methods will use the pool as a
     transient space while receiving data and copy into process local
     storage before freeing the underlying pool data. */

} dragonFLIRecvAttr_t;

/**
 * @brief An opaque fli descriptor
 *
 * When a file like interface adapter is created, an fli descriptor is
 * initialized for the current process. These fli descriptors may be
 * shared with other processes by first serializing them, and then passing the
 * serialized descriptor to another process. The other process must then
 * attach to the fli adapter using the serialized descriptor. Attaching and
 * creating are the two means of initializing an fli descriptor. Serializing
 * and attaching are provided as convenience functions. FLI adapters can also
 * be re-created from their component parts, but a serialized descriptor
 * encapsulates the necessary component parts.
 *
*/
typedef struct dragonFLIDescr_st {
    uint64_t _idx;
} dragonFLIDescr_t;

/**
 * @brief A serialized FLI adapter
 *
 * A serialized FLI adapter can be passed to other processes as a convenience for
 * attaching from other processes. FLI adapters can also be re-created from their
 * constituent parts.
*/

typedef struct dragonFLISerial_st {
    size_t len; /*!< The length of the serialized descriptor in bytes. */
    uint8_t * data; /* !<  The serialized descriptor data to be shared. */
} dragonFLISerial_t;

/***/
/**
 * @brief An FLI Send Handle Descriptor
 *
 * When an adapter is open for sending, a send handle descriptor is provided
 * which is initialized and used until closed. The send handle descriptor is
 * an opaque reference to a send handle.
 *
*/
typedef struct dragonFLISendHandleDescr_st {
    dragonULInt _idx;
} dragonFLISendHandleDescr_t;

/**
 * @brief An FLI Receive Handle Descriptor
 *
 * When an adapter is open for receiving, a recv handle descriptor is provided
 * which is initialized and used until closed. The recv handle descriptor is
 * an opaque reference to a recv handle.
 *
*/
typedef struct dragonFLIRecvHandleDescr_st {
    dragonULInt _idx;
} dragonFLIRecvHandleDescr_t;

/** @} */ // end of fli_structs group.

dragonError_t
dragon_fli_attr_init(dragonFLIAttr_t* attr);

dragonError_t
dragon_fli_task_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
    dragonChannelDescr_t* mgr_ch, dragonChannelDescr_t* task_sem, dragonMemoryPoolDescr_t* pool,
    const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
    const bool use_buffered_protocol, dragonFLIAttr_t* attrs);

dragonError_t
dragon_fli_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                  dragonChannelDescr_t* mgr_ch, dragonMemoryPoolDescr_t* pool,
                  const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                  const bool use_buffered_protocol, dragonFLIAttr_t* attrs);

dragonError_t
dragon_fli_destroy(dragonFLIDescr_t* adapter);

dragonError_t
dragon_fli_serialize(const dragonFLIDescr_t* adapter, dragonFLISerial_t* serial);

dragonError_t
dragon_fli_serial_free(dragonFLISerial_t* serial);

dragonError_t
dragon_fli_attach(const dragonFLISerial_t* serial, const dragonMemoryPoolDescr_t* pool,
                  dragonFLIDescr_t* adapter);

dragonError_t
dragon_fli_detach(dragonFLIDescr_t* adapter);

dragonError_t
dragon_fli_get_available_streams(dragonFLIDescr_t* adapter, uint64_t* num_streams,
                                const timespec_t* timeout);

dragonError_t
dragon_fli_is_buffered(const dragonFLIDescr_t* adapter, bool* is_buffered);

dragonError_t
dragon_fli_new_task(const dragonFLIDescr_t* adapter, const timespec_t* timeout);

dragonError_t
dragon_fli_task_done(const dragonFLIDescr_t* adapter, const timespec_t* timeout);

dragonError_t
dragon_fli_join(const dragonFLIDescr_t* adapter, const timespec_t* timeout);

dragonError_t
dragon_fli_send_attr_init(dragonFLISendAttr_t* attrs);

dragonError_t
dragon_fli_open_send_handle(const dragonFLIDescr_t* adapter, dragonFLISendHandleDescr_t* send_handle,
    dragonChannelDescr_t* strm_ch, dragonFLISendAttr_t* attrs, const timespec_t* timeout);

dragonError_t
dragon_fli_close_send_handle(dragonFLISendHandleDescr_t* send_handle, const timespec_t* timeout);

dragonError_t
dragon_fli_recv_attr_init(dragonFLIRecvAttr_t* attrs);

dragonError_t
dragon_fli_open_recv_handle(const dragonFLIDescr_t* adapter, dragonFLIRecvHandleDescr_t* recv_handle,
    dragonChannelDescr_t* strm_ch, dragonFLIRecvAttr_t* attrs, const timespec_t* timeout);

dragonError_t
dragon_fli_close_recv_handle(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout);

dragonError_t
dragon_fli_set_free_flag(dragonFLIRecvHandleDescr_t* recv_handle);

dragonError_t
dragon_fli_reset_free_flag(dragonFLIRecvHandleDescr_t* recv_handle);

dragonError_t
dragon_fli_stream_received(dragonFLIRecvHandleDescr_t* recv_handle, bool* stream_received);

dragonError_t
dragon_fli_create_writable_fd(dragonFLISendHandleDescr_t* send_handle, int* fd_ptr,
                            const bool buffer, size_t chunk_size,
                            const uint64_t arg, const timespec_t* timeout);

dragonError_t
dragon_fli_finalize_writable_fd(dragonFLISendHandleDescr_t* send_handle);

dragonError_t
dragon_fli_create_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle, int* fd_ptr,
                              const timespec_t* timeout);

dragonError_t
dragon_fli_finalize_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle);

dragonError_t
dragon_fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout);

dragonError_t
dragon_fli_get_buffered_bytes(dragonFLISendHandleDescr_t* send_handle,
    dragonMemoryDescr_t* mem_descr, uint64_t* arg, const timespec_t* timeout);

dragonError_t
dragon_fli_send_mem(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem,
                    uint64_t arg, bool transfer_ownership, bool no_copy_read_only, const timespec_t* timeout);

dragonError_t
dragon_fli_recv_bytes(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t** bytes, uint64_t* arg,
                      const timespec_t* timeout);

dragonError_t
dragon_fli_recv_bytes_into(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t* bytes, uint64_t* arg,
                      const timespec_t* timeout);

dragonError_t
dragon_fli_recv_mem(dragonFLIRecvHandleDescr_t* recv_handle, dragonMemoryDescr_t* mem,
                uint64_t* arg, const timespec_t* timeout);

dragonError_t
dragon_fli_poll(const dragonFLIDescr_t* adapter, const timespec_t* timeout);

dragonError_t
dragon_fli_full(const dragonFLIDescr_t* adapter);

dragonError_t
dragon_fli_num_msgs(const dragonFLIDescr_t* adapter, size_t* num_msgs, const timespec_t* timeout);

#ifdef __cplusplus
}
#endif

#endif
