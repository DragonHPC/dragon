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

/** @defgroup fli_consts API Structures
 *
 *  The fli API constants.
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

/** @defgroup fli_lifecycle Channels Lifecycle Functions
 *  @{
 */

/**
 * @brief Initialize attributes for a FLI adapter.
 *
 * Initialize an attributes structure for FLI adapter customization. You
 * initialize first by calling this function and then you can customize
 * any of the attributes contained within the structure to suit your application.
 *
 * @param attr is a pointer to the attributes structure to be initialized.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_attr_init(dragonFLIAttr_t* attr);

/**
 * @brief Create an FLI adapter.
 *
 * An FLI adapter guarantees that a send and receive handle
 * is between one sender and one receiver and will not have to deal with data
 * interleaving from other processes. In addition, data may be streamed between
 * the sender and receiver when the FLI adapter is not used in buffered mode.
 * FLI adapters may be created in one of several modes.
 *
 * When the main channel is provided the FLI adapter will be use in one of
 * three modes.
 *
 * 1. In buffered mode the main channel is used to communicate on a many
 * to many style connection where each send conversation is a complete conversation
 * between a sender and a receiver. In this mode, if multiple sends are done, they
 * are buffered before sending. In buffered mode receivers will receive the sent
 * message as one receive operation, even if multiple sends were performed.
 *
 * 2. In non-buffered mode the main channel is a channel of stream channels and the
 * API manages allocating a stream channel to an open send handle and providing that
 * stream channel to a receiver by placing its serialized descriptor into the main
 * channel to be picked up by opening a receive handle. In this case the main channel
 * is used to manage 1:1 conversations between an open send handle and an open receive handle.
 *
 * When using this mode, the stream channels come from one of two location. Either
 * there is a manager channel which manages a set of stream channels to be used when
 * sending data, OR a sender may provide a stream channel when opening the send handle.
 *
 * 3. There is one special case when a main channel is used in non-buffered mode
 * and it is known that there is a single sender and single receiver using the FLI
 * adapter. In this case, both the sender and receiver must specify a special
 * constant of STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION for the stream channel
 * argument when opening the send handle and when opening the receive handle.
 *
 * If the mgr_ch is not NULL, then it is used in one of a couple different
 * ways. Please note: When creating an FLI adapter using the buffered protocol
 * no manager channel should be specified.
 *
 * 1. When created in non-buffered mode, the manager channel contains a set
 * of serialized descriptors for stream channels that will be provided
 * to send handles when they are opened. If no main channel exists, then
 * the stream channel must be provided when the receiver opens a receive
 * handle. If stream channels are provided when the receive handle is opened
 * then no main channel is required.
 *
 * 2. When the fli adapter is created, the user may provide a main channel,
 * a manager channel, and a set of stream channels. In this case, the fli
 * adapter will maintain the stream channels and dole them out and re-use them
 * as send and receive handles are opened and closed, always guaranteeing that
 * any conversation between sender and receiver is will not be interleaved with
 * data from other processes.
 *
 * If desired, a stream channel can be provided on either send handle open or
 * receive handle open operations. In that way, the stream channel can be
 * allocated by either the sender or receiver, but not both. When a stream
 * channel is provided on a send or receive open operation your application
 * must decide whether the senders or receivers will be supplying the stream
 * channels. When stream channels are provided on send handle open operations,
 * a manager channel is not necessary. When stream channels are provided on
 * receive handle open operations, a main channel is not necessary.
 *
 * Sharing FLI adapters is possible either by serializing and attaching to the
 * adapter or by re-creating it from its constituent parts. NOTE: When
 * re-creating an adapter by calling this function, the strm_channels should
 * only be provided on the initial call to create. Providing them a second
 * time will result in the channels being added more than once into the
 * adapter which could lead to unpredictable results.
 *
 * @param adapter is a descriptor and opaque handle to the FLI adapter and is
 * initialized by this call.
 *
 * @param main_ch is a channel descriptor for the main channel of this FLI
 * adapter. It is used internally in the adapter. After the life of the
 * adapter it is up to user code to clean up this channel.
 *
 * @param mgr_ch is a channel used internally by the FLI adapter and not to be
 * touched by user code during the life of the adapter. After the life of the
 * adapter it is up to user code to clean up this channel. Supplying a NULL
 * mgr_ch argument indicates this is either a buffered FLI adapter and must be
 * accompanied by a value of 0 for the num_fli_chs argument or a stream channel
 * will be supplied on all send operations.
 *
 * @param pool is a pool to use for internal allocations necessary for the
 * operation of the pool. If pool is NULL, then the pool of the
 * main_ch channel will be used for required adapter allocations. If the main
 * channel is not local, the default local pool will be used.
 *
 * @param num_strm_chs is the number of supplied stream channels that are provided
 * on the creation of the FLI adapter. Each stream channel may be re-used and
 * is used for one stream of messages that result from an open, multiple sends, and a
 * close operation.
 *
 * @param strm_channels is an array of channel descriptors, num_strm_chs of them, that
 * are being supplied on the adapter creation. See the longer discussion in the description
 * of the adapter create above. The application is responsible for the clean up of these
 * channels at the end of their life.
 *
* @param use_buffered_protocol if true then only a main channel should be provided and no
 * manager channel or stream channels are required. In this case all sent data is
 * buffered into one message for all file write operations (all writes on an open send
 * handle). The receiving side receives one message per conversation.
 *
 * @param attr is a pointer to the attributes structure that was previously
 * inited. If the attr arg is NULL the default attributes will be used.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                  dragonChannelDescr_t* mgr_ch, dragonMemoryPoolDescr_t* pool,
                  const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                  const bool use_buffered_protocol, dragonFLIAttr_t* attrs);

/**
 * @brief Destroy the adapter.
 *
 * All internal, process local resources are freed by making this call. Calling
 * destroy does not destroy the underlying channels which were provided when
 * the adapter was created.
 *
 * @param adapter is a descriptor and opaque handle to the FLI adapter.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_destroy(dragonFLIDescr_t* adapter);

/**
 * @brief Serialize a FLI adapter
 *
 * This enable sharing with another process. When sharing
 * an FLI adapter with another process  you may use this function to create a
 * shareable serialized descriptor. This creates a binary string which may not
 * be ASCII compliant. Before sharing, if ASCII compliance is required, call a
 * base64 encoder like the dragon_base64_encode found in dragon/utils.h before
 * sharing and dragon_base64_decode before attaching from the other process.
 *
 * NOTE: You must call dragon_fli_serial_free to free a serialized descriptor
 * after calling this function to free the extra space allocated by this
 * function once you are done with the serialized descriptor.
 *
 * @param adapter is a valid FLI adapter that has previously been created or attached.
 *
 * @param serial is a serialized descriptor that will be initialized with the correct
 * byte count and serialized bytes for so it can be passed to another process.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_serialize(const dragonFLIDescr_t* adapter, dragonFLISerial_t* serial);


/**
 * @brief Free the internal resources of a serialized FLI descriptor
 *
 * This frees internal structures of a serialized FLI descriptor. It does not
 * destroy the FLI adapter itself.
 *
 * @param serial is a serialized FLI descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_serial_free(dragonFLISerial_t* serial);


/**
 * @brief Attach to an FLI adapter
 *
 * Calling this attaches to a FLI adapter by using a serialized FLI descriptor
 * that was passed to this process. The serialized FLI descriptor must have
 * been created using the dragon_FLI_serialize function.
 *
 * @param serial is a pointer to the serialized FLI descriptor.
 *
 * @param pool is the pool to use for memory allocations when sending or
 * receiving on this adapter. If NULL is provided, then the default node-local
 * memory pool will be used.
 *
 * @param adapter is a pointer to an FLI descriptor that will be initialized by
 * this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_attach(const dragonFLISerial_t* serial, const dragonMemoryPoolDescr_t* pool,
                  dragonFLIDescr_t* adapter);

/**
 * @brief Detach from an adapter.
 *
 * All internal, process local resources are freed by making this call. Calling
 * detach does not destroy or detach the underlying channels which were
 * provided when the adapter was created.
 *
 * @param adapter is a descriptor and opaque handle to the FLI adapter.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_detach(dragonFLIDescr_t* adapter);

/**
 * @brief Get available stream channels from adapter.
 *
 * Get the number of stream channels currently available in the manager
 * FLI. This provides a count of the number of channels currently held
 * in reserve in the FLI
 *
 * @param adapter is a descriptor and opaque handle to the FLI adapter.
 *
 * @param count is a pointer to an integer result when DRAGON_SUCCESS is
 * returned. Otherwise the value will be 0.
 *
 * @param timeout is the amount of time to wait. A NULL timeout means to wait
 * indefinitely.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_get_available_streams(dragonFLIDescr_t* adapter, uint64_t* num_streams,
                                const timespec_t* timeout);

/**
 * @brief Query if this FLI is a buffered FLI.
 *
 * Sets the is_buffered flag accordingly if the FLI is buffered or not.
 *
 * @param adapter is a descriptor and opaque handle to the FLI adapter.
 *
 * @param is_buffered is a pointer to a bool result.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_fli_is_buffered(const dragonFLIDescr_t* adapter, bool* is_buffered);


/** @} */ // end of fli_lifecycle group.

/** @defgroup fli_handles
 *  FLI Send/Receive Handle Management
 *  @{
 */

/**
 * @brief Open a Send Handle
 *
 * When writing to the file like adapter interface you must first open a send
 * handle, write using the send operation, and then close the send handle. The
 * adapter guarantees that a receiver will receive the data in the same order
 * it was sent, but not necessarily in the same size chunks.
 *
 * @param adapter is a created or attached FLI descriptor.
 *
 * @param send_handle is a send handle that will be initialized by this call and
 * is to be used on subsequent send operations until this stream is closed.
 *
 * @param strm_ch is a stream channel to be used as a direct connection to a
 * receiving process. A stream channel can only be specified for a receiver or
 * a sender, but not both.
 *
 * As a special case, when there is a known single receiver and single sending
 * using this FLI adapter, the special constant
 * STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION may be used for this stream channel
 * argument. In that case, the same constant must be used for the stream channel
 * when opening the receive handle. No manager channel should exist in this case.
 * As the constant indicates, the main channel will be used as the stream channel
 * in this special case.
 *
 * @param dest_pool is a pool descriptor that can be used to indicate which pool
 * will be used as the destination for sending data through a channel. This
 * pool descriptor can be either a local or remote channel but must exist on
 * the node where the
 *
 * @param allow_strm_term If true, then when the receiver closes the receive handle
 * the sender will be notified via a DRAGON_EOT return code. The FLI will return
 * this on any subsequent send operations once the receiver has canceled the stream.
 * After stream cancelation, the sender should close the send handle since no more
 * data will be sent. For short streams, one or two sent messages, this should
 * be set to false. For longer streams of multiple message sends this should be
 * set to true if there is some situation where the receiver may wish to terminate
 * the stream prematurely. The sender controls this though, not the receiver. If
 * the receiver closes a receive handle prematurely on a non-terminating stream,
 * the rest of the stream will be sent by the sender and discarded automatically
 * the FLI code when the receiver closes the receive handle.
 *
 * @param turbo_mode This may be set as an attribute on the FLI too. If set
 * on the FLI, this argument is ignored. Otherwise, true indicates that
 * data sent with transfer of ownership will be sent and sends will return
 * immediately. This means that the sender may not know of a failure in sending
 * and a receiver should likely have a timeout on receiving should something go wrong.
 *
 * Note: Stream termination is unavailable if the FLI is a buffered FLI.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_open_send_handle(const dragonFLIDescr_t* adapter, dragonFLISendHandleDescr_t* send_handle,
                            dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool, bool allow_strm_term, bool turbo_mode,
                            const timespec_t* timeout);

/**
 * @brief Close a Send Handle
 *
 * All send operations between an open and a close operation are guaranteed to be received
 * in order by a receiving process. A send handle should be closed once the sender has
 * completed sending data. Any buffered data is sent upon closing the send handle.
 *
 * @param send_handle is the open send handle to be closed.
 *
 * @param timeout to be used in attempting to send to the adapter's channel.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_close_send_handle(dragonFLISendHandleDescr_t* send_handle,
                             const timespec_t* timeout);

/**
 * @brief Open a Receive Handle
 *
 * When receiving from the file like adapter interface you must first open a
 * receive handle, receive using the recv operation, and then close the
 * receive handle. The adapter guarantees that a receiver will receive the
 * data in the same order it was sent, but not necessarily in the same size
 * chunks.
 *
 * @param adapter is a created or attached FLI descriptor.
 *
 * @param recv_handle is a receive handle that will be initialized by this call and
 * is to be used on subsequent recv operations until this stream is closed.
 *
 * @param strm_ch is a stream channel to be used as a direct connection to a
 * receiving process. A stream channel can only be specified for a receiver or
 * a sender, but not both. When using the buffered protocol it is not valid
 * to use a stream channel. When not providing a stream channel, NULL should be
 * specified.
 *
 * As a special case, when there is a known single receiver and single sending
 * using this FLI adapter, the special constant
 * STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION may be used for this stream channel
 * argument. In that case, the same constant must be used for the stream channel
 * when opening the send handle. No manager channel should exist in this case.
 * As the constant indicates, the main channel will be used as the stream channel
 * in this special case.
 *
 * @param dest_pool is a pool descriptor pointer that is used to copy the
 * received message into when messages are received from the stream channel.
 * While useful when using recv_mem, other receiving methods will use the pool
 * as a transient space while receiving data and copy into process local
 * storage while freeing the underlying pool data.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_open_recv_handle(const dragonFLIDescr_t* adapter, dragonFLIRecvHandleDescr_t* recv_handle,
                            dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool, const timespec_t* timeout);

/**
 * @brief Close a Recv Handle
 *
 * All receive operations between an open and a close operation are guaranteed to be received
 * in order by a receiving process. A recv handle should be closed once the sender has
 * completed sending data. End of transmission will be indicated by a return code on a recv
 * operation.
 *
 * @param recv_handle is the open receive handle to be closed.
 *
 * @param timeout is used for returning the stream channel to the adapter in some
 * configurations. Otherwise it is ignored.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_close_recv_handle(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout);

/**
 * @brief Set the free memory flag.
 *
 * Set a flag in receive handle to indicate that the dragon managed memory that is received on
 * this receive handle should be freed. This is the default behavior.
 *
 * @param recv_handle is an open receive handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_set_free_flag(dragonFLIRecvHandleDescr_t* recv_handle);

/**
 * @brief Reset the free memory flag.
 *
 * Reset a flag in receive handle to indicate that the dragon managed memory that is received on
 * this receive handle should be freed. Resetting it means that the memory will not be freed as
 * it is received.
 *
 * @param recv_handle is an open receive handle.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_reset_free_flag(dragonFLIRecvHandleDescr_t* recv_handle);

/**
 * @brief Check that a Stream is completely received
 *
 * Check a receive handle to see if a stream has been completely received.
 *
 * @param recv_handle is the open receive handle to be queried.
 *
 * @param stream_received is set upon successful completion.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_fli_stream_received(dragonFLIRecvHandleDescr_t* recv_handle, bool* stream_received);

/**
 * @brief Create a file descriptor to send bytes over an FLI adapter.
 *
 * All writes to the file descriptor will be sent over the FLI adapter. Writes
 * are either buffered or sent immediately as chosen on the call to this
 * function. Internally, an send handle is opened when the file descriptor is
 * created and closed when the file descriptor is closed. A stream channel may
 * be supplied depending on the chosen form of transport. The timeout
 *
 * @param adapter is a created or attached FLI descriptor.
 *
 * @param send_handle is a send_handle for the FLI descriptor. It should
 * be initialized before calling this function. After closing the returned
 * file descriptor, the send_handle should also be closed to insure proper
 * operation.
 *
 * @param fd_ptr is a pointer to an integer. The integer will be initialized to
 * the file descriptor value.
 *
 * @param buffer is a constant of either false (or 0 or NULL), which means use
 * the default behavior, or true in which case it buffers the data until
 * the file descriptor is closed.
 *
 * @param chunk_size is the size of chunks that are attempted to be read from
 * the file descriptor on each send operation. This can be used to fine-tune
 * message sending efficiency through the file descriptor. A chunk size of 0
 * will result in using the default chunk size of 1K chunks.
 *
 * @param arg is a user-defined 64-bit argument to be passed through on the writes
 * to the fli. This argument is not retrievable via a readable file descriptor, but
 * it is accessible via other method of reading from the fli.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout to open the file descriptor. If not NULL, then wait for the
 * specified amount of time and return DRAGON_TIMEOUT if not sucessful. If 0,0
 * is provided, then that indicates that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_fli_create_writable_fd(dragonFLISendHandleDescr_t* send_handle, int* fd_ptr,
                            const bool buffer, size_t chunk_size,
                            const uint64_t arg, const timespec_t* timeout);

/**
 * @brief Finalize and destroy the writable file descriptor.
 *
 * This should be called after closing the created writable file descriptor to
 * insure that all buffers are flushed before continuing to use the send handle.
 * Note that calling this will hang if the file descriptor has not been closed
 * prior to this call.
 *
 * @param send_handle is a valid send handle that was previously used to create a
 * writable file descriptor.
 *
 * @return DRAGON_SUCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_finalize_writable_fd(dragonFLISendHandleDescr_t* send_handle);


/**
 * @brief Create a file descriptor to receive bytes over an FLI adapter.
 *
 * All reads from the file descriptor will be received over the FLI adapter.
 * Receives are returned as they were sent. If buffering was used during
 * sending, then reads from the file descriptor may not match in size and
 * quantity that writes were done. However, total quantity of bytes of
 * information will be as it was sent. Internally, a receive handle is opened
 * when the file descriptor is created and closed when the file descriptor
 * signals end of stream. A stream channel may be supplied depending on the
 * chosen form of transport.
 *
 * @param adapter is a created or attached FLI descriptor.
 *
 * @param recv_handle is a recv_handle for the FLI descriptor. It should
 * be initialized before calling this function. After closing the returned
 * file descriptor, the recv_handle should also be closed to insure proper
 * operation.
 *
 * @param fd_ptr is a pointer to an integer. The integer will be initialized to
 * the file descriptor value.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout to open the file descriptor. If not NULL, then wait for the
 * specified amount of time and return DRAGON_TIMEOUT if not sucessful. If 0,0
 * is provided, then that indicates that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_fli_create_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle, int* fd_ptr,
                              const timespec_t* timeout);

/**
 * @brief Finalize and destroy the readable file descriptor.
 *
 * This should be called after closing the created readable file descriptor to
 * insure that all buffers are flushed before continuing to use the receive handle.
 * Note that calling this will hang if the file descriptor has not been closed
 * prior to this call.
 *
 * @param recv_handle is a valid receive handle that was previously used to create a
 * readable file descriptor.
 *
 * @return DRAGON_SUCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_fli_finalize_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle);

/** @} */ // end of fli_lifecycle group.

/** @defgroup fli_sendrecv
 *  FLI Send/Receive Functions
 *  @{
 */

/**
 * @brief Send bytes through the FLI adapter.
 *
 * All send operations between an open and a close of a send handle are guaranteed to
 * be received by one receiver in the order they were sent.
 *
 * @param send_handle is an open send handle.
 *
 * @param num_bytes is the number of bytes to be sent and must be greater than zero.
 *
 * @param bytes is a pointer to the data to be sent.
 *
 * @param arg is meta-data assigned in a 64-bit field that can be set and will be
 * received by the receiving side. It does not affect the message itself. When using
 * the buffered protocol, only the first write into an open send handle will allow
 * this arg to be passed along. All other values of this arg on subsequent writes
 * to an open send handle are ignored. The value of 0xFFFFFFFFFFFFFFFF is used
 * internally and is not allowed.
 *
 * @param buffer is a constant of either false (or 0 or NULL), which means use
 * the default behavior, or true in which case it buffers the data until
 * it is told to flush the data by either sending more data with buffer == false
 * or by closing the send handle. This is only valid when NOT using the
 * buffered protocol and you want to buffer the data into one message before
 * sending. This argument is ignored when sending via a buffered adapter.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout);

/**
 * @brief Send shared memory through the FLI adapter.
 *
 * All send operations between an open and a close of a send handle are guaranteed to
 * be received by one receiver in the order they were sent.
 *
 * @param mem is a memory descriptor pointer to Dragon managed memory to be sent.
 *
 * @param arg is meta-data assigned in a 64-bit field that can be set and will be
 * received by the receiving side. It does not affect the message itself. When using
 * the buffered protocol, only the first write into an open send handle will allow
 * this arg to be passed along. All other values of this arg on subsequent writes
 * to an open send handle are ignored. The value of 0xFFFFFFFFFFFFFFFF is used
 * internally and is not allowed.
 *
 * @param transfer_ownership is true if ownership of the managed memory should
 * be transferred to the receiver. Passing false means the ownership remains
 * with the sender. This also implies a copy is made on sending.
 *
 * @param no_copy_read_only is true when the original memory allocation should
 * be sent, but will not be cleaned up by anything receiving it due to the
 * read-only nature of the memory being sent. The receiver must be aware of this
 * read-only nature. It will not be notified of this attribute by the sender.
 *
 * @param no_copy_read_only is true when the original memory allocation should
 * be sent, but will not be cleaned up by anything receiving it due to the
 * read-only nature of the memory being sent. The receiver must be aware of this
 * read-only nature. It will not be notified of this attribute by the sender.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/

dragonError_t
dragon_fli_send_mem(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem,
                    uint64_t arg, bool transfer_ownership, bool no_copy_read_only, const timespec_t* timeout);
/**
 * @brief Receive data from the FLI adapter.
 *
 * All receive operations between an open and a close of a recv handle are
 * guaranteed to be received by one receiver in the order they were sent. If
 * the return code comes comes back with DRAGON_EOT, then there is no more
 * data to be received. When DRAGON_EOT is returned there may be valid data
 * with it. The num_bytes will always indicate the amount of valid data returned.
 *
 * @param recv_handle is an open send handle.
 *
 * @param requested_size is the maximum number of bytes to receive. There may be
 * less bytes received. num_bytes provides the actual number of bytes read. If
 * requested_size==0 then all available bytes are read.
 *
 * @param received_size is a pointer to a variable that will be initialized with the
 * number of received bytes.
 *
 * @param bytes points to a pointer that will be initialized with the received bytes.
 * The space pointed to by bytes after this call must be freed.
 *
 * @param arg is a pointer to meta-data assigned in a 64-bit unsigned integer by
 * the sender when the data was sent.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS, DRAGON_EOT or a return code to indicate what problem
 * occurred. When DRAGON_EOT is returned there may also be bytes that were read
 * or there may be zero bytes read.
 **/
dragonError_t
dragon_fli_recv_bytes(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t** bytes, uint64_t* arg,
                      const timespec_t* timeout);

/**
 * @brief Receive data from the FLI adapter.
 *
 * All receive operations between an open and a close of a recv handle are
 * guaranteed to be received by one receiver in the order they were sent. If
 * the return code comes comes back with DRAGON_EOT, then there is no more
 * data to be received. When DRAGON_EOT is returned there may be valid data
 * with it. The num_bytes will always indicate the amount of valid data returned.
 *
 * @param recv_handle is an open send handle.
 *
 * @param requested_size is the maximum number of bytes to receive. There may be
 * less bytes received. num_bytes provides the actual number of bytes read. If
 * requested_size==0 then all available bytes are read.
 *
 * @param received_size is a pointer to a variable that will be initialized with the
 * number of received bytes.
 *
 * @param bytes is a pointer that points to space at least of requested_size. This
 * is provided by the caller of this function and will be filled in with
 * received_size bytes upon successful completion of this call.
 *
 * @param arg is a pointer to meta-data assigned in a 64-bit unsigned integer by
 * the sender when the data was sent.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS, DRAGON_EOT or a return code to indicate what problem
 * occurred. When DRAGON_EOT is returned there may also be bytes that were read
 * or there may be zero bytes read.
 **/
dragonError_t
dragon_fli_recv_bytes_into(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t* bytes, uint64_t* arg,
                      const timespec_t* timeout);

/**
 * @brief Receive a Memory Descriptor from the FLI adapter.
 *
 * All receive operations between an open and a close of a recv handle are
 * guaranteed to be received by one receiver in the order they were sent. This
 * operation is a lower-level receive operation that returns the memory descriptor
 * that was read from the channel.
 *
 * @param recv_handle is an open receive handle.
 *
 * @param mem is a memory descriptor that will be initialized (upon DRAGON_SUCCESS
 * completion) with the shared memory where the message is located.
 *
 * @param arg is a pointer to meta-data assigned in a 64-bit unsigned integer by
 * the sender when the data was sent.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait forever
 * with no timeout. If not NULL, then wait for the specified amount of time and
 * return DRAGON_TIMEOUT if not sucessful. If 0,0 is provided, then that indicates
 * that a try-once attempt is to be made.
 *
 * @return DRAGON_SUCCESS, DRAGON_EOT or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_fli_recv_mem(dragonFLIRecvHandleDescr_t* recv_handle, dragonMemoryDescr_t* mem,
                uint64_t* arg, const timespec_t* timeout);

/** @} */ // end of fli_sendrecv group.

#ifdef __cplusplus
}
#endif

#endif