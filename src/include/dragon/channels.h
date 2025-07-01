/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_CHANNELS_H
#define HAVE_DRAGON_CHANNELS_H

#include <dragon/bcast.h>
#include <dragon/global_types.h>
#include <dragon/managed_memory.h>
#include <dragon/return_codes.h>

#include <signal.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
#include <atomic>
using namespace std;
#else
#include <stdatomic.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_GW_TEST_SLEEP "DRAGON_GW_TEST_SLEEP"
#define DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK 256
#define DRAGON_CHANNEL_DEFAULT_CAPACITY 100

/** @defgroup channels_constants Channels Constants
 *
 *  The channels API constants.
 *  @{
 */

/** @brief Open mode with existing channel
 *
 *  Not yet implemented.
 */
typedef enum dragonChannelOFlag_st {
    DRAGON_CHANNEL_EXCLUSIVE = 0,
    DRAGON_CHANNEL_NONEXCLUSIVE
} dragonChannelOFlag_t;

/** @brief Flow control options
 *
 *  Not yet implemented
 */
typedef enum dragonChannelFC_st {
    DRAGON_CHANNEL_FC_NONE = 0,
    DRAGON_CHANNEL_FC_RESOURCES,
    DRAGON_CHANNEL_FC_MEMORY,
    DRAGON_CHANNEL_FC_MSGS
} dragonChannelFC_t;

/** @brief Channel Poll Event Mask
 *
 *  This defines the valid poll operations. Combinations of the values below
 *  are only allowed when explicitly given in these constants. The POLLINOUT
 *  combination is allowed, but all other constants are valid only individually
 *  for channel poll operations.
*/
typedef enum dragonChannelEvent_st {
    DRAGON_CHANNEL_POLLNOTHING = 0,
        /*!< Used for resetting an event mask. Setting an event mask to this value clears all flags. */

    DRAGON_CHANNEL_POLLIN = 1,
        /*!< Wait for available message. If a timeout occurs, then poll will return with an appropriate
             return code. Otherwise, upon successful completion, a message arrived in the channel. NOTE:
             Due to the nature of channels, another process/thread may have already retrieved the message
             by the time the current process attempts to receive it. */

    DRAGON_CHANNEL_POLLOUT = 2,
        /*!< Wait for available space in channel. If no timeout occurs, the poll operation returns when there
             is space in the channel. As with POLLIN, while space is available when poll returns from POLLOUT
             the space may be gone again before this process can send to the channel if multiple processes
             are sending to the channel simultaneously.*/

    DRAGON_CHANNEL_POLLINOUT = 3,
        /*!< Get notified of available messages or available space in channel. This effectively means that
             a process is notified of any changes to a channel. */

    DRAGON_CHANNEL_POLLEMPTY = 4,
        /*!< Check that channel is empty. The poll operation returns when the channel becomes empty or
             when a timeout occurrs. If the channel is empty when poll is called it returns immediately. */

    DRAGON_CHANNEL_POLLFULL = 5,
        /*!< Check whether channel is full. The poll operation returns when the channel becomes full or
             when a timeout occurs. If the channel is full when poll is called it returns immediately. */

    DRAGON_CHANNEL_POLLSIZE = 6,
        /*!< Get the number of messages in the channel. In this case, unlike other calls to poll, the number of
             messages currently in the channel are return in place of the return code. */

    DRAGON_CHANNEL_POLLRESET = 7,
        /*!< Resets the channel, immediately deleting all messages. The deleted messages are immediately cleared
             with no cleanup. The message contents are not freed, so resetting a channel does not change the
             reference counts of any messages or memory that might have been in the channel at the time of this call. */

    DRAGON_CHANNEL_POLLBARRIER_WAIT = 8,
        /*!< When channel is used as a barrier, wait on barrier with this. Using the channel barrier support is further
             discussed in the channels description. */

    DRAGON_CHANNEL_POLLBARRIER_ABORT = 9,
        /*!< When channel is a barrier, abort the barrier wait and notify current waiters. */

    DRAGON_CHANNEL_POLLBARRIER_RELEASE = 10,
        /*!< When channel is a barrier, release all waiters from the barrier. */

    DRAGON_CHANNEL_POLLBARRIER_ISBROKEN = 11,
        /*!< Check whether the barrier channel is broken or not. Broken is a state it can get into. */

    DRAGON_CHANNEL_POLLBARRIER_WAITERS = 12,
        /*!< Return the number of waiters on the barrier channel. */

    DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS = 13,
        /*!< Return the number of blocked receivers on a channel. */

    DRAGON_SEMAPHORE_P = 14,
        /*!< A Semaphore P operation on a channel of capacity==1. It decrements the semaphore's value or
             waits if the value is 0 for the specified timeout. If a zero timeout is supplied, then it
             returns the appropriate return code when the value in the channel is 0. */

    DRAGON_SEMAPHORE_V = 15,
        /*!< A Semaphore V operation on a channel of capacity==1. It increments the semaphore's value. */

    DRAGON_SEMAPHORE_VZ = 16,
        /*!< A Semaphore wait that unblocks when the count of the semaphore drops to 0. */

    DRAGON_SEMAPHORE_PEEK = 17
        /*!< Peek at the value of the semaphore. Useful in debugging. */


} dragonChannelEvent_t;

/**
 * @brief Channel flags for Testing
 *
 * These flags are for test purposes only.
*/
typedef enum dragonChannelFlags_st {
    DRAGON_CHANNEL_FLAGS_NONE = 0, /*!< Provides the default value. */
    DRAGON_CHANNEL_FLAGS_MASQUERADE_AS_REMOTE = 1 /*!< This is a process local attribute only for testing */
} dragonChannelFlags_t;

/**
 * @brief Constant to be used for transfer of ownership of the message payload area
   on a send operation.
*/
static dragonMemoryDescr_t* const DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP = (dragonMemoryDescr_t*)1;

/**
 * @brief Constant to be used for no-copy send operations where the receiver is assumed to only access the data
 in a read-only fashion.
*/
static dragonMemoryDescr_t* const DRAGON_CHANNEL_SEND_NO_COPY_READ_ONLY = (dragonMemoryDescr_t*)2;

/**
 * @brief Constant to be used for no timeout.
 *
 * For send_msg and get_msg this would mean to wait with no
 * timeout.
*/
static timespec_t const DRAGON_CHANNEL_BLOCKING_NOTIMEOUT = { INT32_MAX, INT32_MAX };

/**
 * @brief Try Once Timeout
 *
 * This indicates a try-once attempt. This works both on-node and off-node.
*/
static timespec_t const DRAGON_CHANNEL_TRYONCE_TIMEOUT = { 0, 0 };

/** @} */ // end of channels_constants group.

/** @defgroup channels_structs Channels API Structures
 *
 *  The channels API structures.
 *  @{
 */

/**
 * @brief The attributes structure of a Channel.
 *
 * This structure contains members that can tune the channel size
 * and performance characteristics. It is also used when retrieving
 * the attributes from an existing channel. Setting and getting attributes from
 * a channel only works on-node.
 *
 **/

typedef struct dragonChannelAttr_st {
    dragonC_UID_t c_uid; /*!< The channel's cuid. Read Only. Set it using a
    separate argument on dragon_channel_create. */

    size_t bytes_per_msg_block; /*!< The bytes per message block. Larger messages
    will be stored indirectly. Read/Write. Ingored if semaphore is true. */

    size_t capacity; /*!< The number of blocks in the channel. Read/Write.
    Ignored if semaphore is true. */

    dragonLockKind_t lock_type; /*!< The type of locks to use inside the channel.
    Read/Write */

    dragonChannelOFlag_t oflag; /*!< Not implemented. */

    dragonChannelFC_t fc_type; /*!< Not implemented. */

    dragonULInt flags; /*!< must be a bitwise combination of dragonChannelFlags_t
    Read/Write */

    dragonMemoryPoolDescr_t* buffer_pool; /*!< The memory pool that is used for
    internal allocations. Read/Write */

    size_t max_spinners; /*!< The maximum allowed spin processes when waiting on
    the channel. More waiters are allowed, but will be IDLE waiters instead.
    Read/Write */

    size_t max_event_bcasts; /*!< The number of channelsets that can register
    this channel. Read/Write */

    int blocked_receivers; /*!< Number of receivers that are currently blocked.
    The value may change before the attributes are returned. Read Only. */

    int blocked_senders; /*!< Number of blocked receivers. The value may change
    before the attributes are returned. Read Only. */

    size_t num_msgs; /*!< Number of messages in the channel. The value may change
    before the attributes are returned. Read Only. */

    size_t num_avail_blocks; /*!< Number of available message blocks in the
    channel. It will always be capacity - num_msgs. It is a snapshot only.
    Read Only. */

    bool broken_barrier; /*!< A channel being used as a barrier is broken if this
    value is non-zero. Read-only. */

    int barrier_count; /*!< The number of waiters on this barrier. Waiting is a
    two-step operation, so this may differ from blocked_receivers. Read Only.
    */

    bool semaphore; /*!< When true the channel will be created as a Semaphore
    channel. You cannot create a send or receive handle on semaphore
    channels. Semaphores are controlled via semaphore poll operations. */

    bool bounded; /*!< Indicates the semaphore is a bounded semaphore. */

    dragonULInt initial_sem_value; /*!< The initial value assigned to the
    semaphore. */

} dragonChannelAttr_t;

/**
 * @brief Receive Notification Type
 *
 * Not yet implemented.
*/
typedef enum dragonChannelRecvNotif_st {
    DRAGON_RECV_SYNC_SIGNAL = 0,
    DRAGON_RECV_SYNC_MANUAL = 1
} dragonChannelRecvNotif_t;

/**
 * @brief Send Handle Attributes
 *
 * These attributes are provided on send handle creation.
*/
typedef struct dragonChannelSendAttr_st {
    dragonUUID sendhid; /*!< Used internally by the Dragon run-time services. */
    dragonChannelSendReturnWhen_t return_mode; /*!< When to return from a send. */
    timespec_t default_timeout; /*!< Default timeout used when NULL is provided as the timeout override. */
    dragonWaitMode_t wait_mode; /*!< Either IDLE wait or SPIN wait may be specified. */
} dragonChannelSendAttr_t;

/**
 * @brief Receive Handle Attributes
 *
 * The attributes are provided on receive handle creation.
*/
typedef struct dragonChannelRecvAttr_st {
    dragonChannelRecvNotif_t default_notif_type; /*!< Notification type is currently unused. */
    timespec_t default_timeout; /*!< Default timeout used when NULL is provided as the timeout override. */
    int signal; /*!< Signal is currently unused. */
    dragonWaitMode_t wait_mode; /*!< Either IDLE wait or SPIN wait may be specified. */
} dragonChannelRecvAttr_t;

/**
 * @brief An opaque channel descriptor
 *
 * When a channel created, a channel descriptor is initialized for
 * the current process. These channel descriptors may be shared with other processes
 * by first serializing them, and then passing the serialized descriptor to another
 * process. The other process must then attach to the channel using the
 * serialized descriptor. Attaching and creating are the two means of initializing
 * a channel descriptor.
 *
*/
typedef struct dragonChannelDescr_st {
    dragonRT_UID_t _rt_idx;
    dragonC_UID_t _idx;
} dragonChannelDescr_t;

/**
 * @brief A serialized channel descriptor
 *
 * This is a two part structure providing an array of bytes and
 * the length of that array.
 *
*/
typedef struct dragonChannelSerial_st {
    size_t len; /*!< The length of the serialized descriptor in bytes. */
    uint8_t * data; /* !<  The serialized descriptor data to be shared. */
} dragonChannelSerial_t;

/**
 * @brief An Opaque Channel Send Handle
 *
 * A send handle must be declared, intialized, and opened before sending to a channel.
 *
*/
typedef struct dragonChannelSendh_st {
    uint8_t _opened;
    dragonChannelDescr_t _ch;
    dragonChannelDescr_t _gw;
    dragonChannelSendAttr_t _attrs;
} dragonChannelSendh_t;

/**
 * @brief An Opaque Channel Receive Handle
 *
 * A receive handle must be declared, initialized, and opened prior to receiving data.
 *
*/
typedef struct dragonChannelRecvh_st {
    uint8_t _opened;
    dragonChannelDescr_t _ch;
    dragonChannelDescr_t _gw;
    dragonChannelRecvAttr_t _attrs;
} dragonChannelRecvh_t;

/**
 * @brief The attributes structure for tuning a Message.
 *
 * This structure contains members that can tune Message behavior and can be used
 * by a transport agent for operation handling for operations on remote
 * Channels.
 *
 **/
typedef struct dragonMessageAttr_st {
    dragonULInt hints;    /*!< Placeholder of future hints about this message */
    dragonULInt clientid; /*!< An identifier of the process that sent this message */
    dragonUUID sendhid;   /*!< An identifier of the send handle for the sending process used for ordering */
    bool send_transfer_ownership; /*!< Used to indicate cleanup by receiver. This can also be specified by providing dest_mem as DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP. */
    bool no_copy_read_only; /*!< Used to indicate the message should not be copied on send, but without transfer of ownership. Receiver must comply! */
} dragonMessageAttr_t;

/**
 *  @brief An Opaque Message structure.
 *
 *  A message must be declared and intialized before it can be sent. Certain attributes
 *  can be provided when initializing the message by declaring and initializing a
 *  dragonMessageAttr_t structure.
 *
 **/
typedef struct dragonMessage_st {
    dragonMessageAttr_t _attr;
    dragonMemoryDescr_t* _mem_descr;
} dragonMessage_t;

/**
 * @brief An Event Notification structure
 *
 * This is the event that occurred in a channelset notification.
 *
 **/
typedef struct dragonChannelEventNotification_st {
    int user_token; /*!< An identifier associated with this event when it was registered. */
    short revent; /*!< The event that occurred. */
} dragonChannelEventNotification_t;

/**
 * @brief Types of GatewayMessages
 *
 * This structure defines the types of GatewayMessages that can be ceated and
 * must be supported by a transport agent.
 *
 **/
typedef enum dragonGatewayMessageKind_st {
    DRAGON_GATEWAY_MESSAGE_SEND, /*!< A send operation */
    DRAGON_GATEWAY_MESSAGE_GET,  /*!< A get operation */
    DRAGON_GATEWAY_MESSAGE_EVENT /*!< An event monitoring operation */
} dragonGatewayMessageKind_t;

/**
 * @brief The Gateway Message Header
 *
 * This is provided here but used internally in the channels
 * implementation and by a transport service when reading gateway messages
 * from a gateway channel. All fields are internal use only unless
 * definining a new transport service beyond those provided with Dragon.
 * All fields are set via gateway message creation.
*/
typedef struct dragonGatewayMessageHeader_st {
    dragonGatewayMessageKind_t* msg_kind; /*!< A send, get, or event message */
    dragonULInt* target_hostid; /*!< Hostid identifying the target of this request. */
    dragonULInt* has_deadline; /*!< If there is a timeout, then this points to true. */
    dragonULInt* deadline_sec; /*!< Seconds of the timeout */
    dragonULInt* deadline_nsec; /*!< Nanoseconds part of timeout */
    atomic_int_fast64_t* client_cmplt; /*!< Set to 1 when client has completed pickup. */
    dragonULInt* transport_cmplt_timestamp; /*!< Used for reporting after timeout. */
    dragonULInt* client_pid; /*!< Client PID useful in debugging. */
    dragonULInt* client_puid; /*!< non-zero when available. */
    dragonULInt* cmplt_bcast_offset; /*!< Offset of the completion bcast */
    dragonULInt* target_ch_ser_offset; /*!< The serialized descriptor of the target channel */
    dragonULInt* target_ch_ser_nbytes; /*!< Number of bytes in target channel serialized descriptor. */
    dragonULInt* send_payload_cleanup_required; /*!< Whether the transport is required to clean up. */
    dragonULInt* send_payload_buffered; /*!< If points to true, then send_payload_offset is offset to
                                           serialized descriptor */
    dragonULInt* send_payload_offset; /*!< When sending, the location of the message to send */
    dragonULInt* send_payload_nbytes; /*!< Number of bytes in send payload. */
    dragonULInt* send_clientid; /*!< used internally for message ordering. */
    dragonULInt* send_hints; /*!< provided on send operation and passed along. */
    dragonULInt* send_return_mode; /*!< provided on send operation and passed along. */
    dragonULInt* has_dest_mem_descr; /*!< On a get, a destination for the received message may be specified. */
    dragonULInt* dest_mem_descr_ser_offset; /*!< Where to put it if a destination was provided. */
    dragonULInt* dest_mem_descr_ser_nbytes; /*!< Size of descriptor when told where to place received message. */
    dragonULInt* op_rc; /*!< The gateway operation return code */
    dragonULInt* event_mask; /*!< On poll this is the poll event to monitor. On poll response it is the event that occurred. */
    dragonUUID* sendhid; /*!< Used in send ordering. */
} dragonGatewayMessageHeader_t;

/**
 *  @brief The Gateway Message structure for interacting with a transport agent.
 *
 *  This structure groups together all information about a message needed by a transport
 *  agent for completing the operation.  Public members can be directly accessed for
 *  fast access to relevant data about the operation while private members facilitate
 *  coordination with a client process.
 *
 **/
typedef struct dragonGatewayMessage_st {
    // clang-format off
    dragonMemoryDescr_t _obj_mem_descr;             /*!< Internal use only. */
    void* _obj_ptr;                                 /*!< Internal use only. */
    dragonBCastDescr_t _cmplt_bcast;                /*!< Internal use only. */
    dragonGatewayMessageHeader_t _header;           /*!< Internal use only. */
    dragonChannelSerial_t target_ch_ser;            /*!< Serialize descriptor of the target Channel */
    dragonGatewayMessageKind_t msg_kind;            /*!< The kind of message (send, get, event) */
    dragonULInt target_hostid;                      /*!< The hostid the target Channel is on */
    timespec_t deadline;                            /*!< When the operation must be completed by */
    dragonChannelSendReturnWhen_t send_return_mode; /*!< When a send operation should return to the caller */
    dragonMessage_t send_payload_message;           /*!< The message being sent for send operations */
    dragonMemorySerial_t* send_dest_mem_descr_ser;  /*!< Optional destination serialized memory descriptor for sends */
    dragonMemorySerial_t* get_dest_mem_descr_ser;   /*!< Optional destination serialized memory descriptor for gets */
    short event_mask;                               /*!< Mask of events to monitor for event operations */
    bool _send_immediate_buffered_complete;         /*!< This process has already called complete on this send kind of gateway message */
    // clang-format on
} dragonGatewayMessage_t;

/**
 *  A serialized Gateway Message handle.
 *
 *  This structure cannot be used directly to manipulate a message. All manipulation of the Gateway Message
 *  occurs via its API functions. A process gets a serialized Gateway Message by calling serialize on a valid
 *  Gateway Message object.
 *
 *  The data is a serialized representation of a Gateway Message object that can be passed between
 *  processes and used to attach to the same object in another process or thread, such as a transport agent.
 *
 **/
typedef struct dragonGatewayMessageSerial_st {
    size_t len;    /**< The length in bytes of the data member */
    uint8_t* data; /**< A pointer to buffer containing the serialized representation */
} dragonGatewayMessageSerial_t;

/** @} */ // end of channels_structs group.

dragonError_t
dragon_channel_create(dragonChannelDescr_t* ch, const dragonC_UID_t c_uid,
                      dragonMemoryPoolDescr_t* pool_descr, const dragonChannelAttr_t* attr);
dragonError_t
dragon_channel_attr_init(dragonChannelAttr_t* attr);

dragonError_t
dragon_channel_attr_destroy(dragonChannelAttr_t* attr);

dragonError_t
dragon_channel_get_attr(const dragonChannelDescr_t* ch, dragonChannelAttr_t* attr);

dragonError_t
dragon_channel_destroy(dragonChannelDescr_t* ch);

dragonError_t
dragon_channel_serialize(const dragonChannelDescr_t* ch, dragonChannelSerial_t* ch_ser);

dragonError_t
dragon_channel_serial_free(dragonChannelSerial_t* ch_ser);

dragonError_t
dragon_channel_attach(const dragonChannelSerial_t* ch_ser, dragonChannelDescr_t* ch);

dragonError_t
dragon_channel_detach(dragonChannelDescr_t* ch);

dragonError_t
dragon_channel_descr_clone(dragonChannelDescr_t * newch_descr, const dragonChannelDescr_t * oldch_descr);

dragonError_t
dragon_channel_get_pool(const dragonChannelDescr_t* ch, dragonMemoryPoolDescr_t* pool_descr);

dragonError_t
dragon_channel_get_hostid(const dragonChannelDescr_t* ch, dragonULInt* hostid);

dragonError_t
dragon_channel_get_uid(const dragonChannelSerial_t* ch_ser, dragonULInt* cuid);

dragonError_t
dragon_channel_pool_get_uid_fname(const dragonChannelSerial_t* ch_ser, dragonULInt* muid, char** pool_fname);

dragonError_t
dragon_channel_send_attr_init(dragonChannelSendAttr_t* send_attr);

dragonError_t
dragon_channel_recv_attr_init(dragonChannelRecvAttr_t* recv_attr);

dragonError_t
dragon_channel_send_attr_destroy(dragonChannelSendAttr_t* send_attr);

dragonError_t
dragon_channel_recv_attr_destroy(dragonChannelRecvAttr_t* recv_attr);

dragonError_t
dragon_channel_sendh(const dragonChannelDescr_t* ch, dragonChannelSendh_t* ch_sh,
                     const dragonChannelSendAttr_t* attr);

dragonError_t
dragon_chsend_get_attr(const dragonChannelSendh_t* ch_sh, dragonChannelSendAttr_t* attr);

dragonError_t
dragon_chsend_set_attr(dragonChannelSendh_t* ch_sh, const dragonChannelSendAttr_t* attr);

dragonError_t
dragon_channel_recvh(const dragonChannelDescr_t* ch, dragonChannelRecvh_t* ch_rh,
                     const dragonChannelRecvAttr_t* attr);

dragonError_t
dragon_chrecv_get_attr(const dragonChannelRecvh_t* ch_rh, dragonChannelRecvAttr_t* attr);

dragonError_t
dragon_channel_message_attr_init(dragonMessageAttr_t* attr);

dragonError_t
dragon_channel_message_attr_destroy(dragonMessageAttr_t* attr);

dragonError_t
dragon_channel_message_getattr(const dragonMessage_t* msg, dragonMessageAttr_t* attr);

dragonError_t
dragon_channel_message_setattr(dragonMessage_t* msg, const dragonMessageAttr_t* attr);

dragonError_t
dragon_channel_message_init(dragonMessage_t* msg, dragonMemoryDescr_t* mem_descr,
                            const dragonMessageAttr_t* attr);

dragonError_t
dragon_channel_message_destroy(dragonMessage_t* msg, const bool free_mem_descr);

dragonError_t
dragon_channel_message_get_mem(const dragonMessage_t* msg, dragonMemoryDescr_t* mem_descr);

dragonError_t
dragon_channel_message_count(const dragonChannelDescr_t* ch, uint64_t* count);

dragonError_t
dragon_channel_barrier_waiters(const dragonChannelDescr_t* ch, uint64_t* count);

dragonError_t
dragon_channel_blocked_receivers(const dragonChannelDescr_t* ch, uint64_t* count);

bool
dragon_channel_barrier_is_broken(const dragonChannelDescr_t* ch);

bool
dragon_channel_is_local(const dragonChannelDescr_t* ch);

dragonError_t
dragon_chsend_open(dragonChannelSendh_t* ch_sh);

dragonError_t
dragon_chsend_close(dragonChannelSendh_t* ch_sh);

dragonError_t
dragon_chsend_send_msg(const dragonChannelSendh_t* ch_sh, const dragonMessage_t* msg_send,
                       dragonMemoryDescr_t* dest_mem_descr, const timespec_t* timeout);
dragonError_t
dragon_chrecv_open(dragonChannelRecvh_t* ch_rh);

dragonError_t
dragon_chrecv_close(dragonChannelRecvh_t* ch_rh);

dragonError_t
dragon_chrecv_get_msg(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv);

dragonError_t
dragon_chrecv_get_msg_notify(dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv, dragonBCastDescr_t* bd);

dragonError_t
dragon_chrecv_get_msg_blocking(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_recv,
                               const timespec_t* timeout);

dragonError_t
dragon_chrecv_peek_msg(const dragonChannelRecvh_t* ch_rh, dragonMessage_t* msg_peek);

dragonError_t
dragon_chrecv_pop_msg(const dragonChannelRecvh_t* ch_rh);

dragonError_t
dragon_channel_poll(const dragonChannelDescr_t* ch, dragonWaitMode_t wait_mode, const short event_mask,
                    const timespec_t* timeout, dragonULInt* result);

dragonError_t
dragon_channel_add_event_bcast(dragonChannelDescr_t* ch, dragonBCastSerial_t* ser_bcast,
                               const short event_mask, int user_token, dragonULInt* channel_token);

dragonError_t
dragon_channel_remove_event_bcast(dragonChannelDescr_t* ch, dragonULInt channel_token);

dragonError_t
dragon_channel_update_event_mask(dragonChannelDescr_t* ch, dragonULInt channel_token, const short event_mask);

dragonError_t
dragon_channel_register_gateways_from_env();

dragonError_t
dragon_channel_discard_gateways();

dragonError_t
dragon_channel_register_gateway(dragonChannelDescr_t* ch);

dragonError_t
dragon_channel_unregister_gateway(dragonChannelDescr_t* ch);

dragonError_t
dragon_channel_gatewaymessage_send_create(dragonMemoryPoolDescr_t* pool_descr,
                                          const dragonMessage_t* send_msg,
                                          dragonMemoryDescr_t* dest_mem_descr,
                                          const dragonChannelDescr_t* target_ch,
                                          const dragonChannelSendAttr_t* send_attr,
                                          const timespec_t* deadline, dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_get_create(dragonMemoryPoolDescr_t* pool_descr,
                                         dragonMemoryDescr_t* dest_mem_descr,
                                         const dragonChannelDescr_t* target_ch, const timespec_t* deadline,
                                         dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_event_create(dragonMemoryPoolDescr_t* pool_descr, short events,
                                           const dragonChannelDescr_t* target_ch, const timespec_t* deadline,
                                           dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_destroy(dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_serialize(const dragonGatewayMessage_t* gmsg,
                                        dragonGatewayMessageSerial_t* gmsg_ser);

dragonError_t
dragon_channel_gatewaymessage_serial_free(dragonGatewayMessageSerial_t* gmsg_ser);

dragonError_t
dragon_channel_gatewaymessage_attach(const dragonGatewayMessageSerial_t* gmsg_ser,
                                     dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_detach(dragonGatewayMessage_t* gmsg);

dragonError_t
dragon_channel_gatewaymessage_transport_start_send_cmplt(dragonGatewayMessage_t* gmsg, const dragonError_t op_err, timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_transport_check_send_cmplt(dragonGatewayMessage_t* gmsg, timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_transport_send_cmplt(dragonGatewayMessage_t* gmsg, const dragonError_t op_err);

dragonError_t
dragon_channel_gatewaymessage_client_send_cmplt(dragonGatewayMessage_t* gmsg, const dragonWaitMode_t wait_mode);

dragonError_t
dragon_channel_gatewaymessage_transport_get_cmplt(dragonGatewayMessage_t* gmsg, dragonMessage_t* msg_recv,
                                                  const dragonError_t op_err);

dragonError_t
dragon_channel_gatewaymessage_transport_start_get_cmplt(dragonGatewayMessage_t* gmsg, dragonMessage_t* msg_recv,
                                                        const dragonError_t op_err, timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_transport_check_get_cmplt(dragonGatewayMessage_t * gmsg, timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_client_get_cmplt(dragonGatewayMessage_t* gmsg, dragonMessage_t* msg_recv, const dragonWaitMode_t wait_mode);

dragonError_t
dragon_channel_gatewaymessage_transport_start_event_cmplt(dragonGatewayMessage_t* gmsg,
                                                          const dragonULInt event_result,
                                                          const dragonError_t op_err,
                                                          timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_transport_check_event_cmplt(dragonGatewayMessage_t* gmsg, timespec_t *deadline);

dragonError_t
dragon_channel_gatewaymessage_transport_event_cmplt(dragonGatewayMessage_t* gmsg,
                                                    const dragonULInt event_result,
                                                    const dragonError_t op_err);

dragonError_t
dragon_channel_gatewaymessage_client_event_cmplt(dragonGatewayMessage_t* gmsg, dragonULInt* event, const dragonWaitMode_t wait_mode);

void
dragon_gatewaymessage_silence_timeouts();

dragonError_t
dragon_create_process_local_channel(dragonChannelDescr_t* ch, uint64_t muid, uint64_t block_size, uint64_t capacity, const timespec_t* timeout);

dragonError_t
dragon_destroy_process_local_channel(dragonChannelDescr_t* ch, const timespec_t* timeout);

#ifdef __cplusplus
}
#endif

#endif
