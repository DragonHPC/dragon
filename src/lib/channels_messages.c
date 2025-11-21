#include "_channels.h"
#include "err.h"
#include "_utils.h"
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>
#include <unistd.h>

//Uncomment the next line when testing for client timeout and gateway message completion.
//Currently only remote get_message has the sleep code, but other messages could. Better
//to test one at a time or you may have one interfere with another.
//#define TEST_GW_TIMEOUT
#define DRAGON_CHANNEL_GWHEADER_NULINTS ((sizeof(dragonGatewayMessageHeader_t)/sizeof(dragonULInt*))-1)
//#define DRAGON_CHANNEL_EXTRA_CHECKS
static timespec_t TRANSPORT_PATIENCE_ON_CLIENT_COMPLETE = {30,0};
static dragonULInt MY_PUID = 0UL;
static dragonULInt MY_PID = 0UL;
static bool init_constants = true;
static bool silence_gw_timeout_msgs = false;

static void _init_consts() {
    init_constants = false;
    MY_PUID = dragon_get_my_puid();
    MY_PID = getpid();
}

static dragonULInt _get_my_puid() {
    if (init_constants)
        _init_consts();
    return MY_PUID;
}

#ifdef TEST_GW_TIMEOUT
static uint _get_test_sleep_secs() {
    uint test_client_sleep = dragon_get_env_var_as_ulint(DRAGON_GW_TEST_SLEEP);
    return test_client_sleep;
}
#endif

static dragonULInt _getpid() {
    if (init_constants)
        _init_consts();
    return MY_PID;
}

static dragonError_t
_gateway_message_bcast_size(size_t payload_sz, size_t * bcast_nbytes)
{
    dragonError_t err = dragon_bcast_size(payload_sz, 1UL, NULL, bcast_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to obtain object size for BCast.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_gateway_message_send_size(size_t ch_ser_nbytes, size_t payload_nbytes, size_t dest_mem_ser_nbtyes, size_t * alloc_size)
{
    size_t bcast_nbytes;
    dragonError_t err = _gateway_message_bcast_size(0, &bcast_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determine BCast size during overall object size calculation.");

    *alloc_size = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);
    *alloc_size += ch_ser_nbytes + bcast_nbytes + payload_nbytes + dest_mem_ser_nbtyes;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_gateway_message_get_size(size_t ch_ser_nbytes, size_t * alloc_size)
{
    size_t bcast_nbytes;
    size_t max_mem_ser_nbytes = dragon_memory_max_serialized_len();
    dragonError_t err = _gateway_message_bcast_size(0, &bcast_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determine BCast size during overall object size calculation.");

    *alloc_size = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);
    *alloc_size += ch_ser_nbytes + bcast_nbytes + max_mem_ser_nbytes;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_gateway_message_event_size(size_t ch_ser_nbytes, size_t * alloc_size)
{
    size_t bcast_nbytes;
    dragonError_t err = _gateway_message_bcast_size(0, &bcast_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determine BCast size during overall object size calculation.");

    *alloc_size = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);
    *alloc_size += ch_ser_nbytes + bcast_nbytes;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_map_gateway_message_header(dragonGatewayMessage_t * gmsg)
{
    dragonULInt * gulptr = gmsg->_obj_ptr;

    gmsg->_header.msg_kind = (dragonGatewayMessageKind_t *)&gulptr[0];
    gmsg->_header.target_hostid = &gulptr[1];
    gmsg->_header.has_deadline = &gulptr[2];
    gmsg->_header.deadline_sec = &gulptr[3];
    gmsg->_header.deadline_nsec = &gulptr[4];
    gmsg->_header.client_cmplt = (atomic_int_fast64_t *)&gulptr[5];
    gmsg->_header.transport_cmplt_timestamp = &gulptr[6];
    gmsg->_header.client_pid = &gulptr[7];
    gmsg->_header.client_puid = &gulptr[8];
    gmsg->_header.cmplt_bcast_offset = &gulptr[9];
    gmsg->_header.target_ch_ser_offset = &gulptr[10];
    gmsg->_header.target_ch_ser_nbytes = &gulptr[11];
    gmsg->_header.send_payload_cleanup_required = &gulptr[12];
    gmsg->_header.send_payload_buffered = &gulptr[13];
    gmsg->_header.send_payload_offset = &gulptr[14];
    gmsg->_header.send_payload_nbytes = &gulptr[15];
    gmsg->_header.send_clientid = &gulptr[16];
    gmsg->_header.send_hints = &gulptr[17];
    gmsg->_header.send_return_mode = &gulptr[18];
    gmsg->_header.has_dest_mem_descr = &gulptr[19];
    gmsg->_header.dest_mem_descr_ser_offset = &gulptr[20];
    gmsg->_header.dest_mem_descr_ser_nbytes = &gulptr[21];
    gmsg->_header.op_rc = &gulptr[22];
    gmsg->_header.event_mask = &gulptr[23];
    gmsg->_header.sendhid = (dragonUUID *)&gulptr[24];

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_assign_gateway_message_header_send(dragonGatewayMessage_t * gmsg, dragonULInt target_hostid,
                                    const struct timespec* deadline, size_t ch_ser_nbytes, dragonULInt payload_cleanup_required,
                                    size_t payload_nbytes, size_t payload_ser_nbytes, size_t dest_mem_ser_nbytes,
                                    dragonChannelSendReturnWhen_t return_mode, const dragonUUID sendhid,
                                    dragonULInt clientid, dragonULInt send_hints)
{
    *(gmsg->_header.msg_kind) = DRAGON_GATEWAY_MESSAGE_SEND;
    *(gmsg->_header.target_hostid) = target_hostid;

    if (deadline != NULL) {
        *(gmsg->_header.has_deadline) = 1UL;
        *(gmsg->_header.deadline_sec) = (dragonULInt)deadline->tv_sec;
        *(gmsg->_header.deadline_nsec) = (dragonULInt)deadline->tv_nsec;
    } else {
        *(gmsg->_header.has_deadline) = 0UL;
    }

    *(gmsg->_header.send_payload_cleanup_required) = payload_cleanup_required;

    *(gmsg->_header.client_cmplt) = 0UL;
    *(gmsg->_header.client_pid) = _getpid();
    *(gmsg->_header.client_puid) = _get_my_puid();

    if (payload_nbytes < payload_ser_nbytes)
        *(gmsg->_header.send_payload_buffered) = 1UL;
    else
        *(gmsg->_header.send_payload_buffered) = 0UL;

    /* On a send, if dest_mem_ser_nbytes is greater than 0 then there is a supplied
       destination memory descriptor. On a receive this is not true. */

    if (dest_mem_ser_nbytes > 0UL)
        *(gmsg->_header.has_dest_mem_descr) = 1UL;
    else
        *(gmsg->_header.has_dest_mem_descr) = 0UL;

    *(gmsg->_header.send_return_mode) = (dragonULInt)return_mode;
    *(gmsg->_header.send_clientid) = clientid;
    *(gmsg->_header.send_hints) = send_hints;
    dragonError_t err = dragon_encode_uuid(sendhid, (void *)gmsg->_header.sendhid);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to encode sendhid UUID into gateway message");

    dragonULInt last_offset = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);

    *(gmsg->_header.cmplt_bcast_offset) = last_offset;
    size_t bsize;
    err = _gateway_message_bcast_size(0, &bsize);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determined bcast object size during header mapping.");
    last_offset += bsize;

    *(gmsg->_header.target_ch_ser_offset) = last_offset;
    last_offset += ch_ser_nbytes;

    *(gmsg->_header.send_payload_offset) = last_offset;
    if (*(gmsg->_header.send_payload_buffered))
        last_offset += payload_nbytes;
    else
        last_offset += payload_ser_nbytes;

    *(gmsg->_header.dest_mem_descr_ser_offset) = last_offset;
    *(gmsg->_header.dest_mem_descr_ser_nbytes) = dest_mem_ser_nbytes;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_assign_gateway_message_header_get(dragonGatewayMessage_t * gmsg, dragonULInt target_hostid,
                                    const struct timespec* deadline, size_t ch_ser_nbytes)
{
    dragonError_t err;

    *(gmsg->_header.msg_kind) = DRAGON_GATEWAY_MESSAGE_GET;
    *(gmsg->_header.target_hostid) = target_hostid;

    if (deadline != NULL) {
        *(gmsg->_header.has_deadline) = 1UL;
        *(gmsg->_header.deadline_sec) = (dragonULInt)deadline->tv_sec;
        *(gmsg->_header.deadline_nsec) = (dragonULInt)deadline->tv_nsec;
    } else {
        *(gmsg->_header.has_deadline) = 0UL;
    }

    *(gmsg->_header.client_cmplt) = 0UL;
    *(gmsg->_header.client_pid) = _getpid();
    *(gmsg->_header.client_puid) = _get_my_puid();

    dragonULInt last_offset = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);

    *(gmsg->_header.cmplt_bcast_offset) = last_offset;
    size_t bsize;
    err = _gateway_message_bcast_size(0, &bsize);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determined bcast object size during header mapping.");
    last_offset += bsize;

    *(gmsg->_header.target_ch_ser_offset) = last_offset;
    last_offset += ch_ser_nbytes;

    *(gmsg->_header.dest_mem_descr_ser_offset) = last_offset;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_assign_gateway_message_header_event(dragonGatewayMessage_t * gmsg, dragonULInt target_hostid,
                                    const struct timespec* deadline, size_t ch_ser_nbytes)
{
    dragonError_t err;

    *(gmsg->_header.msg_kind) = DRAGON_GATEWAY_MESSAGE_EVENT;
    *(gmsg->_header.target_hostid) = target_hostid;

    if (deadline != NULL) {
        *(gmsg->_header.has_deadline) = 1UL;
        *(gmsg->_header.deadline_sec) = (dragonULInt)deadline->tv_sec;
        *(gmsg->_header.deadline_nsec) = (dragonULInt)deadline->tv_nsec;
    } else {
        *(gmsg->_header.has_deadline) = 0UL;
    }

    *(gmsg->_header.client_cmplt) = 0UL;
    *(gmsg->_header.client_pid) = _getpid();
    *(gmsg->_header.client_puid) = _get_my_puid();

    dragonULInt last_offset = DRAGON_CHANNEL_GWHEADER_NULINTS * sizeof(dragonULInt) + sizeof(dragonUUID);

    *(gmsg->_header.cmplt_bcast_offset) = last_offset;
    size_t bsize;
    err = _gateway_message_bcast_size(0, &bsize);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determined bcast object size during header mapping.");
    last_offset += bsize;

    *(gmsg->_header.target_ch_ser_offset) = last_offset;
    last_offset += ch_ser_nbytes;

    *(gmsg->_header.dest_mem_descr_ser_offset) = last_offset;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_encode_gateway_message_objects(dragonGatewayMessage_t * gmsg, dragonChannelSerial_t * ch_ser,
                                dragonMemoryDescr_t * payload_mem_descr, dragonMemoryDescr_t * dest_mem_descr,
                                short events)
{
    void * gptr = gmsg->_obj_ptr;

    void * obj_ptr = gptr + *(gmsg->_header.cmplt_bcast_offset);

    size_t bcast_nbytes;
    dragonError_t err = _gateway_message_bcast_size(0, &bcast_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determine BCast size during object encoding.");

    dragonBCastAttr_t bcast_attr;
    err = dragon_bcast_attr_init(&bcast_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to initialize bcast attributes for gateway message.");

    /* Creating as synchronized will guarantee either client or transport can arrive at waiting/triggering
       first and they will rendezvous without having to worry about which one gets there first */
    bcast_attr.sync_type = DRAGON_SYNC;
    bcast_attr.sync_num = 1;

    err = dragon_bcast_create_at(obj_ptr, bcast_nbytes, 0, 1UL, &bcast_attr, &gmsg->_cmplt_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to create BCast");

    obj_ptr = gptr + *(gmsg->_header.target_ch_ser_offset);
    *(gmsg->_header.target_ch_ser_nbytes) = ch_ser->len;
    memcpy(obj_ptr, ch_ser->data, ch_ser->len);

    if (payload_mem_descr != NULL) {

        obj_ptr = gptr + *(gmsg->_header.send_payload_offset);

        if (*(gmsg->_header.send_payload_buffered)) {
            /* In this case, the payload is small enough it is being included
            in the gateway message. Otherwise (below), a serialized descriptor
            to the payload is included in the gateway message */

            void * payload_ptr;
            dragonError_t err = dragon_memory_get_pointer(payload_mem_descr, &payload_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "could not obtain pointer from message memory descriptor");

            size_t payload_nbytes;
            err = dragon_memory_get_size(payload_mem_descr, &payload_nbytes);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "could not obtain size of send payload");

            memcpy(obj_ptr, payload_ptr, payload_nbytes);
            *(gmsg->_header.send_payload_nbytes) = payload_nbytes;

            if (*(gmsg->_header.send_payload_cleanup_required)) {
                /* if TRANSFER_OF_OWNERSHIP was specified and the
                payload is copied into the gateway message then the
                time for cleanup of the payload memory descriptor is now */
                *(gmsg->_header.send_payload_cleanup_required) = 0UL;

                err = dragon_memory_free(payload_mem_descr);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not free payload memory on remote send operation.");
            }

        } else {

            /* In this case, since a serialized descriptor to the payload is copied into the
            gateway message, the gateway message destroy is responsible for freeing the
            payload memory if the gmsg->_header.send_payload_cleanup_required was set to true. This
            would indicate transfer of ownership was requested. */

            dragonMemorySerial_t payload_mem_ser;
            dragonError_t err = dragon_memory_serialize(&payload_mem_ser, payload_mem_descr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "could not serialize message memory descriptor");

            memcpy(obj_ptr, payload_mem_ser.data, payload_mem_ser.len);
            *(gmsg->_header.send_payload_nbytes) = payload_mem_ser.len;

            err = dragon_memory_serial_free(&payload_mem_ser);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "could not free payload serialized memory descriptor");
        }
    } else {
        *(gmsg->_header.send_payload_cleanup_required) = 0;
        *(gmsg->_header.send_payload_nbytes) = 0;
    }

    if (dest_mem_descr != NULL) {

        obj_ptr = gptr + *(gmsg->_header.dest_mem_descr_ser_offset);

        dragonMemorySerial_t dest_mem_ser;
        dragonError_t err = dragon_memory_serialize(&dest_mem_ser, dest_mem_descr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "could not serialize destination memory descriptor");

        memcpy(obj_ptr, dest_mem_ser.data, dest_mem_ser.len);
        *(gmsg->_header.dest_mem_descr_ser_nbytes) = dest_mem_ser.len;
        *(gmsg->_header.has_dest_mem_descr) = 1UL;

        err = dragon_memory_serial_free(&dest_mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "could not free destination serialized memory descriptor");
    } else {
        *(gmsg->_header.has_dest_mem_descr) = 0UL;
        *(gmsg->_header.dest_mem_descr_ser_nbytes) = 0UL;
    }

    *(gmsg->_header.event_mask) = events;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_gateway_message_from_header(dragonGatewayMessage_t * gmsg)
{
    dragonError_t err;

    void * gptr = gmsg->_obj_ptr;

    gmsg->msg_kind = *(gmsg->_header.msg_kind);
    gmsg->target_hostid = *(gmsg->_header.target_hostid);

    gmsg->target_ch_ser.len = *(gmsg->_header.target_ch_ser_nbytes);
    gmsg->target_ch_ser.data = gptr + *(gmsg->_header.target_ch_ser_offset);

    if (*(gmsg->_header.has_deadline)) {
        gmsg->deadline.tv_sec = *(gmsg->_header.deadline_sec);
        gmsg->deadline.tv_nsec = *(gmsg->_header.deadline_nsec);
    } else {
        gmsg->deadline.tv_sec = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_sec;
        gmsg->deadline.tv_nsec = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_nsec;
    }

    void * bcast_ptr = gptr + *(gmsg->_header.cmplt_bcast_offset);

    err = dragon_bcast_attach_at(bcast_ptr, &gmsg->_cmplt_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to attach to completion BCast object.");

    gmsg->send_dest_mem_descr_ser = NULL;
    gmsg->get_dest_mem_descr_ser = NULL;
    /* event_mask points at one of two values. On the send side it is the event_mask
       and on the completion side it is the event result when there is a result.  */
    gmsg->event_mask = *(gmsg->_header.event_mask);

    if (gmsg->msg_kind == DRAGON_GATEWAY_MESSAGE_SEND) {

        gmsg->send_return_mode = *(gmsg->_header.send_return_mode);

        dragonMessageAttr_t mattr;
        err = dragon_channel_message_attr_init(&mattr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message attributes");

        mattr.clientid = *(gmsg->_header.send_clientid);
        mattr.hints = *(gmsg->_header.send_hints);
        err = dragon_decode_uuid((void *)gmsg->_header.sendhid, mattr.sendhid);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize send handle id from message attributes");

        dragonMemoryDescr_t payload_mem_descr;


        if (*(gmsg->_header.send_payload_buffered)) {

            /* use the object's memory descriptor and clone to an offset and length that is just the buffered msg */
            size_t length = *(gmsg->_header.send_payload_nbytes);


            dragonError_t err = dragon_memory_descr_clone(&payload_mem_descr, &gmsg->_obj_mem_descr,
                                                          *(gmsg->_header.send_payload_offset), &length);

            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Failed to clone a memory descriptor for buffered payload.");

        } else {

            dragonMemorySerial_t pmem_ser;
            pmem_ser.data = gptr + *(gmsg->_header.send_payload_offset);
            pmem_ser.len = *(gmsg->_header.send_payload_nbytes);

            dragonError_t err = dragon_memory_attach(&payload_mem_descr, &pmem_ser);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Failed to attach to payload serialized memory descriptor.");

        }

        err = dragon_channel_message_init(&gmsg->send_payload_message, &payload_mem_descr, &mattr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to construct message structure for payload.");

        if (*(gmsg->_header.has_dest_mem_descr)) {

            gmsg->send_dest_mem_descr_ser = malloc(sizeof(dragonMemorySerial_t));
            if (gmsg->send_dest_mem_descr_ser == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL,
                           "Failed to allocate memory for send destination serialized memory descriptor");

            gmsg->send_dest_mem_descr_ser->data = gptr + *(gmsg->_header.dest_mem_descr_ser_offset);
            gmsg->send_dest_mem_descr_ser->len = *(gmsg->_header.dest_mem_descr_ser_nbytes);
        }

    } else {

        gmsg->send_return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE;
        dragonError_t err = dragon_channel_message_init(&gmsg->send_payload_message, NULL, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to construct empty message for non-sending request.");

    }

    if (gmsg->msg_kind == DRAGON_GATEWAY_MESSAGE_GET) {

        if (*(gmsg->_header.has_dest_mem_descr)) {

            gmsg->get_dest_mem_descr_ser = malloc(sizeof(dragonMemorySerial_t));
            if (gmsg->get_dest_mem_descr_ser == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL,
                           "Failed to allocate memory for get destination serialized memory descriptor");

            gmsg->get_dest_mem_descr_ser->data = gptr + *(gmsg->_header.dest_mem_descr_ser_offset);
            gmsg->get_dest_mem_descr_ser->len = *(gmsg->_header.dest_mem_descr_ser_nbytes);
        }
    } else
        gmsg->get_dest_mem_descr_ser = NULL;

    no_err_return(DRAGON_SUCCESS);
}

static void
_get_deadline_for_cmplt(timespec_t *deadline)
{
    timespec_t now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    dragon_timespec_add(deadline, &now, &TRANSPORT_PATIENCE_ON_CLIENT_COMPLETE);
}

static dragonError_t
_check_client_cmplt(dragonGatewayMessage_t * gmsg, timespec_t *deadline)
{
    timespec_t now;

    if (atomic_load(gmsg->_header.client_cmplt) == 0UL) {
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (dragon_timespec_le(deadline, &now)) {
            no_err_return(DRAGON_TIMEOUT);
        } else {
            no_err_return(DRAGON_EAGAIN);
        }
    } else {
        no_err_return(DRAGON_SUCCESS);
    }
}


static dragonError_t
_validate_and_copy_msg_attr(const dragonMessageAttr_t * mattr, dragonMessageAttr_t * attr_cpy)
{
    /* take all values and copy them.  sendhid may be set, but the library will override if it needs to */
    *attr_cpy = *mattr;

    no_err_return(DRAGON_SUCCESS);
}

// BEGIN USER API

/** @defgroup messages_lifecycle Message Lifecycle Functions
 *  @{
 */

/**
 * @brief Initialize a message attributes structure.
 *
 * When creating user-defined send attributes, this function should be called
 * first, to initialize it. Then the user may override desired attributes
 * before using it in creating a send handle.
 *
 * @param attr A pointer to the send attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_attr_init(dragonMessageAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "message attr cannot be NULL");

    attr->clientid = 0UL;
    attr->hints = 0UL;
    dragon_zero_uuid(attr->sendhid);
    attr->send_transfer_ownership = false;
    attr->no_copy_read_only = false;

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Destroy channle message attributes
 *
 * Calling this guarantees cleanup of any resources that were allocated
 * during the dragon_channel_message_attr_init function call. These two
 * functions should be executed pairwise.
 *
 * @param attr A pointer to a send attributes structure that was previously
 * initialized.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channel_message_attr_destroy(dragonMessageAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "message attr cannot be NULL");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Initialize a new message for transfer through Channels.
 *
 * Create a new Message that either wraps up an existing Managed Memory descriptor or is left empty so it can be
 * later updated with a Managed Memory descriptor.
 *
 * @param msg is a pointer to the dragonMessage structure to update.
 *
 * @param mem_descr is a pointer to a Managed Memory descriptor to wrap up or NULL.
 *
 * @param mattr is a pointer to Message attributes for the new Message or NULL to use default values.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_init(dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr, const dragonMessageAttr_t * mattr)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    dragonError_t err = dragon_channel_message_attr_init(&msg->_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not initialize message attributes.");
    if (mattr != NULL) {
        err = _validate_and_copy_msg_attr(mattr, &msg->_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "message attributes are invalid.");
    }

    /* if they gave NULL for the descriptor, we're building up an empty message container */
    if (mem_descr == NULL) {

        // nothing special to do for now
        msg->_mem_descr = NULL;
        no_err_return(DRAGON_SUCCESS);

    }

    /* attempt to resolve the memory descriptor to a pointer locally */
    void * ptr;
    err = dragon_memory_get_pointer(mem_descr, &ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    msg->_mem_descr = malloc(sizeof(dragonMemoryDescr_t));
    if (msg->_mem_descr == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate memory for new memory descriptor");

    err = dragon_memory_descr_clone(msg->_mem_descr, mem_descr, 0UL, NULL);
    if (err != DRAGON_SUCCESS) {
        free(msg->_mem_descr);
        append_err_return(err, "failed to clone memory descriptor for message");
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy a Message.
 *
 * Destroy the Message and optionally free an underlying Managed Memory allocation associated with it.
 *
 * @param msg is a pointer to the dragonMessage structure to destroy.
 *
 * @param free_mem_descr is a boolean indicating whether or not to free the underlying managed memory allocation.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_destroy(dragonMessage_t * msg, const _Bool free_mem_descr)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    if (free_mem_descr == true) {

        if (msg->_mem_descr == NULL)
            err_return(DRAGON_INVALID_ARGUMENT, "cannot free null memory descriptor");

        dragonError_t err = dragon_memory_free(msg->_mem_descr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot release message buffer memory back to pool");
    }

    if (msg->_mem_descr != NULL) {
        free(msg->_mem_descr);
        msg->_mem_descr = NULL;
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @} */ // end of group.

/** @defgroup messages_functionality Message Functions
 *  @{
 */

/**
 * @brief Get a Managed Memory descriptor associated with a Message.
 *
 * Get a descriptor for the Managed Memory allocation associated with the Message.  An error will be returned
 * if the Message has no Managed Memory descriptor associated with it.
 *
 * @param msg is a pointer to the dragonMessage structure.
 *
 * @param mem_descr is a pointer to update with a Managed Memory descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_get_mem(const dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    if (msg->_mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "no memory descriptor associated with this message");

    /* TODO: Is this cloned descriptor ever freed? */
    dragonError_t err = dragon_memory_descr_clone(mem_descr, msg->_mem_descr, 0, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot clone memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the virtual address and size of the buffer associated with a Message.
 *
 * Get the virtual address and size of the buffer associated with the Message. An error will be returned
 * if the Message has no Managed Memory descriptor associated with it.
 *
 * @param msg is a pointer to the dragonMessage structure.
 *
 * @param ptr is a double pointer to update with the virtual address of the underlying buffer.
 *
 * @param size is a pointer to update with the size in bytes of the underlying buffer.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_get_ptr_info(const dragonMessage_t * msg, void ** ptr, size_t * size)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    if (msg->_mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "no memory descriptor associated with this message");

    dragonError_t err = dragon_memory_get_ptr_info(msg->_mem_descr, ptr, size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get Message attributes.
 *
 * Get the attributes for an existing Message.
 *
 * @param msg is a pointer to the dragonMessage structure.
 *
 * @param attr is a pointer to update with an attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_getattr(const dragonMessage_t * msg, dragonMessageAttr_t * attr)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message attributes");

    *attr = msg->_attr;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Set Message attributes.
 *
 * Set the attributes for an existing Message.
 *
 * @param msg is a pointer to the dragonMessage structure to update with new attributes.
 *
 * @param attr is a pointer to an attributes structure.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_message_setattr(dragonMessage_t * msg, const dragonMessageAttr_t * attr)
{
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message");

    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid message attributes");

    msg->_attr = *attr;

    no_err_return(DRAGON_SUCCESS);
}

/** @} */ // end of group.

/** @defgroup gateway_messages_lifecycle Gateway Message Lifecycle Functions
 *  @{
 */

/**
 * @brief Create a GatewayMessage for sending.
 *
 * Create a new GatewayMessage for a send operation.  The GatewayMessage will be allocated out of the given Managed
 * Memory Pool.  After creation, the message can be serialized and sent into a Gateway Channel for processing
 * by a transport agent.  The client synchronizes for completion with the transport agent by calling
 * dragon_channel_gatewaymessage_client_send_cmplt().
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param pool_descr is a pointer a Managed Memory Pool from which to allocate the message.
 *
 * @param send_msg is a pointer to the message with a payload to send to a remote Channel.
 *
 * @param dest_mem_descr is a pointer to a destination memory descriptor or NULL to let Channels decide where to
 * place the payload on the remote side.
 *
 * @param target_ch is a pointer to a descriptor for the remote target Channel.
 *
 * @param send_attr is a pointer to the send handle attributes associated with the send operation.
 *
 * @param deadline is a pointer to a struct indicating when the actual send operation must finish by as processed by
 * a transport agent.  A deadline in the past is equivalent to returning immediately once the operation has been
 * attempted (ie try once without blocking). A NULL values indicates no deadline and
 * dragon_channel_gatewaymessage_transport_send_cmplt() will only return when the operation completes.
 *
 * @param gmsg is a pointer to the Gateway Message to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_send_create(dragonMemoryPoolDescr_t * pool_descr, const dragonMessage_t * send_msg,
                                          dragonMemoryDescr_t * dest_mem_descr,
                                          const dragonChannelDescr_t * target_ch,
                                          const dragonChannelSendAttr_t * send_attr,
                                          const timespec_t * deadline,
                                          dragonGatewayMessage_t * gmsg)
{
    dragonError_t err;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool_descr cannot be NULL.");

    if (send_msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The send_msg cannot be NULL.");

    if (target_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The target_ch cannot be NULL.");

    if (send_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The send_attr cannot be NULL.");

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The gmsg cannot be NULL.");

    /* We allow dest_mem_descr to specify transfer of ownership. If it is passed that way, then
       set it in the message attributes and reset the destination memory descriptor. */
    if (dest_mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP ||
        dest_mem_descr == DRAGON_CHANNEL_SEND_NO_COPY_READ_ONLY) {

        dragonMessage_t* modifiable_msg = (dragonMessage_t*) send_msg;

        if (dest_mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
            modifiable_msg->_attr.send_transfer_ownership = true;

        if (dest_mem_descr == DRAGON_CHANNEL_SEND_NO_COPY_READ_ONLY)
            modifiable_msg->_attr.no_copy_read_only = true;

        dest_mem_descr = NULL;
    }

    dragonChannelSendReturnWhen_t return_mode = send_attr->return_mode;
    if (deadline!=NULL && deadline->tv_sec == 0 && deadline->tv_nsec == 0 &&
        send_attr->return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED) {
        /* You can't have a try-once (indicated by zero timeout) with
           RETURN_WHEN_BUFFERED. It doesn't make sense to do a send
           and not know whether it worked or not. */
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot have a return mode of WHEN_BUFFERED with a try-once timeout value.");
        /* An alternative would be to just change the return_mode here. This
           might be the right solution if we have code that wants to try once
           with no timeout, then depending on return code, try again with a
           timeout. */
        // return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED;
    }

    dragonULInt target_hostid;
    err = dragon_channel_get_hostid(target_ch, &target_hostid);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to obtain hostid for target channel.");

    if (dest_mem_descr != NULL) {
        dragonULInt dest_mem_host_id;
        dragonMemoryPoolDescr_t dest_mem_pool;
        err = dragon_memory_get_pool(dest_mem_descr, &dest_mem_pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to obtain pool for destination of message.");

        err = dragon_memory_pool_get_hostid(&dest_mem_pool, &dest_mem_host_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to obtain hostid for destination of message.");

        if (dest_mem_host_id != target_hostid)
            err_return(DRAGON_INVALID_ARGUMENT, "The target channel and destination memory must be on the same node and they are not.");
    }

    dragonMemoryDescr_t msg_mem;
    err = dragon_channel_message_get_mem(send_msg, &msg_mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to extract memory descriptor from payload message.");

    size_t max_mem_ser_nbytes = dragon_memory_max_serialized_len();

    size_t msg_ser_nbytes = max_mem_ser_nbytes;
    size_t msg_nbytes;
    err = dragon_memory_get_size(&msg_mem, &msg_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to determine size of payload message.");

    dragonChannelSerial_t target_ch_ser;
    err = dragon_channel_serialize(target_ch, &target_ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to serialize target channel.");

    bool cleanup_payload_required = false;
    if (send_msg->_attr.send_transfer_ownership)
        cleanup_payload_required = true;

    size_t dest_mem_ser_nbytes = 0UL;
    if (dest_mem_descr != NULL)
        dest_mem_ser_nbytes = max_mem_ser_nbytes;

    size_t required_nbytes;
    if (msg_ser_nbytes < msg_nbytes)
        err = _gateway_message_send_size(target_ch_ser.len, msg_ser_nbytes, dest_mem_ser_nbytes, &required_nbytes);
    else
        err = _gateway_message_send_size(target_ch_ser.len, msg_nbytes, dest_mem_ser_nbytes, &required_nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to determine allocation size needed for gateway message.");

    timespec_t timeout;
    err = dragon_timespec_remaining(deadline, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not compute timeout ahead of blocking allocation.");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_alloc_blocking(&gmsg->_obj_mem_descr, pool_descr, required_nbytes, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not allocate space for GatewayMessage from Pool.");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_get_pointer(&gmsg->_obj_mem_descr, &gmsg->_obj_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Unable to get pointer from GatewayMessage memory descriptor.");
        goto gwmsg_alloc_fail;
    }

    memset(gmsg->_obj_ptr, 0, required_nbytes);

    /* Note that sendhid must come in through the send attr as it's a property of the original send handle.
       We cannot get sendhid from the original message because it did not get applied by Channels.  That
       happens only in the on-node send path. clientid and hints, however, are passed via the message attributes */
    dragonMessageAttr_t mattr;
    err = dragon_channel_message_getattr(send_msg, &mattr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to extract attributes from message being sent.");
        goto gwmsg_alloc_fail;
    }

    err = _map_gateway_message_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not map the gateway message header.");
        goto gwmsg_alloc_fail;
    }

    err = _assign_gateway_message_header_send(gmsg, target_hostid, deadline, target_ch_ser.len, cleanup_payload_required,
                                              msg_nbytes, msg_ser_nbytes, dest_mem_ser_nbytes, return_mode,
                                              send_attr->sendhid, mattr.clientid, mattr.hints);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not assign values into gateway message header.");
        goto gwmsg_alloc_fail;
    }

    err = _encode_gateway_message_objects(gmsg, &target_ch_ser, &msg_mem, dest_mem_descr, DRAGON_CHANNEL_POLLNOTHING);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to instatiate objects for gateway message.");
        goto gwmsg_alloc_fail;
    }

    err = _gateway_message_from_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to map gateway message structure from header.");
        goto gwmsg_alloc_fail;
    }

    no_err_return(DRAGON_SUCCESS);

gwmsg_alloc_fail:
    dragon_memory_free(&gmsg->_obj_mem_descr);
gwmsg_serial_fail:
    dragon_channel_serial_free(&target_ch_ser);

    return err;
}

/**
 * @brief Create a GatewayMessage for getting a message.
 *
 * Create a new GatewayMessage for a get operation.  The GatewayMessage will be allocated out of the given Managed
 * Memory Pool.  After creation, the message can be serialized and sent into a Gateway Channel for processing
 * by a transport agent.  The client synchronizes for completion with the transport agent by calling
 * dragon_channel_gatewaymessage_client_get_cmplt().
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param pool_descr is a pointer a Managed Memory Pool from which to allocate the message.
 *
 * @param dest_mem_descr is a descriptor to Managed Memory to place the message into or NULL, which allows the library to
 * decide where to place the message.  The final destination of the message is given by the call to
 * dragon_channel_gatewaymessage_client_get_cmplt().
 *
 * @param target_ch is a pointer to a descriptor for the remote target Channel.
 *
 * @param deadline is a pointer to a struct indicating when the actual send operation must finish by as processed by
 * a transport agent.  A deadline in the past is equivalent to returning immediately once the operation has been
 * attempted (ie try once without blocking).
 *
 * @param gmsg is a pointer to the Gateway Message to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_get_create(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryDescr_t * dest_mem_descr,
                                         const dragonChannelDescr_t * target_ch,
                                         const timespec_t * deadline,
                                         dragonGatewayMessage_t * gmsg)
{
    dragonError_t err;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    if (target_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid target Channel descriptor");

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid gateway message");

    dragonULInt target_hostid;
    err = dragon_channel_get_hostid(target_ch, &target_hostid);
    if (err != DRAGON_SUCCESS) {
        err_noreturn("failed to obtain hostid for target channel");
    }

    if (dest_mem_descr != NULL) {
        dragonMemoryPoolDescr_t pool_descr;
        err = dragon_memory_get_pool(dest_mem_descr, &pool_descr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to determine pool of dest_mem.");

        if (!dragon_memory_pool_is_local(&pool_descr))
            err_return(DRAGON_INVALID_ARGUMENT, "Cannot get remote message into remote destination memory. Destination must be local to get operation.");
    }

    dragonChannelSerial_t target_ch_ser;
    err = dragon_channel_serialize(target_ch, &target_ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to serialize target channel");

    size_t required_nbytes;

    err = _gateway_message_get_size(target_ch_ser.len, &required_nbytes);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to determine allocation size needed for gateway message.");
        goto gwmsg_serial_fail;
    }

    timespec_t timeout;
    err = dragon_timespec_remaining(deadline, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could compute timeout ahead of blocking allocation");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_alloc_blocking(&gmsg->_obj_mem_descr, pool_descr, required_nbytes, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not allocate space for GatewayMessage from Pool");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_get_pointer(&gmsg->_obj_mem_descr, &gmsg->_obj_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("unable to get pointer from GatewayMessage memory descriptor");
        goto gwmsg_alloc_fail;
    }

    memset(gmsg->_obj_ptr, 0, required_nbytes);

    err = _map_gateway_message_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not map header");
        goto gwmsg_alloc_fail;
    }

    err = _assign_gateway_message_header_get(gmsg, target_hostid, deadline, target_ch_ser.len);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not assign values into gateway message header");
        goto gwmsg_alloc_fail;
    }

    err = _encode_gateway_message_objects(gmsg, &target_ch_ser, NULL, dest_mem_descr, DRAGON_CHANNEL_POLLNOTHING);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to instatiate objects for gateway message");
        goto gwmsg_alloc_fail;
    }

    err = _gateway_message_from_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to map gateway message structure from header.");
        goto gwmsg_alloc_fail;
    }

    no_err_return(DRAGON_SUCCESS);

gwmsg_alloc_fail:
    dragon_memory_free(&gmsg->_obj_mem_descr);
gwmsg_serial_fail:
    dragon_channel_serial_free(&target_ch_ser);

    return err;
}

/**
 * @brief Create a GatewayMessage for an event.
 *
 * Create a new GatewayMessage for monitoring for events.  The GatewayMessage will be allocated out of the given
 * Managed Memory Pool.  After creation, the message can be serialized and sent into a Gateway Channel for processing
 * by a transport agent.  The client synchronizes for completion with the transport agent by calling
 * dragon_channel_gatewaymessage_client_event_cmplt().
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param pool_descr is a pointer a Managed Memory Pool from which to allocate the message.
 *
 * @param events is a mask of events to monitor for.  If any of the requested events occur the operation will
 * complete.  The actual event that triggered will be returned in dragon_channel_gatewaymessage_client_event_cmplt().
 *
 * @param target_ch is a pointer to a Channel descriptor for the remote target Channel.
 *
 * @param deadline is a pointer to a struct indicating when the actual send operation must finish by as processed by
 * a transport agent.  A deadline in the past is equivalent to returning immediately once the operation has been
 * attempted (ie try once without blocking).
 *
 * @param gmsg is a pointer to the Gateway Message to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_event_create(dragonMemoryPoolDescr_t * pool_descr, short events,
                                           const dragonChannelDescr_t * target_ch,
                                           const timespec_t * deadline,
                                           dragonGatewayMessage_t * gmsg)
{
    dragonError_t err;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    if (target_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid target Channel descriptor");

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid gateway message");

    dragonULInt target_hostid;
    err = dragon_channel_get_hostid(target_ch, &target_hostid);
    if (err != DRAGON_SUCCESS) {
        err_noreturn("failed to obtain hostid for target channel");
    }

    dragonChannelSerial_t target_ch_ser;
    err = dragon_channel_serialize(target_ch, &target_ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to serialize target channel");

    size_t required_nbytes;

    err = _gateway_message_event_size(target_ch_ser.len, &required_nbytes);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to determine allocation size needed for gateway message.");
        goto gwmsg_serial_fail;
    }

    timespec_t timeout;
    err = dragon_timespec_remaining(deadline, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could compute timeout ahead of blocking allocation");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_alloc_blocking(&gmsg->_obj_mem_descr, pool_descr, required_nbytes, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not allocate space for GatewayMessage from Pool");
        goto gwmsg_serial_fail;
    }

    err = dragon_memory_get_pointer(&gmsg->_obj_mem_descr, &gmsg->_obj_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("unable to get pointer from GatewayMessage memory descriptor");
        goto gwmsg_alloc_fail;
    }

    memset(gmsg->_obj_ptr, 0, required_nbytes);

    err = _map_gateway_message_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not map header");
        goto gwmsg_alloc_fail;
    }

    err = _assign_gateway_message_header_event(gmsg, target_hostid, deadline, target_ch_ser.len);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("could not assign values into gateway message header");
        goto gwmsg_alloc_fail;
    }

    err = _encode_gateway_message_objects(gmsg, &target_ch_ser, NULL, NULL, events);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("failed to instatiate objects for gateway message");
        goto gwmsg_alloc_fail;
    }

    err = _gateway_message_from_header(gmsg);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to map gateway message structure from header.");
        goto gwmsg_alloc_fail;
    }

    no_err_return(DRAGON_SUCCESS);

gwmsg_alloc_fail:
    dragon_memory_free(&gmsg->_obj_mem_descr);
gwmsg_serial_fail:
    dragon_channel_serial_free(&target_ch_ser);

    return err;
}

/**
 * @brief Destroy a GatewayMessage.
 *
 * Destroy a GatewayMessage of any operation type.  This call exists to allow a process to cleanup a GatewayMessage
 * that is not otherwise needed and its use by the completion calls.  Typically a direct call to this function is
 * not needed.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message to destroy.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_destroy(dragonGatewayMessage_t * gmsg)
{
    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    dragonError_t err;

    err = dragon_bcast_destroy(&gmsg->_cmplt_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy gateway message bcast object.");

    /* We don't call dragon_channel_serial_free on the target_ch_ser because
       the serialized descriptor is embedded in the gateway message. */

    if (gmsg->msg_kind == DRAGON_GATEWAY_MESSAGE_SEND) {
        /* We destroy the original message here since we are now done with whatever
        action was required and we free the underlying managed memory for it as well
        if transfer of ownership was requested */
        bool cleanup_required = *(gmsg->_header.send_payload_cleanup_required);

        err = dragon_channel_message_destroy(&gmsg->send_payload_message, cleanup_required);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not destroy the gateway message payload.");
    }

    err = dragon_memory_free(&gmsg->_obj_mem_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the gateway message memory.");

    if (gmsg->send_dest_mem_descr_ser != NULL) {
        free(gmsg->send_dest_mem_descr_ser);
        gmsg->send_dest_mem_descr_ser = NULL;
    }

    if (gmsg->get_dest_mem_descr_ser != NULL) {
        free(gmsg->get_dest_mem_descr_ser);
        gmsg->get_dest_mem_descr_ser = NULL;
    }

    /* zero out the entire gmsg because we don't want any left over values,
       especially pointers. Alternatively we would have to zero out
       _obj_ptr, all pointers in the header structure, and the
       send_dest_mem_descr_ser and the get_dest_mem_descr_ser
       pointers so we'll call memset to take care of all of them at
       once. */

    memset(gmsg, 0, sizeof(dragonGatewayMessage_t));

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Serialize a GatewayMessage.
 *
 * Serialize a GatewayMessage so that another process can interact with the GatewayMessage once the serialized
 * representation is attached to.  The serialized representation can be communicated with another process (the
 * transport agent) through any means (typically a Gateway Channel).
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message to serialize.
 *
 * @param gmsg_ser is a pointer to the serialized Gateway Message message structure to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_serialize(const dragonGatewayMessage_t * gmsg, dragonGatewayMessageSerial_t * gmsg_ser)
{
    dragonMemorySerial_t mem_ser;
    dragonError_t err = dragon_memory_serialize(&mem_ser, &gmsg->_obj_mem_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize gateway message");

    gmsg_ser->data = mem_ser.data;
    gmsg_ser->len = mem_ser.len;

    // This may actually be all that is needed since to attach we just need the base pointer of the object
    // Perhaps there is some validation we could add, however

    return DRAGON_SUCCESS;
}

/**
 * @brief Free a serialize GatewayMessage.
 *
 * Clean up a serialized representation of a Gateway Message.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg_ser is a pointer to the serialized Gateway Message message structure to clean up.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_serial_free(dragonGatewayMessageSerial_t * gmsg_ser)
{
    if (gmsg_ser->data != NULL)
        free(gmsg_ser->data);
    gmsg_ser->data = NULL;
    gmsg_ser->len = 0;

    return DRAGON_SUCCESS;
}

/**
 * @brief Attach to a serialized GatewayMessage.
 *
 * Attach to an existing Gateway Message from a serialized representation.  Once attached, a process can interact
 * with the Gateway Message and access its members.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg_ser is a pointer to the serialized Gateway Message message structure that should be attached.
 *
 * @param gmsg is a pointer to the Gateway Message to intialize with the attached message.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_attach(const dragonGatewayMessageSerial_t * gmsg_ser, dragonGatewayMessage_t * gmsg)
{
    dragonMemorySerial_t mem_ser;

    dragonError_t err;

    if (gmsg_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Gateway serialized message cannot be NULL.");

    if (gmsg_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized gateway message structure was not initialized.");

    if (gmsg_ser->len == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized gateway message structure cannot have 0 length and be valid.");

    mem_ser.data = gmsg_ser->data;
    mem_ser.len = gmsg_ser->len;

    gmsg->_send_immediate_buffered_complete = false; /* this process will set to true once complete is called */

    err = dragon_memory_attach(&gmsg->_obj_mem_descr, &mem_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach gateway message memory");

    err = dragon_memory_get_pointer(&gmsg->_obj_mem_descr, &gmsg->_obj_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to get pointer from GatewayMessage memory descriptor.");

    err = _map_gateway_message_header(gmsg);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not map header");

    err = _gateway_message_from_header(gmsg);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize gateway message from shared memory.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Detach from a GatewayMessage.
 *
 * Detaching from a Gateway Message is not typically done directly.  The completion functions will internally do
 * this and use this function.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message from which to detach.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_detach(dragonGatewayMessage_t * gmsg)
{
    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    dragonError_t err;

    err = dragon_bcast_detach(&gmsg->_cmplt_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from gateway message bcast object.");

    /* We don't call dragon_channel_serial_free on the target_ch_ser because
       the serialized descriptor is embedded in the gateway message. */

    if (gmsg->msg_kind == DRAGON_GATEWAY_MESSAGE_SEND) {
        err = dragon_channel_message_destroy(&gmsg->send_payload_message, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the gateway message payload.");
    }

    if (gmsg->send_dest_mem_descr_ser != NULL) {
        free(gmsg->send_dest_mem_descr_ser);
        gmsg->send_dest_mem_descr_ser = NULL;
    }

    if (gmsg->get_dest_mem_descr_ser != NULL) {
        free(gmsg->get_dest_mem_descr_ser);
        gmsg->get_dest_mem_descr_ser = NULL;
    }

    gmsg->_obj_ptr = NULL;

    err = dragon_memory_detach(&gmsg->_obj_mem_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from gateway message memory descriptor.");

    /* zero out the entire gmsg because we don't want any left over values,
       especially pointers. Alternatively we would have to zero out
       _obj_ptr, all pointers in the header structure, and the
       send_dest_mem_descr_ser and the get_dest_mem_descr_ser
       pointers so we'll call memset to take care of all of them at
       once. */

    memset(gmsg, 0, sizeof(dragonGatewayMessage_t));

    no_err_return(DRAGON_SUCCESS);
}

/** @} */ // end of group.

/** @defgroup gateway_messages_functionality Gateway Message Functions
 *  @{
 */

/**
 * @brief Start the completion of a send operation from a transport agent.
 *
 * Once a transport agent has completed a send operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for send to complete.
 *
 * @param op_err is an error code to propagate back to the client.
 *
 * @param deadline is the timeout for coordinating with the client.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_start_send_cmplt(dragonGatewayMessage_t *gmsg, const dragonError_t op_err, timespec_t *deadline)
{
    dragonError_t err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    if (gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED ||
        gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED ||
        (gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED && *(gmsg->_header.send_payload_buffered) == 0UL)) {

        if ((*(gmsg->_header.client_cmplt)) != 0UL)
            err_return(DRAGON_INVALID_OPERATION, "Gateway transport send complete already called. Operation ignored.");

        /* DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY and DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED
           don't wait for the transport service to complete the send action so in those two cases
           we skip this step. DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE is only valid for get and event
           gateway messages, so it is not checked here */

        /* Store the return code in the shared gateway message object */
        *(gmsg->_header.op_rc) = op_err;

        /* Here we get the time for debugging purposes. We want to know the amount of time
        from when the transport calls this trigger_all to when the client actually wakes
        up and signals completion of the gateway message interaction. */
        double time_val = dragon_get_current_time_as_double();
        *((double*)gmsg->_header.transport_cmplt_timestamp) = time_val;

        err = dragon_bcast_trigger_all(&gmsg->_cmplt_bcast, NULL, NULL, 0);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not trigger the completion bcast for the gateway message on behalf of the transport service.");
    } else {

        if (gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED &&
            op_err != DRAGON_SUCCESS) {
            /* If the message was buffered inside the gateway message, and
               return when buffered was specified, then the transport agent
               cannot communicate a non-zero return code back to the user
               so the transport agent must report the error in that case. */
            char err_str[200];
            snprintf(err_str, 199, "The return code of %u was ignored by the channels gateway library because DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED was specified.", op_err);
            err_return(DRAGON_INVALID_OPERATION, err_str);
        }

        if (gmsg->_send_immediate_buffered_complete)
            err_return(DRAGON_INVALID_OPERATION, "Gateway transport send complete already called in this process. Operation ignored.");

        gmsg->_send_immediate_buffered_complete = true;
    }

    _get_deadline_for_cmplt(deadline);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check on the completion of a send operation from a transport agent.
 *
 * Once a transport agent has completed a send operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client. The given Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for send to complete.
 *
 * @param deadline is the timeout for coordinating with the client.
 *
 * @return DRAGON_SUCCESS, DRAGON_EAGAIN, DRAGON_TIMEOUT, or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_check_send_cmplt(dragonGatewayMessage_t *gmsg, timespec_t *deadline)
{
    dragonError_t err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    if (gmsg->_header.client_cmplt == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "client_cmplt pointer is NULL.");

    if (gmsg->_header.client_pid == NULL || gmsg->_header.client_puid == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "client pid and puid info are NULL.");

    /* When the gmsg->_header.client_cmplt is set, the client has detached from the gateway message and we
       can safely destroy the message */
    err = _check_client_cmplt(gmsg, deadline);
    if (err == DRAGON_TIMEOUT) {
        dragonULInt desired = 1UL;
        /* Try one more time, but also mark the flag so client knows no one is waiting. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired))
            err = DRAGON_SUCCESS;

        if (err == DRAGON_TIMEOUT && !silence_gw_timeout_msgs) {
            /* The completion interaction with the client timed out. We'll gather some more information. */
            /* We print to stderr so it is picked up by a monitoring thread and logged in the Dragon logs. */
            char err_str[200];
            snprintf(err_str, 199, "ERROR: GATEWAY SEND MSG COMPLETION ERROR (EC=%s) Client PID=%llu and PUID(if available)=%llu\n", dragon_get_rc_string(err), *gmsg->_header.client_pid, *gmsg->_header.client_puid);
            fprintf(stderr, "%s\n", err_str);
        }
    }

    no_err_return(err);
}

/**
 * @brief Complete a send operation from a transport agent.
 *
 * Once a transport agent has completed a send operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.  This function will manage cleanup of the Gateway Message.  The given
 * Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for send to complete.
 *
 * @param op_err is an error code to propagate back to the client.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_send_cmplt(dragonGatewayMessage_t * gmsg, const dragonError_t op_err)
{
    dragonError_t err = DRAGON_SUCCESS;
    timespec_t deadline;

    err = dragon_channel_gatewaymessage_transport_start_send_cmplt(gmsg, op_err, &deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not start the completion of the gateway request.");

    do {
        err = dragon_channel_gatewaymessage_transport_check_send_cmplt(gmsg, &deadline);
        PAUSE();
    } while (err == DRAGON_EAGAIN);

    if (err != DRAGON_SUCCESS && err != DRAGON_TIMEOUT)
        append_err_return(err, "Problem while waiting on client during gateway completion handshake.");

    /* Since we have logged the error we return success at the end because transport processing
        is now complete. */
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Complete a send operation from a client.
 *
 * Wait for completion of the given Gateway Message send operation.  This function will manage cleanup of the Gateway
 * Message.  The given Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for send to complete.
 *
 * @param wait_mode is the type of waiting to do while the send completes: spin, idle, or adaptive.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_client_send_cmplt(dragonGatewayMessage_t * gmsg, const dragonWaitMode_t wait_mode)
{
    dragonError_t err = DRAGON_SUCCESS;
    dragonError_t send_err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The gateway message cannot be NULL");

    /* This check is global. The processing was already done */
    if ((*(gmsg->_header.client_cmplt)) != 0UL)
        err_return(DRAGON_INVALID_OPERATION, "Gateway client send complete already called. Operation ignored.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_SEND)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call client send complete on non-send kind of gateway message");

    if (gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED ||
        gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED ||
        (gmsg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED && *(gmsg->_header.send_payload_buffered) == 0UL)) {

        /* DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY and DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED (when the
           message was buffered inside the gateway message) don't wait for the transport service to complete
           the send action so in those two cases we skip this step. DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE is
           only valid for get and event gateway messages, so it is not checked here */

        err = dragon_bcast_wait(&gmsg->_cmplt_bcast, wait_mode, NULL, NULL, NULL, NULL, NULL);

        send_err = *(gmsg->_header.op_rc); /* get the return code from the send operation to return to client */
    } else {
        send_err = DRAGON_SUCCESS;
    }

    if (send_err != DRAGON_SUCCESS)
        err_noreturn("The transport signaled a non-successful completion to the send request.");

    if (err == DRAGON_SUCCESS) {
        dragonULInt desired = 1UL;

        /* Mark the transaction as complete to release the transport, but check that the transport
        has not quit waiting. If it has, then we may not be able to trust the data. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired) != 0UL)
            err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;
    } else if (err == DRAGON_OBJECT_DESTROYED)
        err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;

    if (send_err == DRAGON_SUCCESS && err != DRAGON_SUCCESS) {
        char err_str[200];
        char* saved_err_msg = dragon_getlasterrstr();
        double end_time = dragon_get_current_time_as_double();
        double start_time = *((double*)gmsg->_header.transport_cmplt_timestamp);
        double diff = end_time-start_time;
        snprintf(err_str, 199, "The completion of the send gateway message, for process GW_PID=%llu, PID=%llu and GW_PUID(if available)=%llu,PUID=%llu , timed out in the transport with a time of %f seconds.", *gmsg->_header.client_pid, _getpid(), *gmsg->_header.client_puid, _get_my_puid(),diff);
        dragon_channel_gatewaymessage_detach(gmsg);
        err_noreturn(saved_err_msg);
        free(saved_err_msg);
        append_err_return(err, err_str);
    }

    dragonError_t derr = dragon_channel_gatewaymessage_detach(gmsg);
    if (send_err == DRAGON_SUCCESS && err == DRAGON_SUCCESS && derr != DRAGON_SUCCESS)
        append_err_return(derr, "The client send completion could not detach from the gateway message for some reason.");


    return send_err;
}

/**
 * @brief Start the completion of a get operation from a transport agent.
 *
 * Once a transport agent has completed a get operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for get to complete.
 *
 * @param msg_recv is a pointer to the received Message that will be provided back to the client when the
 * op_err argument is set to DRAGON_SUCCESS. Otherwise, it should be NULL.
 *
 * @param op_err is an error code to propagate back to the client. DRAGON_SUCCESS indicates the requested
 * message is returned. Otherwise, the error code reflects the error that occurred.
 *
 * @param deadine is the timeout for coordination with the client
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred on the call.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_start_get_cmplt(dragonGatewayMessage_t *gmsg, dragonMessage_t *msg_recv,
                                                        const dragonError_t op_err, timespec_t *deadline)
{
    dragonError_t err;
    dragonMemoryDescr_t msg_mem;
    dragonMemorySerial_t msg_mem_ser;
    dragonMessageAttr_t msg_attrs;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    /* This is a global check that this gateway message processing is complete already */
    if ((*(gmsg->_header.client_cmplt)) != 0UL)
        err_return(DRAGON_INVALID_OPERATION, "Gateway transport get complete already called. Operation ignored.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_GET)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call transport get complete on non-get kind of gateway message");

    *(gmsg->_header.op_rc) = op_err; /* store the return code in shared memory of gateway message */

    if (op_err == DRAGON_SUCCESS) {

        err = dragon_channel_message_get_mem(msg_recv, &msg_mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to get memory from received message in transport get complete operation.");

        if (*(gmsg->_header.has_dest_mem_descr)) {

            /****************************************************************************************/
            /* Check that transport supplied memory is same as user supplied memory, when specified
               From here to the end of this comment could be factored out eventually when we have a
               working, verified transport service. */
            /****************************************************************************************/

            dragonMemoryDescr_t dest_mem;
            dragonMemorySerial_t dest_mem_ser;
            void * dest_mem_ptr;
            void * msg_mem_ptr;
            dest_mem_ser.data = gmsg->_obj_ptr + *(gmsg->_header.dest_mem_descr_ser_offset);
            dest_mem_ser.len = *(gmsg->_header.dest_mem_descr_ser_nbytes);

            err = dragon_memory_attach(&dest_mem, &dest_mem_ser);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not attach to destination memory for verification");

            err = dragon_memory_get_pointer(&dest_mem, &dest_mem_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get pointer to destination memory for verification");

            err = dragon_memory_get_pointer(&msg_mem, &msg_mem_ptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get pointer to message memory for verification");

            if (dest_mem_ptr != NULL && msg_mem_ptr != dest_mem_ptr)
                err_return(DRAGON_INVALID_ARGUMENT, "The client requested a destination for the message and the transport agent did not comply.");

            if (dest_mem_ptr == NULL) {
                /* This indicates a zero byte allocation was specified for the destination. In that
                   case the pools should be identical. */
                dragonMemoryPoolDescr_t dest_pool;
                dragonMemoryPoolDescr_t msg_pool;
                dragonULInt dest_pool_muid;
                dragonULInt msg_pool_muid;

                err = dragon_memory_get_pool(&dest_mem, &dest_pool);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not get memory pool of destination memory.");

                err = dragon_memory_get_pool(&msg_mem, &msg_pool);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not get memory pool of message memory.");

                err = dragon_memory_pool_muid(&dest_pool, &dest_pool_muid);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not get memory pool muid of destination memory.");

                err = dragon_memory_pool_muid(&msg_pool, &msg_pool_muid);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not get memory pool muid of message memory.");

                if (dest_pool_muid != msg_pool_muid)
                    err_return(DRAGON_INVALID_ARGUMENT, "The client requested a destination pool for the message and the transport agent did not comply.");
            }

            /****************************************************************************************/
            /* End of Extra Checks that could be factored out once we have a working, verified
               transport service. Clip between the lines when satisfied we do the right thing. */
            /****************************************************************************************/
        }

        /* Even if a user provided a destination for the message, the actual length will be in a clone
        of the memory descriptor, so we serialize again here regardless of whether provided to
        insure we get the right length */
        err = dragon_memory_serialize(&msg_mem_ser, &msg_mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to serialize message memory for transport get complete operation.");

        err = dragon_channel_message_getattr(msg_recv, &msg_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to retrieve attributes from message.");

        void * obj_ptr = gmsg->_obj_ptr + *(gmsg->_header.dest_mem_descr_ser_offset);

        memcpy(obj_ptr, msg_mem_ser.data, msg_mem_ser.len);
        *(gmsg->_header.dest_mem_descr_ser_nbytes) = msg_mem_ser.len;
        *(gmsg->_header.send_hints) = msg_attrs.hints;
        *(gmsg->_header.send_clientid) = msg_attrs.clientid;
    }

    /* Here we get the time for debugging purposes. We want to know the amount of time
       from when the transport calls this trigger_all to when the client actually wakes
       up and signals completion of the gateway message interaction. */
    double time_val = dragon_get_current_time_as_double();
    *((double*)gmsg->_header.transport_cmplt_timestamp) = time_val;

    err = dragon_bcast_trigger_all(&gmsg->_cmplt_bcast, NULL, NULL, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not trigger the completion bcast for the gateway message on behalf of the transport service.");

    if (op_err == DRAGON_SUCCESS) {
        err = dragon_memory_serial_free(&msg_mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "The serialized descriptor could not be freed.");
    }

    _get_deadline_for_cmplt(deadline);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check on the completion of a get operation from a transport agent.
 *
 * Once a transport agent has completed a get operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client. The given Gateway Message is not usable after a successful return from
 * this call.
 *
 * This function is not intended for most Channels users. It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for get to complete.
 *
 * @param deadine is the timeout for coordination with the client
 *
 * @return DRAGON_SUCCESS, DRAGON_EAGAIN, DRAGON_TIMEOUT, or a return code to indicate what problem occurred on the call.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_check_get_cmplt(dragonGatewayMessage_t *gmsg, timespec_t *deadline)
{
    dragonError_t err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_GET)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call transport get complete on non-get kind of gateway message");

    /* When the gmsg->_header.client_cmplt is set, the client has detached from the gateway message and we
       can safely destroy the message */
    err = _check_client_cmplt(gmsg, deadline);
    if (err == DRAGON_TIMEOUT) {
        char* bcast_state = NULL;

        if (!silence_gw_timeout_msgs)
            bcast_state = dragon_bcast_state(&gmsg->_cmplt_bcast);

        dragonULInt desired = 1UL;
        /* Try one more time, but also mark the flag so client knows no one is waiting. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired))
            err = DRAGON_SUCCESS;

        if (!silence_gw_timeout_msgs) {
            /* The completion interaction with the client timed out. We'll gather some more information. */
            /* We print to stderr so it is picked up by a monitoring thread and logged in the Dragon logs. */
            char err_str[200];
            snprintf(err_str, 199, "ERROR: GATEWAY GET MSG COMPLETION ERROR (EC=%s) Client PID=%llu and PUID(if available)=%llu\n", dragon_get_rc_string(err), *gmsg->_header.client_pid, *gmsg->_header.client_puid);
            fprintf(stderr, "%s\n", err_str);
            if (bcast_state != NULL) {
                fprintf(stderr, "%s\n", bcast_state);
                char* dbg_str = getenv("__DRAGON_BCAST_DEBUG");
                if (dbg_str != NULL) {
                    FILE *fptr;
                    // Open a file in writing mode
                    fptr = fopen("bcast_gw_timeout.txt", "w");

                    // Write some text to the file
                    fprintf(fptr, "This should never happen. It indicates that a process was waiting for the transport\n");
                    fprintf(fptr, "to complete a remote send/recv/event operation. The transport completed it,\n");
                    fprintf(fptr, "and the user process did not pick up the completion of the operation for some reason.\n");
                    fprintf(fptr, "Here is the BCAST STATE when the error occurred along with error message.\n");
                    fprintf(fptr, "%s\n", err_str);
                    fprintf(fptr, "%s\n", bcast_state);

                    // Close the file
                    fclose(fptr);
                }
                free(bcast_state);
            }
        }
    }

    no_err_return(err);
}

/**
 * @brief Complete a get operation from a transport agent.
 *
 * Once a transport agent has completed a get operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.  This function will manage cleanup of the Gateway Message.  The given
 * Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for get to complete.
 *
 * @param msg_recv is a pointer to the received Message that will be provided back to the client when the
 * op_err argument is set to DRAGON_SUCCESS. Otherwise, it should be NULL.
 *
 * @param op_err is an error code to propagate back to the client. DRAGON_SUCCESS indicates the requested
 * message is returned. Otherwise, the error code reflects the error that occurred.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred on the call.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_get_cmplt(dragonGatewayMessage_t * gmsg, dragonMessage_t * msg_recv,
                                                  const dragonError_t op_err)
{
    dragonError_t err;
    timespec_t deadline;

    err = dragon_channel_gatewaymessage_transport_start_get_cmplt(gmsg, msg_recv, op_err, &deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not start the completion of the gateway request.");

    do {
        err = dragon_channel_gatewaymessage_transport_check_get_cmplt(gmsg, &deadline);
        PAUSE();
    } while (err == DRAGON_EAGAIN);

    if (err != DRAGON_SUCCESS && err != DRAGON_TIMEOUT)
        append_err_return(err, "Problem while waiting on client during gateway completion handshake.");

    /* Since we have logged the error we return success at the end because transport processing
    is now complete. */
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Complete a get operation from a client.
 *
 * Wait for completion of the given Gateway Message get operation.  This function will manage cleanup of the Gateway
 * Message.  The given Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for get to complete.
 *
 * @param msg_recv is a pointer to a Message that will be updated with the received message when the return
 * code is DRAGON_SUCCESS. Otherwise, the message is left uninitialized.
 *
 * @param wait_mode is the type of waiting to do while the get completes: spin, idle, or adaptive.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_client_get_cmplt(dragonGatewayMessage_t * gmsg, dragonMessage_t * msg_recv, const dragonWaitMode_t wait_mode)
{
    dragonError_t err;
    dragonMemoryDescr_t msg_mem;
    dragonMemorySerial_t msg_mem_ser;
    dragonError_t get_rc;
    dragonMessageAttr_t msg_attrs;
    int called_path=0;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The gateway message cannot be NULL");

    if (msg_recv == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot specify NULL msg_recv when completing remote get operation.");

    if ((*(gmsg->_header.client_cmplt)) != 0UL)
        err_return(DRAGON_INVALID_OPERATION, "Gateway client get complete already called. Operation ignored.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_GET)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call client get complete on non-get kind of gateway message");

    err = dragon_bcast_wait(&gmsg->_cmplt_bcast, wait_mode, NULL, NULL, NULL, NULL, NULL);

    get_rc = *(gmsg->_header.op_rc);
#ifdef TEST_BAD_HEADER
    if(*gmsg->_header.client_pid != _getpid()) {
        err_noreturn("The header was likely corrupted. The _header.client_pid did not match the process pid");
    }
#endif

    if (get_rc == DRAGON_SUCCESS && err == DRAGON_SUCCESS) {

        msg_mem_ser.data = gmsg->_obj_ptr + *(gmsg->_header.dest_mem_descr_ser_offset);
        msg_mem_ser.len = *(gmsg->_header.dest_mem_descr_ser_nbytes);

        err = dragon_memory_attach(&msg_mem, &msg_mem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach serialized message memory in client get complete gateway operation.");

        err = dragon_channel_message_attr_init(&msg_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message attributes in get completion of gateway receive.");

        size_t sz;
        dragon_memory_get_size(&msg_mem, &sz);

        msg_attrs.hints = *(gmsg->_header.send_hints);
        msg_attrs.clientid = *(gmsg->_header.send_clientid);

        err = dragon_channel_message_init(msg_recv, &msg_mem, &msg_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize message in get completion of gateway receive.");
    } else {
        /* Since DRAGON_SUCCESS was not achieved, we'll init the message to empty */
        dragon_channel_message_init(msg_recv, NULL, NULL);
        /* We don't check the return code of this call to message_init because there
           will be an error returned from the given gateway message and that is more
           important to get that error code. Besides, the above call will not fail. */
        err_noreturn("There was an error returned from the remote side by the transport or the local transport timed out while waiting for gateway message completion.");
    }

    if (err == DRAGON_SUCCESS) {
        called_path=1;
        dragonULInt desired = 1UL;

#ifdef TEST_GW_TIMEOUT
        unsigned int sleep_secs = _get_test_sleep_secs();
        if (sleep_secs > 0) {
            sleep(sleep_secs);
        }
#endif

        /* Mark the transaction as complete to release the transport, but check that the transport
        has not quit waiting. If it has, then we may not be able to trust the data. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired) != 0UL) {
            err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;
        }
    } else if (err == DRAGON_OBJECT_DESTROYED) {
        called_path=2;
        err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;
    }

    if (get_rc == DRAGON_SUCCESS && err != DRAGON_SUCCESS) {
        char err_str[200];
        char* saved_err_msg = dragon_getlasterrstr();
        double end_time = dragon_get_current_time_as_double();
        double start_time = *((double*)gmsg->_header.transport_cmplt_timestamp);
        double diff = end_time-start_time;
        snprintf(err_str, 199, "The completion of the get gateway message, for process PID=%llu and PUID(if available)=%llu, timed out in the transport  on path %d with a time of %f seconds.", *gmsg->_header.client_pid, *gmsg->_header.client_puid, called_path, diff);
        dragon_channel_gatewaymessage_detach(gmsg);
        err_noreturn(saved_err_msg);
        free(saved_err_msg);
        append_err_return(err, err_str);
    }

    dragonError_t derr = dragon_channel_gatewaymessage_detach(gmsg);
    if (get_rc == DRAGON_SUCCESS && derr != DRAGON_SUCCESS)
        append_err_return(derr, "The client get completion could not detach from the gateway message for some reason.");

    no_err_return(get_rc);
}

/**
 * @brief Start the completion of an event operation from a transport agent.
 *
 * Once a transport agent has completed an event operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for event to complete.
 *
 * @param result is the result of calling the poll operation if there was a result to return. A valid result
 * is only for those poll operations that return a result when the return value is DRAGON_SUCCESS.
 *
 * @param op_err is an error code to propagate back to the client.
 *
 * @param deadline is the timeout for the coordination with the client.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_start_event_cmplt(dragonGatewayMessage_t *gmsg, const dragonULInt result,
                                                          const dragonError_t op_err, timespec_t *deadline)
{
    dragonError_t err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_EVENT)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call transport event complete on non-get kind of gateway message");

    if ((*(gmsg->_header.client_cmplt)) != 0UL)
        err_return(DRAGON_INVALID_OPERATION, "Gateway transport event complete already called. Operation ignored.");

    *(gmsg->_header.op_rc) = op_err; /* store the return code in shared memory of gateway message */
    *(gmsg->_header.event_mask) = result; /* store the result of the event in the shared header. Re-use the event_mask field. */

    /* Here we get the time for debugging purposes. We want to know the amount of time
    from when the transport calls this trigger_all to when the client actually wakes
    up and signals completion of the gateway message interaction. */
    double time_val = dragon_get_current_time_as_double();
    *((double*)gmsg->_header.transport_cmplt_timestamp) = time_val;

    err = dragon_bcast_trigger_all(&gmsg->_cmplt_bcast, NULL, NULL, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not trigger the completion bcast for the gateway message on behalf of the transport service.");

    _get_deadline_for_cmplt(deadline);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check on the completion of an event operation from a transport agent.
 *
 * Once a transport agent has completed an event operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client. The given Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users. It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for event to complete.
 *
 * @param deadline is the timeout for the coordination with the client.
 *
 * @return DRAGON_SUCCESS, DRAGON_EAGAIN, DRAGON_TIMEOUT, or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_check_event_cmplt(dragonGatewayMessage_t *gmsg, timespec_t *deadline)
{
    dragonError_t err;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "GatewayMessage cannot be NULL.");

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "deadline cannot be NULL.");

    /* When the gmsg->_header.client_cmplt is set, the client has detached from the gateway message and we
       can safely destroy the message */
    err = _check_client_cmplt(gmsg, deadline);
    if (err == DRAGON_TIMEOUT) {
        dragonULInt desired = 1UL;
        /* Try one more time, but also mark the flag so client knows no one is waiting. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired))
            err = DRAGON_SUCCESS;

        if (err == DRAGON_TIMEOUT && !silence_gw_timeout_msgs) {
            /* The completion interaction with the client timed out. We'll gather some more information. */
            /* We print to stderr so it is picked up by a monitoring thread and logged in the Dragon logs. */
            char err_str[200];
            snprintf(err_str, 199, "ERROR: GATEWAY EVENT COMPLETION ERROR (EC=%s) Client PID=%llu and PUID(if available)=%llu\n", dragon_get_rc_string(err), *gmsg->_header.client_pid, *gmsg->_header.client_puid);
            fprintf(stderr, "%s\n", err_str);
        }
    }

    no_err_return(err);
}

/**
 * @brief Complete an event operation from a transport agent.
 *
 * Once a transport agent has completed an event operation for the Gateway Message, this call is used to coordinate
 * completion with the requesting client.  This function will manage cleanup of the Gateway Message.  The given
 * Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for event to complete.
 *
 * @param result is the result of calling the poll operation if there was a result to return. A valid result
 * is only for those poll operations that return a result when the return value is DRAGON_SUCCESS.
 *
 * @param op_err is an error code to propagate back to the client.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_transport_event_cmplt(dragonGatewayMessage_t * gmsg, const dragonULInt result,
                                                    const dragonError_t op_err)
{
    dragonError_t err;
    timespec_t deadline;

    err = dragon_channel_gatewaymessage_transport_start_event_cmplt(gmsg, result, op_err, &deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not start the completion of the gateway request.");

    do {
        err = dragon_channel_gatewaymessage_transport_check_event_cmplt(gmsg, &deadline);
        PAUSE();
    } while (err == DRAGON_EAGAIN);

    if (err != DRAGON_SUCCESS && err != DRAGON_TIMEOUT)
        append_err_return(err, "Problem while waiting on client during gateway completion handshake.");

    /* Since we have logged the error we return success at the end because transport processing
    is now complete. */
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Complete an event operation from a client.
 *
 * Wait for completion of the given Gateway Message event operation.  This function will manage cleanup of the Gateway
 * Message.  The given Gateway Message is not usable after return from this call.
 *
 * This function is not intended for most Channels users.  It is used internally to support Channels operations
 * on remote nodes and other purpose-built libraries.
 *
 * @param gmsg is a pointer to the Gateway Message for send to complete.
 *
 * @param event_result is a pointer that will be updated with the triggered event.
 *
 * @param wait_mode The type of wait to use while waiting: spin, idle, or adaptive.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channel_gatewaymessage_client_event_cmplt(dragonGatewayMessage_t * gmsg, dragonULInt * event_result, const dragonWaitMode_t wait_mode)
{
    dragonError_t err = DRAGON_SUCCESS;
    dragonError_t event_rc = DRAGON_SUCCESS;

    if (gmsg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The gateway message cannot be NULL");

    if ((*(gmsg->_header.client_cmplt)) != 0UL)
        err_return(DRAGON_INVALID_OPERATION, "Gateway client event complete already called. Operation ignored.");

    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_EVENT)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempt to call client event complete on non-get kind of gateway message");

    err = dragon_bcast_wait(&gmsg->_cmplt_bcast, wait_mode, NULL, NULL, NULL, NULL, NULL);

    if (err == DRAGON_SUCCESS) {
        *event_result = *(gmsg->_header.event_mask);
        event_rc = *(gmsg->_header.op_rc);
        if (event_rc != DRAGON_SUCCESS)
            err_noreturn("There was a non-successful completion of the poll request.");

        dragonULInt desired = 1UL;

        /* Mark the transaction as complete to release the transport, but check that the transport
        has not quit waiting. If it has, then we may not be able to trust the data. */
        if (atomic_exchange(gmsg->_header.client_cmplt, desired) != 0UL)
            err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;
    } else if (err == DRAGON_OBJECT_DESTROYED)
        err = DRAGON_CHANNEL_GATEWAY_TRANSPORT_WAIT_TIMEOUT;

    if (event_rc == DRAGON_SUCCESS && err != DRAGON_SUCCESS) {
        char err_str[200];
        char* saved_err_msg = dragon_getlasterrstr();
        double end_time = dragon_get_current_time_as_double();
        double start_time = *((double*)gmsg->_header.transport_cmplt_timestamp);
        double diff = end_time-start_time;
        snprintf(err_str, 199, "The completion of the event gateway message, for process PID=%llu and PUID(if available)=%llu, timed out in the transport with a time of %f seconds.", *gmsg->_header.client_pid, *gmsg->_header.client_puid, diff);
        dragon_channel_gatewaymessage_detach(gmsg);
        err_noreturn(saved_err_msg);
        free(saved_err_msg);
        append_err_return(err, err_str);
    }

    dragonError_t derr = dragon_channel_gatewaymessage_detach(gmsg);
    if (event_rc == DRAGON_SUCCESS && err == DRAGON_SUCCESS && derr != DRAGON_SUCCESS)
        append_err_return(derr, "The client event completion could not detach from the gateway message for some reason.");

    no_err_return(event_rc);
}

/** @} */ // end of group.

/**********************************************************************/
/* Following NOT IMPLEMENTED but left here for possible future use.   */
/**********************************************************************/

/**
 * @brief Create a GatewayMessage for Infrastructure RPC Operations.
 *
 * Create a new GatewayMessage for infrastructure remote procedure calls. The
 * GatewayMessage will be allocated out of the given Managed Memory Pool.
 * After creation, the message can be serialized and sent into a Gateway
 * Channel for processing by a transport agent. The client synchronizes for
 * completion with the transport agent by calling
 * dragon_gatewaymessage_client_rpc_cmplt().
 *
 * This function is not intended for users. It is used internally to support the
 * Dragon infrastructure on remote nodes. It does not encompass user code. The
 * operation is a Dragon infrastructure operation.
 *
 * @param pool_descr is a pointer a Managed Memory Pool from which to allocate
 * the message.
 *
 * @param rpc_id is an integer identifier indicating which remote operation
 * should be performed.
 *
 * @param arg is a dragonMessage_t structure holding argument data to be sent to
 * the remote node. The contents of the message is remote operation specific
 * and is passed here as unstructured, contiguous data as defined by a
 * structure that is specific to the operation being performed. Packing of the
 * structure is assumed to be done the same on both the sending and receiving
 * side. If no argument data is needed, this argument will be NULL.
 *
 * @param target_host_id is a hostid of a remote host.
 *
 * @param deadline is a pointer to a struct indicating when the operation must
 * finish by as processed by a transport agent. A deadline in the past is
 * equivalent to returning immediately once the operation has been attempted
 * (ie try once without blocking). A NULL value indicates no deadline and
 * dragon_channel_gatewaymessage_transport_rpc_cmplt() will only return when
 * the operation completes.
 *
 * @param gmsg is a pointer to the Gateway Message to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_gatewaymessage_rpc_create(dragonMemoryPoolDescr_t * pool_descr,
                                          const uint64_t rpc_id,
                                          dragonMessage_t * arg,
                                          const dragonULInt target_host_id,
                                          const timespec_t * deadline,
                                          dragonGatewayMessage_t * gmsg)
{
    no_err_return(DRAGON_NOT_IMPLEMENTED);
}

/**
 * @brief Complete an Infrastructure RPC from a transport agent.
 *
 * Once a transport agent has completed an RPC operation for the Gateway Message,
 * this call is used to coordinate completion with the requesting client. This
 * function will manage cleanup of the Gateway Message. The given Gateway
 * Message is not usable after return from this call.
 *
 * This function is not intended for users. It is used internally to support
 * dragon Infrastructure RPC operations on remote nodes.
 *
 * @param gmsg is a pointer to the Gateway Message for get to complete.
 *
 * @param result is a dragonMessage_t pointer that is filled in with the result
 * data from making the remote procedure call. The contents of the message are
 * dependent on the remote operation that was performed. It can be NULL if
 * there is no data to return.
 *
 * @param op_err is an error code to propagate back to the client. DRAGON_SUCCESS
 * indicates the requested message is returned. Otherwise, the error code
 * reflects the error that occurred.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred on
 * the call.
 */
dragonError_t
dragon_gatewaymessage_transport_rpc_cmplt(dragonGatewayMessage_t * gmsg, dragonMessage_t * result,
                                                  dragonError_t op_err)
{
    no_err_return(DRAGON_NOT_IMPLEMENTED);
}


/**
 * @brief Complete an Infrastructure RPC from a client.
 *
 * Wait for completion of the given Gateway Message RPC operation. This function
 * will manage cleanup of the Gateway Message. The given Gateway Message is
 * not usable after return from this call.
 *
 * This function is not intended for users. It is used internally to support
 * dragon Infrastructure RPC operations on remote nodes.
 *
 * @param gmsg is a pointer to the Gateway Message for the RPC completion.
 *
 * @param result is a pointer to a dragonMessage_t that is filled in with the
 * result of making the remote RPC call. If there is no data to return it will
 * be NULL.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_gatewaymessage_client_rpc_cmplt(dragonGatewayMessage_t * gmsg, dragonMessage_t * result)
{
    no_err_return(DRAGON_NOT_IMPLEMENTED);
}

/**
 * @brief Silence Gateway Completion Timeouts
 *
 * Silence timeout messages that are printed to stderr by the transport
 * agent when gateway message completion times out. While not necessary
 * for HSTA, the TCP transport displays all these messages to the user
 * and in general they are not helpful.
 *
 */
void dragon_gatewaymessage_silence_timeouts() {
    silence_gw_timeout_msgs = true;
}

/**
 * @brief The RPC dispatch function.
 *
 * When called, the rpc_id indicates the type of function/operation to perform. The argument
 * data is interpreted dependent on the rpc_id. All RPC operations accept one argument and
 * return one argument which in both cases is a pointer to a message.
 *
 * This function is not intended for users. It is used internally to support
 * dragon Infrastructure RPC operations on remote nodes.
 *
 * @param rpc_id An integer which is an identifier selecting an RPC operation.
 *
 * @param target_host_id The host id of the node on which to run this remote
 * procedure count.
 *
 * @param arg is a pointer to Dragon Message that is passed in as an argument to
 * the RPC operation. If no argument is required, the value will be NULL.
 *
 * @param result is a pointer to Dragon Message that will be updated with the
 * result when the return code is DRAGON_SUCCESS if there is a return value to
 * return. Otherwise the result will be NULL. The caller is responsible for
 * destroying the message once it is no longer needed.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_inf_rpc(dragonULInt rpc_id, const dragonULInt target_host_id, void* arg, void** result)
{
    no_err_return(DRAGON_NOT_IMPLEMENTED);
}
