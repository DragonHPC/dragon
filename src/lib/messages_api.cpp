#include <stdlib.h>
#include <string.h>
#include <unordered_map>
#include <functional>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <dragon/messages.hpp>
#include <dragon/exceptions.hpp>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <capnp/serialize.h>
#include "err.h"
#include <dragon/utils.h>
#include "_message_tcs.hpp"
#include <dragon/shared_lock.h>
#include "shared_lock.h"
#include <dragon/channels.h>

static uint64_t ls_tag = 0;

uint64_t inc_ls_tag() {
    uint64_t tmp = ls_tag;
    ls_tag+=1;
    return tmp;
}

static uint64_t logging_tag = 0;
static dragonFLIDescr_t logging_fli;
static bool logging_fli_attached = false;
static const char* DRAGON_LOGGER_SDESC = "DRAGON_LOGGER_SDESC";

uint64_t inc_logging_tag() {
    uint64_t tmp = logging_tag;
    logging_tag+=1;
    return tmp;
}

using namespace dragon;
dragonError_t
recv_fli_msg(dragonFLIRecvHandleDescr_t* recvh, DragonMsg** msg, const timespec_t* timeout)
{
    dragonMemoryDescr_t mem;
    uint64_t arg = 0;
    void* mem_ptr = NULL;
    size_t mem_size = 0;

    try {
        dragonError_t err;

        err = dragon_fli_recv_mem(recvh, &mem, &arg, timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not receive message from fli.");
        }

        err = dragon_memory_get_pointer(&mem, &mem_ptr);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not get pointer to memory returned from fli.");
        }

        err = dragon_memory_get_size(&mem, &mem_size);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not get size of memory returned from fli.");
        }


        kj::ArrayPtr<const capnp::word> words(reinterpret_cast<const capnp::word*>(mem_ptr), mem_size / sizeof(capnp::word));
        capnp::FlatArrayMessageReader message(words);
        MessageDef::Reader reader = message.getRoot<MessageDef>();
        MessageType tc = (MessageType)reader.getTc();

        if (deserializeFunctions.count(tc) == 0) {
            append_err_return(DRAGON_INVALID_MESSAGE, dragon_msg_tc_name(tc));
        }

        err = (deserializeFunctions.at(tc))(reader, msg);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not deserialize message.");
        }

        err = dragon_memory_free(&mem);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not free memory allocation for the message.");
        }


    } catch (...) {
        err_return(DRAGON_INVALID_OPERATION, "There was an error while receiving the message from the fli.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_deserialize_msg(const void* buf, size_t len, DragonMsg** msg)
{
    if (buf == nullptr || msg == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The buf and msg arguments cannot be NULL.");

    if (len == 0 || len % sizeof(capnp::word) != 0)
        err_return(DRAGON_INVALID_MESSAGE, "The buffer length is not a whole number of capnp words.");

    try {
        // Copy so the reader gets word-aligned memory regardless of where buf came from.
        kj::Array<capnp::word> words = kj::heapArray<capnp::word>(len / sizeof(capnp::word));
        memcpy(words.begin(), buf, len);

        capnp::FlatArrayMessageReader message(words);
        MessageDef::Reader reader = message.getRoot<MessageDef>();
        MessageType tc = (MessageType)reader.getTc();

        if (deserializeFunctions.count(tc) == 0)
            append_err_return(DRAGON_INVALID_MESSAGE, dragon_msg_tc_name(tc));

        dragonError_t err = (deserializeFunctions.at(tc))(reader, msg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not deserialize message.");
    } catch (...) {
        err_return(DRAGON_INVALID_MESSAGE, "The buffer does not contain a valid capnp message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* dragon_msg_tc_name(uint64_t tc)
{
    auto tc_enum = static_cast<MessageType>(tc);
    if (tcMap.count(tc_enum) == 0) {
        std::stringstream err_str;
        err_str << "Typecode " << tc << " is not a valid message type.";
        char* err_msg = (char*)malloc(err_str.str().size()+1);
        strcpy(err_msg, err_str.str().c_str());
        return err_msg;
    }

    return tcMap.at(tc_enum).c_str();
}

//#include "err.h"
char * dragon_getlasterrstr();

using namespace std;

/* This is used to support talking to the local services on the same node. The following
   code provides a thread lock for multi-threaded support of communication the LS. */

static void* ls_return_lock_space = NULL;
static dragonLock_t ls_return_lock;
static bool ls_return_lock_initd = false;

dragonError_t init_ls_return_lock() {
    dragonError_t err;

    if (ls_return_lock_initd == false) {
        ls_return_lock_space = malloc(dragon_lock_size(DRAGON_LOCK_FIFO_LITE));
        if (ls_return_lock_space == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for ls_return lock.");

        err = dragon_lock_init(&ls_return_lock, ls_return_lock_space, DRAGON_LOCK_FIFO_LITE);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize the threading ls_return_lock.");
        ls_return_lock_initd = true;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_ls_return_cd(char** ls_return_cd)
{
    if (ls_return_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ls_return_cd argument cannot be NULL.");

    *ls_return_cd = getenv("DRAGON_LS_RET_QD");
    if (*ls_return_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The DRAGON_LS_RET_QD environment variable was not set.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_ls_cd(char** ls_cd)
{
    if (ls_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ls_cd argument cannot be NULL.");

    *ls_cd = getenv("DRAGON_LOCAL_LS_QD");
    if (*ls_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The DRAGON_LOCAL_LS_QD environment variable was not set.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_ls_fli(dragonFLIDescr_t* ls_fli)
{

    dragonError_t err;
    char* ls_cd;
    dragonFLISerial_t ls_ser;

    if (ls_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ls_fli argument cannot be NULL.");

    err = dragon_get_ls_cd(&ls_cd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not do send/receive operation since Local Services cd environment variable was not correctly set.");

    ls_ser.data = dragon_base64_decode(ls_cd, &ls_ser.len);

    err = dragon_fli_attach(&ls_ser, NULL, ls_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to Local Services input channel.");

    err = dragon_fli_serial_free(&ls_ser);
    if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free the serialized channel structure.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _dragon_get_return_ls_fli(dragonFLIDescr_t* return_fli, dragonFLISerial_t *ls_return_ser)
{

    dragonError_t err;

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    if (ls_return_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ls_return_ser argument cannot be NULL.");

    err = dragon_fli_attach(ls_return_ser, NULL, return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to Local Services return FLI.");

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t dragon_get_return_ls_fli(dragonFLIDescr_t* return_fli)
{

    dragonError_t err;
    dragonFLISerial_t ls_return_ser;
    char* ls_ret_qd;

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    err = dragon_get_ls_return_cd(&ls_ret_qd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not do send/receive operation since Local Services return cd environment variable was not correctly set.");

    ls_return_ser.data = dragon_base64_decode(ls_ret_qd, &ls_return_ser.len);

    err = _dragon_get_return_ls_fli(return_fli, &ls_return_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to get return FLI");

    err = dragon_fli_serial_free(&ls_return_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized FLI structure.");

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t
dragon_logging_attach()
{
    if (!logging_fli_attached) {
        dragonError_t err;
        char* logger_sdesc = getenv(DRAGON_LOGGER_SDESC);
        dragonFLISerial_t ser_fli;

        if (logger_sdesc == NULL)
            err_return(DRAGON_INVALID_ARGUMENT, "The DRAGON_LOGGER_SDESC environment variable must be set to log messages.");

        // Decode Serialized Descriptor and Attach to FLI

        ser_fli.data = dragon_base64_decode(logger_sdesc, &ser_fli.len);

        err = dragon_fli_attach(&ser_fli, NULL, &logging_fli);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to serialized FLI.");

        err = dragon_fli_serial_free(&ser_fli);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free the serialized FLI structure.");

        logging_fli_attached = true;
    }
    return DRAGON_SUCCESS;
}

dragonError_t
dragon_log_message(
    const char* name,
    const char* msg,
    const char* time,
    const char* func,
    const char* hostname,
    const char* ipAddress,  //
    uint16_t port,
    const char* service,
    uint8_t level,
    const timespec_t* timeout
)
{
    dragonError_t err;

    if (!logging_fli_attached)
        append_err_return(DRAGON_INVALID_OPERATION, "The dragon logging FLI is not attached.");

    // Check inputs
    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The message argument cannot be NULL.");

    // Open FLI SendHandle
    dragonFLISendHandleDescr_t sendh;

    err = dragon_fli_open_send_handle(&logging_fli, &sendh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS) {
        std::cout << "error opening send handle" << err << std::endl;
        append_err_return(err, "Could not open send handle.");
    }

    // Build and Send Message
    CPLoggingMessage cp_logging_msg(
        inc_logging_tag(),
        name == NULL ? "" : name,
        msg,
        time == NULL ? "" : time,
        func == NULL ? "" : func,
        hostname == NULL ? "" : hostname,
        ipAddress == NULL ? "" : ipAddress,
        port,
        service == NULL ? "" : service,
        level
    );

    err = cp_logging_msg.send(&sendh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send DragonMsg.");

    err = dragon_fli_close_send_handle(&sendh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_logging_detach()
{
    if (logging_fli_attached) {
        dragonError_t err;

        err = dragon_fli_detach(&logging_fli);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from logging FLI.");

        logging_fli_attached = false;
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ls_send_receive(DragonMsg* req_msg, DragonResponseMsg** resp_msg, MessageType expected_msg_type,
                       dragonFLIDescr_t* return_fli, const timespec_t* timeout)
{
    dragonError_t err;
    DragonMsg* msg;
    dragonFLIDescr_t ls_fli;
    dragonFLISendHandleDescr_t sendh;
    dragonFLIRecvHandleDescr_t recvh;
    /* The header is temporary while the local services still uses connection to receive bytes. */
    uint64_t req_tag = req_msg->tag();
    bool have_resp = false;

    if (req_msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The req_msg argument cannot be NULL.");

    if (resp_msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The resp_msg argument cannot be NULL.");

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    err = init_ls_return_lock();
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not intialize the ls_return thread lock.");

    err = dragon_get_ls_fli(&ls_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not create FLI descriptor from local services");
    }

    err = dragon_lock(&ls_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not lock the ls_return channel");

    err = dragon_fli_open_send_handle(&ls_fli, &sendh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&ls_return_lock);
        append_err_return(err, "Could not open send handle.");
    }

    err = req_msg->send(&sendh, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&ls_return_lock);
        append_err_return(err, "Could not send DragonMsg.");
    }

    err = dragon_fli_close_send_handle(&sendh, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&ls_return_lock);
        append_err_return(err, "Could not close send handle.");
    }

    err = dragon_fli_open_recv_handle(return_fli, &recvh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&ls_return_lock);
        append_err_return(err, "Could not open receive handle.");
    }

    /* This while loop is here out of an abundance of caution in case
       a previous request timed out and then later returned a response
       to the channel. If that happened, we may need to throw away some
       messages. */
    while (!have_resp) {
        err = recv_fli_msg(&recvh, &msg, timeout);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&ls_return_lock);
            append_err_return(err, "Could not open receive response message.");
        }

        *resp_msg = static_cast<DragonResponseMsg*>(msg);

        if ((*resp_msg)->ref() == req_tag)
            have_resp = true;
        else /* toss it */
            delete msg;
    }

    err = dragon_unlock(&ls_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not unlock the ls_return channel.");

    err = dragon_fli_close_recv_handle(&recvh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle.");

    if ((*resp_msg)->tc() != expected_msg_type) {
        char err_msg[200];
        snprintf(err_msg, 199, "Expected a response message type of %d and got %d instead.", expected_msg_type, (*resp_msg)->tc());
        err_return(err, err_msg);
    }

    no_err_return(DRAGON_SUCCESS);
}
/**
 * @brief Create a Channel whose lifetime is the lifetime of a Process and is
 * co-located with it.
 *
 * Calling this will communicate with Local Services to create a unique channel
 * that can be used by the current process. When the process exits, Local
 * Services will clean up the channel automatically, though it can be
 * destroyed earlier by calling the related destroy process local channel.
 *
 * @param ch A pointer to a channel descriptor object/structure. This will
 * be initialized after successful completion of this call.
 *
 * @param muid The muid of the pool in which to allocate the channel. A value
 * of 0 will result in using the default pool on the node.
 *
 * @param block_size The desired block size for messages in the channel. Passing
 * in anything less than the default block size will result in using the
 * minimum block size.
 *
 * @param capacity The desired capacity of the channel. Passing in zero will
 * result in using the default capacity.
 *
 * @param timeout A pointer to a timespec_t structure that holds the desired
 * timeout. If NULL is passed, the function call with not timeout.
 *
 * @return DRAGON_SUCCESS or an error code indicating the problem.
 **/

dragonError_t
dragon_create_process_local_channel(dragonChannelDescr_t* ch, uint64_t muid, uint64_t block_size, uint64_t capacity, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSCreateProcessLocalChannelResponseMsg* resp;
    dragonChannelSerial_t ch_ser;
    dragonMemoryPoolDescr_t pool;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ch argument cannot be NULL.");

    if (block_size < DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK)
        block_size = DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK;

    if (capacity == 0)
        capacity = DRAGON_CHANNEL_DEFAULT_CAPACITY;

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    puid_str = getenv("DRAGON_MY_PUID");
    if (puid_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The DRAGON_MY_PUID environment variable was not set.");

    const long puid = strtol(puid_str, &end, 10);

    if (muid == 0) {
        // attach to default
        err = dragon_memory_pool_attach_default(&pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to the default pool.");

        dragon_memory_pool_muid(&pool, &muid);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pool muid.");
    }

    LSCreateProcessLocalChannelMsg msg(inc_ls_tag(), puid, muid, block_size, capacity, ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSCreateProcessLocalChannelResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSCreateProcessLocalChannelResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    const char* ser_chan = resp->serChannel();

    ch_ser.data = dragon_base64_decode(ser_chan, &ch_ser.len);

    err = dragon_channel_attach(&ch_ser, ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to process local channel.");

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy a channel created as a process local channel.
 *
 * This deregisters the channel from Local Services and destroys it. The
 * channel must have been created using dragon_create_process_local_channel.
 *
 * @param ch A pointer to a valid, initialized channel descriptor object.
 *
 * @param timeout A pointer to a timespec_t structure that holds the desired
 * timeout. If NULL is passed, the function call with not timeout.
 *
 * @return DRAGON_SUCCESS or an error code indicating the problem.
 **/

dragonError_t
dragon_destroy_process_local_channel(dragonChannelDescr_t* ch, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSDestroyProcessLocalChannelResponseMsg* resp;
    uint64_t cuid = 0;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ch argument cannot be NULL.");

    /* This is the channel's cuid. */
    cuid = ch->_idx;

    err = dragon_channel_detach(ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from process local channel.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    puid_str = getenv("DRAGON_MY_PUID");
    if (puid_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The DRAGON_MY_PUID environment variable was not set.");

    const long puid = strtol(puid_str, &end, 10);

    LSDestroyProcessLocalChannelMsg msg(inc_ls_tag(), puid, cuid, ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSDestroyProcessLocalChannelResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSDestroyProcessLocalChannelResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_create_process_local_pool(dragonMemoryPoolDescr_t* pool, size_t bytes, const char* name, dragonMemoryPoolAttr_t* attr, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSCreateProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;
    dragonMemoryPoolAttr_t pool_attrs;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    if (name == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The name argument cannot be NULL.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    puid_str = getenv("DRAGON_MY_PUID");
    if (puid_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The DRAGON_MY_PUID environment variable was not set.");

    const long puid = strtol(puid_str, &end, 10);

    if (attr == NULL) {
        attr = &pool_attrs;
        dragon_memory_attr_init(attr);
    }

    LSCreateProcessLocalPoolMsg msg(inc_ls_tag(), puid, bytes, attr->data_min_block_size,
       name, attr->pre_allocs, attr->npre_allocs, ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSCreateProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSCreateProcessLocalPoolResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    const char* ser_pool = resp->serPool();

    pool_ser.data = dragon_base64_decode(ser_pool, &pool_ser.len);

    err = dragon_memory_pool_attach(pool, &pool_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to process local pool.");

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_register_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char* pool_ser_str;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSRegisterProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    err = dragon_memory_pool_serialize(&pool_ser, pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the pool");

    pool_ser_str = dragon_base64_encode(pool_ser.data, pool_ser.len);

    err = dragon_memory_pool_serial_free(&pool_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized pool structure.");

    puid_str = getenv("DRAGON_MY_PUID");
    if (puid_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The DRAGON_MY_PUID environment variable was not set.");

    const long puid = strtol(puid_str, &end, 10);

    LSRegisterProcessLocalPoolMsg msg(inc_ls_tag(), puid, pool_ser_str, ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSRegisterProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSRegisterProcessLocalPoolResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_deregister_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char* pool_ser_str;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSDeregisterProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    err = dragon_memory_pool_serialize(&pool_ser, pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the pool");

    pool_ser_str = dragon_base64_encode(pool_ser.data, pool_ser.len);

    err = dragon_memory_pool_serial_free(&pool_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized pool structure.");

    puid_str = getenv("DRAGON_MY_PUID");
    if (puid_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The DRAGON_MY_PUID environment variable was not set.");

    const long puid = strtol(puid_str, &end, 10);

    LSDeregisterProcessLocalPoolMsg msg(inc_ls_tag(), puid, pool_ser_str, ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSDeregisterProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSDeregisterProcessLocalPoolResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ls_set_kv(const unsigned char* key, const unsigned char* value, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSSetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    LSSetKVMsg msg(inc_ls_tag(), reinterpret_cast<const char*>(key), reinterpret_cast<const char*>(value), ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSSetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSSetKVResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_dragon_ls_get_kv(const unsigned char* key, char** value, const timespec_t* timeout, dragonFLISerial_t *return_chser)
{
    dragonError_t err;
    char* ser_fli;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSGetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = _dragon_get_return_ls_fli(&return_fli, return_chser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    LSGetKVMsg msg(inc_ls_tag(), reinterpret_cast<const char*>(key), ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSGetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSGetKVResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    const char* source = resp->value();

    *value = (char*)malloc(strlen(source)+1);

    if (*value == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for value.");

    strcpy(*value, source);

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_ls_get_kv(const unsigned char* key, char** value, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    LSGetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = dragon_get_return_ls_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    LSGetKVMsg msg(inc_ls_tag(), reinterpret_cast<const char*>(key), ser_fli);

    err = dragon_ls_send_receive(&msg, &resp_msg, LSGetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<LSGetKVResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    const char* source = resp->value();

    *value = (char*)malloc(strlen(source)+1);

    if (*value == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for value.");

    strcpy(*value, source);

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}
