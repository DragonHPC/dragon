#include <stdlib.h>
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

static uint64_t sh_tag = 0;

uint64_t inc_sh_tag() {
    uint64_t tmp = sh_tag;
    sh_tag+=1;
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

const char* dragon_msg_tc_name(uint64_t tc)
{
    auto tc_enum = static_cast<MessageType>(tc);
    if (tcMap.count(tc_enum) == 0) {
        std::stringstream err_str;
        err_str << "Typecode " << tc << " is not a valid message type.";
        return err_str.str().c_str();
    }

    return tcMap.at(tc_enum).c_str();
}

//#include "err.h"
char * dragon_getlasterrstr();

using namespace std;

/* This is used to support talking to the local services on the same node. The following
   code provides a thread lock for multi-threaded support of communication the LS. */

static void* sh_return_lock_space = NULL;
static dragonLock_t sh_return_lock;
static bool sh_return_lock_initd = false;

dragonError_t init_sh_return_lock() {
    dragonError_t err;

    if (sh_return_lock_initd == false) {
        sh_return_lock_space = malloc(dragon_lock_size(DRAGON_LOCK_FIFO_LITE));
        if (sh_return_lock_space == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for sh_return lock.");

        err = dragon_lock_init(&sh_return_lock, sh_return_lock_space, DRAGON_LOCK_FIFO_LITE);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize the threading sh_return_lock.");
        sh_return_lock_initd = true;
    }

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t
dragon_get_shep_return_cd(char** shep_return_cd)
{
    if (shep_return_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The shep_return_cd argument cannot be NULL.");

    *shep_return_cd = getenv("DRAGON_SHEP_RET_CD");
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_shep_cd(char** shep_cd)
{
    if (shep_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The shep_cd argument cannot be NULL.");

    *shep_cd = getenv("DRAGON_LOCAL_SHEP_CD");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_shep_fli(dragonFLIDescr_t* shep_fli)
{

    dragonError_t err;
    char* shep_cd;
    dragonChannelSerial_t shep_ser;
    dragonChannelDescr_t shep_ch;

    if (shep_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The shep_fli argument cannot be NULL.");

    err = dragon_get_shep_cd(&shep_cd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not do send/receive operation since Local Services cd environment variable was not correctly set.");

    shep_ser.data = dragon_base64_decode(shep_cd, &shep_ser.len);

    err = dragon_channel_attach(&shep_ser, &shep_ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to Local Services input channel.");

    err = dragon_channel_serial_free(&shep_ser);
    if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free the serialized channel structure.");

    err = dragon_fli_create(shep_fli, &shep_ch, NULL, NULL, 0, NULL, true, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create main Local Services FLI.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _dragon_get_return_sh_fli(dragonFLIDescr_t* return_fli, dragonChannelSerial_t *shep_return_ser)
{

    dragonError_t err;
    dragonChannelDescr_t shep_return_ch;

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    if (shep_return_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_chser argument cannot be NULL.");

    err = dragon_channel_attach(shep_return_ser, &shep_return_ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to Local Services return channel.");

    err = dragon_fli_create(return_fli, &shep_return_ch, NULL, NULL, 0, NULL, true, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create return Local Services FLI.");

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t dragon_get_return_sh_fli(dragonFLIDescr_t* return_fli)
{

    dragonError_t err;
    dragonChannelSerial_t shep_return_ser;
    char* shep_ret_cd;

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    err = dragon_get_shep_return_cd(&shep_ret_cd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not do send/receive operation since Local Services return cd environment variable was not correctly set.");

    shep_return_ser.data = dragon_base64_decode(shep_ret_cd, &shep_return_ser.len);

    err = _dragon_get_return_sh_fli(return_fli, &shep_return_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to get return FLI");

    err = dragon_channel_serial_free(&shep_return_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized channel structure.");

    no_err_return(DRAGON_SUCCESS);

}


dragonError_t
dragon_sh_send_receive(DragonMsg* req_msg, DragonResponseMsg** resp_msg, MessageType expected_msg_type,
                       dragonFLIDescr_t* return_fli, const timespec_t* timeout)
{
    dragonError_t err;
    DragonMsg* msg;
    dragonFLIDescr_t shep_fli;
    dragonFLISendHandleDescr_t sendh;
    dragonFLIRecvHandleDescr_t recvh;
    /* The header is temporary while the local services still uses connection to receive bytes. */
    uint64_t header = 0xFFFFFFFFFFFFFF40;
    uint64_t req_tag = req_msg->tag();
    bool have_resp = false;

    if (req_msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The req_msg argument cannot be NULL.");

    if (resp_msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The resp_msg argument cannot be NULL.");

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    err = init_sh_return_lock();
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not intialize the sh_return thread lock.");

    err = dragon_get_shep_fli(&shep_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not create FLI descriptor from local services");
    }

    err = dragon_lock(&sh_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not lock the sh_return channel");

    err = dragon_fli_open_send_handle(&shep_fli, &sendh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&sh_return_lock);
        append_err_return(err, "Could not open send handle.");
    }

    /* The following is sent temporarily while the local services still uses the old
       connection.recv code to receive messages. Since unpickle is looking for header
       data, we send the following 8 byte header that tells unpickle to receive it
       as bytes. A couple other minor modifications in the Peer2PeerReadingChannelFile and
       in connection.py were needed as well to allow the bytes data to pass through. The
       other modifications allow a size greater than the number of bytes to be passed in
       the header since the size is not known before it is written. The other change, in
       connection.py, allows a bytes object to be returned when it can't be unpickled. */

    err = dragon_fli_send_bytes(&sendh, sizeof(header), (uint8_t*)&header, 0, true, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&sh_return_lock);
        append_err_return(err, "Could not send header.");
    }

    err = req_msg->send(&sendh, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&sh_return_lock);
        append_err_return(err, "Could not send DragonMsg.");
    }

    err = dragon_fli_close_send_handle(&sendh, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&sh_return_lock);
        append_err_return(err, "Could not close send handle.");
    }

    err = dragon_fli_open_recv_handle(return_fli, &recvh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_unlock(&sh_return_lock);
        append_err_return(err, "Could not open receive handle.");
    }

    /* This while loop is here out of an abundance of caution in case
       a previous request timed out and then later returned a response
       to the channel. If that happened, we may need to throw away some
       messages. */
    while (!have_resp) {
        err = recv_fli_msg(&recvh, &msg, timeout);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&sh_return_lock);
            append_err_return(err, "Could not open receive response message.");
        }

        *resp_msg = static_cast<DragonResponseMsg*>(msg);

        if ((*resp_msg)->ref() == req_tag)
            have_resp = true;
        else /* toss it */
            delete msg;
    }

    err = dragon_unlock(&sh_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not unlock the sh_return channel.");

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
    SHCreateProcessLocalChannelResponseMsg* resp;
    dragonChannelSerial_t ch_ser;
    dragonMemoryPoolDescr_t pool;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ch argument cannot be NULL.");

    if (block_size < DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK)
        block_size = DRAGON_CHANNEL_MINIMUM_BYTES_PER_BLOCK;

    if (capacity == 0)
        capacity = DRAGON_CHANNEL_DEFAULT_CAPACITY;

    err = dragon_get_return_sh_fli(&return_fli);
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
            append_err_return(err, "Could not attach to default pool.");

        dragon_memory_pool_muid(&pool, &muid);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pool muid.");
    }

    SHCreateProcessLocalChannelMsg msg(inc_sh_tag(), puid, muid, block_size, capacity, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHCreateProcessLocalChannelResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHCreateProcessLocalChannelResponseMsg*>(resp_msg);

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
    SHDestroyProcessLocalChannelResponseMsg* resp;
    uint64_t cuid = 0;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ch argument cannot be NULL.");

    /* This is the channel's cuid. */
    cuid = ch->_idx;

    err = dragon_channel_detach(ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from process local channel.");

    err = dragon_get_return_sh_fli(&return_fli);
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

    SHDestroyProcessLocalChannelMsg msg(inc_sh_tag(), puid, cuid, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHDestroyProcessLocalChannelResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHDestroyProcessLocalChannelResponseMsg*>(resp_msg);

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
    SHCreateProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;
    dragonMemoryPoolAttr_t pool_attrs;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    if (name == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The name argument cannot be NULL.");

    err = dragon_get_return_sh_fli(&return_fli);
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

    SHCreateProcessLocalPoolMsg msg(inc_sh_tag(), puid, bytes, attr->data_min_block_size,
       name, attr->pre_allocs, attr->npre_allocs, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHCreateProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHCreateProcessLocalPoolResponseMsg*>(resp_msg);

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
    SHRegisterProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    err = dragon_get_return_sh_fli(&return_fli);
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

    SHRegisterProcessLocalPoolMsg msg(inc_sh_tag(), puid, pool_ser_str, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHRegisterProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHRegisterProcessLocalPoolResponseMsg*>(resp_msg);

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
    SHDeregisterProcessLocalPoolResponseMsg* resp;
    dragonMemoryPoolSerial_t pool_ser;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool argument cannot be NULL.");

    err = dragon_get_return_sh_fli(&return_fli);
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

    SHDeregisterProcessLocalPoolMsg msg(inc_sh_tag(), puid, pool_ser_str, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHDeregisterProcessLocalPoolResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHDeregisterProcessLocalPoolResponseMsg*>(resp_msg);

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
    SHSetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = dragon_get_return_sh_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    SHSetKVMsg msg(inc_sh_tag(), (char*)key, (char*)value, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHSetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHSetKVResponseMsg*>(resp_msg);

    if (resp->err() != DRAGON_SUCCESS)
        err_return(resp->err(), resp->errInfo());

    delete resp;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_dragon_ls_get_kv(const unsigned char* key, char** value, const timespec_t* timeout, dragonChannelSerial_t *return_chser)
{
    dragonError_t err;
    char* ser_fli;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonResponseMsg* resp_msg;
    SHGetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = _dragon_get_return_sh_fli(&return_fli, return_chser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    SHGetKVMsg msg(inc_sh_tag(), (char*)key, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHGetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHGetKVResponseMsg*>(resp_msg);

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
    SHGetKVResponseMsg* resp;

    if (key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The key argument cannot be NULL.");

    if (value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The value argument cannot be NULL.");

    err = dragon_get_return_sh_fli(&return_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the Local Services return channel.");

    err = dragon_fli_serialize(&return_fli, &return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize the return fli");

    ser_fli = dragon_base64_encode(return_fli_ser.data, return_fli_ser.len);

    err = dragon_fli_serial_free(&return_fli_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized fli structure.");

    SHGetKVMsg msg(inc_sh_tag(), (char*)key, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, SHGetKVResponseMsg::TC, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    resp = static_cast<SHGetKVResponseMsg*>(resp_msg);

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
