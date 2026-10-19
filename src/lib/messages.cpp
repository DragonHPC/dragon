#include <stdlib.h>
#include <functional>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <dragon/messages.hpp>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <capnp/serialize.h>
#include "err.h"
#include <dragon/utils.h>
#include "_message_tcs.hpp"
#include <dragon/shared_lock.h>
#include "shared_lock.h"
#include <dragon/channels.h>
#include <dragon/messages_api.h>

namespace dragon {

/********************************************************************************************************/
/* base message */

DragonMsg::DragonMsg(MessageType tc, uint64_t tag)
{
    this->mTC = tc;
    this->mTag = tag;
}

DragonMsg::~DragonMsg() {}

void
DragonMsg::builder(MessageDef::Builder& msg)
{
    msg.setTc(mTC);
    msg.setTag(mTag);
}

dragonError_t
DragonMsg::send(dragonFLISendHandleDescr_t* sendh, const timespec_t* timeout)
{
    dragonError_t err;

    try {
        capnp::MallocMessageBuilder message;
        MessageDef::Builder msg = message.initRoot<MessageDef>();
        this->builder(msg);

        kj::Array<capnp::word> words = capnp::messageToFlatArray(message);
        kj::ArrayPtr<kj::byte> bytes = words.asBytes();

        err = dragon_fli_send_bytes(sendh, bytes.size(), bytes.begin(), 0, false, timeout);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not send bytes for capnp message.");

    } catch (...) {
        err_return(DRAGON_INVALID_OPERATION, "There was an error while attempting to send the message over the fli.");
    }

    no_err_return(DRAGON_SUCCESS);
}


MessageType
DragonMsg::tc()
{
    return mTC;
}

uint64_t
DragonMsg::tag()
{
    return mTag;
}

/********************************************************************************************************/
/* base response message */

DragonResponseMsg::DragonResponseMsg(MessageType tc, uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo) :
    DragonMsg(tc, tag), mRef(ref), mErr(err), mErrInfo(errInfo) {}

DragonResponseMsg::~DragonResponseMsg() {}

uint64_t DragonResponseMsg::ref()
{
    return mRef;
}

dragonError_t DragonResponseMsg::err()
{
    return mErr;
}

const char* DragonResponseMsg::errInfo()
{
    return mErrInfo.c_str();
}

void
DragonResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    ResponseDef::Builder builder = msg.getResponseOption().initValue();
    builder.setRef(mRef);
    builder.setErr(mErr);
    builder.setErrInfo(mErrInfo);
}

/********************************************************************************************************/
/* local services create process local channel */

LSCreateProcessLocalChannelMsg::LSCreateProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t muid, uint64_t blockSize, uint64_t capacity, const char* respFLI):
    DragonMsg(LSCreateProcessLocalChannelMsg::TC, tag), mPUID(puid), mMUID(muid), mBlockSize(blockSize), mCapacity(capacity), mFLI(respFLI) {}

void
LSCreateProcessLocalChannelMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSCreateProcessLocalChannelDef::Builder builder = msg.initLsCreateProcessLocalChannel();
    builder.setPuid(mPUID);
    builder.setMuid(mMUID);
    builder.setBlockSize(mBlockSize);
    builder.setCapacity(mCapacity);
    builder.setRespFLI(mFLI);
}

dragonError_t
LSCreateProcessLocalChannelMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        LSCreateProcessLocalChannelDef::Reader lsCPLCReader = reader.getLsCreateProcessLocalChannel();

        (*msg) = new LSCreateProcessLocalChannelMsg(
            reader.getTag(),
            lsCPLCReader.getPuid(),
            lsCPLCReader.getMuid(),
            lsCPLCReader.getBlockSize(),
            lsCPLCReader.getCapacity(),
            lsCPLCReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSCreateProcessLocalChannel message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
LSCreateProcessLocalChannelMsg::respFLI()
{
    return mFLI.c_str();
}

const uint64_t
LSCreateProcessLocalChannelMsg::puid()
{
    return mPUID;
}

const uint64_t
LSCreateProcessLocalChannelMsg::muid()
{
    return mMUID;
}

const uint64_t
LSCreateProcessLocalChannelMsg::blockSize()
{
    return mBlockSize;
}

const uint64_t
LSCreateProcessLocalChannelMsg::capacity()
{
    return mCapacity;
}

/********************************************************************************************************/
/* local services create process local channel response */

LSCreateProcessLocalChannelResponseMsg::LSCreateProcessLocalChannelResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serChannel):
    DragonResponseMsg(LSCreateProcessLocalChannelResponseMsg::TC, tag, ref, err, errInfo), mSerChannel(serChannel) {}

void
LSCreateProcessLocalChannelResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    LSCreateProcessLocalChannelResponseDef::Builder builder = msg.initLsCreateProcessLocalChannelResponse();
    builder.setSerChannel(mSerChannel);
}

dragonError_t
LSCreateProcessLocalChannelResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        LSCreateProcessLocalChannelResponseDef::Reader lsCPLCResponseReader = reader.getLsCreateProcessLocalChannelResponse();

        (*msg) = new LSCreateProcessLocalChannelResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            lsCPLCResponseReader.getSerChannel().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSCreateProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
LSCreateProcessLocalChannelResponseMsg::serChannel()
{
    return this->mSerChannel.c_str();
}

/********************************************************************************************************/
/* local services destroy process local channel */

LSDestroyProcessLocalChannelMsg::LSDestroyProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t cuid, const char* respFLI):
    DragonMsg(LSDestroyProcessLocalChannelMsg::TC, tag), mPUID(puid), mCUID(cuid), mFLI(respFLI) {}

void
LSDestroyProcessLocalChannelMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSDestroyProcessLocalChannelDef::Builder builder = msg.initLsDestroyProcessLocalChannel();
    builder.setPuid(this->mPUID);
    builder.setCuid(this->mCUID);
    builder.setRespFLI(this->mFLI);
}

dragonError_t
LSDestroyProcessLocalChannelMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        LSDestroyProcessLocalChannelDef::Reader lsDPLCReader = reader.getLsDestroyProcessLocalChannel();

        (*msg) = new LSDestroyProcessLocalChannelMsg(
            reader.getTag(),
            lsDPLCReader.getPuid(),
            lsDPLCReader.getCuid(),
            lsDPLCReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSDestroyProcessLocalChannel message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
LSDestroyProcessLocalChannelMsg::respFLI()
{
    return mFLI.c_str();
}

const uint64_t
LSDestroyProcessLocalChannelMsg::puid()
{
    return mPUID;
}

const uint64_t
LSDestroyProcessLocalChannelMsg::cuid()
{
    return mCUID;
}

/********************************************************************************************************/
/* local services Destroy Process Local Channel Response */

LSDestroyProcessLocalChannelResponseMsg::LSDestroyProcessLocalChannelResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(LSDestroyProcessLocalChannelResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
LSDestroyProcessLocalChannelResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new LSDestroyProcessLocalChannelResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSDestroyProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}


/********************************************************************************************************/
/* local services Create Process Local Pool */

LSCreateProcessLocalPoolMsg::LSCreateProcessLocalPoolMsg(uint64_t tag, uint64_t puid, uint64_t size, uint64_t minBlockSize,
                                                      const char* name, const size_t* preAllocs, const size_t numPreAllocs,
                                                      const char* respFLI):
    DragonMsg(LSCreateProcessLocalPoolMsg::TC, tag), mPUID(puid), mRespFLI(respFLI), mSize(size), mMinBlockSize(minBlockSize),
    mName(name) {

    for (size_t k=0;k<numPreAllocs;k++)
        mPreAllocs.push_back(preAllocs[k]);
}

void
LSCreateProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSCreateProcessLocalPoolDef::Builder builder = msg.initLsCreateProcessLocalPool();
    builder.setPuid(mPUID);
    builder.setSize(mSize);
    builder.setMinBlockSize(mMinBlockSize);
    builder.setName(mName);
    builder.setRespFLI(mRespFLI);
    ::capnp::List<uint64_t>::Builder allocs = builder.initPreAllocs(mPreAllocs.size());

    for (size_t k=0;k<mPreAllocs.size();k++)
        allocs.set(k, mPreAllocs[k]);
}

dragonError_t
LSCreateProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        LSCreateProcessLocalPoolDef::Reader mReader = reader.getLsCreateProcessLocalPool();

        size_t* alloc_arr = NULL;
        size_t numPreAllocs = 0;
        ::capnp::List<uint64_t>::Reader allocs = mReader.getPreAllocs();

        if (allocs.size() > 0) {
            numPreAllocs = allocs.size();
            alloc_arr = (size_t*)malloc(sizeof(size_t)*numPreAllocs);
            size_t idx = 0;

            for (size_t alloc : allocs) {
                alloc_arr[idx] = alloc;
                idx += 1;
            }
        }

        (*msg) = new LSCreateProcessLocalPoolMsg(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getSize(),
            mReader.getMinBlockSize(),
            mReader.getName().cStr(),
            alloc_arr,
            numPreAllocs,
            mReader.getRespFLI().cStr());

        if (alloc_arr != NULL)
            free(alloc_arr);

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSCreateProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* LSCreateProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

const uint64_t LSCreateProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char* LSCreateProcessLocalPoolMsg::name() {
    return mName.c_str();
}

const uint64_t LSCreateProcessLocalPoolMsg::minBlockSize() {
    return mMinBlockSize;
}

const uint64_t LSCreateProcessLocalPoolMsg::size() {
    return mSize;
}

const size_t LSCreateProcessLocalPoolMsg::preAlloc(int idx) {
    return mPreAllocs[idx];
}

const size_t LSCreateProcessLocalPoolMsg::numPreAllocs() {
    return mPreAllocs.size();
}

/********************************************************************************************************/
/* local services Create Process Local Pool Response */

LSCreateProcessLocalPoolResponseMsg::LSCreateProcessLocalPoolResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err,
                                                                   const char* errInfo, const char* serPool):
    DragonResponseMsg(LSCreateProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo), mSerPool(serPool) {}

void
LSCreateProcessLocalPoolResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    LSCreateProcessLocalPoolResponseDef::Builder builder = msg.initLsCreateProcessLocalPoolResponse();
    builder.setSerPool(mSerPool);
}

dragonError_t
LSCreateProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        LSCreateProcessLocalPoolResponseDef::Reader mReader = reader.getLsCreateProcessLocalPoolResponse();

        (*msg) = new LSCreateProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getSerPool().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSCreateProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* LSCreateProcessLocalPoolResponseMsg::serPool() {
    return mSerPool.c_str();

}

/********************************************************************************************************/
/* local services Register Process Local Pool */

LSRegisterProcessLocalPoolMsg::LSRegisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI):
    DragonMsg(LSRegisterProcessLocalPoolMsg::TC, tag), mPUID(puid), mSerPool(serPool), mRespFLI(respFLI) {}

void
LSRegisterProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSRegisterProcessLocalPoolDef::Builder builder = msg.initLsRegisterProcessLocalPool();
    builder.setPuid(mPUID);
    builder.setSerPool(mSerPool.c_str());
    builder.setRespFLI(mRespFLI.c_str());
}

dragonError_t
LSRegisterProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        LSRegisterProcessLocalPoolDef::Reader mReader = reader.getLsRegisterProcessLocalPool();

        (*msg) = new LSRegisterProcessLocalPoolMsg(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getSerPool().cStr(),
            mReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSRegisterProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
LSRegisterProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char*
LSRegisterProcessLocalPoolMsg::serPool() {
    return mSerPool.c_str();
}

const char*
LSRegisterProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

/********************************************************************************************************/
/* local services Register Process Local Pool Response */

LSRegisterProcessLocalPoolResponseMsg::LSRegisterProcessLocalPoolResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(LSRegisterProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
LSRegisterProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new LSRegisterProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSRegisterProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Deregister Process Local Pool */

LSDeregisterProcessLocalPoolMsg::LSDeregisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI):
    DragonMsg(LSDeregisterProcessLocalPoolMsg::TC, tag), mPUID(puid), mSerPool(serPool), mRespFLI(respFLI) {}

void
LSDeregisterProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSDeregisterProcessLocalPoolDef::Builder builder = msg.initLsDeregisterProcessLocalPool();
    builder.setPuid(mPUID);
    builder.setSerPool(mSerPool.c_str());
    builder.setRespFLI(mRespFLI.c_str());
}

dragonError_t
LSDeregisterProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        LSDeregisterProcessLocalPoolDef::Reader mReader = reader.getLsDeregisterProcessLocalPool();

        (*msg) = new LSDeregisterProcessLocalPoolMsg(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getSerPool().cStr(),
            mReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSDeregisterProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
LSDeregisterProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char*
LSDeregisterProcessLocalPoolMsg::serPool() {
    return mSerPool.c_str();
}

const char*
LSDeregisterProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

/********************************************************************************************************/
/* local services Deregister Process Local Pool Response */

LSDeregisterProcessLocalPoolResponseMsg::LSDeregisterProcessLocalPoolResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(LSDeregisterProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
LSDeregisterProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new LSDeregisterProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSDeregisterProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Set Key/Value Pair */

LSSetKVMsg::LSSetKVMsg(uint64_t tag, const char* key, const char* value, const char* respFLI):
    DragonMsg(LSSetKVMsg::TC, tag), mKey(key), mValue(value), mFLI(respFLI) {}

void LSSetKVMsg::builder(MessageDef::Builder& msg) {
    DragonMsg::builder(msg);
    LSSetKVDef::Builder builder = msg.initLsSetKV();
    builder.setKey(this->mKey);
    builder.setValue(this->mValue);
    builder.setRespFLI(this->mFLI);
}

dragonError_t LSSetKVMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        LSSetKVDef::Reader lsSetKVReader = reader.getLsSetKV();

        (*msg) = new LSSetKVMsg(
            reader.getTag(),
            lsSetKVReader.getKey().cStr(),
            lsSetKVReader.getValue().cStr(),
            lsSetKVReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSSetKV message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* LSSetKVMsg::key() {
    return mKey.c_str();
}

const char* LSSetKVMsg::value() {
    return mValue.c_str();
}

const char* LSSetKVMsg::respFLI() {
    return mFLI.c_str();
}

/********************************************************************************************************/
/* local services Set Key/Value Pair Response */

LSSetKVResponseMsg::LSSetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(LSSetKVResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
LSSetKVResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new LSSetKVResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSSetKVResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Get Key/Value Pair */

LSGetKVMsg::LSGetKVMsg(uint64_t tag, const char* key, const char* respFLI):
    DragonMsg(LSGetKVMsg::TC, tag), mKey(key), mFLI(respFLI) {}

void LSGetKVMsg::builder(MessageDef::Builder& msg) {
    DragonMsg::builder(msg);
    LSGetKVDef::Builder builder = msg.initLsGetKV();
    builder.setKey(this->mKey);
    builder.setRespFLI(this->mFLI);
}

dragonError_t LSGetKVMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        LSGetKVDef::Reader lsGetKVReader = reader.getLsGetKV();

        (*msg) = new LSGetKVMsg(
            reader.getTag(),
            lsGetKVReader.getKey().cStr(),
            lsGetKVReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSGetKV message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* LSGetKVMsg::key() {
    return mKey.c_str();
}

const char* LSGetKVMsg::respFLI() {
    return mFLI.c_str();
}

/********************************************************************************************************/
/* local services Get Key/Value Pair Response */

LSGetKVResponseMsg::LSGetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* value):
    DragonResponseMsg(LSGetKVResponseMsg::TC, tag, ref, err, errInfo), mValue(value) {}

void
LSGetKVResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    LSGetKVResponseDef::Builder builder = msg.initLsGetKVResponse();
    builder.setValue(mValue);
}

dragonError_t
LSGetKVResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        LSGetKVResponseDef::Reader lsGetKVReponseReader = reader.getLsGetKVResponse();
        (*msg) = new LSGetKVResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            lsGetKVReponseReader.getValue().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSGetKVResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* LSGetKVResponseMsg::value() {
    return mValue.c_str();
}


/********************************************************************************************************/
/* ddict register client */
DDRegisterClientMsg::DDRegisterClientMsg(uint64_t tag, const char* respFLI, const char* bufferedRespFLI) :
    DragonMsg(DDRegisterClientMsg::TC, tag), mFLI(respFLI), mBufferedFLI(bufferedRespFLI) {}

void
DDRegisterClientMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRegisterClientDef::Builder builder = msg.initDdRegisterClient();
    builder.setRespFLI(this->mFLI);
    builder.setBufferedRespFLI(this->mBufferedFLI);
}

dragonError_t
DDRegisterClientMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDRegisterClientDef::Reader registerClientReader = reader.getDdRegisterClient();

        (*msg) = new DDRegisterClientMsg(
            reader.getTag(),
            registerClientReader.getRespFLI().cStr(),
            registerClientReader.getBufferedRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the RegisterClient message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDRegisterClientMsg::respFLI()
{
    return mFLI.c_str();
}

const char*
DDRegisterClientMsg::bufferedRespFLI()
{
    return mBufferedFLI.c_str();
}

/********************************************************************************************************/
/* ddict register client response */

DDRegisterClientResponseMsg::DDRegisterClientResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t clientID, uint64_t numManagers, uint64_t managerID, uint64_t timeout, bool waitForKeys) :
    DragonResponseMsg(DDRegisterClientResponseMsg::TC, tag, ref, err, errInfo),
    mClientID(clientID),
    mNumManagers(numManagers),
    mManagerID(managerID),
    mTimeout(timeout),
    mWaitForKeys(waitForKeys) {}

dragonError_t
DDRegisterClientResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDRegisterClientResponseDef::Reader registerClientResponseReader = reader.getDdRegisterClientResponse();

        DDRegisterClientResponseMsg* resp_msg;
        resp_msg = new DDRegisterClientResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            registerClientResponseReader.getClientID(),
            registerClientResponseReader.getNumManagers(),
            registerClientResponseReader.getManagerID(),
            registerClientResponseReader.getTimeout(),
            registerClientResponseReader.getWaitForKeys());

        capnp::List<capnp::Text>::Reader manager_nodes_reader = registerClientResponseReader.getManagerNodes();

        for (auto it=manager_nodes_reader.begin() ; it!=manager_nodes_reader.end() ; it++)
            resp_msg->mManagerNodes.push_back(it->cStr());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSCreateProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDRegisterClientResponseMsg::clientID()
{
    return mClientID;
}

uint64_t
DDRegisterClientResponseMsg::numManagers()
{
    return mNumManagers;
}

uint64_t
DDRegisterClientResponseMsg::managerID()
{
    return mManagerID;
}

const vector<std::string>&
DDRegisterClientResponseMsg::managerNodes()
{
    return mManagerNodes;
}

uint64_t
DDRegisterClientResponseMsg::timeout()
{
    return mTimeout;
}

bool
DDRegisterClientResponseMsg::waitForKeys()
{
    return mWaitForKeys;
}

void
DDRegisterClientResponseMsg::builder(MessageDef::Builder& msg)
{
    // This builder is missing code that sets the managerNodes in the message.
    // This message is not currently built in C++ so not a big deal. If it were
    // built in C++ later (for instance, the manager was rewritten in C++) then
    // we would need to complete this implementation much like the equivalent
    // Python code.
    // msg_mgr_nodes = client_msg.init('managerNodes', len(self._managerNodes))
    // for i in range(len(self._managerNodes)):
    //     msg_mgr_nodes[i] = self._managerNodes[i]
    DragonResponseMsg::builder(msg);
    DDRegisterClientResponseDef::Builder builder = msg.initDdRegisterClientResponse();
    builder.setClientID(mClientID);
    builder.setNumManagers(mNumManagers);
    builder.setManagerID(mManagerID);
    builder.setTimeout(mTimeout);
    builder.setWaitForKeys(mWaitForKeys);
}


/********************************************************************************************************/

DDDeregisterClientMsg::DDDeregisterClientMsg(uint64_t tag, uint64_t clientID, const char* respFLI) :
    DragonMsg(DDDeregisterClientMsg::TC, tag), mClientID(clientID), mFLI(respFLI) {}

void
DDDeregisterClientMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDDeregisterClientDef::Builder builder = msg.initDdDeregisterClient();
    builder.setClientID(mClientID);
    builder.setRespFLI(mFLI);
}

dragonError_t
DDDeregisterClientMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        uint64_t tag = reader.getTag();

        DDDeregisterClientDef::Reader deregisterClientReader = reader.getDdDeregisterClient();

        (*msg) = new DDDeregisterClientMsg(
            tag,
            deregisterClientReader.getClientID(),
            deregisterClientReader.getRespFLI().cStr()
        );
    } catch(...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DeregisterClient message.");
    }

    no_err_return(DRAGON_SUCCESS);

}

uint64_t
DDDeregisterClientMsg::clientID()
{
    return mClientID;
}

const char*
DDDeregisterClientMsg::respFLI()
{
    return mFLI.c_str();
}


DDDeregisterClientResponseMsg::DDDeregisterClientResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDDeregisterClientResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDDeregisterClientResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDDeregisterClientResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDDeregisterClientResponseMsg message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict destroy message. The message is not in use for now. */

DDDestroyMsg::DDDestroyMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDDestroyMsg::TC, tag), mFLI(respFLI) {}

void
DDDestroyMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDDestroyDef::Builder builder = msg.initDdDestroy();
    builder.setRespFLI(this->mFLI);
}

dragonError_t
DDDestroyMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        uint64_t tag = reader.getTag();

        DDDestroyDef::Reader destroyReader = reader.getDdDestroy();

        (*msg) = new DDDestroyMsg(tag, destroyReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the Destroy message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDDestroyMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict destroy response. The message is not in use for now. */

DDDestroyResponseMsg::DDDestroyResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDDestroyResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDDestroyResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDDestroyResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDDestroyResponseMsg message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict destroy manager message. This message is not in use for now. */

DDDestroyManagerMsg::DDDestroyManagerMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDDestroyManagerMsg::TC, tag), mFLI(respFLI) {}

void
DDDestroyManagerMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDDestroyManagerDef::Builder builder = msg.initDdDestroyManager();
    builder.setRespFLI(this->mFLI);
}

dragonError_t
DDDestroyManagerMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        uint64_t tag = reader.getTag();

        DDDestroyManagerDef::Reader destroyManagerReader = reader.getDdDestroyManager();

        (*msg) = new DDDestroyManagerMsg(tag, destroyManagerReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the Destroy message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDDestroyManagerMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict destroy manager response message. This message is not in use for now. */

DDDestroyManagerResponseMsg::DDDestroyManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDDestroyManagerResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDDestroyManagerResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDDestroyManagerResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDDestroyManagerResponseMsg message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict register manager message. This message is not in use for now. */

DDRegisterManagerMsg::DDRegisterManagerMsg(uint64_t tag, const char* mainFLI, const char* respFLI) :
    DragonMsg(DDRegisterManagerMsg::TC, tag), mMainFLI(mainFLI), mRespFLI(respFLI) {}

void
DDRegisterManagerMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRegisterManagerDef::Builder builder = msg.initDdRegisterManager();
    builder.setMainFLI(mMainFLI);
    builder.setRespFLI(mRespFLI);
}

dragonError_t
DDRegisterManagerMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        uint64_t tag = reader.getTag();

        DDRegisterManagerDef::Reader registerManagerReader = reader.getDdRegisterManager();

        (*msg) = new DDRegisterManagerMsg (
            tag, registerManagerReader.getMainFLI().cStr(),
            registerManagerReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDRegisterManager message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDRegisterManagerMsg::mainFLI()
{
    return mMainFLI.c_str();
}

const char*
DDRegisterManagerMsg::respFLI()
{
    return mRespFLI.c_str();
}

/********************************************************************************************************/
/* ddict register manager response message. This message is not in use for now. */

DDRegisterManagerResponseMsg::DDRegisterManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDRegisterManagerResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDRegisterManagerResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDRegisterManagerResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDRegisterManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict register client id message */

DDRegisterClientIDMsg::DDRegisterClientIDMsg(uint64_t tag, uint64_t clientID, const char* respFLI, const char* bufferedRespFLI) :
    DragonMsg(DDRegisterClientIDMsg::TC, tag), mClientID(clientID), mRespFLI(respFLI), mBufferedRespFLI(bufferedRespFLI) {}

void
DDRegisterClientIDMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRegisterClientIDDef::Builder builder = msg.initDdRegisterClientID();
    builder.setClientID(mClientID);
    builder.setRespFLI(mRespFLI);
    builder.setBufferedRespFLI(mBufferedRespFLI);
}

dragonError_t
DDRegisterClientIDMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        DDRegisterClientIDDef::Reader registerClientReader = reader.getDdRegisterClientID();

        (*msg) = new DDRegisterClientIDMsg (
            reader.getTag(),
            registerClientReader.getClientID(),
            registerClientReader.getRespFLI().cStr(),
            registerClientReader.getBufferedRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDRegisterClientID message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDRegisterClientIDMsg::clientID() {
    return mClientID;
}

const char*
DDRegisterClientIDMsg::respFLI()
{
    return mRespFLI.c_str();
}

const char*
DDRegisterClientIDMsg::bufferedRespFLI()
{
    return mBufferedRespFLI.c_str();
}

/********************************************************************************************************/
/* ddict register client id response message */

DDRegisterClientIDResponseMsg::DDRegisterClientIDResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDRegisterClientIDResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDRegisterClientIDResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDRegisterClientIDResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDRegisterManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict put message */

DDPutMsg::DDPutMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID=0, bool persist, bool waitFor) :
    DragonMsg(DDPutMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mPersist(persist), mWaitFor(waitFor) {}

void
DDPutMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPutDef::Builder builder = msg.initDdPut();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setPersist(mPersist);
    builder.setWaitFor(mWaitFor);
}

dragonError_t
DDPutMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDPutDef::Reader putReader = reader.getDdPut();

        (*msg) = new DDPutMsg (
            reader.getTag(),
            putReader.getClientID(),
            putReader.getChkptID(),
            putReader.getPersist(),
            putReader.getWaitFor());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPut message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDPutMsg::clientID()
{
    return mClientID;
}

uint64_t
DDPutMsg::chkptID()
{
    return mChkptID;
}

bool
DDPutMsg::persist()
{
    return mPersist;
}

bool
DDPutMsg::waitFor()
{
    return mWaitFor;
}

/********************************************************************************************************/
/* ddict put response message */

DDPutResponseMsg::DDPutResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDPutResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDPutResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDPutResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPutResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict get message */

DDGetMsg::DDGetMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen, bool fetchAdd, int fetchAddVal) :
    DragonMsg(DDGetMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mKey(key), mKeyLen(keyLen), mFetchAdd(fetchAdd), mFetchAddVal(fetchAddVal) {}

void
DDGetMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetDef::Builder builder = msg.initDdGet();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setKey(kj::arrayPtr(mKey, mKeyLen));
    builder.setFetchAdd(mFetchAdd);
    builder.setFetchAddVal(mFetchAddVal);
}

dragonError_t
DDGetMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetDef::Reader getReader = reader.getDdGet();

        (*msg) = new DDGetMsg (
            reader.getTag(),
            getReader.getClientID(),
            getReader.getChkptID(),
            getReader.getKey().begin(),
            getReader.getKey().size(),
            getReader.getFetchAdd(),
            getReader.getFetchAddVal());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetMsg::clientID()
{
    return mClientID;
}

uint64_t
DDGetMsg::chkptID()
{
    return mChkptID;
}

const unsigned char*
DDGetMsg::key()
{
    return mKey;
}

size_t
DDGetMsg::keyLen()
{
    return mKeyLen;
}

bool
DDGetMsg::fetchAdd()
{
    return mFetchAdd;
}

int
DDGetMsg::fetchAddVal()
{
    return mFetchAddVal;
}

/********************************************************************************************************/
/* ddict get response message */

DDGetResponseMsg::DDGetResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool freeMem):
    DragonResponseMsg(DDGetResponseMsg::TC, tag, ref, err, errInfo), mFreeMem(freeMem) {}


dragonError_t
DDGetResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetResponseDef::Reader getResponseReader = reader.getDdGetResponse();

        (*msg) = new DDGetResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            getResponseReader.getFreeMem());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

bool
DDGetResponseMsg::freeMem()
{
    return mFreeMem;
}

/********************************************************************************************************/
/* ddict pop message */

DDPopMsg::DDPopMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen) :
    DragonMsg(DDPopMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mKey(key), mKeyLen(keyLen) {}

void
DDPopMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPopDef::Builder builder = msg.initDdPop();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setKey(kj::arrayPtr(mKey, mKeyLen));
}

dragonError_t
DDPopMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDPopDef::Reader popReader = reader.getDdPop();

        (*msg) = new DDPopMsg (
            reader.getTag(),
            popReader.getClientID(),
            popReader.getChkptID(),
            popReader.getKey().begin(),
            popReader.getKey().size());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPop message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDPopMsg::clientID()
{
    return mClientID;
}

uint64_t
DDPopMsg::chkptID()
{
    return mChkptID;
}

const unsigned char*
DDPopMsg::key()
{
    return mKey;
}

size_t
DDPopMsg::keyLen()
{
    return mKeyLen;
}

/********************************************************************************************************/
/* ddict pop response message */

DDPopResponseMsg::DDPopResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool freeMem):
    DragonResponseMsg(DDPopResponseMsg::TC, tag, ref, err, errInfo), mFreeMem(freeMem) {}

dragonError_t
DDPopResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDPopResponseDef::Reader popResponseReader = reader.getDdPopResponse();

        (*msg) = new DDPopResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            popResponseReader.getFreeMem());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPopResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

bool
DDPopResponseMsg::freeMem()
{
    return mFreeMem;
}

/********************************************************************************************************/
/* ddict contains message */

DDContainsMsg::DDContainsMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen) :
    DragonMsg(DDContainsMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mKey(key), mKeyLen(keyLen) {}

void
DDContainsMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDContainsDef::Builder builder = msg.initDdContains();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setKey(kj::arrayPtr(mKey, mKeyLen));
}

dragonError_t
DDContainsMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDContainsDef::Reader containsReader = reader.getDdContains();

        (*msg) = new DDContainsMsg (
            reader.getTag(),
            containsReader.getClientID(),
            containsReader.getChkptID(),
            containsReader.getKey().begin(),
            containsReader.getKey().size());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDContains message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDContainsMsg::clientID()
{
    return mClientID;
}

uint64_t
DDContainsMsg::chkptID()
{
    return mChkptID;
}

const unsigned char*
DDContainsMsg::key()
{
    return mKey;
}

size_t
DDContainsMsg::keyLen()
{
    return mKeyLen;
}

/********************************************************************************************************/
/* ddict contains response message */

DDContainsResponseMsg::DDContainsResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDContainsResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDContainsResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDContainsResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDContainsResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict get length message */

DDLengthMsg::DDLengthMsg(uint64_t tag, uint64_t clientID, const char* respFLI, uint64_t chkptID=0, bool broadcast=true) :
    DragonMsg(DDLengthMsg::TC, tag), mClientID(clientID), mRespFLI(respFLI), mChkptID(chkptID), mBroadcast(broadcast) {}

void
DDLengthMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDLengthDef::Builder lengthBuilder = msg.initDdLength();
    lengthBuilder.setClientID(mClientID);
    lengthBuilder.setRespFLI(mRespFLI);
    lengthBuilder.setChkptID(mChkptID);
    lengthBuilder.setBroadcast(mBroadcast);
}

dragonError_t
DDLengthMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDLengthDef::Reader lengthReader = reader.getDdLength();

        (*msg) = new DDLengthMsg (
            reader.getTag(),
            lengthReader.getClientID(),
            lengthReader.getRespFLI().cStr(),
            lengthReader.getBroadcast());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDLength message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDLengthMsg::clientID()
{
    return mClientID;
}

const char*
DDLengthMsg::respFLI()
{
    return mRespFLI.c_str();
}

uint64_t
DDLengthMsg::chkptID()
{
    return mChkptID;
}

bool
DDLengthMsg::broadcast()
{
    return mBroadcast;
}

/********************************************************************************************************/
/* ddict get length response message */

DDLengthResponseMsg::DDLengthResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t length):
    DragonResponseMsg(DDLengthResponseMsg::TC, tag, ref, err, errInfo), mLength(length) {}

dragonError_t
DDLengthResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDLengthResponseDef::Reader lengthResponseReader = reader.getDdLengthResponse();

        (*msg) = new DDLengthResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            lengthResponseReader.getLength());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDLengthResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDLengthResponseMsg::length()
{
    return mLength;
}

/********************************************************************************************************/
/* ddict keys message */

DDKeysMsg::DDKeysMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const char* respFLI):
    DragonMsg(DDKeysMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mFLI(respFLI) {}

void
DDKeysMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDKeysDef::Builder builder = msg.initDdKeys();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setRespFLI(mFLI);
}

dragonError_t
DDKeysMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDKeysDef::Reader keysReader = reader.getDdKeys();

        (*msg) = new DDKeysMsg (
            reader.getTag(),
            keysReader.getClientID(),
            keysReader.getChkptID(),
            keysReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDKeys message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDKeysMsg::clientID()
{
    return mClientID;
}

uint64_t
DDKeysMsg::chkptID()
{
    return mChkptID;
}

const char*
DDKeysMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict keys response message */

DDKeysResponseMsg::DDKeysResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDKeysResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDKeysResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDKeysResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDKeysResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}


/********************************************************************************************************/
/* ddict clear message */

DDClearMsg::DDClearMsg(uint64_t tag, uint64_t clientID, const char* respFLI, uint64_t chkptID=0, bool broadcast=true) :
    DragonMsg(DDClearMsg::TC, tag), mClientID(clientID), mFLI(respFLI), mChkptID(chkptID), mBroadcast(broadcast) {}

void
DDClearMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDClearDef::Builder builder = msg.initDdClear();
    builder.setClientID(mClientID);
    builder.setRespFLI(mFLI);
    builder.setChkptID(mChkptID);
    builder.setBroadcast(mBroadcast);
}

dragonError_t
DDClearMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDClearDef::Reader clearReader = reader.getDdClear();

        (*msg) = new DDClearMsg (
            reader.getTag(),
            clearReader.getClientID(),
            clearReader.getRespFLI().cStr(),
            clearReader.getChkptID(),
            clearReader.getBroadcast());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDClear message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDClearMsg::clientID()
{
    return mClientID;
}

const char*
DDClearMsg::respFLI()
{
    return mFLI.c_str();
}

uint64_t
DDClearMsg::chkptID()
{
    return mChkptID;
}

bool
DDClearMsg::broadcast()
{
    return mBroadcast;
}

/********************************************************************************************************/
/* ddict clear response message */

DDClearResponseMsg::DDClearResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDClearResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDClearResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDClearResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDClearResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict manager get newest chkpt ID message */

DDManagerNewestChkptIDMsg::DDManagerNewestChkptIDMsg(uint64_t tag, const char* respFLI, bool broadcast=true) :
    DragonMsg(DDManagerNewestChkptIDMsg::TC, tag), mFLI(respFLI), mBroadcast(broadcast) {}

void
DDManagerNewestChkptIDMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDManagerNewestChkptIDDef::Builder builder = msg.initDdManagerNewestChkptID();
    builder.setRespFLI(mFLI);
    builder.setBroadcast(mBroadcast);
}

dragonError_t
DDManagerNewestChkptIDMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDManagerNewestChkptIDDef::Reader newestChkptIDReader = reader.getDdManagerNewestChkptID();

        (*msg) = new DDManagerNewestChkptIDMsg (
            reader.getTag(),
            newestChkptIDReader.getRespFLI().cStr(),
            newestChkptIDReader.getBroadcast());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerNewestChkptID message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDManagerNewestChkptIDMsg::respFLI()
{
    return mFLI.c_str();
}

bool
DDManagerNewestChkptIDMsg::broadcast()
{
    return mBroadcast;
}

/********************************************************************************************************/
/* ddict manager get newest chkpt ID response message */

DDManagerNewestChkptIDResponseMsg::DDManagerNewestChkptIDResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t managerID, uint64_t chkptID):
    DragonResponseMsg(DDManagerNewestChkptIDResponseMsg::TC, tag, ref, err, errInfo), mManagerID(managerID), mChkptID(chkptID) {}

dragonError_t
DDManagerNewestChkptIDResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDManagerNewestChkptIDResponseDef::Reader newestChkptIDResponseReader = reader.getDdManagerNewestChkptIDResponse();

        (*msg) = new DDManagerNewestChkptIDResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            newestChkptIDResponseReader.getManagerID(),
            newestChkptIDResponseReader.getChkptID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerNewestChkptIDResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDManagerNewestChkptIDResponseMsg::managerID()
{
    return mManagerID;
}

uint64_t
DDManagerNewestChkptIDResponseMsg::chkptID()
{
    return mChkptID;
}

/********************************************************************************************************/
/* ddict empty managers message */

DDEmptyManagersMsg::DDEmptyManagersMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDEmptyManagersMsg::TC, tag), mFLI(respFLI) {}

void
DDEmptyManagersMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDEmptyManagersDef::Builder builder = msg.initDdEmptyManagers();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDEmptyManagersMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDEmptyManagersDef::Reader emptyManagersReader = reader.getDdEmptyManagers();

        (*msg) = new DDEmptyManagersMsg (
            reader.getTag(),
            emptyManagersReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDEmptyManager message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDEmptyManagersMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict empty managers response message */

DDEmptyManagersResponseMsg::DDEmptyManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDEmptyManagersResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDEmptyManagersResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDEmptyManagersResponseDef::Reader emptyManagersResponseReader = reader.getDdEmptyManagersResponse();

        DDEmptyManagersResponseMsg* resp_msg;
        resp_msg = new DDEmptyManagersResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        auto managers = emptyManagersResponseReader.getManagers();
        for (auto manager: managers)
            resp_msg->mManagers.push_back(manager);

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDEmptyManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const vector<uint64_t>&
DDEmptyManagersResponseMsg::managers()
{
    return mManagers;
}

/********************************************************************************************************/
/* ddict get managers message */

DDGetManagersMsg::DDGetManagersMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDGetManagersMsg::TC, tag), mFLI(respFLI) {}

void
DDGetManagersMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetManagersDef::Builder builder = msg.initDdGetManagers();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDGetManagersMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetManagersDef::Reader getManagersReader = reader.getDdGetManagers();

        (*msg) = new DDGetManagersMsg (
            reader.getTag(),
            getManagersReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetManagers message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDGetManagersMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict get managers response message */

DDGetManagersResponseMsg::DDGetManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDGetManagersResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDGetManagersResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetManagersResponseDef::Reader getManagersResponseReader = reader.getDdGetManagersResponse();

        DDGetManagersResponseMsg* resp_msg;
        resp_msg = new DDGetManagersResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        auto managers = getManagersResponseReader.getManagers();
        for (auto manager: managers)
            resp_msg->mManagers.push_back(manager);

        auto emptyManagers = getManagersResponseReader.getEmptyManagers();
        for (auto emptyManager: emptyManagers)
            resp_msg->mEmptyManagers.push_back(emptyManager);

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDEmptyManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const std::vector<bool>&
DDGetManagersResponseMsg::emptyManagers()
{
    return mEmptyManagers;
}

const std::vector<std::string>&
DDGetManagersResponseMsg::managers()
{
    return mManagers;
}

/********************************************************************************************************/
/* ddict manager sync message */

DDManagerSyncMsg::DDManagerSyncMsg(uint64_t tag, const char* respFLI, const char* emptyManagerFLI) :
    DragonMsg(DDManagerSyncMsg::TC, tag), mFLI(respFLI), mEmptyManagerFLI(emptyManagerFLI) {}

void
DDManagerSyncMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDManagerSyncDef::Builder builder = msg.initDdManagerSync();
    builder.setRespFLI(mFLI);
    builder.setEmptyManagerFLI(mEmptyManagerFLI);
}

dragonError_t
DDManagerSyncMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDManagerSyncDef::Reader managerSyncReader = reader.getDdManagerSync();

        (*msg) = new DDManagerSyncMsg (
            reader.getTag(),
            managerSyncReader.getRespFLI().cStr(),
            managerSyncReader.getEmptyManagerFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerSync message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDManagerSyncMsg::respFLI()
{
    return mFLI.c_str();
}

const char *
DDManagerSyncMsg::emptyManagerFLI()
{
    return mEmptyManagerFLI.c_str();
}

/********************************************************************************************************/
/* ddict manager sync response message */

DDManagerSyncResponseMsg::DDManagerSyncResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDManagerSyncResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDManagerSyncResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        DDManagerSyncResponseMsg* resp_msg;
        resp_msg = new DDManagerSyncResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerSyncResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict unmark drained managers message */

DDUnmarkDrainedManagersMsg::DDUnmarkDrainedManagersMsg(uint64_t tag, const char* respFLI, std::vector<uint64_t>& managerIDs) :
    DragonMsg(DDUnmarkDrainedManagersMsg::TC, tag), mFLI(respFLI), mManagerIDs(managerIDs) {}

void
DDUnmarkDrainedManagersMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDUnmarkDrainedManagersDef::Builder builder = msg.initDdUnmarkDrainedManagers();
    builder.setRespFLI(mFLI);
    ::capnp::List<uint64_t>::Builder manager_ids = builder.initManagerIDs(mManagerIDs.size());
    for (size_t k=0 ; k<mManagerIDs.size() ; k++)
        manager_ids.set(k, mManagerIDs[k]);
}

dragonError_t
DDUnmarkDrainedManagersMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDUnmarkDrainedManagersDef::Reader unmarkDrainedManagersReader = reader.getDdUnmarkDrainedManagers();
        auto managerIDs = unmarkDrainedManagersReader.getManagerIDs();
        std::vector<uint64_t> manager_ids;
        for (uint64_t id: managerIDs)
            manager_ids.push_back(id);

        (*msg) = new DDUnmarkDrainedManagersMsg (
            reader.getTag(),
            unmarkDrainedManagersReader.getRespFLI().cStr(),
            manager_ids);
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDUnmarkDrainedManagers message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDUnmarkDrainedManagersMsg::respFLI()
{
    return mFLI.c_str();
}

const std::vector<uint64_t>&
DDUnmarkDrainedManagersMsg::managerIDs()
{
    return mManagerIDs;
}

/********************************************************************************************************/
/* ddict unmark drained managerIDs response message */

DDUnmarkDrainedManagersResponseMsg::DDUnmarkDrainedManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDUnmarkDrainedManagersResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDUnmarkDrainedManagersResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        DDUnmarkDrainedManagersResponseMsg* resp_msg;
        resp_msg = new DDUnmarkDrainedManagersResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDUnmarkDrainedManagersResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict mark drained managers message */

DDMarkDrainedManagersMsg::DDMarkDrainedManagersMsg(uint64_t tag, const char* respFLI, std::vector<uint64_t>& managerIDs) :
    DragonMsg(DDMarkDrainedManagersMsg::TC, tag), mFLI(respFLI), mManagerIDs(managerIDs) {}

void
DDMarkDrainedManagersMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDMarkDrainedManagersDef::Builder builder = msg.initDdMarkDrainedManagers();
    builder.setRespFLI(mFLI);
    ::capnp::List<uint64_t>::Builder manager_ids = builder.initManagerIDs(mManagerIDs.size());
    for (size_t k=0 ; k<mManagerIDs.size() ; k++)
        manager_ids.set(k, mManagerIDs[k]);
}

dragonError_t
DDMarkDrainedManagersMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDMarkDrainedManagersDef::Reader markDrainedManagersReader = reader.getDdMarkDrainedManagers();
        auto managerIDs = markDrainedManagersReader.getManagerIDs();
        std::vector<uint64_t> manager_ids;
        for (uint64_t id: managerIDs)
            manager_ids.push_back(id);

        (*msg) = new DDMarkDrainedManagersMsg (
            reader.getTag(),
            markDrainedManagersReader.getRespFLI().cStr(),
            manager_ids);
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDMarkDrainedManagers message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDMarkDrainedManagersMsg::respFLI()
{
    return mFLI.c_str();
}

const std::vector<uint64_t>&
DDMarkDrainedManagersMsg::managerIDs()
{
    return mManagerIDs;
}

/********************************************************************************************************/
/* ddict mark drained managerIDs response message */

DDMarkDrainedManagersResponseMsg::DDMarkDrainedManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDMarkDrainedManagersResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDMarkDrainedManagersResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        DDMarkDrainedManagersResponseMsg* resp_msg;
        resp_msg = new DDMarkDrainedManagersResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDMarkDrainedManagersResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict get meta data message */

DDGetMetaDataMsg::DDGetMetaDataMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDGetMetaDataMsg::TC, tag), mFLI(respFLI) {}

void
DDGetMetaDataMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetMetaDataDef::Builder builder = msg.initDdGetMetaData();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDGetMetaDataMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetMetaDataDef::Reader getMetaDataReader = reader.getDdGetMetaData();

        (*msg) = new DDGetMetaDataMsg (
            reader.getTag(),
            getMetaDataReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetMetaData message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDGetMetaDataMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict get meta data response message */

DDGetMetaDataResponseMsg::DDGetMetaDataResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serializedDdict, const uint64_t numManagers) :
    DragonResponseMsg(DDGetMetaDataResponseMsg::TC, tag, ref, err, errInfo), mSerializedDdict(serializedDdict), mNumManagers(numManagers) {}

dragonError_t
DDGetMetaDataResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetMetaDataResponseDef::Reader getMetaDataResponseReader = reader.getDdGetMetaDataResponse();

        DDGetMetaDataResponseMsg* resp_msg;
        resp_msg = new DDGetMetaDataResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            getMetaDataResponseReader.getSerializedDdict().cStr(),
            getMetaDataResponseReader.getNumManagers());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetMetaDataResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDGetMetaDataResponseMsg::serializedDdict()
{
    return mSerializedDdict.c_str();
}

const uint64_t
DDGetMetaDataResponseMsg::numManagers()
{
    return mNumManagers;
}

/********************************************************************************************************/
/* ddict manager nodes message */

DDManagerNodesMsg::DDManagerNodesMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDManagerNodesMsg::TC, tag), mFLI(respFLI) {}

void
DDManagerNodesMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDManagerNodesDef::Builder builder = msg.initDdManagerNodes();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDManagerNodesMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDManagerNodesDef::Reader managerNodesReader = reader.getDdManagerNodes();

        (*msg) = new DDManagerNodesMsg (
            reader.getTag(),
            managerNodesReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerNodes message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char *
DDManagerNodesMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict manager nodes response message */

DDManagerNodesResponseMsg::DDManagerNodesResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDManagerNodesResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDManagerNodesResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDManagerNodesResponseDef::Reader managerNodesResponseReader = reader.getDdManagerNodesResponse();

        DDManagerNodesResponseMsg* resp_msg;
        resp_msg = new DDManagerNodesResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        auto huids = managerNodesResponseReader.getHuids();
        for (auto huid: huids)
            resp_msg->mHuids.push_back(huid);

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDManagerNodesResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const vector<uint64_t>&
DDManagerNodesResponseMsg::huids()
{
    return mHuids;
}

/********************************************************************************************************/
/* ddict get iterator message */

DDIteratorMsg::DDIteratorMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDIteratorMsg::TC, tag), mClientID(clientID) {}

void
DDIteratorMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDIteratorDef::Builder builder = msg.initDdIterator();
    builder.setClientID(mClientID);
}

dragonError_t
DDIteratorMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDIteratorDef::Reader iteratorReader = reader.getDdIterator();

        (*msg) = new DDIteratorMsg(reader.getTag(), iteratorReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDIterator message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDIteratorMsg::clientID()
{
    return mClientID;
}

/********************************************************************************************************/
/* ddict get iterator response message */

DDIteratorResponseMsg::DDIteratorResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t iterID):
    DragonResponseMsg(DDIteratorResponseMsg::TC, tag, ref, err, errInfo), mIterID(iterID) {}

dragonError_t
DDIteratorResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDIteratorResponseDef::Reader iteratorReader = reader.getDdIteratorResponse();

        (*msg) = new DDIteratorResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            iteratorReader.getIterID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDIteratorResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDIteratorResponseMsg::iterID()
{
    return mIterID;
}

/********************************************************************************************************/
/* ddict iterator next message */

DDIteratorNextMsg::DDIteratorNextMsg(uint64_t tag, uint64_t clientID, uint64_t iterID) :
    DragonMsg(DDIteratorNextMsg::TC, tag), mClientID(clientID), mIterID(iterID) {}

void
DDIteratorNextMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDIteratorNextDef::Builder builder = msg.initDdIteratorNext();
    builder.setClientID(mClientID);
    builder.setIterID(mIterID);
}

dragonError_t
DDIteratorNextMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDIteratorNextDef::Reader iteratorNextReader = reader.getDdIteratorNext();

        (*msg) = new DDIteratorNextMsg (
            reader.getTag(),
            iteratorNextReader.getClientID(),
            iteratorNextReader.getIterID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDIteratorNext message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDIteratorNextMsg::clientID()
{
    return mClientID;
}

uint64_t
DDIteratorNextMsg::iterID()
{
    return mIterID;
}

/********************************************************************************************************/
/* ddict iterator next response message */

DDIteratorNextResponseMsg::DDIteratorNextResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDIteratorNextResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDIteratorNextResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDIteratorNextResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDIteratorNextResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict connect to manager message */

DDConnectToManagerMsg::DDConnectToManagerMsg(uint64_t tag, uint64_t client_id, uint64_t manager_id):
    DragonMsg(DDConnectToManagerMsg::TC, tag), mClientID(client_id), mManagerID(manager_id) {}


void
DDConnectToManagerMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDConnectToManagerDef::Builder builder = msg.initDdConnectToManager();
    builder.setClientID(mClientID);
    builder.setManagerID(mManagerID);
}

dragonError_t
DDConnectToManagerMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDConnectToManagerDef::Reader connectReader = reader.getDdConnectToManager();

        (*msg) = new DDConnectToManagerMsg(
            reader.getTag(),
            connectReader.getClientID(),
            connectReader.getManagerID()
        );

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the ConnectToManager message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDConnectToManagerMsg::clientID()
{
    return mClientID;
}

uint64_t
DDConnectToManagerMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict connect to manager response */

DDConnectToManagerResponseMsg::DDConnectToManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* manager_fli):
    DragonResponseMsg(DDConnectToManagerResponseMsg::TC, tag, ref, err, errInfo), mManagerFLI(manager_fli){}


dragonError_t
DDConnectToManagerResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDConnectToManagerResponseDef::Reader connectToManagerReader = reader.getDdConnectToManagerResponse();

        (*msg) = new DDConnectToManagerResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            connectToManagerReader.getManager().cStr()
        );

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the ConnectToManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDConnectToManagerResponseMsg::managerFLI()
{
    return mManagerFLI.c_str();
}

/********************************************************************************************************/
/* ddict connect to random manager message */

DDRandomManagerMsg::DDRandomManagerMsg(uint64_t tag, const char* respFLI):
    DragonMsg(DDRandomManagerMsg::TC, tag), mRespFLI(respFLI) {}

void
DDRandomManagerMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRandomManagerDef::Builder builder = msg.initDdRandomManager();
    builder.setRespFLI(mRespFLI);
}

dragonError_t
DDRandomManagerMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDRandomManagerDef::Reader randomManagerReader = reader.getDdRandomManager();

        (*msg) = new DDRandomManagerMsg(
            reader.getTag(),
            randomManagerReader.getRespFLI().cStr()
        );

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the GetRandomManager message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict connect to random manager response */

DDRandomManagerResponseMsg::DDRandomManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* manager_fli, uint64_t manager_id):
    DragonResponseMsg(DDRandomManagerResponseMsg::TC, tag, ref, err, errInfo), mManagerFLI(manager_fli), mManagerID(manager_id) {}

dragonError_t
DDRandomManagerResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDRandomManagerResponseDef::Reader randomManagerResponseReader = reader.getDdRandomManagerResponse();

        (*msg) = new DDRandomManagerResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            randomManagerResponseReader.getManager().cStr(),
            randomManagerResponseReader.getManagerID()
        );

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the GetRandomManagerResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDRandomManagerResponseMsg::managerFLI()
{
    return mManagerFLI.c_str();
}

uint64_t
DDRandomManagerResponseMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict get frozen message */

DDGetFreezeMsg::DDGetFreezeMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDGetFreezeMsg::TC, tag), mClientID(clientID) {}

void
DDGetFreezeMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetFreezeDef::Builder builder = msg.initDdGetFreeze();
    builder.setClientID(mClientID);
}

dragonError_t
DDGetFreezeMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetFreezeDef::Reader getFreezeReader = reader.getDdGetFreeze();

        (*msg) = new DDGetFreezeMsg (
            reader.getTag(),
            getFreezeReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the get frozen message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetFreezeMsg::clientID()
{
    return mClientID;
}

/********************************************************************************************************/
/* ddict get frozen response message */

DDGetFreezeResponseMsg::DDGetFreezeResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool frozen):
    DragonResponseMsg(DDGetFreezeResponseMsg::TC, tag, ref, err, errInfo), mFrozen(frozen) {}

dragonError_t
DDGetFreezeResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetFreezeResponseDef::Reader getFreezeResponseReader = reader.getDdGetFreezeResponse();

        (*msg) = new DDGetFreezeResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            getFreezeResponseReader.getFreeze());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the get frozen response message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

bool
DDGetFreezeResponseMsg::frozen()
{
    return mFrozen;
}

/********************************************************************************************************/
/* ddict freeze message */

DDFreezeMsg::DDFreezeMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDFreezeMsg::TC, tag), mFLI(respFLI) {}

void
DDFreezeMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDFreezeDef::Builder builder = msg.initDdFreeze();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDFreezeMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDFreezeDef::Reader freezeReader = reader.getDdFreeze();

        (*msg) = new DDFreezeMsg (
            reader.getTag(),
            freezeReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the freeze message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDFreezeMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict freeze response message */

DDFreezeResponseMsg::DDFreezeResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDFreezeResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDFreezeResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDFreezeResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the freeze response message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict unfreeze message */

DDUnFreezeMsg::DDUnFreezeMsg(uint64_t tag, const char* respFLI) :
    DragonMsg(DDUnFreezeMsg::TC, tag), mFLI(respFLI) {}

void
DDUnFreezeMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDFreezeDef::Builder builder = msg.initDdFreeze();
    builder.setRespFLI(mFLI);
}

dragonError_t
DDUnFreezeMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDUnFreezeDef::Reader unFreezeReader = reader.getDdUnFreeze();

        (*msg) = new DDUnFreezeMsg (
            reader.getTag(),
            unFreezeReader.getRespFLI().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the unfreeze message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
DDUnFreezeMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict unfreeze response message */

DDUnFreezeResponseMsg::DDUnFreezeResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDUnFreezeResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDUnFreezeResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDUnFreezeResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the unfreeze response message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict batch put message */

DDBatchPutMsg::DDBatchPutMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID=0, bool persist=true) :
    DragonMsg(DDBatchPutMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mPersist(persist) {}

void
DDBatchPutMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDBatchPutDef::Builder builder = msg.initDdBatchPut();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setPersist(mPersist);
}

dragonError_t
DDBatchPutMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDBatchPutDef::Reader batchPutReader = reader.getDdBatchPut();

        (*msg) = new DDBatchPutMsg (
            reader.getTag(),
            batchPutReader.getClientID(),
            batchPutReader.getChkptID(),
            batchPutReader.getPersist());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDBatchPut message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDBatchPutMsg::clientID()
{
    return mClientID;
}

uint64_t
DDBatchPutMsg::chkptID()
{
    return mChkptID;
}

bool
DDBatchPutMsg::persist()
{
    return mPersist;
}

/********************************************************************************************************/
/* ddict batch put response message */

DDBatchPutResponseMsg::DDBatchPutResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t numPuts, uint64_t managerID):
    DragonResponseMsg(DDBatchPutResponseMsg::TC, tag, ref, err, errInfo), mNumPuts(numPuts), mManagerID(managerID) {}

dragonError_t
DDBatchPutResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDBatchPutResponseDef::Reader batchPutResponseReader = reader.getDdBatchPutResponse();

        (*msg) = new DDBatchPutResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            batchPutResponseReader.getNumPuts(),
            batchPutResponseReader.getManagerID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDBatchPutResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDBatchPutResponseMsg::numPuts()
{
    return mNumPuts;
}

uint64_t
DDBatchPutResponseMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict bput message */
DDBPutMsg::DDBPutMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const char* respFLI, std::vector<uint64_t>& managers, bool batch) :
    DragonMsg(DDBPutMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mFLI(respFLI), mManagers(managers), mBatch(batch) {}

void
DDBPutMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDBPutDef::Builder builder = msg.initDdBPut();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setRespFLI(mFLI);
    builder.setBatch(mBatch);

    ::capnp::List<uint64_t>::Builder managers = builder.initManagers(mManagers.size());
    for (size_t k=0 ; k<mManagers.size() ; k++)
        managers.set(k, mManagers[k]);
}

uint64_t
DDBPutMsg::clientID()
{
    return mClientID;
}

uint64_t
DDBPutMsg::chkptID()
{
    return mChkptID;
}

const char *
DDBPutMsg::respFLI()
{
    return mFLI.c_str();
}

const std::vector<uint64_t>&
DDBPutMsg::managers()
{
    return mManagers;
}

bool
DDBPutMsg::batch()
{
    return mBatch;
}

/********************************************************************************************************/
/* ddict bput response message */

DDBPutResponseMsg::DDBPutResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t numPuts, uint64_t managerID):
    DragonResponseMsg(DDBPutResponseMsg::TC, tag, ref, err, errInfo), mNumPuts(numPuts), mManagerID(managerID) {}

dragonError_t
DDBPutResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDBPutResponseDef::Reader bPutResponseReader = reader.getDdBPutResponse();

        (*msg) = new DDBPutResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            bPutResponseReader.getNumPuts(),
            bPutResponseReader.getManagerID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDBPutResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDBPutResponseMsg::numPuts()
{
    return mNumPuts;
}

uint64_t
DDBPutResponseMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict advance message */
DDAdvanceMsg::DDAdvanceMsg(uint64_t tag, uint64_t clientID, const char* respFLI) :
    DragonMsg(DDAdvanceMsg::TC, tag), mClientID(clientID), mFLI(respFLI) {}

void
DDAdvanceMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDAdvanceDef::Builder builder = msg.initDdAdvance();
    builder.setClientID(mClientID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDAdvanceMsg::clientID()
{
    return mClientID;
}

const char *
DDAdvanceMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict advance response message */

DDAdvanceResponseMsg::DDAdvanceResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t chkptID):
    DragonResponseMsg(DDAdvanceResponseMsg::TC, tag, ref, err, errInfo), mChkptID(chkptID) {}

dragonError_t
DDAdvanceResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDAdvanceResponseDef::Reader advanceResponseReader = reader.getDdAdvanceResponse();

        (*msg) = new DDAdvanceResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            advanceResponseReader.getChkptID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDAdvanceResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDAdvanceResponseMsg::chkptID()
{
    return mChkptID;
}

/********************************************************************************************************/
/* ddict chkpt avail message */
DDChkptAvailMsg::DDChkptAvailMsg(uint64_t tag, uint64_t chkptID, const char* respFLI) :
    DragonMsg(DDChkptAvailMsg::TC, tag), mChkptID(chkptID), mFLI(respFLI) {}

void
DDChkptAvailMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDChkptAvailDef::Builder builder = msg.initDdChkptAvail();
    builder.setChkptID(mChkptID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDChkptAvailMsg::chkptID()
{
    return mChkptID;
}

const char *
DDChkptAvailMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict chkpt avail response message */

DDChkptAvailResponseMsg::DDChkptAvailResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool available, uint64_t managerID):
    DragonResponseMsg(DDChkptAvailResponseMsg::TC, tag, ref, err, errInfo), mAvailable(available), mManagerID(managerID) {}

dragonError_t
DDChkptAvailResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDChkptAvailResponseDef::Reader chkptAvailResponseReader = reader.getDdChkptAvailResponse();

        (*msg) = new DDChkptAvailResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            chkptAvailResponseReader.getAvailable(),
            chkptAvailResponseReader.getManagerID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDChkptAvailResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

bool
DDChkptAvailResponseMsg::available()
{
    return mAvailable;
}

uint64_t
DDChkptAvailResponseMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict persist message */
DDPersistMsg::DDPersistMsg(uint64_t tag, uint64_t chkptID, const char* respFLI) :
    DragonMsg(DDPersistMsg::TC, tag), mChkptID(chkptID), mFLI(respFLI) {}

void
DDPersistMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPersistDef::Builder builder = msg.initDdPersist();
    builder.setChkptID(mChkptID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDPersistMsg::chkptID()
{
    return mChkptID;
}

const char *
DDPersistMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict persist response message */

DDPersistResponseMsg::DDPersistResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDPersistResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDPersistResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDPersistResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPersistResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict persisted chkpt avail message */
DDPersistedChkptAvailMsg::DDPersistedChkptAvailMsg(uint64_t tag, uint64_t chkptID, const char* respFLI) :
    DragonMsg(DDPersistedChkptAvailMsg::TC, tag), mChkptID(chkptID), mFLI(respFLI) {}

void
DDPersistedChkptAvailMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPersistedChkptAvailDef::Builder builder = msg.initDdPersistedChkptAvail();
    builder.setChkptID(mChkptID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDPersistedChkptAvailMsg::chkptID()
{
    return mChkptID;
}

const char *
DDPersistedChkptAvailMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict persisted chkpt avail response message */

DDPersistedChkptAvailResponseMsg::DDPersistedChkptAvailResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool available, uint64_t managerID):
    DragonResponseMsg(DDPersistedChkptAvailResponseMsg::TC, tag, ref, err, errInfo), mAvailable(available), mManagerID(managerID) {}

dragonError_t
DDPersistedChkptAvailResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDPersistedChkptAvailResponseDef::Reader persistedChkptAvailResponseReader = reader.getDdPersistedChkptAvailResponse();

        (*msg) = new DDPersistedChkptAvailResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            persistedChkptAvailResponseReader.getAvailable(),
            persistedChkptAvailResponseReader.getManagerID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPersistedChkptAvailResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

bool
DDPersistedChkptAvailResponseMsg::available()
{
    return mAvailable;
}

uint64_t
DDPersistedChkptAvailResponseMsg::managerID()
{
    return mManagerID;
}

/********************************************************************************************************/
/* ddict restore message */
DDRestoreMsg::DDRestoreMsg(uint64_t tag, uint64_t chkptID, uint64_t clientID, const char* respFLI) :
    DragonMsg(DDRestoreMsg::TC, tag), mChkptID(chkptID), mClientID(clientID), mFLI(respFLI) {}

void
DDRestoreMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRestoreDef::Builder builder = msg.initDdRestore();
    builder.setChkptID(mChkptID);
    builder.setClientID(mClientID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDRestoreMsg::chkptID()
{
    return mChkptID;
}

uint64_t
DDRestoreMsg::clientID()
{
    return mClientID;
}

const char *
DDRestoreMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict restore response message */

DDRestoreResponseMsg::DDRestoreResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDRestoreResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDRestoreResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDRestoreResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDRestoreResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict persist chkpts message */
DDPersistChkptsMsg::DDPersistChkptsMsg(uint64_t tag, uint64_t clientID, const char* respFLI) :
    DragonMsg(DDPersistChkptsMsg::TC, tag), mClientID(clientID), mFLI(respFLI) {}

void
DDPersistChkptsMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPersistChkptsDef::Builder builder = msg.initDdPersistChkpts();
    builder.setClientID(mClientID);
    builder.setRespFLI(mFLI);
}

uint64_t
DDPersistChkptsMsg::clientID()
{
    return mClientID;
}

const char *
DDPersistChkptsMsg::respFLI()
{
    return mFLI.c_str();
}

/********************************************************************************************************/
/* ddict persist chkpts response message */

DDPersistChkptsResponseMsg::DDPersistChkptsResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDPersistChkptsResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
DDPersistChkptsResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDPersistChkptsResponseDef::Reader persistChkptsResponseReader = reader.getDdPersistChkptsResponse();

        DDPersistChkptsResponseMsg * resp_msg;
        resp_msg = new DDPersistChkptsResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

        auto persisted_chkpts = persistChkptsResponseReader.getChkptIDs();
        for (auto chkpt: persisted_chkpts)
            resp_msg->mChkptIDs.push_back(chkpt);

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDPersistChkptsResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const std::vector<uint64_t>&
DDPersistChkptsResponseMsg::chkptIDs()
{
    return mChkptIDs;
}

/********************************************************************************************************/
/* PMIx message used to store data in the dictionary during a PMIx Fence operation */

PMIxFenceMsg::PMIxFenceMsg(uint64_t tag, size_t ndata, const char* data):
    DragonMsg(PMIxFenceMsg::TC, tag), mNdata(ndata), mData(data) {}

void PMIxFenceMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    PMIxFenceMsgDef::Builder builder = msg.initPmIxFenceMsg();
    fprintf(stderr, "PMIxFence builder. Setting ndata to %ld\n", mNdata);fflush(stderr);
    builder.setNdata(mNdata);
    builder.setData(mData);
}

dragonError_t PMIxFenceMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        PMIxFenceMsgDef::Reader PMIxFenceMsgReader = reader.getPmIxFenceMsg();

        (*msg) = new PMIxFenceMsg(
            reader.getTag(),
            PMIxFenceMsgReader.getNdata(),
            PMIxFenceMsgReader.getData().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the PMIxFenceMsg message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

size_t PMIxFenceMsg::ndata() {
    return mNdata;
}

const char* PMIxFenceMsg::data() {
    return mData.c_str();
}

/********************************************************************************************************/
/* CPLoggingMessage */

CPLoggingMessage::CPLoggingMessage(uint64_t tag, const char *name, const char* msg, const char *time, const char *func, const char *hostname, const char *ipAddress, uint16_t port, const char *service, uint8_t level):
    DragonMsg(CPLoggingMessage::TC, tag), mName(name), mMsg(msg), mTime(time), mFunc(func), mHostname(hostname), mIpAddress(ipAddress), mPort(port), mService(service), mLevel(level) {}

void CPLoggingMessage::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    CPLoggingMessageDef::Builder builder = msg.initCpLoggingMessage();
    builder.setName(mName);
    builder.setMsg(mMsg);
    builder.setTime(mTime);
    builder.setFunc(mFunc);
    builder.setHostname(mHostname);
    builder.setIpAddress(mIpAddress);
    builder.setPort(mPort);
    builder.setService(mService);
    builder.setLevel(mLevel);
}

dragonError_t CPLoggingMessage::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        CPLoggingMessageDef::Reader CPLoggingMessageReader = reader.getCpLoggingMessage();

        (*msg) = new CPLoggingMessage(
            reader.getTag(),
            CPLoggingMessageReader.getName().cStr(),
            CPLoggingMessageReader.getMsg().cStr(),
            CPLoggingMessageReader.getTime().cStr(),
            CPLoggingMessageReader.getFunc().cStr(),
            CPLoggingMessageReader.getHostname().cStr(),
            CPLoggingMessageReader.getIpAddress().cStr(),
            CPLoggingMessageReader.getPort(),
            CPLoggingMessageReader.getService().cStr(),
            CPLoggingMessageReader.getLevel());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the PMIxFenceMsg message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* CPLoggingMessage::name() {
    return mName.c_str();
}

const char* CPLoggingMessage::msg() {
    return mMsg.c_str();
}

const char* CPLoggingMessage::time() {
    return mTime.c_str();
}

const char* CPLoggingMessage::func() {
    return mFunc.c_str();
}

const char* CPLoggingMessage::hostname() {
    return mHostname.c_str();
}

const char* CPLoggingMessage::ipAddress() {
    return mIpAddress.c_str();
}

uint8_t CPLoggingMessage::port() {
    return mPort;
}

const char* CPLoggingMessage::service() {
    return mService.c_str();
}

uint8_t CPLoggingMessage::level() {
    return mLevel;
}

/********************************************************************************************************/
/* local services external register */

LSExternalRegisterMsg::LSExternalRegisterMsg(uint64_t tag, const char* authToken, uint64_t pid,
                                             const char* procRespSdesc,
                                             const char* runtimeUid, const char* attemptId):
    DragonMsg(LSExternalRegisterMsg::TC, tag), mAuthToken(authToken), mPid(pid), mProcRespSdesc(procRespSdesc),
    mRuntimeUid(runtimeUid), mAttemptId(attemptId) {}

void
LSExternalRegisterMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSExternalRegisterDef::Builder builder = msg.initLsExternalRegister();
    builder.setAuthToken(mAuthToken);
    builder.setPid(mPid);
    builder.setProcRespSdesc(mProcRespSdesc);
    builder.setRuntimeIncarnation(mRuntimeUid);
    builder.setAttemptId(mAttemptId);
}

dragonError_t
LSExternalRegisterMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        LSExternalRegisterDef::Reader lsERReader = reader.getLsExternalRegister();

        (*msg) = new LSExternalRegisterMsg(
            reader.getTag(),
            lsERReader.getAuthToken().cStr(),
            lsERReader.getPid(),
            lsERReader.getProcRespSdesc().cStr(),
            lsERReader.getRuntimeIncarnation().cStr(),
            lsERReader.getAttemptId().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the LSExternalRegister message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
LSExternalRegisterMsg::authToken()
{
    return mAuthToken.c_str();
}

const uint64_t
LSExternalRegisterMsg::pid()
{
    return mPid;
}

const char*
LSExternalRegisterMsg::procRespSdesc()
{
    return mProcRespSdesc.c_str();
}

const char*
LSExternalRegisterMsg::runtimeUid()
{
    return mRuntimeUid.c_str();
}

const char*
LSExternalRegisterMsg::attemptId()
{
    return mAttemptId.c_str();
}

/********************************************************************************************************/
/* local services external resources ready */

LSExternalResourcesReadyMsg::LSExternalResourcesReadyMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo,
    const char* runtimeUid, const char* attemptId, uint64_t nodeIndex, uint64_t pUid,
    const char* lsReturnQd, const char* gsReturnQd, uint64_t numGwChannelsPerNode,
    const std::vector<std::pair<std::string, std::string>>& gwEnv, const char* defaultPd, const char* infPd):
    DragonResponseMsg(LSExternalResourcesReadyMsg::TC, tag, ref, err, errInfo),
    mRuntimeUid(runtimeUid), mAttemptId(attemptId), mNodeIndex(nodeIndex),
    mPUid(pUid), mLsReturnQd(lsReturnQd), mGsReturnQd(gsReturnQd),
    mNumGwChannelsPerNode(numGwChannelsPerNode), mGwEnv(gwEnv), mDefaultPd(defaultPd), mInfPd(infPd) {}

void
LSExternalResourcesReadyMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    LSExternalResourcesReadyDef::Builder builder = msg.initLsExternalResourcesReady();
    builder.setRuntimeIncarnation(mRuntimeUid);
    builder.setAttemptId(mAttemptId);
    builder.setNodeIndex(mNodeIndex);
    builder.setPUid(mPUid);
    builder.setLsReturnQd(mLsReturnQd);
    builder.setGsReturnQd(mGsReturnQd);
    builder.setNumGwChannelsPerNode(mNumGwChannelsPerNode);
    ::capnp::List<GwEnvEntryDef>::Builder entries = builder.initGwEnv(mGwEnv.size());
    for (size_t index = 0; index < mGwEnv.size(); ++index) {
        entries[index].setKey(mGwEnv[index].first);
        entries[index].setValue(mGwEnv[index].second);
    }
    builder.setDefaultPd(mDefaultPd);
    builder.setInfPd(mInfPd);
}

dragonError_t
LSExternalResourcesReadyMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader response = reader.getResponseOption().getValue();
        LSExternalResourcesReadyDef::Reader resources = reader.getLsExternalResourcesReady();
        std::vector<std::pair<std::string, std::string>> gwEnv;
        for (GwEnvEntryDef::Reader entry : resources.getGwEnv())
            gwEnv.push_back(std::make_pair(std::string(entry.getKey().cStr()), std::string(entry.getValue().cStr())));
        *msg = new LSExternalResourcesReadyMsg(
            reader.getTag(), response.getRef(), (dragonError_t)response.getErr(), response.getErrInfo().cStr(),
            resources.getRuntimeIncarnation().cStr(), resources.getAttemptId().cStr(),
            resources.getNodeIndex(), resources.getPUid(), resources.getLsReturnQd().cStr(),
            resources.getGsReturnQd().cStr(), resources.getNumGwChannelsPerNode(), gwEnv,
            resources.getDefaultPd().cStr(), resources.getInfPd().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception deserializing LSExternalResourcesReady.");
    }
    no_err_return(DRAGON_SUCCESS);
}

const char* LSExternalResourcesReadyMsg::runtimeUid() { return mRuntimeUid.c_str(); }
const char* LSExternalResourcesReadyMsg::attemptId() { return mAttemptId.c_str(); }
const uint64_t LSExternalResourcesReadyMsg::nodeIndex() { return mNodeIndex; }
const uint64_t LSExternalResourcesReadyMsg::pUid() { return mPUid; }
const char* LSExternalResourcesReadyMsg::lsReturnQd() { return mLsReturnQd.c_str(); }
const char* LSExternalResourcesReadyMsg::gsReturnQd() { return mGsReturnQd.c_str(); }
const uint64_t LSExternalResourcesReadyMsg::numGwChannelsPerNode() { return mNumGwChannelsPerNode; }
const std::vector<std::pair<std::string, std::string>>& LSExternalResourcesReadyMsg::gwEnv() { return mGwEnv; }
const char* LSExternalResourcesReadyMsg::defaultPd() { return mDefaultPd.c_str(); }
const char* LSExternalResourcesReadyMsg::infPd() { return mInfPd.c_str(); }

/********************************************************************************************************/
/* global services external register completion */

GSExternalRegisterCompleteMsg::GSExternalRegisterCompleteMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t pUid):
    DragonResponseMsg(GSExternalRegisterCompleteMsg::TC, tag, ref, err, errInfo), mPUid(pUid) {}

void
GSExternalRegisterCompleteMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    msg.initGsExternalRegisterComplete().setPUid(mPUid);
}

dragonError_t
GSExternalRegisterCompleteMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader response = reader.getResponseOption().getValue();
        GSExternalRegisterCompleteDef::Reader completion = reader.getGsExternalRegisterComplete();
        *msg = new GSExternalRegisterCompleteMsg(
            reader.getTag(), response.getRef(), (dragonError_t)response.getErr(), response.getErrInfo().cStr(),
            completion.getPUid());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception deserializing GSExternalRegisterComplete.");
    }
    no_err_return(DRAGON_SUCCESS);
}

const uint64_t GSExternalRegisterCompleteMsg::pUid() { return mPUid; }

/********************************************************************************************************/
/* global services process detach acknowledgement */

GSProcessDetachAckMsg::GSProcessDetachAckMsg(uint64_t tag, uint64_t pUid, uint64_t detachRef):
    DragonMsg(GSProcessDetachAckMsg::TC, tag), mPUid(pUid), mDetachRef(detachRef) {}

void
GSProcessDetachAckMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    GSProcessDetachAckDef::Builder builder = msg.initGsProcessDetachAck();
    builder.setPUid(mPUid);
    builder.setDetachRef(mDetachRef);
}

dragonError_t
GSProcessDetachAckMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        GSProcessDetachAckDef::Reader ack = reader.getGsProcessDetachAck();
        *msg = new GSProcessDetachAckMsg(reader.getTag(), ack.getPUid(), ack.getDetachRef());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception deserializing GSProcessDetachAck.");
    }
    no_err_return(DRAGON_SUCCESS);
}

const uint64_t GSProcessDetachAckMsg::pUid() { return mPUid; }
const uint64_t GSProcessDetachAckMsg::detachRef() { return mDetachRef; }

/********************************************************************************************************/
/* local services external register cancel */

LSExternalRegisterCancelMsg::LSExternalRegisterCancelMsg(uint64_t tag,
                                                         const char* runtimeUid, const char* attemptId,
                                                         uint64_t pUid):
    DragonMsg(LSExternalRegisterCancelMsg::TC, tag),
    mRuntimeUid(runtimeUid), mAttemptId(attemptId), mPUid(pUid) {}

void
LSExternalRegisterCancelMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    LSExternalRegisterCancelDef::Builder cancel = msg.initLsExternalRegisterCancel();
    cancel.setRuntimeIncarnation(mRuntimeUid);
    cancel.setAttemptId(mAttemptId);
    cancel.setPUid(mPUid);
}

dragonError_t
LSExternalRegisterCancelMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        LSExternalRegisterCancelDef::Reader cancel = reader.getLsExternalRegisterCancel();
        *msg = new LSExternalRegisterCancelMsg(
            reader.getTag(), cancel.getRuntimeIncarnation().cStr(),
            cancel.getAttemptId().cStr(), cancel.getPUid());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception deserializing LSExternalRegisterCancel.");
    }
    no_err_return(DRAGON_SUCCESS);
}

const char* LSExternalRegisterCancelMsg::runtimeUid() { return mRuntimeUid.c_str(); }
const char* LSExternalRegisterCancelMsg::attemptId() { return mAttemptId.c_str(); }
const uint64_t LSExternalRegisterCancelMsg::pUid() { return mPUid; }

/* attachable runtime descriptor */

AttachableRuntimeDescrMsg::AttachableRuntimeDescrMsg(uint64_t tag, uint64_t nodeIndex, const char* gsQd,
                                                     const char* lsQd, const char* defaultPd,
                                                     const char* infPd, const char* rtUid):
    DragonMsg(AttachableRuntimeDescrMsg::TC, tag),
    mNodeIndex(nodeIndex), mGsQd(gsQd), mLsQd(lsQd), mDefaultPd(defaultPd), mInfPd(infPd), mRtUid(rtUid) {}

void
AttachableRuntimeDescrMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    AttachableRuntimeDescrDef::Builder descr = msg.initAttachableRuntimeDescr();
    descr.setNodeIndex(mNodeIndex);
    descr.setGsQd(mGsQd);
    descr.setLsQd(mLsQd);
    descr.setDefaultPd(mDefaultPd);
    descr.setInfPd(mInfPd);
    descr.setRtUid(mRtUid);
}

dragonError_t
AttachableRuntimeDescrMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        AttachableRuntimeDescrDef::Reader descr = reader.getAttachableRuntimeDescr();
        *msg = new AttachableRuntimeDescrMsg(
            reader.getTag(), descr.getNodeIndex(), descr.getGsQd().cStr(), descr.getLsQd().cStr(),
            descr.getDefaultPd().cStr(), descr.getInfPd().cStr(), descr.getRtUid().cStr());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception deserializing AttachableRuntimeDescr.");
    }
    no_err_return(DRAGON_SUCCESS);
}

const uint64_t AttachableRuntimeDescrMsg::nodeIndex() { return mNodeIndex; }
const char* AttachableRuntimeDescrMsg::gsQd() { return mGsQd.c_str(); }
const char* AttachableRuntimeDescrMsg::lsQd() { return mLsQd.c_str(); }
const char* AttachableRuntimeDescrMsg::defaultPd() { return mDefaultPd.c_str(); }
const char* AttachableRuntimeDescrMsg::infPd() { return mInfPd.c_str(); }
const char* AttachableRuntimeDescrMsg::rtUid() { return mRtUid.c_str(); }

/********************************************************************************************************/
/* global services process detach */

GSProcessDetachMsg::GSProcessDetachMsg(uint64_t tag, uint64_t pUid, uint64_t rCuid):
    DragonMsg(GSProcessDetachMsg::TC, tag), mPUid(pUid), mRCuid(rCuid) {}

void
GSProcessDetachMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    GSProcessDetachDef::Builder builder = msg.initGsProcessDetach();
    builder.setPUid(mPUid);
    builder.setRCuid(mRCuid);
}

dragonError_t
GSProcessDetachMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        GSProcessDetachDef::Reader gsPDReader = reader.getGsProcessDetach();

        (*msg) = new GSProcessDetachMsg(
            reader.getTag(),
            gsPDReader.getPUid(),
            gsPDReader.getRCuid());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the GSProcessDetach message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const uint64_t
GSProcessDetachMsg::pUid()
{
    return mPUid;
}

const uint64_t
GSProcessDetachMsg::rCuid()
{
    return mRCuid;
}

/********************************************************************************************************/
/* global services process detach response */

GSProcessDetachResponseMsg::GSProcessDetachResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(GSProcessDetachResponseMsg::TC, tag, ref, err, errInfo) {}

dragonError_t
GSProcessDetachResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new GSProcessDetachResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the GSProcessDetachResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

} // end dragon namespace
