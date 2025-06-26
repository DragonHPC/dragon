#include <stdlib.h>
#include <unordered_map>
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

static uint64_t sh_tag = 0;

uint64_t inc_sh_tag() {
    uint64_t tmp = sh_tag;
    sh_tag+=1;
    return tmp;
}

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

SHCreateProcessLocalChannelMsg::SHCreateProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t muid, uint64_t blockSize, uint64_t capacity, const char* respFLI):
    DragonMsg(SHCreateProcessLocalChannelMsg::TC, tag), mPUID(puid), mMUID(muid), mBlockSize(blockSize), mCapacity(capacity), mFLI(respFLI) {}

void
SHCreateProcessLocalChannelMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHCreateProcessLocalChannelDef::Builder builder = msg.initShCreateProcessLocalChannel();
    builder.setPuid(mPUID);
    builder.setMuid(mMUID);
    builder.setBlockSize(mBlockSize);
    builder.setCapacity(mCapacity);
    builder.setRespFLI(mFLI);
}

dragonError_t
SHCreateProcessLocalChannelMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        SHCreateProcessLocalChannelDef::Reader shCPLCReader = reader.getShCreateProcessLocalChannel();

        (*msg) = new SHCreateProcessLocalChannelMsg(
            reader.getTag(),
            shCPLCReader.getPuid(),
            shCPLCReader.getMuid(),
            shCPLCReader.getBlockSize(),
            shCPLCReader.getCapacity(),
            shCPLCReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalChannel message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
SHCreateProcessLocalChannelMsg::respFLI()
{
    return mFLI.c_str();
}

const uint64_t
SHCreateProcessLocalChannelMsg::puid()
{
    return mPUID;
}

const uint64_t
SHCreateProcessLocalChannelMsg::muid()
{
    return mMUID;
}

const uint64_t
SHCreateProcessLocalChannelMsg::blockSize()
{
    return mBlockSize;
}

const uint64_t
SHCreateProcessLocalChannelMsg::capacity()
{
    return mCapacity;
}

/********************************************************************************************************/
/* local services create process local channel response */

SHCreateProcessLocalChannelResponseMsg::SHCreateProcessLocalChannelResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serChannel):
    DragonResponseMsg(SHCreateProcessLocalChannelResponseMsg::TC, tag, ref, err, errInfo), mSerChannel(serChannel) {}

void
SHCreateProcessLocalChannelResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    SHCreateProcessLocalChannelResponseDef::Builder builder = msg.initShCreateProcessLocalChannelResponse();
    builder.setSerChannel(mSerChannel);
}

dragonError_t
SHCreateProcessLocalChannelResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        SHCreateProcessLocalChannelResponseDef::Reader shCPLCResponseReader = reader.getShCreateProcessLocalChannelResponse();

        (*msg) = new SHCreateProcessLocalChannelResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            shCPLCResponseReader.getSerChannel().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
SHCreateProcessLocalChannelResponseMsg::serChannel()
{
    return this->mSerChannel.c_str();
}

/********************************************************************************************************/
/* local services destroy process local channel */

SHDestroyProcessLocalChannelMsg::SHDestroyProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t cuid, const char* respFLI):
    DragonMsg(SHDestroyProcessLocalChannelMsg::TC, tag), mPUID(puid), mCUID(cuid), mFLI(respFLI) {}

void
SHDestroyProcessLocalChannelMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHDestroyProcessLocalChannelDef::Builder builder = msg.initShDestroyProcessLocalChannel();
    builder.setPuid(this->mPUID);
    builder.setCuid(this->mCUID);
    builder.setRespFLI(this->mFLI);
}

dragonError_t
SHDestroyProcessLocalChannelMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        SHDestroyProcessLocalChannelDef::Reader shDPLCReader = reader.getShDestroyProcessLocalChannel();

        (*msg) = new SHDestroyProcessLocalChannelMsg(
            reader.getTag(),
            shDPLCReader.getPuid(),
            shDPLCReader.getCuid(),
            shDPLCReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHDestroyProcessLocalChannel message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
SHDestroyProcessLocalChannelMsg::respFLI()
{
    return mFLI.c_str();
}

const uint64_t
SHDestroyProcessLocalChannelMsg::puid()
{
    return mPUID;
}

const uint64_t
SHDestroyProcessLocalChannelMsg::cuid()
{
    return mCUID;
}

/********************************************************************************************************/
/* local services Destroy Process Local Channel Response */

SHDestroyProcessLocalChannelResponseMsg::SHDestroyProcessLocalChannelResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(SHDestroyProcessLocalChannelResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
SHDestroyProcessLocalChannelResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new SHDestroyProcessLocalChannelResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHDestroyProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}


/********************************************************************************************************/
/* local services Create Process Local Pool */

SHCreateProcessLocalPoolMsg::SHCreateProcessLocalPoolMsg(uint64_t tag, uint64_t puid, uint64_t size, uint64_t minBlockSize,
                                                      const char* name, const size_t* preAllocs, const size_t numPreAllocs,
                                                      const char* respFLI):
    DragonMsg(SHCreateProcessLocalPoolMsg::TC, tag), mPUID(puid), mRespFLI(respFLI), mSize(size), mMinBlockSize(minBlockSize),
    mName(name) {

    for (size_t k=0;k<numPreAllocs;k++)
        mPreAllocs.push_back(preAllocs[k]);
}

void
SHCreateProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHCreateProcessLocalPoolDef::Builder builder = msg.initShCreateProcessLocalPool();
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
SHCreateProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        SHCreateProcessLocalPoolDef::Reader mReader = reader.getShCreateProcessLocalPool();

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

        (*msg) = new SHCreateProcessLocalPoolMsg(
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
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* SHCreateProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

const uint64_t SHCreateProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char* SHCreateProcessLocalPoolMsg::name() {
    return mName.c_str();
}

const uint64_t SHCreateProcessLocalPoolMsg::minBlockSize() {
    return mMinBlockSize;
}

const uint64_t SHCreateProcessLocalPoolMsg::size() {
    return mSize;
}

const size_t SHCreateProcessLocalPoolMsg::preAlloc(int idx) {
    return mPreAllocs[idx];
}

const size_t SHCreateProcessLocalPoolMsg::numPreAllocs() {
    return mPreAllocs.size();
}

/********************************************************************************************************/
/* local services Create Process Local Pool Response */

SHCreateProcessLocalPoolResponseMsg::SHCreateProcessLocalPoolResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err,
                                                                   const char* errInfo, const char* serPool):
    DragonResponseMsg(SHCreateProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo), mSerPool(serPool) {}

void
SHCreateProcessLocalPoolResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    SHCreateProcessLocalPoolResponseDef::Builder builder = msg.initShCreateProcessLocalPoolResponse();
    builder.setSerPool(mSerPool);
}

dragonError_t
SHCreateProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        SHCreateProcessLocalPoolResponseDef::Reader mReader = reader.getShCreateProcessLocalPoolResponse();

        (*msg) = new SHCreateProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getSerPool().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* SHCreateProcessLocalPoolResponseMsg::serPool() {
    return mSerPool.c_str();

}

/********************************************************************************************************/
/* local services Register Process Local Pool */

SHRegisterProcessLocalPoolMsg::SHRegisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI):
    DragonMsg(SHRegisterProcessLocalPoolMsg::TC, tag), mPUID(puid), mSerPool(serPool), mRespFLI(respFLI) {}

void
SHRegisterProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHRegisterProcessLocalPoolDef::Builder builder = msg.initShRegisterProcessLocalPool();
    builder.setPuid(mPUID);
    builder.setSerPool(mSerPool.c_str());
    builder.setRespFLI(mRespFLI.c_str());
}

dragonError_t
SHRegisterProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        SHRegisterProcessLocalPoolDef::Reader mReader = reader.getShRegisterProcessLocalPool();

        (*msg) = new SHRegisterProcessLocalPoolMsg(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getSerPool().cStr(),
            mReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHRegisterProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
SHRegisterProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char*
SHRegisterProcessLocalPoolMsg::serPool() {
    return mSerPool.c_str();
}

const char*
SHRegisterProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

/********************************************************************************************************/
/* local services Register Process Local Pool Response */

SHRegisterProcessLocalPoolResponseMsg::SHRegisterProcessLocalPoolResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(SHRegisterProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
SHRegisterProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new SHRegisterProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHRegisterProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Deregister Process Local Pool */

SHDeregisterProcessLocalPoolMsg::SHDeregisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI):
    DragonMsg(SHDeregisterProcessLocalPoolMsg::TC, tag), mPUID(puid), mSerPool(serPool), mRespFLI(respFLI) {}

void
SHDeregisterProcessLocalPoolMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHDeregisterProcessLocalPoolDef::Builder builder = msg.initShDeregisterProcessLocalPool();
    builder.setPuid(mPUID);
    builder.setSerPool(mSerPool.c_str());
    builder.setRespFLI(mRespFLI.c_str());
}

dragonError_t
SHDeregisterProcessLocalPoolMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {

        SHDeregisterProcessLocalPoolDef::Reader mReader = reader.getShDeregisterProcessLocalPool();

        (*msg) = new SHDeregisterProcessLocalPoolMsg(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getSerPool().cStr(),
            mReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHDeregisterProcessLocalPool message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
SHDeregisterProcessLocalPoolMsg::puid() {
    return mPUID;
}

const char*
SHDeregisterProcessLocalPoolMsg::serPool() {
    return mSerPool.c_str();
}

const char*
SHDeregisterProcessLocalPoolMsg::respFLI() {
    return mRespFLI.c_str();
}

/********************************************************************************************************/
/* local services Deregister Process Local Pool Response */

SHDeregisterProcessLocalPoolResponseMsg::SHDeregisterProcessLocalPoolResponseMsg(
    uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(SHDeregisterProcessLocalPoolResponseMsg::TC, tag, ref, err, errInfo)
    {}

dragonError_t
SHDeregisterProcessLocalPoolResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new SHDeregisterProcessLocalPoolResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHDeregisterProcessLocalPoolResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Set Key/Value Pair */

SHSetKVMsg::SHSetKVMsg(uint64_t tag, const char* key, const char* value, const char* respFLI):
    DragonMsg(SHSetKVMsg::TC, tag), mKey(key), mValue(value), mFLI(respFLI) {}

void SHSetKVMsg::builder(MessageDef::Builder& msg) {
    DragonMsg::builder(msg);
    SHSetKVDef::Builder builder = msg.initShSetKV();
    builder.setKey(this->mKey);
    builder.setValue(this->mValue);
    builder.setRespFLI(this->mFLI);
}

dragonError_t SHSetKVMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        SHSetKVDef::Reader shSetKVReader = reader.getShSetKV();

        (*msg) = new SHSetKVMsg(
            reader.getTag(),
            shSetKVReader.getKey().cStr(),
            shSetKVReader.getValue().cStr(),
            shSetKVReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHSetKV message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* SHSetKVMsg::key() {
    return mKey.c_str();
}

const char* SHSetKVMsg::value() {
    return mValue.c_str();
}

const char* SHSetKVMsg::respFLI() {
    return mFLI.c_str();
}

/********************************************************************************************************/
/* local services Set Key/Value Pair Response */

SHSetKVResponseMsg::SHSetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(SHSetKVResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
SHSetKVResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new SHSetKVResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHSetKVResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* local services Get Key/Value Pair */

SHGetKVMsg::SHGetKVMsg(uint64_t tag, const char* key, const char* respFLI):
    DragonMsg(SHGetKVMsg::TC, tag), mKey(key), mFLI(respFLI) {}

void SHGetKVMsg::builder(MessageDef::Builder& msg) {
    DragonMsg::builder(msg);
    SHGetKVDef::Builder builder = msg.initShGetKV();
    builder.setKey(this->mKey);
    builder.setRespFLI(this->mFLI);
}

dragonError_t SHGetKVMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg) {
    try {
        SHGetKVDef::Reader shGetKVReader = reader.getShGetKV();

        (*msg) = new SHGetKVMsg(
            reader.getTag(),
            shGetKVReader.getKey().cStr(),
            shGetKVReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHGetKV message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* SHGetKVMsg::key() {
    return mKey.c_str();
}

const char* SHGetKVMsg::respFLI() {
    return mFLI.c_str();
}

/********************************************************************************************************/
/* local services Get Key/Value Pair Response */

SHGetKVResponseMsg::SHGetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* value):
    DragonResponseMsg(SHGetKVResponseMsg::TC, tag, ref, err, errInfo), mValue(value) {}

void
SHGetKVResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    SHGetKVResponseDef::Builder builder = msg.initShGetKVResponse();
    builder.setValue(mValue);
}

dragonError_t
SHGetKVResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        SHGetKVResponseDef::Reader shGetKVReponseReader = reader.getShGetKVResponse();
        (*msg) = new SHGetKVResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            shGetKVReponseReader.getValue().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHGetKVResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char* SHGetKVResponseMsg::value() {
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

DDRegisterClientResponseMsg::DDRegisterClientResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t clientID, uint64_t numManagers, uint64_t managerID, uint64_t timeout) :
    DragonResponseMsg(DDRegisterClientResponseMsg::TC, tag, ref, err, errInfo),
    mClientID(clientID),
    mNumManagers(numManagers),
    mManagerID(managerID),
    mTimeout(timeout) {}

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
            registerClientResponseReader.getTimeout());

        capnp::List<capnp::Text>::Reader manager_nodes_reader = registerClientResponseReader.getManagerNodes();

        for (auto it=manager_nodes_reader.begin() ; it!=manager_nodes_reader.end() ; it++)
            resp_msg->mManagerNodes.push_back(it->cStr());

        *msg = resp_msg;

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalChannelResponse message.");
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

DDPutMsg::DDPutMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID=0, bool persist=true) :
    DragonMsg(DDPutMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mPersist(persist) {}

void
DDPutMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPutDef::Builder builder = msg.initDdPut();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setPersist(mPersist);
}

dragonError_t
DDPutMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDPutDef::Reader putReader = reader.getDdPut();

        (*msg) = new DDPutMsg (
            reader.getTag(),
            putReader.getClientID(),
            putReader.getChkptID()),
            putReader.getPersist();
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

DDGetMsg::DDGetMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen) :
    DragonMsg(DDGetMsg::TC, tag), mClientID(clientID), mChkptID(chkptID), mKey(key), mKeyLen(keyLen) {}

void
DDGetMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetDef::Builder builder = msg.initDdGet();
    builder.setClientID(mClientID);
    builder.setChkptID(mChkptID);
    builder.setKey(kj::arrayPtr(mKey, mKeyLen));
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
            getReader.getKey().size());

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

typedef dragonError_t (*deserializeFun)(MessageDef::Reader& reader, DragonMsg** msg);

static unordered_map<MessageType, deserializeFun> deserializeFunctions
{
    {SH_CREATE_PROCESS_LOCAL_CHANNEL, &SHCreateProcessLocalChannelMsg::deserialize},
    {SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE, &SHCreateProcessLocalChannelResponseMsg::deserialize},
    {SH_DESTROY_PROCESS_LOCAL_CHANNEL, &SHDestroyProcessLocalChannelMsg::deserialize},
    {SH_DESTROY_PROCESS_LOCAL_CHANNEL_RESPONSE, &SHDestroyProcessLocalChannelResponseMsg::deserialize},
    {SH_CREATE_PROCESS_LOCAL_POOL, &SHCreateProcessLocalPoolMsg::deserialize},
    {SH_CREATE_PROCESS_LOCAL_POOL_RESPONSE, &SHCreateProcessLocalPoolResponseMsg::deserialize},
    {SH_REGISTER_PROCESS_LOCAL_POOL, &SHRegisterProcessLocalPoolMsg::deserialize},
    {SH_REGISTER_PROCESS_LOCAL_POOL_RESPONSE, &SHRegisterProcessLocalPoolResponseMsg::deserialize},
    {SH_DEREGISTER_PROCESS_LOCAL_POOL, &SHDeregisterProcessLocalPoolMsg::deserialize},
    {SH_DEREGISTER_PROCESS_LOCAL_POOL_RESPONSE, &SHDeregisterProcessLocalPoolResponseMsg::deserialize},
    {SH_SET_KV, &SHSetKVMsg::deserialize},
    {SH_SET_KV_RESPONSE, &SHSetKVResponseMsg::deserialize},
    {SH_GET_KV, &SHGetKVMsg::deserialize},
    {SH_GET_KV_RESPONSE, &SHGetKVResponseMsg::deserialize},
    {DD_REGISTER_CLIENT, &DDRegisterClientMsg::deserialize},
    {DD_REGISTER_CLIENT_RESPONSE, &DDRegisterClientResponseMsg::deserialize},
    {DD_DEREGISTER_CLIENT, &DDDeregisterClientMsg::deserialize},
    {DD_DEREGISTER_CLIENT_RESPONSE, &DDDeregisterClientResponseMsg::deserialize},
    {DD_DESTROY, &DDDestroyMsg::deserialize},
    {DD_DESTROY_RESPONSE, &DDDestroyResponseMsg::deserialize},
    {DD_DESTROY_MANAGER, &DDDestroyManagerMsg::deserialize},
    {DD_DESTROY_MANAGER_RESPONSE, &DDDestroyManagerResponseMsg::deserialize},
    {DD_REGISTER_MANAGER, &DDRegisterManagerMsg::deserialize},
    {DD_REGISTER_MANAGER_RESPONSE, &DDRegisterManagerResponseMsg::deserialize},
    {DD_REGISTER_CLIENT_ID, &DDRegisterClientIDMsg::deserialize},
    {DD_REGISTER_CLIENT_ID_RESPONSE, &DDRegisterClientIDResponseMsg::deserialize},
    {DD_PUT, &DDPutMsg::deserialize},
    {DD_PUT_RESPONSE, &DDPutResponseMsg::deserialize},
    {DD_GET, &DDGetMsg::deserialize},
    {DD_GET_RESPONSE, &DDGetResponseMsg::deserialize},
    {DD_POP, &DDPopMsg::deserialize},
    {DD_POP_RESPONSE, &DDPopResponseMsg::deserialize},
    {DD_CONTAINS, &DDContainsMsg::deserialize},
    {DD_CONTAINS_RESPONSE, &DDContainsResponseMsg::deserialize},
    {DD_KEYS, &DDKeysMsg::deserialize},
    {DD_KEYS_RESPONSE, &DDKeysResponseMsg::deserialize},
    {DD_LENGTH, &DDLengthMsg::deserialize},
    {DD_LENGTH_RESPONSE, &DDLengthResponseMsg::deserialize},
    {DD_CLEAR, &DDClearMsg::deserialize},
    {DD_CLEAR_RESPONSE, &DDClearResponseMsg::deserialize},
    {DD_ITERATOR, &DDIteratorMsg::deserialize},
    {DD_ITERATOR_RESPONSE, &DDIteratorResponseMsg::deserialize},
    {DD_ITERATOR_NEXT, &DDIteratorNextMsg::deserialize},
    {DD_ITERATOR_NEXT_RESPONSE, &DDIteratorNextResponseMsg::deserialize},
    {DD_CONNECT_TO_MANAGER, &DDConnectToManagerMsg::deserialize},
    {DD_CONNECT_TO_MANAGER_RESPONSE, &DDConnectToManagerResponseMsg::deserialize},
    {DD_RANDOM_MANAGER, &DDRandomManagerMsg::deserialize},
    {DD_RANDOM_MANAGER_RESPONSE, &DDRandomManagerResponseMsg::deserialize},
    {DD_MANAGER_NEWEST_CHKPT_ID, &DDManagerNewestChkptIDMsg::deserialize},
    {DD_MANAGER_NEWEST_CHKPT_ID_RESPONSE, &DDManagerNewestChkptIDResponseMsg::deserialize},
    {DD_EMPTY_MANAGERS, &DDEmptyManagersMsg::deserialize},
    {DD_EMPTY_MANAGERS_RESPONSE, &DDEmptyManagersResponseMsg::deserialize},
    {DD_MANAGER_NODES, &DDManagerNodesMsg::deserialize},
    {DD_MANAGER_NODES_RESPONSE, &DDManagerNodesResponseMsg::deserialize},
    {DD_GET_MANAGERS, &DDGetManagersMsg::deserialize},
    {DD_GET_MANAGERS_RESPONSE, &DDGetManagersResponseMsg::deserialize},
    {DD_MANAGER_SYNC, &DDManagerSyncMsg::deserialize},
    {DD_MANAGER_SYNC_RESPONSE, &DDManagerSyncResponseMsg::deserialize},
    {DD_UNMARK_DRAINED_MANAGERS, &DDUnmarkDrainedManagersMsg::deserialize},
    {DD_UNMARK_DRAINED_MANAGERS_RESPONSE, &DDUnmarkDrainedManagersResponseMsg::deserialize},
    {DD_MARK_DRAINED_MANAGERS, &DDMarkDrainedManagersMsg::deserialize},
    {DD_MARK_DRAINED_MANAGERS_RESPONSE, &DDMarkDrainedManagersResponseMsg::deserialize},
    {DD_GET_META_DATA, &DDGetMetaDataMsg::deserialize},
    {DD_GET_META_DATA_RESPONSE, &DDGetMetaDataResponseMsg::deserialize}
    // TODO: Add entries here for new messages (GetRandom, ConnectManager, responses)
};

/* From here on down we should put this in an api.cpp and api.hpp to house Dragon API code. */

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
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not receive message from fli.");

        err = dragon_memory_get_pointer(&mem, &mem_ptr);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not get pointer to memory returned from fli.");

        err = dragon_memory_get_size(&mem, &mem_size);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not get size of memory returned from fli.");

        kj::ArrayPtr<const capnp::word> words(reinterpret_cast<const capnp::word*>(mem_ptr), mem_size / sizeof(capnp::word));
        capnp::FlatArrayMessageReader message(words);
        MessageDef::Reader reader = message.getRoot<MessageDef>();
        MessageType tc = (MessageType)reader.getTc();

        if (deserializeFunctions.count(tc) == 0)
            err_return(DRAGON_INVALID_MESSAGE, dragon_msg_tc_name(tc));

        err = (deserializeFunctions.at(tc))(reader, msg);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not deserialize message.");

        err = dragon_memory_free(&mem);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not free memory allocation for the message.");


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

DragonError::DragonError(const dragonError_t err, const char* err_str):
    mErr(err), mErrStr(err_str), mTraceback(dragon_getlasterrstr())
{}

DragonError::~DragonError() {}

dragonError_t DragonError::get_rc() const {
    return mErr;
}

const char* DragonError::get_err_str() const {
    return mErrStr.c_str();
}

const char* DragonError::get_tb() const {
    return mTraceback.c_str();
}

std::ostream& operator<<(std::ostream& os, const DragonError& obj) {
    os << "DragonError(" << dragon_get_rc_string(obj.get_rc()) << ", \"" << obj.get_err_str() << "\")";
    if (strlen(obj.get_tb()) > 0)
        os << "\n" << obj.get_tb();
    return os;
}

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

    if (*shep_return_cd == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The local shepherd return channel descriptor is not set in the environment.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
dragon_get_shep_cd(char** shep_cd)
{
    if (shep_cd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The shep_cd argument cannot be NULL.");

    *shep_cd = getenv("DRAGON_LOCAL_SHEP_CD");

    if (*shep_cd == NULL)
        err_return(DRAGON_INVALID_OPERATION, "The local shepherd channel descriptor is not set in the environment.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_get_return_sh_fli(dragonFLIDescr_t* return_fli)
{
    dragonError_t err;
    dragonChannelDescr_t shep_return_ch;
    dragonChannelSerial_t shep_return_ser;
    char* shep_ret_cd;

    if (return_fli == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The return_fli argument cannot be NULL.");

    err = dragon_get_shep_return_cd(&shep_ret_cd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not do send/receive operation since Local Services return cd environment variable was not correctly set.");

    shep_return_ser.data = dragon_base64_decode(shep_ret_cd, &shep_return_ser.len);

    err = dragon_channel_attach(&shep_return_ser, &shep_return_ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to Local Services return channel.");

    err = dragon_channel_serial_free(&shep_return_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free the serialized channel structure.");

    err = dragon_fli_create(return_fli, &shep_return_ch, NULL, NULL, 0, NULL, true, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create return Local Services FLI.");

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t
dragon_sh_send_receive(DragonMsg* req_msg, DragonResponseMsg** resp_msg, MessageType expected_msg_type, dragonFLIDescr_t* return_fli, const timespec_t* timeout)
{
    dragonError_t err;
    DragonMsg* msg;
    char* shep_cd;
    dragonChannelSerial_t shep_ser;
    dragonChannelDescr_t shep_ch;
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
        append_err_return(err, "Could not do intialize the sh_return thread lock.");

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

    err = dragon_fli_create(&shep_fli, &shep_ch, NULL, NULL, 0, NULL, true, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create main Local Services FLI.");

    err = dragon_lock(&sh_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not lock the sh_return channel");

    err = dragon_fli_open_send_handle(&shep_fli, &sendh, NULL, NULL, false, true, timeout);
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

    err = dragon_channel_detach(ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from process local channel.");

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
