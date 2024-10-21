#include <stdlib.h>
#include <unordered_map>
#include <functional>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <dragon/messages.hpp>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "err.h"
#include <dragon/utils.h>
#include "_message_tcs.hpp"
#include <dragon/shared_lock.h>
#include "shared_lock.h"

using namespace std;

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
    int fd;

    try {
        capnp::MallocMessageBuilder message;
        MessageDef::Builder msg = message.initRoot<MessageDef>();
        this->builder(msg);

        err = dragon_fli_create_writable_fd(sendh, &fd, true, 0, 0, timeout);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not create writable fd to send the message.");

        capnp::writePackedMessageToFd(fd, message);

        close(fd);

        err = dragon_fli_finalize_writable_fd(sendh);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not finalize the fd after sending message.");

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

SHCreateProcessLocalChannel::SHCreateProcessLocalChannel(uint64_t tag, uint64_t puid, const char* respFLI):
    DragonMsg(SHCreateProcessLocalChannel::TC, tag), mPUID(puid), mFLI(respFLI) {}

void
SHCreateProcessLocalChannel::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    SHCreateProcessLocalChannelDef::Builder builder = msg.initShCreateProcessLocalChannel();
    builder.setPuid(this->mPUID);
    builder.setRespFLI(this->mFLI);
}

dragonError_t
SHCreateProcessLocalChannel::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {

        SHCreateProcessLocalChannelDef::Reader mReader = reader.getShCreateProcessLocalChannel();

        (*msg) = new SHCreateProcessLocalChannel(
            reader.getTag(),
            mReader.getPuid(),
            mReader.getRespFLI().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalChannel message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
SHCreateProcessLocalChannel::respFLI()
{
    return mFLI.c_str();
}

const uint64_t
SHCreateProcessLocalChannel::puid()
{
    return mPUID;
}

/********************************************************************************************************/
/* local services create process local channel response */

SHCreateProcessLocalChannelResponse::SHCreateProcessLocalChannelResponse(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serChannel):
    DragonResponseMsg(SHCreateProcessLocalChannelResponse::TC, tag, ref, err, errInfo), mSerChannel(serChannel) {}

void
SHCreateProcessLocalChannelResponse::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    SHCreateProcessLocalChannelResponseDef::Builder builder = msg.initShCreateProcessLocalChannelResponse();
    builder.setSerChannel(mSerChannel);
}

dragonError_t
SHCreateProcessLocalChannelResponse::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        SHCreateProcessLocalChannelResponseDef::Reader mReader = reader.getShCreateProcessLocalChannelResponse();

        (*msg) = new SHCreateProcessLocalChannelResponse(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getSerChannel().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the SHCreateProcessLocalChannelResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

const char*
SHCreateProcessLocalChannelResponse::serChannel()
{
    return this->mSerChannel.c_str();
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
        SHSetKVDef::Reader mReader = reader.getShSetKV();

        (*msg) = new SHSetKVMsg(
            reader.getTag(),
            mReader.getKey().cStr(),
            mReader.getValue().cStr(),
            mReader.getRespFLI().cStr());

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
        SHGetKVDef::Reader mReader = reader.getShGetKV();

        (*msg) = new SHGetKVMsg(
            reader.getTag(),
            mReader.getKey().cStr(),
            mReader.getRespFLI().cStr());

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
        SHGetKVResponseDef::Reader mReader = reader.getShGetKVResponse();
        (*msg) = new SHGetKVResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getValue().cStr());

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
    DragonMsg(DDRegisterClientMsg::TC, tag), mFLI(respFLI), bFLI(bufferedRespFLI) {}

void
DDRegisterClientMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDRegisterClientDef::Builder builder = msg.initDdRegisterClient();
    builder.setRespFLI(this->mFLI);
    builder.setBufferedRespFLI(this->bFLI);
}

dragonError_t
DDRegisterClientMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDRegisterClientDef::Reader mReader = reader.getDdRegisterClient();

        (*msg) = new DDRegisterClientMsg(
            reader.getTag(),
            mReader.getRespFLI().cStr(),
            mReader.getBufferedRespFLI().cStr());

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
    return bFLI.c_str();
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
        DDRegisterClientResponseDef::Reader mReader = reader.getDdRegisterClientResponse();

        (*msg) = new DDRegisterClientResponseMsg(
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getClientID(),
            mReader.getNumManagers(),
            mReader.getManagerID(),
            mReader.getTimeout());

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

uint64_t
DDRegisterClientResponseMsg::timeout()
{
    return mTimeout;
}

void
DDRegisterClientResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonResponseMsg::builder(msg);
    DDRegisterClientResponseDef::Builder builder = msg.initDdRegisterClientResponse();
    builder.setClientID(mClientID);
    builder.setNumManagers(mNumManagers);
    builder.setManagerID(mManagerID);
    builder.setTimeout(mTimeout);
}


/********************************************************************************************************/


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

        DDDestroyDef::Reader mReader = reader.getDdDestroy();

        (*msg) = new DDDestroyMsg(tag, mReader.getRespFLI().cStr());
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
/* ddict destroy manager message */

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

        DDDestroyManagerDef::Reader mReader = reader.getDdDestroyManager();

        (*msg) = new DDDestroyManagerMsg(tag, mReader.getRespFLI().cStr());
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
/* ddict destroy manager response message */

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
/* ddict register manager message */

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

        DDRegisterManagerDef::Reader mReader = reader.getDdRegisterManager();

        (*msg) = new DDRegisterManagerMsg(tag, mReader.getMainFLI().cStr(), mReader.getRespFLI().cStr());
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
/* ddict register manager response message */

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
        uint64_t tag = reader.getTag();

        DDRegisterClientIDDef::Reader mReader = reader.getDdRegisterClientID();

        (*msg) = new DDRegisterClientIDMsg(tag, mReader.getClientID(), mReader.getRespFLI().cStr(), mReader.getBufferedRespFLI().cStr());
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

DDPutMsg::DDPutMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDPutMsg::TC, tag), mClientID(clientID) {}

void
DDPutMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPutDef::Builder builder = msg.initDdPut();
    builder.setClientID(mClientID);
}

dragonError_t
DDPutMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDPutDef::Reader mReader = reader.getDdPut();

        (*msg) = new DDPutMsg(reader.getTag(), mReader.getClientID());
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

DDGetMsg::DDGetMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDGetMsg::TC, tag), mClientID(clientID) {}

void
DDGetMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetDef::Builder builder = msg.initDdGet();
    builder.setClientID(mClientID);
}

dragonError_t
DDGetMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetDef::Reader mReader = reader.getDdGet();

        (*msg) = new DDGetMsg(reader.getTag(), mReader.getClientID());
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

/********************************************************************************************************/
/* ddict get response message */

DDGetResponseMsg::DDGetResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDGetResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDGetResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDGetResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict pop message */

DDPopMsg::DDPopMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDPopMsg::TC, tag), mClientID(clientID) {}

void
DDPopMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDPopDef::Builder builder = msg.initDdPop();
    builder.setClientID(mClientID);
}

dragonError_t
DDPopMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDPopDef::Reader mReader = reader.getDdPop();

        (*msg) = new DDPopMsg(reader.getTag(), mReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDPopMsg::clientID()
{
    return mClientID;
}

/********************************************************************************************************/
/* ddict pop response message */

DDPopResponseMsg::DDPopResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo):
    DragonResponseMsg(DDPopResponseMsg::TC, tag, ref, err, errInfo) {}


dragonError_t
DDPopResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();

        (*msg) = new DDPopResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict contains message */

DDContainsMsg::DDContainsMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDContainsMsg::TC, tag), mClientID(clientID) {}

void
DDContainsMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDContainsDef::Builder builder = msg.initDdContains();
    builder.setClientID(mClientID);
}

dragonError_t
DDContainsMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDContainsDef::Reader mReader = reader.getDdContains();

        (*msg) = new DDContainsMsg(reader.getTag(), mReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDContainsMsg::clientID()
{
    return mClientID;
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
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict get length message */

DDGetLengthMsg::DDGetLengthMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDGetLengthMsg::TC, tag), mClientID(clientID) {}

void
DDGetLengthMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetLengthDef::Builder builder = msg.initDdGetLength();
    builder.setClientID(mClientID);
}

dragonError_t
DDGetLengthMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetLengthDef::Reader mReader = reader.getDdGetLength();

        (*msg) = new DDGetLengthMsg(reader.getTag(), mReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetLengthMsg::clientID()
{
    return mClientID;
}

/********************************************************************************************************/
/* ddict get length response message */

DDGetLengthResponseMsg::DDGetLengthResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t length):
    DragonResponseMsg(DDGetLengthResponseMsg::TC, tag, ref, err, errInfo), mLength(length) {}

void
DDGetLengthResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetLengthResponseDef::Builder builder = msg.initDdGetLengthResponse();
    builder.setLength(mLength);
}

dragonError_t
DDGetLengthResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetLengthResponseDef::Reader mReader = reader.getDdGetLengthResponse();


        (*msg) = new DDGetLengthResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getLength());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetLengthResponseMsg::length()
{
    return mLength;
}

/********************************************************************************************************/
/* ddict clear message */

DDClearMsg::DDClearMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDClearMsg::TC, tag), mClientID(clientID) {}

void
DDClearMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDClearDef::Builder builder = msg.initDdClear();
    builder.setClientID(mClientID);
}

dragonError_t
DDClearMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDClearDef::Reader mReader = reader.getDdClear();

        (*msg) = new DDClearMsg(reader.getTag(), mReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDClearMsg::clientID()
{
    return mClientID;
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
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/
/* ddict get iterator message */

DDGetIteratorMsg::DDGetIteratorMsg(uint64_t tag, uint64_t clientID) :
    DragonMsg(DDGetIteratorMsg::TC, tag), mClientID(clientID) {}

void
DDGetIteratorMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetIteratorDef::Builder builder = msg.initDdGetIterator();
    builder.setClientID(mClientID);
}

dragonError_t
DDGetIteratorMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        DDGetIteratorDef::Reader mReader = reader.getDdGetIterator();

        (*msg) = new DDGetIteratorMsg(reader.getTag(), mReader.getClientID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetIteratorMsg::clientID()
{
    return mClientID;
}

/********************************************************************************************************/
/* ddict get iterator response message */

DDGetIteratorResponseMsg::DDGetIteratorResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t iterID):
    DragonResponseMsg(DDGetIteratorResponseMsg::TC, tag, ref, err, errInfo), mIterID(iterID) {}

void
DDGetIteratorResponseMsg::builder(MessageDef::Builder& msg)
{
    DragonMsg::builder(msg);
    DDGetIteratorResponseDef::Builder builder = msg.initDdGetIteratorResponse();
    builder.setIterID(mIterID);
}

dragonError_t
DDGetIteratorResponseMsg::deserialize(MessageDef::Reader& reader, DragonMsg** msg)
{
    try {
        ResponseDef::Reader rReader = reader.getResponseOption().getValue();
        DDGetIteratorResponseDef::Reader mReader = reader.getDdGetIteratorResponse();


        (*msg) = new DDGetIteratorResponseMsg (
            reader.getTag(),
            rReader.getRef(),
            (dragonError_t)rReader.getErr(),
            rReader.getErrInfo().cStr(),
            mReader.getIterID());

    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

uint64_t
DDGetIteratorResponseMsg::iterID()
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
        DDIteratorNextDef::Reader mReader = reader.getDdIteratorNext();

        (*msg) = new DDIteratorNextMsg(reader.getTag(), mReader.getClientID(), mReader.getIterID());
    } catch (...) {
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGet message.");
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
        err_return(DRAGON_FAILURE, "There was an exception while deserializing the DDGetResponse message.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/********************************************************************************************************/

typedef dragonError_t (*deserializeFun)(MessageDef::Reader& reader, DragonMsg** msg);

static unordered_map<MessageType, deserializeFun> deserializeFunctions
{
    {SH_CREATE_PROCESS_LOCAL_CHANNEL, &SHCreateProcessLocalChannel::deserialize},
    {SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE, &SHCreateProcessLocalChannelResponse::deserialize},
    {SH_SET_KV, &SHSetKVMsg::deserialize},
    {SH_SET_KV_RESPONSE, &SHSetKVResponseMsg::deserialize},
    {SH_GET_KV, &SHGetKVMsg::deserialize},
    {SH_GET_KV_RESPONSE, &SHGetKVResponseMsg::deserialize},
    {DD_REGISTER_CLIENT, &DDRegisterClientMsg::deserialize},
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
    {DD_GET_LENGTH, &DDGetLengthMsg::deserialize},
    {DD_GET_LENGTH_RESPONSE, &DDGetLengthResponseMsg::deserialize},
    {DD_CLEAR, &DDClearMsg::deserialize},
    {DD_CLEAR_RESPONSE, &DDClearResponseMsg::deserialize},
    {DD_GET_ITERATOR, &DDGetIteratorMsg::deserialize},
    {DD_GET_ITERATOR_RESPONSE, &DDGetIteratorResponseMsg::deserialize},
    {DD_ITERATOR_NEXT, &DDIteratorNextMsg::deserialize},
    {DD_ITERATOR_NEXT_RESPONSE, &DDIteratorNextResponseMsg::deserialize}
};

/* From here on down we should put this in an api.cpp and api.hpp to house Dragon API code. */

dragonError_t
recv_fli_msg(dragonFLIRecvHandleDescr_t* recvh, DragonMsg** msg, const timespec_t* timeout)
{
    int fd;

    try {
        dragonError_t err;
        err = dragon_fli_create_readable_fd(recvh, &fd, timeout);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not create readable file descriptor to read message.");

        ::capnp::PackedFdMessageReader message(fd);

        close(fd);

        err = dragon_fli_finalize_readable_fd(recvh);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not finalize readable file descriptor.");

        MessageDef::Reader reader = message.getRoot<MessageDef>();
        MessageType tc = (MessageType)reader.getTc();

        if (deserializeFunctions.count(tc) == 0)
            err_return(DRAGON_INVALID_MESSAGE, dragon_msg_tc_name(tc));

        err = (deserializeFunctions.at(tc))(reader, msg);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not deserialize message.");

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
    err(err), err_str(err_str)
{}

DragonError::~DragonError() {}

dragonError_t DragonError::get_rc() const {
    return err;
}

const char* DragonError::get_err_str() const {
    return err_str.c_str();
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
dragon_sh_send_receive(DragonMsg* req_msg, DragonMsg** resp_msg, dragonFLIDescr_t* return_fli, const timespec_t* timeout)
{
    dragonError_t err;
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
        err = recv_fli_msg(&recvh, resp_msg, timeout);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&sh_return_lock);
            append_err_return(err, "Could not open receive response message.");
        }

        DragonResponseMsg* resp = static_cast<DragonResponseMsg*>(*resp_msg);

        if (resp->ref() == req_tag)
            have_resp = true;
        else /* toss it */
            delete resp;
    }

    err = dragon_unlock(&sh_return_lock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not unlock the sh_return channel.");

    err = dragon_fli_close_recv_handle(&recvh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_create_process_local_channel(dragonChannelDescr_t* ch, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    char *end;
    const char* puid_str;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonMsg* resp_msg;
    SHCreateProcessLocalChannelResponse* resp;
    dragonChannelSerial_t ch_ser;

    if (ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The ch argument cannot be NULL.");

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

    SHCreateProcessLocalChannel msg(inc_sh_tag(), puid, ser_fli);

    err = dragon_sh_send_receive(&msg, &resp_msg, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    if (resp_msg->tc() != SHCreateProcessLocalChannelResponse::TC)
        err_return(err, "Expected an SHCreateProcessLocalChannelResponse and did not get it.");

    resp = static_cast<SHCreateProcessLocalChannelResponse*>(resp_msg);

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

dragonError_t
dragon_ls_set_kv(const unsigned char* key, const unsigned char* value, const timespec_t* timeout)
{
    dragonError_t err;
    char* ser_fli;
    dragonFLIDescr_t return_fli;
    dragonFLISerial_t return_fli_ser;
    DragonMsg* resp_msg;
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

    err = dragon_sh_send_receive(&msg, &resp_msg, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    if (resp_msg->tc() != SHSetKVResponseMsg::TC)
        err_return(err, "Expected an SHSetKVResponse and did not get it.");

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
    DragonMsg* resp_msg;
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

    err = dragon_sh_send_receive(&msg, &resp_msg, &return_fli, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not complete send/receive operation.");

    if (resp_msg->tc() != SHGetKVResponseMsg::TC)
        err_return(err, "Expected an SHSetKVResponse and did not get it.");

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
