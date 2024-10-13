#ifndef messages_hpp
#define messages_hpp

#include <dragon/fli.h>
#include <dragon/message_tcs.hpp>
#include <dragon/message_defs.capnp.h>
#include <dragon/return_codes.h>
#include <string>
#include <vector>

//namespace DragonInfra {
class DragonError {
    public:
    DragonError(const dragonError_t err, const char* err_str);
    ~DragonError();
    dragonError_t get_rc() const;
    const char* get_err_str() const;

    private:
    dragonError_t err;
    std::string err_str;
};

class DragonMsg {
    public:
    DragonMsg(MessageType tc, uint64_t tag);
    virtual ~DragonMsg();
    dragonError_t send(dragonFLISendHandleDescr_t* sendh, const timespec_t* timeout);
    MessageType tc();
    uint64_t tag();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    MessageType mTC;
    uint64_t mTag;
};

class DragonResponseMsg: public DragonMsg {
    public:
    DragonResponseMsg(MessageType tc, uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    virtual ~DragonResponseMsg();
    uint64_t ref();
    dragonError_t err();
    const char* errInfo();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mRef;
    dragonError_t mErr;
    std::string mErrInfo;
};

class SHCreateProcessLocalChannel: public DragonMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_CHANNEL;

    SHCreateProcessLocalChannel(uint64_t tag, uint64_t puid, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const uint64_t puid();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    std::string mFLI;
};

class SHCreateProcessLocalChannelResponse: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE;

    SHCreateProcessLocalChannelResponse(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serChannel);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* serChannel();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mSerChannel;
};


class SHSetKVMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_SET_KV;

    SHSetKVMsg(uint64_t tag, const char* key, const char* value, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* key();
    const char* value();
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mKey;
    std::string mValue;
    std::string mFLI;
};

class SHSetKVResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_SET_KV_RESPONSE;

    SHSetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class SHGetKVMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_GET_KV;

    SHGetKVMsg(uint64_t tag, const char* key, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* key();
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mKey;
    std::string mFLI;
};

class SHGetKVResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_GET_KV_RESPONSE;

    SHGetKVResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* value);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* value();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mValue;
};

class DDRegisterClientMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_REGISTER_CLIENT;

    DDRegisterClientMsg(uint64_t tag, const char* respFLI, const char* bufferedRespFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const char* bufferedRespFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
    std::string bFLI;
};

class DDRegisterClientResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_REGISTER_CLIENT_RESPONSE;

    DDRegisterClientResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t clientID, uint64_t numManagers, uint64_t managerID, uint64_t timeout);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t numManagers();
    uint64_t managerID();
    uint64_t timeout();


    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mNumManagers;
    uint64_t mManagerID;
    uint64_t mTimeout;
};

class DDDestroyMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_DESTROY;

    DDDestroyMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDDestroyResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_DESTROY_RESPONSE;

    DDDestroyResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDDestroyManagerMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_DESTROY_MANAGER;

    DDDestroyManagerMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDDestroyManagerResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_DESTROY_MANAGER_RESPONSE;

    DDDestroyManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDRegisterManagerMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_REGISTER_MANAGER;

    DDRegisterManagerMsg(uint64_t tag, const char* mainFLI, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const char* mainFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mMainFLI;
    std::string mRespFLI;
};

class DDRegisterManagerResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_REGISTER_MANAGER_RESPONSE;

    DDRegisterManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDRegisterClientIDMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_REGISTER_CLIENT_ID;

    DDRegisterClientIDMsg(uint64_t tag, uint64_t clientID, const char* respFLI, const char* bufferedRespFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const char* bufferedRespFLI();
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    std::string mRespFLI;
    std::string mBufferedRespFLI;
};

class DDRegisterClientIDResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_REGISTER_CLIENT_ID_RESPONSE;

    DDRegisterClientIDResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDPutMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_PUT;

    DDPutMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
    bool persist = true;
};

class DDPutResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_PUT_RESPONSE;

    DDPutResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDGetMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_GET;

    DDGetMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDGetResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_RESPONSE;

    DDGetResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDPopMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_POP;

    DDPopMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDPopResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_POP_RESPONSE;

    DDPopResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDContainsMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_CONTAINS;

    DDContainsMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDContainsResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_CONTAINS_RESPONSE;

    DDContainsResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDGetLengthMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_GET_LENGTH;

    DDGetLengthMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDGetLengthResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_LENGTH_RESPONSE;

    DDGetLengthResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t length);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t length();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mLength;
};

class DDClearMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_CLEAR;

    DDClearMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDClearResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_CLEAR_RESPONSE;

    DDClearResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDGetIteratorMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_GET_ITERATOR;

    DDGetIteratorMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDGetIteratorResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_ITERATOR_RESPONSE;

    DDGetIteratorResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t iterID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t iterID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mIterID;
};

class DDIteratorNextMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_ITERATOR_NEXT;

    DDIteratorNextMsg(uint64_t tag, uint64_t clientID, uint64_t iterID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t iterID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mIterID;
};

class DDIteratorNextResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_ITERATOR_NEXT_RESPONSE;

    DDIteratorNextResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

dragonError_t
recv_fli_msg(dragonFLIRecvHandleDescr_t* recvh, DragonMsg** msg, const timespec_t* timeout);

const char*
dragon_msg_tc_name(uint64_t tc);

//}

#endif
