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
    const char* get_tb() const;

    private:
    dragonError_t mErr;
    std::string mErrStr;
    std::string mTraceback;
};

std::ostream& operator<<(std::ostream& os, const DragonError& obj);

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

class SHCreateProcessLocalChannelMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_CHANNEL;

    SHCreateProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t muid, uint64_t blockSize, uint64_t capacity, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const uint64_t puid();
    const uint64_t muid();
    const uint64_t blockSize();
    const uint64_t capacity();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    uint64_t mMUID;
    uint64_t mBlockSize;
    uint64_t mCapacity;
    std::string mFLI;
};

class SHCreateProcessLocalChannelResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE;

    SHCreateProcessLocalChannelResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serChannel);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* serChannel();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mSerChannel;
};

class SHDestroyProcessLocalChannelMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_DESTROY_PROCESS_LOCAL_CHANNEL;

    SHDestroyProcessLocalChannelMsg(uint64_t tag, uint64_t puid, uint64_t cuid, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const uint64_t puid();
    const uint64_t cuid();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    uint64_t mCUID;
    std::string mFLI;
};

class SHDestroyProcessLocalChannelResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_DESTROY_PROCESS_LOCAL_CHANNEL_RESPONSE;

    SHDestroyProcessLocalChannelResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class SHCreateProcessLocalPoolMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_POOL;

    SHCreateProcessLocalPoolMsg(uint64_t tag, uint64_t puid, uint64_t size, uint64_t minBlockSize, const char* name, const size_t* preAllocs, const size_t numPreAllocs, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const uint64_t puid();
    const char* name();
    const uint64_t minBlockSize();
    const uint64_t size();
    const size_t preAlloc(int idx);
    const size_t numPreAllocs();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    std::string mRespFLI;
    uint64_t mSize;
    uint64_t mMinBlockSize;
    std::string mName;
    std::vector<size_t> mPreAllocs;
};

class SHCreateProcessLocalPoolResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_CREATE_PROCESS_LOCAL_POOL_RESPONSE;

    SHCreateProcessLocalPoolResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* serPool);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* serPool();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mSerPool;
};

class SHRegisterProcessLocalPoolMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_REGISTER_PROCESS_LOCAL_POOL;

    SHRegisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t puid();
    const char* respFLI();
    const char* serPool();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    std::string mSerPool;
    std::string mRespFLI;
};

class SHRegisterProcessLocalPoolResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_REGISTER_PROCESS_LOCAL_POOL_RESPONSE;

    SHRegisterProcessLocalPoolResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class SHDeregisterProcessLocalPoolMsg: public DragonMsg {
    public:
    static const MessageType TC = SH_DEREGISTER_PROCESS_LOCAL_POOL;

    SHDeregisterProcessLocalPoolMsg(uint64_t tag, uint64_t puid, const char* serPool, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t puid();
    const char* respFLI();
    const char* serPool();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mPUID;
    std::string mSerPool;
    std::string mRespFLI;
};

class SHDeregisterProcessLocalPoolResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = SH_DEREGISTER_PROCESS_LOCAL_POOL_RESPONSE;

    SHDeregisterProcessLocalPoolResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
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
    std::string mBufferedFLI;
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
    const vector<std::string>& managerNodes(); // returns a reference to the internal vector. Don't destroy the message and try to continue to using the vector after destroying.

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mNumManagers;
    uint64_t mManagerID;
    uint64_t mTimeout;
    vector<std::string> mManagerNodes;
};

class DDDeregisterClientMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_DEREGISTER_CLIENT;

    DDDeregisterClientMsg(uint64_t tag, uint64_t clientID, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    std::string mFLI;
};

class DDDeregisterClientResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_DEREGISTER_CLIENT_RESPONSE;

    DDDeregisterClientResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

// The message is not in use for now.
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

// The message is not in use for now.
class DDDestroyResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_DESTROY_RESPONSE;

    DDDestroyResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

// The message is not in use for now.
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

// The message is not in use for now.
class DDDestroyManagerResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_DESTROY_MANAGER_RESPONSE;

    DDDestroyManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

// The message is not in use for now.
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

// The message is not in use for now.
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

class DDConnectToManagerMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_CONNECT_TO_MANAGER;

    DDConnectToManagerMsg(uint64_t tag, uint64_t client_id, uint64_t manager_id);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t managerID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mManagerID;
};

class DDConnectToManagerResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_CONNECT_TO_MANAGER_RESPONSE;

    DDConnectToManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* manager_fli);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* managerFLI();

    private:
    std::string mManagerFLI;
};

class DDRandomManagerMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_RANDOM_MANAGER;

    DDRandomManagerMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mRespFLI;
};

class DDRandomManagerResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_RANDOM_MANAGER_RESPONSE;

    DDRandomManagerResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* manager_fli, uint64_t managerID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* managerFLI();
    uint64_t managerID();

    private:
    std::string mManagerFLI;
    uint64_t mManagerID;
};

class DDPutMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_PUT;

    DDPutMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, bool persist);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t chkptID();
    bool persist();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mChkptID;
    bool mPersist;
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

    DDGetMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t chkptID();
    const unsigned char* key();
    size_t keyLen();


    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mChkptID;
    const unsigned char* mKey;
    const size_t mKeyLen;
};

class DDGetResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_RESPONSE;

    DDGetResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool freeMem);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    bool freeMem();

    private:
    bool mFreeMem;
};

class DDPopMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_POP;

    DDPopMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t chkptID();
    const unsigned char* key();
    size_t keyLen();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mChkptID;
    const unsigned char* mKey;
    const size_t mKeyLen;
};

class DDPopResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_POP_RESPONSE;

    DDPopResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, bool freeMem);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    bool freeMem();

    private:
    uint64_t mFreeMem;

};

class DDContainsMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_CONTAINS;

    DDContainsMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const unsigned char* key, size_t keyLen);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t chkptID();
    const unsigned char* key();
    size_t keyLen();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mChkptID;
    const unsigned char* mKey;
    const size_t mKeyLen;
};

class DDContainsResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_CONTAINS_RESPONSE;

    DDContainsResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDLengthMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_LENGTH;

    DDLengthMsg(uint64_t tag, uint64_t clientID, const char* respFLI, uint64_t chkptID, bool broadcast);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    const char* respFLI();
    uint64_t chkptID();
    bool broadcast();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    std::string mRespFLI;
    uint64_t mChkptID;
    bool mBroadcast;
};

class DDLengthResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_LENGTH_RESPONSE;

    DDLengthResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t length);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t length();

    private:
    uint64_t mLength;
};

class DDKeysMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_KEYS;

    DDKeysMsg(uint64_t tag, uint64_t clientID, uint64_t chkptID, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    uint64_t chkptID();
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t mChkptID;
    std::string mFLI;
};

class DDKeysResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_KEYS_RESPONSE;

    DDKeysResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDClearMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_CLEAR;

    DDClearMsg(uint64_t tag, uint64_t clientID, const char* respFLI, uint64_t chkptID, bool broadcast);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();
    const char* respFLI();
    uint64_t chkptID();
    bool broadcast();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    std::string mFLI;
    uint64_t mChkptID;
    bool mBroadcast;
};

class DDClearResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_CLEAR_RESPONSE;

    DDClearResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};
class DDManagerNewestChkptIDMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_MANAGER_NEWEST_CHKPT_ID;

    DDManagerNewestChkptIDMsg(uint64_t tag, const char* respFLI, bool broadcast);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    bool broadcast();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
    bool mBroadcast;
};

class DDManagerNewestChkptIDResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_MANAGER_NEWEST_CHKPT_ID_RESPONSE;

    DDManagerNewestChkptIDResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t managerID, uint64_t chkptID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t managerID();
    uint64_t chkptID();

    private:
    uint64_t mManagerID;
    uint64_t mChkptID;
};

class DDEmptyManagersMsg:  public DragonMsg {
    public:
    static const MessageType TC = DD_EMPTY_MANAGERS;

    DDEmptyManagersMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDEmptyManagersResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_EMPTY_MANAGERS_RESPONSE;

    DDEmptyManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const vector<uint64_t>& managers();

    private:
    vector<uint64_t> mManagers;
};

class DDGetManagersMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_GET_MANAGERS;

    DDGetManagersMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDGetManagersResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_MANAGERS_RESPONSE;

    DDGetManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const std::vector<bool>& emptyManagers();
    const std::vector<std::string>& managers();

    private:
    std::vector<bool> mEmptyManagers;
    std::vector<std::string> mManagers;
};

class DDManagerSyncMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_MANAGER_SYNC;

    DDManagerSyncMsg(uint64_t tag, const char* respFLI, const char * emptyManagerFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const char* emptyManagerFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
    std::string mEmptyManagerFLI;
};

class DDManagerSyncResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_MANAGER_SYNC_RESPONSE;

    DDManagerSyncResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDUnmarkDrainedManagersMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_UNMARK_DRAINED_MANAGERS;

    DDUnmarkDrainedManagersMsg(uint64_t tag, const char* respFLI, std::vector<uint64_t>& managerIDs);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const std::vector<uint64_t>& managerIDs();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
    std::vector<uint64_t> mManagerIDs;
};

class DDUnmarkDrainedManagersResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_UNMARK_DRAINED_MANAGERS_RESPONSE;

    DDUnmarkDrainedManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDMarkDrainedManagersMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_MARK_DRAINED_MANAGERS;

    DDMarkDrainedManagersMsg(uint64_t tag, const char* respFLI, std::vector<uint64_t>& managerIDs);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();
    const std::vector<uint64_t>& managerIDs();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
    std::vector<uint64_t> mManagerIDs;
};

class DDMarkDrainedManagersResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_MARK_DRAINED_MANAGERS_RESPONSE;

    DDMarkDrainedManagersResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
};

class DDGetMetaDataMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_GET_META_DATA;

    DDGetMetaDataMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDGetMetaDataResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_GET_META_DATA_RESPONSE;

    DDGetMetaDataResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, const char* respFLI, const uint64_t numManagers);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char * serializedDdict();
    const uint64_t numManagers();

    private:
    std::string mSerializedDdict;
    uint64_t mNumManagers;
};

class DDManagerNodesMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_MANAGER_NODES;

    DDManagerNodesMsg(uint64_t tag, const char* respFLI);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const char* respFLI();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    std::string mFLI;
};

class DDManagerNodesResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_MANAGER_NODES_RESPONSE;

    DDManagerNodesResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    const vector<uint64_t>& huids();

    private:
    vector<uint64_t> mHuids;
};

class DDIteratorMsg: public DragonMsg {
    public:
    static const MessageType TC = DD_ITERATOR;

    DDIteratorMsg(uint64_t tag, uint64_t clientID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t clientID();

    protected:
    virtual void builder(MessageDef::Builder& msg);

    private:
    uint64_t mClientID;
    uint64_t chkptID = 0;
};

class DDIteratorResponseMsg: public DragonResponseMsg {
    public:
    static const MessageType TC = DD_ITERATOR_RESPONSE;

    DDIteratorResponseMsg(uint64_t tag, uint64_t ref, dragonError_t err, const char* errInfo, uint64_t iterID);
    static dragonError_t deserialize(MessageDef::Reader& reader, DragonMsg** msg);
    uint64_t iterID();

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
