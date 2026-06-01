#ifndef _MESSAGES_API_H_
#define _MESSAGES_API_H_

#include <dragon/fli.h>
#include <dragon/return_codes.h>
#include <dragon/utils.h>
#include <dragon/message_tcs.hpp>
#ifdef __cplusplus
#include <string>
#include <vector>

namespace dragon {

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

}
#else
typedef struct DragonMsg DragonMsg;
typedef struct DragonResponseMsg DragonResponseMsg;
#endif

#ifdef __cplusplus
extern "C" {
using namespace dragon;
#endif

dragonError_t recv_fli_msg(dragonFLIRecvHandleDescr_t* recvh, DragonMsg** msg, const timespec_t* timeout);
dragonError_t dragon_sh_send_receive(DragonMsg* req_msg, DragonResponseMsg** resp_msg, enum MessageType expected_msg_type, dragonFLIDescr_t* return_fli, const timespec_t* timeout);
dragonError_t dragon_get_return_sh_fli(dragonFLIDescr_t* return_fli);
dragonError_t dragon_fli_send_recv_capnp(DragonMsg* msg_out, DragonResponseMsg** resp_msg, enum MessageType expected_msg_type, dragonFLIDescr_t* fli_out,
                                         dragonFLIDescr_t* fli_resp, const timespec_t* timeout, bool is_connection);
const char* dragon_msg_tc_name(uint64_t tc);

#ifdef __cplusplus
}
#endif // extern "C"


#endif // _MESSAGES_API_H_