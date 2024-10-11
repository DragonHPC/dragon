#ifndef REQUEST_HPP
#define REQUEST_HPP

#include "extras.hpp"
#include "utils.hpp"
#include <cstdint>

// forward declarations

class Agent;
class CqEvent;

// TODO: rename ReqType to CompletionType?
enum ReqType {
    WORK_REQ_TYPE_SENDMSG = 0,
    WORK_REQ_TYPE_GETMSG,
    WORK_REQ_TYPE_POLL,
    WORK_REQ_TYPE_ERR,
    WORK_REQ_TYPE_LAST
};

enum ProtocolType : uint32_t {
    PROTOCOL_TYPE_SENDMSG = 0,
    PROTOCOL_TYPE_GETMSG,
    PROTOCOL_TYPE_POLL,
    PROTOCOL_TYPE_AGGREGATED,
    PROTOCOL_TYPE_LAST
};

class EventInfo {
public:

    dragonError_t err;
    short mask;
    dragonULInt result;
};

class WorkRequest {
public:

    // data

    Agent *agent;
    ReqType type;
    ProtocolType protocol_type;
    RmaIov *rma_iov;
    dragonMemoryDescr_t dragon_msg_buf;
    dragonMessage_t dragon_msg;
    bool free_dragon_msg_buf;
    int nid;
    dragonULInt c_uid;
    port_t port;
    std::vector<uint8_t> conn_data;
    TargetChan *target_ch;
    CqEvent *cqe_getmsg_resp;
    EventInfo event_info;
    dragonGatewayMessage_t *gw_msg;
    GatewayChan *gw_ch;
    uint64_t timeout;
    timespec_t completion_deadline;
    bool is_response;
    bool is_complete;
    uint64_t clientid;
    uint64_t hints;
    // used for debugging
    uint64_t seqnum;
    uint64_t checksum;
    uint64_t cuid;
    bool already_completed;
    std::string completion_backtrace;

    // functions

    void send_complete(CqEvent *cqe);
    void get_msg_complete(CqEvent *cqe);
    void poll_complete(CqEvent *cqe);
    void err_complete(CqEvent *cqe);
    void complete(CqEvent *cqe);
    bool check_gw_complete();

    void init(Agent *agent,
              ProtocolType protocol_type,
              ReqType type,
              RmaIov *rma_iov,
              int nid,
              Header *header,
              dragonGatewayMessage_t *gw_msg,
              GatewayChan *gw_ch,
              bool is_complete);

    void fini() {}
};

// free functions

DECL_OBJQ_RING_9(
    WorkRequest,
    Agent *,
    ProtocolType,
    ReqType,
    RmaIov *,
    int,
    Header *,
    dragonGatewayMessage_t *,
    GatewayChan *,
    bool
)

#endif // REQUEST_HPP
