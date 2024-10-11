#ifndef CHANNEL_HPP
#define CHANNEL_HPP

#include "dragon/global_types.h"
#include "extras.hpp"
#include "obj_ring.hpp"
#include "request.hpp"
#include "utils.hpp"
#include <cstdint>

// extern decls

extern std::unordered_map<port_t, ObjectRing<CqEvent *>> hsta_cqes_awaiting_resp;

// forward declarations

class Agent;
class CqEvent;
class WorkRequest;

class TargetChan {
public:

    static uint64_t uid_counter;
    uint64_t uid;
    dragonULInt c_uid;
    Agent *agent;
    ObjectRing<CqEvent *> pending_ch_ops[1 + PROTOCOL_TYPE_LAST];
    uint64_t num_pending_ch_ops;
    dragonChannelDescr_t dragon_ch;
    dragonMessageAttr_t mattrs;
    bool is_local;
    std::unordered_map<port_t, dragonChannelSendh_t> dragon_send_handles;
    dragonChannelRecvh_t dragon_recvh;
    dragonMemoryPoolDescr_t dragon_mem_pool;
    std::unordered_map<port_t, uint64_t> seqnum;

    void init(
        Agent *agent,
        dragonULInt c_uid,
        dragonGatewayMessage_t *gw_msg,
        uint8_t *conn_data,
        size_t conn_data_size,
        bool is_local);
    void fini();
    void init_flow(port_t port, dragonChannelSendReturnWhen_t send_return_mode);
    bool try_send(CqEvent *cqe);
    void send_to_channel(CqEvent **cqe_inout);
    bool try_recv(CqEvent *cqe);
    void recv_from_channel(CqEvent **cqe_inout);
    bool try_poll(CqEvent *cqe);
    void poll_channel(CqEvent **cqe_inout);
    bool try_op(CqEvent *cqe);
    uint64_t process_pending_ch_ops(ProtocolType protocol_type);
    bool is_active();
};

DECL_OBJQ_RING_6(
    TargetChan,
    Agent *,
    dragonULInt,
    dragonGatewayMessage_t *,
    uint8_t *,
    size_t,
    bool
)

#endif // CHANNEL_HPP
