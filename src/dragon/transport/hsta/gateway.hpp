#ifndef GATEWAY_HPP
#define GATEWAY_HPP

#include "extras.hpp"
#include "channel.hpp"

// forward declarations

class Agent;

// class definitions

class GatewayChan
{
private:

    int max_batch_size;

public:

    // data

    Agent *agent;
    dragonChannelDescr_t dragon_gw_ch;
    dragonChannelRecvh_t dragon_gw_recvh;

    // functions

    WorkRequest *create_work_req(dragonGatewayMessage_t *gw_msg);
    bool check_gateway();
    void check_gw_msgs();
    void init(Agent *agent, char *gw_descr_str);
    void fini();
};

DECL_OBJQ_RING_2(GatewayChan, Agent *, char *)

#endif // GATEWAY_HPP
