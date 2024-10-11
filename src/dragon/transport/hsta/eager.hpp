#ifndef EAGER_HPP
#define EAGER_HPP

#include "extras.hpp"
#include "cq.hpp"
#include "globals.hpp"
#include "header.hpp"

// forward declarations

class Agent;
class MemDescr;

// class definitions

class AgHeader
{
public:

    uint16_t nid;
    uint16_t size;
};

class EagerBuf
{
private:

    int refcount;

public:

    // data

    uint64_t uid;
    static uint64_t uid_counter;
    Agent *agent;
    AgHeader *ag_header;
    CqEvent *cqe_ag;
    ObjectRing<CqEvent *> stashed_cqes;
    void *header_buf;
    Header tmp_header;
    int fill_count;
    int spin_count;
    bool no_space;
    // NOTE: Multi-nic support means there's no 1:1 correspondence between
    //       queue_index and target_nid. Need to make sure we don't implicitly
    //       assume such a correspondence anywhere.
    int target_nid;
    int queue_index;
    TrafficType traffic_type;
    char *payload;
    MemDescr *mem_descr;

    // functions

    Header *get_next_header(void **eager_data, eager_size_t *eager_size);
    void add_to_payload(Header *header, void *eager_data, size_t eager_size);
    void insert(CqEvent *cqe);
    void inc_refcount(int ref);
    void dec_refcount(int ref);
    void set_refcount(int updated_refcount);
    int get_refcount();
    bool refcount_is_zero();
    void init(Agent *agent, TrafficType traffic_type, int queue_index);
    void fini();

    // TODO: might be able to get rid of some setup work in agent init
    void reset()
    {
        this->ag_header->size = sizeof(AgHeader);
        this->fill_count      = 0;
        this->spin_count      = 0;
        this->no_space        = false;
        this->header_buf      = (void *)((AgHeader *)this->payload + 1);
    }

    bool ready_to_send()
    {
        ++this->spin_count;

        if (   this->fill_count >= HSTA_EAGER_BUF_FILL_COUNT_MIN
            || this->spin_count >= HSTA_EAGER_BUF_SPIN_COUNT_MIN
            || this->no_space)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
};

DECL_OBJQ_RING_3(EagerBuf,
                 Agent *,
                 TrafficType,
                 int)

#endif // EAGER_HPP
