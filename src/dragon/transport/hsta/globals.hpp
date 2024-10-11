#ifndef GLOBAL_VARS_HPP
#define GLOBAL_VARS_HPP

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include <set>
#include <unordered_map>
#include <vector>

// forward declarations

class CqEvent;
class Lock;
class RmaIov;
enum ProtocolType : uint32_t;

// enums

enum TrafficType
{
    TRAFFIC_TYPE_TX = 0,
    TRAFFIC_TYPE_RX,
    TRAFFIC_TYPE_LAST,
};

// typedefs

using eager_size_t = uint16_t;
using port_t       = uint64_t;
using Rkey         = uint64_t;

// global variables

extern thread_local int hsta_thread_idx;
extern int hsta_num_threads;
extern std::vector<pthread_t> hsta_agent_threads;
extern Lock hsta_mr_lock;
extern pthread_barrier_t hsta_thread_barrier;
extern Lock hsta_lock_of_all_locks;
extern std::unordered_set<Lock *> hsta_all_locks;
extern bool hsta_clean_exit;
extern bool hsta_fly_you_fools;
extern bool hsta_dump_net_config;
extern uint64_t hsta_work_stealing_threshold;
extern uint64_t dragon_hsta_max_ejection_bytes;
extern uint64_t dragon_hsta_max_getmsg_bytes;
extern std::unordered_map<dragonULInt, int> hsta_hostid_to_nid;
extern std::unordered_map<dragonULInt, RmaIov *> hsta_rma_iov;
extern thread_local std::vector<bool> hsta_ep_addr_ready_tl;

#ifndef HSTA_NDEBUG
extern FILE *hsta_dbg_file;
extern FILE *hsta_perf_file;
extern int   dragon_hsta_debug;
extern int   dragon_hsta_debug_prehistory;
extern int   dragon_hsta_perf;
extern int   dragon_hsta_data_validation;

// TODO: improve these names, and maybe just use a hash to create a simpler key
using SeqnumMap         = std::unordered_map<uint64_t, uint64_t>;
using SeqnumAtEventMap  = std::unordered_map<std::string, uint64_t>;
using HistoryAtEventMap = std::unordered_map<std::string, std::string>;

extern thread_local SeqnumMap         hsta_cqe_seqnum_ctr;
extern thread_local SeqnumAtEventMap  hsta_cqe_seqnum_at_event;
extern thread_local HistoryAtEventMap hsta_cqe_history_at_event;

extern thread_local std::unordered_map<void *, uint64_t> access_counts;
#endif

#endif  // GLOBAL_VARS_HPP
