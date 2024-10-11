#include "agent.hpp"
#include "cq.hpp"
#include "gateway.hpp"
#include "globals.hpp"
#include "header.hpp"
#include "magic_numbers.hpp"
#include "request.hpp"
#include "tx_queue.hpp"
#include "utils.hpp"
#include <sys/types.h>

// global

Utils hsta_utils;

#ifndef HSTA_NDEBUG
FILE *hsta_dbg_file = stderr;
FILE *hsta_perf_file = nullptr;
thread_local std::unordered_map<void *, uint64_t> access_counts;
#endif

// file-scope

static bool kill_remaining_threads = true;
static Lock exit_lock("graceful exit");

// member function definitions

Lock::Lock(const char *name)
{
    this->name              = name;
    this->has_been_acquired = false;

    auto lock_size = dragon_lock_size(DRAGON_LOCK_FIFO_LITE);

    this->mem = calloc(lock_size, 1ul);
    hsta_dbg_assert(this->mem != nullptr);

    auto dragon_err = dragon_lock_init(&this->dlock, mem, DRAGON_LOCK_FIFO_LITE);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "dragon_lock_init failed");

    // name will be null for lock_of_all_locks
    if (dragon_hsta_debug) {
        if (this->name != nullptr) {
            hsta_lock_acquire(hsta_lock_of_all_locks);
        }
        hsta_all_locks.insert(this);
        if (this->name != nullptr) {
            hsta_lock_release(hsta_lock_of_all_locks);
        }
    }
}

Lock::~Lock()
{
    // there can be a race condition when removing locks from hsta_all_locks,
    // but hsta_all_locks is cleaned up at program exit, so it's fine to just
    // ifdef this out. since this dtor also isn't called until program exit,
    // technically the whole thing is unnecessary
#if 0
    if (dragon_hsta_debug) {
        if (this->name != nullptr) {
            hsta_lock_acquire(hsta_lock_of_all_locks);
        }
        hsta_all_locks.erase(this);
        if (this->name != nullptr) {
            hsta_lock_release(hsta_lock_of_all_locks);
        }
    }
#endif

    auto dragon_err = dragon_lock_destroy(&this->dlock);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "dragon_lock_destroy failed");

    free(this->mem);
}

void Lock::acquire(const char *file, int line)
{
    auto dragon_err = dragon_lock(&this->dlock);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "dragon_lock failed");

    if (dragon_hsta_debug) {
        this->has_been_acquired = true;
        sprintf(this->history, "%s: lock acquired in file %s at line %d", this->name ? this->name : "<no name>", file, line);
    }
}

bool Lock::try_acquire(const char *file, int line)
{
    int got_it;
    auto dragon_err = dragon_try_lock(&this->dlock, &got_it);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "dragon_lock failed");

    if (dragon_hsta_debug && got_it) {
        this->has_been_acquired = true;
        // TODO: this sprintf is causing a segfault during cleanup
        // sprintf(this->history, "%s: lock acquired in file %s at line %d", this->name ? this->name : "<no name>", file, line);
    }

    return (bool)got_it;
}

void Lock::release(const char *file, int line)
{
    if (dragon_hsta_debug) {
        this->has_been_acquired = false;
    }
    auto dragon_err = dragon_unlock(&this->dlock);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "dragon_unlock failed");
}

void Utils::init()
{
#ifndef HSTA_NDEBUG
    auto dbg_str = getenv("DRAGON_HSTA_DEBUG_PREHISTORY");
    if (dbg_str != nullptr) {
        dragon_hsta_debug_prehistory = atoi(dbg_str);
    }

    dbg_str = getenv("DRAGON_HSTA_PERF");
    if (dbg_str != nullptr) {
        dragon_hsta_perf = atoi(dbg_str);
    }

    dbg_str = getenv("DRAGON_HSTA_DATA_VALIDATION");
    if (dbg_str != nullptr) {
        dragon_hsta_data_validation = atoi(dbg_str);
    }

    dbg_str = getenv("DRAGON_HSTA_WORK_STEALING_THRESHOLD");
    if (dbg_str != nullptr) {
        hsta_work_stealing_threshold = atol(dbg_str);
    }

    dbg_str = getenv("DRAGON_HSTA_MAX_EJECTION_MB");
    if (dbg_str != nullptr) {
        dragon_hsta_max_ejection_bytes = atol(dbg_str) * HSTA_NBYTES_IN_MB;
    }

    dbg_str = getenv("DRAGON_HSTA_MAX_GETMSG_MB");
    if (dbg_str != nullptr) {
        dragon_hsta_max_getmsg_bytes = atol(dbg_str) * HSTA_NBYTES_IN_MB;
    }

    if (dragon_hsta_debug) {
        this->init_time = this->get_time();
        this->last_time = this->init_time;
    }
#endif
}

void Utils::fini()
{
#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        fclose(hsta_dbg_file);
    }
#endif
}

void *Utils::open_lib(std::string libname)
{
    return dlopen(libname.c_str(), RTLD_LAZY | RTLD_GLOBAL);
}

void Utils::resolve_symbol(void **ptr_to_func_ptr,
                           void *lib_handle,
                           std::string symbol)
{
    *ptr_to_func_ptr = dlsym(lib_handle, symbol.c_str());
    assert(*ptr_to_func_ptr != nullptr);

    return;
}

#ifndef HSTA_NDEBUG
void Utils::default_case(int64_t value, const char *file, int line)
{
    // TODO: remove all instances of __DATE__ and __TIME__
    if (dragon_hsta_debug) {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file, "[%s:%s] error: bad case in switch statement: value = %ld\n"
                                    "==> in file %s\n"
                                    "==> at line %d\n\n",
                                    __DATE__, __TIME__, value,
                                    file, line);
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);

        hsta_dbg_assert(false);
    }
}

void Utils::log(const char *event)
{
    if (dragon_hsta_debug) {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file, event);
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::log(const char *event, const char *str_param)
{
    if (dragon_hsta_debug) {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file, event, str_param);
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::log(const char *event, int int_param)
{
    if (dragon_hsta_debug) {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file, event, int_param);
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] general debug (time=%lf)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n\n",
                __DATE__, __TIME__,
                this->get_time_from_init(),
                event.c_str(), file, line);
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(CqEvent *cqe,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        char tmp_cstr[2048];

        sprintf(tmp_cstr,
                "%s"
                "[part %d] cqe=(time:%lf, protocol type:%d, header type:%s, remote nid:%d, src_addr:%p, dst_addr:%p, size:%lu, is_aggregated:%d, fabric_request:%p, send_return_mode:%d, eager_buf:%p, work_req = %p, gw_msg = %p, traffic_type:%s)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                "",
                cqe->checkpoint_count,
                Utils::get_time() - cqe->init_time,
                cqe->header->get_protocol_type(),
                cqe->header->type_to_string(),
                cqe->nid,
                cqe->src_addr,
                cqe->dst_addr,
                cqe->size,
                cqe->is_aggregated,
                cqe->fabric_request,
                cqe->header->get_send_return_mode(),
                (void *) cqe->eager_buf,
		        (void *) cqe->work_req,
		        (void *) (cqe->work_req ? cqe->work_req->gw_msg : nullptr),
                hsta_utils.traffic_type_to_string(cqe->traffic_type),
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");

        std::string new_history(tmp_cstr);

        cqe->history += new_history;
        ++cqe->checkpoint_count;

        if (force_print)
        {
            hsta_lock_acquire(this->lock);
            fprintf(hsta_dbg_file, "%s\n", cqe->history.c_str());
            fflush(hsta_dbg_file);
            hsta_lock_release(this->lock);
        }
    }
}

void Utils::dbg_printf(Header *header,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] header=(type:%s, port:%lu, eager_size:%u)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                header->type_to_string(),
                header->port,
                header->eager_size,
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(TxQ *tx_queue,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] tx_queue=(target nid:%d, write_counter:%lu, read_counter:%lu, size:%lu)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                tx_queue->nid,
                tx_queue->write_counter,
                tx_queue->read_counter,
                tx_queue->eager_bufs.size(),
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(Stats& stats,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] stats=(time:%lf, num_complete:%lu, total_reqs:%lu)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                this->get_time_from_init(),
                stats.num_complete,
                stats.total_reqs,
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(GatewayChan *gw_ch,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] gateway channel=(index:%lu)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                gw_ch->dragon_gw_ch._idx,
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(TargetChan *ch,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] target channel=()\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

void Utils::dbg_printf(WorkRequest *work_req,
                       std::string event,
                       bool force_print,
                       const char *file,
                       int line)
{
    if (force_print)
    {
        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file,
                "[%s::%s] work request=(type:%d, addr:%p, size:%lu, nid:%d, port:%lu)\n"
                "==> %s,\n"
                "==> in file %s,\n"
                "==> at line %d\n"
                "%s\n",
                __DATE__, __TIME__,
                work_req->type,
                work_req->rma_iov->get_payload_base(),
                work_req->rma_iov->core.payload_size,
                work_req->nid,
                work_req->port,
                event.c_str(), file, line,
                force_print ? "ASSERTION FAILED\n" : "");
        fflush(hsta_dbg_file);
        hsta_lock_release(this->lock);
    }
}

const char *Utils::traffic_type_to_string(TrafficType traffic_type)
{
    switch (traffic_type)
    {
        case TRAFFIC_TYPE_TX:
        {
            return "TX";
        }
        case TRAFFIC_TYPE_RX:
        {
            return "RX";
        }
        default:
        {
            assert(0);  // TODO: improve this
        }
    }

    return nullptr;
}

void Utils::dump_active_chs(Agent *agent)
{
    if (agent->active_chs.size() > 0) {
        fprintf(hsta_dbg_file, "dump active channels\n");

        for (auto i = 0ul; i < agent->active_chs.size(); ++i) {
            auto *target_ch = agent->active_chs[i];
            fprintf(hsta_dbg_file, "> target_ch: ch vaddr = %p, uid = %lu, num_pending_ch_ops = %lu\n",
                                   (void *) target_ch, target_ch->uid, target_ch->num_pending_ch_ops);
        }

        fprintf(hsta_dbg_file, "\n");
        fflush(hsta_dbg_file);
    }
}

void log_cqes_awaiting_resp(Agent *agent)
{

    for (auto& port_cqes_awaiting_resp: hsta_cqes_awaiting_resp) {
        auto port = port_cqes_awaiting_resp.first;
        auto& cqes_awaiting_resp = port_cqes_awaiting_resp.second;

        if (cqes_awaiting_resp.size() > 0) {
            for (auto i = 0ul; i < cqes_awaiting_resp.size(); ++i) {
                auto cqe = cqes_awaiting_resp[i];
                hsta_log_history(cqe, "still awaiting response from target", false, false);
            }
        }
    }
}

void log_ejection_credits(Agent *agent)
{
    fprintf(hsta_dbg_file, "ejection credits:\n");
    fprintf(hsta_dbg_file, "> pending_ejection_bytes = %lu\n", agent->network.pending_ejection_bytes.load());
    fprintf(hsta_dbg_file, "\n");
}

void Utils::checkpoint_tick()
{
    for (auto *cqe : hsta_my_agent->active_cqes) {
        hsta_log_history(cqe, "tick", false, false);
        cqe->log_active_references();
    }
    log_cqes_awaiting_resp(hsta_my_agent);
    log_ejection_credits(hsta_my_agent);
    this->dump_active_chs(hsta_my_agent);
}

uint64_t Utils::get_access_count(void *obj)
{
    return access_counts[obj];
}

void Utils::inc_access_count(void *obj)
{
    ++access_counts[obj];
}

void Utils::reset_access_count(void *obj)
{
    access_counts[obj] = 0ul;
}

dragonULInt Utils::get_available_msgs(dragonChannelDescr_t *ch_descr)
{
    dragonULInt available_msgs = -1ul;

    // TODO: properly implement this with poll rather than dragon_channel_get_value (which doesn't exist)
#if 0
    dragonULInt *available_msgs_p = nullptr;

    auto dragon_err =
        dragon_channel_get_value(ch_descr, DRAGON_CHANNEL_VALUE_AVAILABLE_MSGS, (void **) &available_msgs_p);

    if (dragon_err != DRAGON_NOT_FOUND) {
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting available_msgs");

        available_msgs = *available_msgs_p;
        free(available_msgs_p);
    }
#endif

    return available_msgs;
}

dragonULInt Utils::get_available_blocks(dragonChannelDescr_t *ch_descr)
{
    dragonULInt available_blocks = -1ul;

    // TODO: properly implement this with poll rather than dragon_channel_get_value (which doesn't exist)
#if 0
    dragonULInt *available_blocks_p = nullptr;

    auto dragon_err =
        dragon_channel_get_value(ch_descr, DRAGON_CHANNEL_VALUE_AVAILABLE_BLOCKS, (void **) &available_blocks_p);

    if (dragon_err != DRAGON_NOT_FOUND) {
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting available_blocks");

        available_blocks = *available_blocks_p;
        free(available_blocks_p);
    }
#endif

    return available_blocks;
}

void Utils::set_signal_handlers(void (*sig_handler)(int))
{
    struct sigaction sa;

    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    // TODO: need better handling of error signals. specifically, we
    // should do the logging aspects of dump_progress_snapshot in graceful_exit
    // and reset signal handlers after the first time we handle an
    // error signal
    std::vector<int> signals_to_catch{
        SIGINT, SIGTERM, SIGFPE, SIGHUP, SIGBUS, SIGILL, SIGPIPE, SIGSTOP, SIGQUIT//, SIGSEGV
    };

    for (auto signal: signals_to_catch) {
        sigaction(signal,  &sa, nullptr);
    }
}

// NOTE: this is always called from the agent threads
void Utils::graceful_exit(int sig)
{
    if (sig != SIGTERM) {
        abort();
    }

    hsta_lock_acquire(exit_lock);

    if (kill_remaining_threads) {
        for (auto i = 0; i < hsta_num_threads; ++i) {
            if (i != hsta_thread_idx) {
                auto t = hsta_agent_threads[i];
                pthread_kill(t, sig);
            }
        }
        kill_remaining_threads = false;
        hsta_fly_you_fools = true;
    }

    hsta_lock_release(exit_lock);
}

void log_unreleased_locks()
{
    auto num_unreleased_locks = 0ul;

    for (auto lock: hsta_all_locks) {
        if (lock->check_acquired()) {
            ++num_unreleased_locks;
        }
    }

    if (num_unreleased_locks > 0) {
        fprintf(hsta_dbg_file, "unreleased locks:\n");
        for (auto lock: hsta_all_locks) {
            if (lock->check_acquired()) {
                fprintf(hsta_dbg_file, "> %s\n", lock->get_history());
            }
        }
        fprintf(hsta_dbg_file, "\n");
        fflush(hsta_dbg_file);
    }
}

void log_total_rndv_bytes(Agent *agent)
{
    // TODO: make this part of the performance analysis done in data.cpp
    fprintf(hsta_dbg_file, "total rendezvous bytes:\n");
    fprintf(hsta_dbg_file, "> agent %d: %lu\n", agent->idx, agent->network.total_rndv_bytes);
    fprintf(hsta_dbg_file, "\n");
    fflush(hsta_dbg_file);
}

void Utils::dump_progress_snapshot()
{
    if (dragon_hsta_debug) {
        auto agent = hsta_my_agent;

        // print back trace and some stats, then dump the history of any active cqes

        auto total_reqs =   agent->tx_stats.total_reqs
                                        + agent->getmsg_stats.total_reqs
                                        + agent->poll_stats.total_reqs
                                        + agent->err_stats.total_reqs;

        // TODO: create functions Utils::get_[active,total]_work_reqs()

        auto stats_active = std::string("active work requests:\n")
                    + "> total   => " + std::to_string(agent->num_pending_work_reqs)                                      + "\n"
                    + "> sendmsg => " + std::to_string(agent->tx_stats.total_reqs     - agent->tx_stats.num_complete)     + "\n"
                    + "> getmsg  => " + std::to_string(agent->getmsg_stats.total_reqs - agent->getmsg_stats.num_complete) + "\n"
                    + "> poll    => " + std::to_string(agent->poll_stats.total_reqs   - agent->poll_stats.num_complete)   + "\n"
                    + "> err     => " + std::to_string(agent->err_stats.total_reqs    - agent->err_stats.num_complete)    + "\n";

        auto stats_all = std::string("all work requests:\n")
                    + "> total   => " + std::to_string(total_reqs)                     + "\n"
                    + "> sendmsg => " + std::to_string(agent->tx_stats.total_reqs)     + "\n"
                    + "> getmsg  => " + std::to_string(agent->getmsg_stats.total_reqs) + "\n"
                    + "> poll    => " + std::to_string(agent->poll_stats.total_reqs)   + "\n"
                    + "> err     => " + std::to_string(agent->err_stats.total_reqs)    + "\n";

        auto stats_rxq = std::string("rx queue info:\n")
                    + "> original size => " + std::to_string(agent->init_rx_queue_size())         + "\n"
                    + "> final size    => " + std::to_string(agent->network.num_pending_eager_rx) + "\n";

        auto& rndv_recv_workq = agent->network.get_rndv_recv_workq();

        hsta_lock_acquire(this->lock);
        fprintf(hsta_dbg_file, "%s\n", stats_active.c_str());
        fprintf(hsta_dbg_file, "%s\n", stats_all.c_str());
        fprintf(hsta_dbg_file, "%s\n", stats_rxq.c_str());

        fprintf(hsta_dbg_file, "number of active cqes: %lu\n", agent->active_cqes.size());
        fprintf(hsta_dbg_file, "number of recv work queue items: %lu\n\n", rndv_recv_workq.size());

        fprintf(hsta_dbg_file, "%s\n\n", hsta_utils.get_backtrace().c_str());

        fflush(hsta_dbg_file);

        log_total_rndv_bytes(agent);
        log_cqes_awaiting_resp(agent);
        log_ejection_credits(agent);
        this->dump_active_chs(agent);
        log_unreleased_locks();
        hsta_lock_release(this->lock);

        // check if any cqes are stuck on a rndv_recv_workq
        for (auto i = 0ul; i < rndv_recv_workq.size(); ++i) {
            auto cqe = rndv_recv_workq[i];
            hsta_log_history(cqe, "stuck on rndv_recv_workq", false, false);
        }

        // log the history of any active cqes
        for (auto cqe: agent->active_cqes) {
            cqe->log_active_references();
            hsta_log_history(cqe, "dumping progress snapshot", true, false);
        }

        if (dragon_hsta_perf) {
            auto ds_work_reqs =
                DataSet<uint64_t>(std::string("work request load imbalance"), agent->total_work_reqs);

            if (agent->network.rank == 0) {
                ds_work_reqs.analyze_and_log();
            }

            auto ds_ch_ops =
                DataSet<uint64_t>(std::string("channel operations load imbalance"), agent->total_ch_ops);

            if (agent->network.rank == 0) {
                ds_ch_ops.analyze_and_log();
            }
        }
    }
}
#endif
