#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include <thread>

#include "agent.hpp"
#include "dragon/channels.h"
#include "extras.hpp"
#include "globals.hpp"
#include "json.hpp"
#include "network.hpp"
#include "parse_args.hpp"
#include "signal.h"
#include "utils.hpp"

using json = nlohmann::json;

// global variables

thread_local Agent *hsta_my_agent = nullptr;
thread_local int hsta_thread_idx = HSTA_INVALID_INT32;
int hsta_num_threads = HSTA_INVALID_INT32;
std::vector<pthread_t> hsta_agent_threads;
pthread_barrier_t hsta_thread_barrier;
Lock hsta_lock_of_all_locks = Lock(nullptr);
std::unordered_set<Lock *> hsta_all_locks;
bool hsta_clean_exit = true;
bool hsta_fly_you_fools = false;
bool hsta_dump_net_config = false;
std::vector<Agent *> hsta_agent;

#ifndef HSTA_NDEBUG
int dragon_hsta_debug = 0;
int dragon_hsta_debug_prehistory = 0;
int dragon_hsta_perf = 0;
int dragon_hsta_data_validation = 0;
#endif

// main functions

static void
hsta_signal_handler(int sig)
{
#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug)
    {
        hsta_utils.log("\nhandling signal: %s\n\n", strsignal(sig));
    }
#endif // !HSTA_NDEBUG

#ifndef HSTA_NDEBUG
    hsta_utils.graceful_exit(sig);
#endif // !HSTA_NDEBUG
}

void *thread_main(void *data)
{
    auto my_idx = *(int *)data;
    hsta_my_agent = hsta_agent[my_idx];
    hsta_thread_idx = my_idx;

    // dragon error strings add ~30us of latency, so we disable
    // them outside of debug mode
    if (!dragon_hsta_debug) {
        dragon_enable_errstr(false);
    }

    // make sure to enable thread local mode before initializing
    // this thread's agent
    dragon_set_thread_local_mode(true);

    hsta_my_agent->init();

    // init thread-local globals

    hsta_ep_addr_ready_tl.resize(hsta_my_agent->network.num_ranks);

    // print info about nic selection now (this must be done after
    // dfabric has been initialized)
    if (dragon_hsta_debug) {
        auto rc = pthread_barrier_wait(&hsta_thread_barrier);
        hsta_dbg_assert_with_rc(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD, rc);

        if (my_idx == 0) {
            hsta_my_agent->network.log_nic_info();
        }
    }

    // exit early if the user only wants us to dump the network config
    if (hsta_dump_net_config) {
        return nullptr;
    }

    // start endpoint address setup progress thread

    pthread_t ep_progress_thread;
    pthread_attr_t th_attr;

    auto rc = pthread_attr_init(&th_attr);
    hsta_dbg_assert_with_rc(rc == 0, rc);

    rc = pthread_attr_setdetachstate(&th_attr, PTHREAD_CREATE_DETACHED);
    hsta_dbg_assert_with_rc(rc == 0, rc);

    rc = pthread_create(
        &ep_progress_thread,
        &th_attr,
        ep_addr_setup_progress,
        hsta_my_agent
    );
    hsta_dbg_assert_with_rc(rc == 0, rc);

    // main progress loop

    auto start_time_sleep = hsta_utils.get_time();
    auto timeout_sleep = (double) 1.0;
    auto must_reset_start_time_sleep = false;

    while (true) {
        hsta_my_agent->progress(true);

        if (hsta_fly_you_fools) {
            auto rc = pthread_barrier_wait(&hsta_thread_barrier);
            hsta_dbg_assert_with_rc(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD, rc);

            break;
        }

        if (hsta_my_agent->nothing_to_do()) {
            if (must_reset_start_time_sleep) {
                start_time_sleep = hsta_utils.get_time();
                must_reset_start_time_sleep = false;
            }

            if (hsta_utils.get_time() - start_time_sleep > timeout_sleep) {
                // cat-nap until there's work to do
                usleep(HSTA_CAT_NAP_TIME);
            }
        } else {
            must_reset_start_time_sleep = true;
        }
    }

    hsta_utils.dump_progress_snapshot();

    // quiesce all network operations and finalize the fabric backend
    hsta_my_agent->quiesce();
    hsta_my_agent->network.finalize();

    return nullptr;
}

int main(int argc, char **argv)
{
    // wait for gdb to attach if requested
    auto enale_gdb_str = getenv("DRAGON_HSTA_ENABLE_GDB");
    if (enale_gdb_str != nullptr) {
        sleep(HSTA_WAIT_TIME_FOR_DEBUGGER_ATTACH);
    }

    hsta_utils.init();
    hsta_utils.set_signal_handlers(hsta_signal_handler);

    // get arguments passed by the user
    auto args = ParseArgs(argc, argv);

    hsta_dump_net_config = args.get_dump_net_config();
    hsta_num_threads     = args.get_num_threads();

    hsta_dbg_assert(0 < hsta_num_threads && hsta_num_threads <= HSTA_MAX_NUM_NICS);

    // get startup info before creating the agents

    if (!hsta_dump_net_config) {
        Network::get_startup_info();
    }

    // create agents

    hsta_agent.resize(hsta_num_threads);
    for (auto i = 0; i < hsta_num_threads; ++i) {
        hsta_agent[i] = new Agent(i);
    }

    hsta_agent_threads.resize(hsta_num_threads);
    hsta_thread_idx = HSTA_MAIN_THREAD_IDX;

    // start agent threads

    auto rc = pthread_barrier_init(
        &hsta_thread_barrier,
        nullptr,
        hsta_num_threads
    );
    hsta_dbg_assert_with_rc(rc == 0, rc);

    // this vector exists solely because pthread_create requires a
    // void pointer for the function argument and we can't use &i
    std::vector<int> thread_idx(hsta_num_threads);

    for (auto i = 0; i < hsta_num_threads; ++i) {
        thread_idx[i] = i;

        rc = pthread_create(
            &hsta_agent_threads[i],
            nullptr,
            thread_main,
            (void *) &thread_idx[i]
        );
        hsta_dbg_assert_with_rc(rc == 0, rc);
    }

    for (auto t : hsta_agent_threads) {
        rc = pthread_join(t, nullptr);
        hsta_dbg_assert_with_rc(rc == 0, rc);
    }

    // dump network config if requested
    if (hsta_dump_net_config) {
        json net_config;
        for (auto agent: hsta_agent) {
            auto ep_addrs_available = agent->network.get_ep_addrs_available();
            net_config["fabric_ep_addrs_available"] = ep_addrs_available;

            if (ep_addrs_available) {
                auto ep_addr_len = agent->network.get_ep_addr_len();
                auto tmp_ep_addr = agent->network.get_ep_addr();
                auto encoded_ep_addr = dragon_base64_encode((uint8_t *) tmp_ep_addr, ep_addr_len);

                hsta_dbg_assert(encoded_ep_addr != nullptr);

                net_config["fabric_ep_addrs"].push_back(encoded_ep_addr);
                net_config["fabric_ep_addr_lens"].push_back(ep_addr_len);

                free(encoded_ep_addr);
            }
        }

        fprintf(stdout, "network config=%s\n", net_config.dump().c_str());
        fflush(stdout);
    }

    hsta_utils.fini();

    for (auto i = 0; i < hsta_num_threads; ++i) {
        delete hsta_agent[i];
    }

    return EXIT_SUCCESS;
}
