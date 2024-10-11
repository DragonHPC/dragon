#ifndef UTILS_HPP
#define UTILS_HPP

// utility functions

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <dlfcn.h>
#include <execinfo.h>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "bit_vector.hpp"
#include "build_constants.hpp"
#include "dyn_ring_buf.hpp"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "obj_queue.hpp"
#include "ugly_macros.hpp"

// macros to help printing to log file

#ifndef HSTA_NDEBUG

#define hsta_make_str(x) #x

#define hsta_log_history(cqe, event, force_print, order_checking)                  \
do {                                                                               \
    if (dragon_hsta_debug) {                                                       \
        (cqe)->log_history(event, force_print, __FILE__, __LINE__);                \
        if (order_checking) {                                                      \
            (cqe)->check_ordering_violation(event);                                \
        }                                                                          \
    }                                                                              \
} while (0)

#define hsta_default_case(value)                                               \
do {                                                                           \
    if (dragon_hsta_debug)                                                     \
    {                                                                          \
        hsta_utils.default_case(value, __FILE__, __LINE__);                    \
    }                                                                          \
} while (0)

#define hsta_dbg_printf(object, event)                                         \
do {                                                                           \
    if (dragon_hsta_debug)                                                     \
    {                                                                          \
        hsta_utils.dbg_printf(object, event, false, __FILE__, __LINE__);       \
    }                                                                          \
} while (0)

#define hsta_dbg_noclass_printf(event)                                         \
do {                                                                           \
    if (dragon_hsta_debug)                                                     \
    {                                                                          \
        hsta_utils.dbg_printf(event, false, __FILE__, __LINE__);               \
    }                                                                          \
} while (0)

#define hsta_dbg_assert(condition)                                             \
do {                                                                           \
    if (dragon_hsta_debug && !(condition))                                     \
    {                                                                          \
        hsta_utils.dbg_printf("ASSERT FAILED: " #condition, !(condition), __FILE__, __LINE__); \
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());\
        fflush(hsta_dbg_file);                                                 \
        hsta_utils.graceful_exit(SIGINT);                                      \
    }                                                                          \
} while (0)

#define hsta_dbg_assert_with_rc(condition, rc)                                 \
do {                                                                           \
    if (dragon_hsta_debug && !(condition))                                     \
    {                                                                          \
        hsta_utils.dbg_printf("error: assert failed--" #condition, !(condition), __FILE__, __LINE__); \
        hsta_utils.log("failing return code: %d\n", rc);                       \
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());\
        fflush(hsta_dbg_file);                                                 \
        hsta_utils.graceful_exit(SIGINT);                                      \
    }                                                                          \
} while (0)

#define hsta_dbg_errcheck(err, exp, object, event)                             \
do {                                                                           \
    if (dragon_hsta_debug && err != exp)                                       \
    {                                                                          \
        std::string dbg_str = event + std::string(", dragon error: ") + std::string(dragon_get_rc_string(err)) + std::string(", ") + std::string(dragon_getlasterrstr()); \
        hsta_utils.dbg_printf(object, dbg_str, err != exp, __FILE__, __LINE__);\
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());\
        fflush(hsta_dbg_file);                                                 \
        hsta_utils.graceful_exit(SIGINT);                                      \
    }                                                                          \
} while (0)

#define hsta_dbg_errcheck_no_abort(err, exp, object, event)                    \
do {                                                                           \
    if (dragon_hsta_debug && err != exp)                                       \
    {                                                                          \
        std::string dbg_str = event + std::string(", dragon error: ") + std::string(dragon_get_rc_string(err)) + std::string(", ") + std::string(dragon_getlasterrstr()); \
        hsta_utils.dbg_printf(object, dbg_str, err != exp, __FILE__, __LINE__);\
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());\
        fflush(hsta_dbg_file);                                                 \
    }                                                                          \
} while (0)

#define hsta_dbg_no_obj_errcheck(err, exp, event)                              \
do {                                                                           \
    if (dragon_hsta_debug && err != exp)                                       \
    {                                                                          \
        std::string dbg_str = event + std::string(", dragon error: ") + std::string(dragon_get_rc_string(err)) + std::string(", ") + std::string(dragon_getlasterrstr()); \
        hsta_utils.dbg_printf(dbg_str, err != exp, __FILE__, __LINE__);        \
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());\
        fflush(hsta_dbg_file);                                                 \
        hsta_utils.graceful_exit(SIGINT);                                      \
    }                                                                          \
} while (0)

#define hsta_push_back(queue, cqe)                                                 \
do {                                                                               \
    if (dragon_hsta_debug) {                                                       \
        cqe->inc_queue_count();                                                    \
        hsta_log_history(cqe, "pushing onto " hsta_make_str(queue), false, true);  \
    }                                                                              \
    (queue)->push_back(cqe);                                                       \
} while (0)

#define hsta_pop_front(queue)                                                      \
do {                                                                               \
    if (dragon_hsta_debug) {                                                       \
        auto *cqe = (queue)->peek_front();                                         \
        if (cqe != nullptr) {                                                      \
            cqe->dec_queue_count();                                                \
            hsta_log_history(cqe, "popping off of " hsta_make_str(queue), false, true);\
        }                                                                          \
    }                                                                              \
    (queue)->pop_front();                                                          \
} while (0)

#define hsta_lock_acquire(lock) (lock).acquire(__FILE__, __LINE__)
#define hsta_lock_try_acquire(lock) (lock).try_acquire(__FILE__, __LINE__)
#define hsta_lock_release(lock) (lock).release(__FILE__, __LINE__)

#else // !HSTA_NDEBUG

#define hsta_make_str(x)
#define hsta_log_history(cqe, event, force_print, order_checking)
#define hsta_default_case(value)
#define hsta_dbg_printf(object, event)
#define hsta_dbg_noclass_printf(event)
#define hsta_dbg_assert(condition)
#define hsta_dbg_errcheck(err, exp, object, event)
#define hsta_dbg_errcheck_no_abort(err, exp, object, event)
#define hsta_dbg_no_obj_errcheck(err, exp, event)
#define hsta_push_back(queue, cqe)
#define hsta_pop_front(queue)

#endif // !HSTA_NDEBUG

// forward declarations of classes

class Agent;
class CqEvent;
class GatewayChan;
class Header;
class TargetChan;
class TxQ;
class WorkRequest;

// class definitions

class Stats
{
public:

    // data

    uint64_t num_complete;
    uint64_t total_reqs;
};

class Lock
{
private:

    dragonLock_t dlock;
    void *mem;
    bool has_been_acquired;
    const char *name;
    char history[200];

public:

    Lock(const char *name);
    ~Lock();
    void acquire(const char *file, int line);
    bool try_acquire(const char *file, int line);
    void release(const char *file, int line);

    bool check_acquired()
    {
        return this->has_been_acquired;
    }

    char *get_history()
    {
        return this->history;
    }
};

class Utils
{
public:

    // data

    Agent *agent;
#ifndef HSTA_NDEBUG
    double init_time;
    double last_time;
    Lock lock = Lock("utils lock");
#endif // !HSTA_NDEBUG

    // functions

    void init();
    void fini();
    static void *open_lib(std::string libname);
    static void resolve_symbol(void **ptr_to_func_ptr, void *lib_handle, std::string symbol);

    static uint64_t get_nanoseconds()
    {
        struct timespec time;
        auto nsec_per_sec = (uint64_t) 1e9;

        clock_gettime(CLOCK_MONOTONIC, &time);

        if (time.tv_sec > std::numeric_limits<int>::max()) {
            time.tv_sec = std::numeric_limits<int>::max();
	    }

        return (uint64_t) (time.tv_sec * nsec_per_sec) + (uint64_t) time.tv_nsec;
    }

    static double get_time()
    {
        struct timespec time;
        auto nsec_per_sec = (double) 1e9;

        clock_gettime(CLOCK_MONOTONIC, &time);

        return (double) time.tv_sec + ((double) time.tv_nsec / nsec_per_sec);
    }

#ifndef HSTA_NDEBUG
    void dump_active_chs(Agent *agent);
    void checkpoint_tick();
    uint64_t get_access_count(void *obj);
    void inc_access_count(void *obj);
    void reset_access_count(void *obj);
    void default_case(int64_t value, const char *file, int line);
    void log(const char *event);
    void log(const char *event, const char *str_param);
    void log(const char *event, int int_param);
    void dbg_printf(std::string event, bool force_print, const char *file, int line);
    void dbg_printf(CqEvent *cqe, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(Header *header, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(TxQ *tx_queue, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(Stats& stats, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(GatewayChan *gw_ch, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(TargetChan *gw_ch, std::string event, bool force_print, const char *file, int line);
    void dbg_printf(WorkRequest *work_req, std::string event, bool force_print, const char *file, int line);
    const char *traffic_type_to_string(TrafficType traffic_type);
    dragonULInt get_available_msgs(dragonChannelDescr_t *ch_descr);
    dragonULInt get_available_blocks(dragonChannelDescr_t *ch_descr);
    void set_signal_handlers(void (*sig_handler)(int));
    void graceful_exit(int sig);
    void dump_progress_snapshot();

    double get_time_from_init()
    {
        auto time = this->get_time() - this->init_time;

        if (time - this->last_time > 0.000004)
        {
            fprintf(hsta_dbg_file, "BIG TIME GAP: gap = %lf\n", time - this->last_time);
            fflush(hsta_dbg_file);
        }

        this->last_time = time;

        return time;
    }

    static std::string get_backtrace()
    {
        const int max_frames = 128;
        void *buffer[max_frames];
        std::string backtrace_str;

        auto num_frames = backtrace(buffer, max_frames);

        auto frame_strs = backtrace_symbols(buffer, num_frames);
        if (frame_strs == nullptr)
        {
            backtrace_str += std::string("error: couldn't generate backtrace\n");
            return backtrace_str;
        }

        backtrace_str += std::string("backtrace:\n");

        for (auto i = 0; i < num_frames; ++i)
        {
            backtrace_str += std::string("> ") + std::string(frame_strs[i]) + std::string("\n");
        }

        free(frame_strs);

        return backtrace_str;
    }
#endif // HSTA_NDEBUG

    // Implementation of the FNV-1 hash function.

    static uint64_t hash(uint64_t val)
    {
        uint64_t result = 0ul;

        for (int i = 0; i < 8; ++i)
        {
            result +=   (result << HSTA_FNV1_SHIFT_0)
                      + (result << HSTA_FNV1_SHIFT_1)
                      + (result << HSTA_FNV1_SHIFT_2)
                      + (result << HSTA_FNV1_SHIFT_3)
                      + (result << HSTA_FNV1_SHIFT_4)
                      + (result << HSTA_FNV1_SHIFT_5);

            result ^= (val & 0xff);

            val >>= HSTA_NBITS_IN_BYTE;
        }

        return result;
    }

    static uint64_t hash128(uint64_t val0, uint64_t val1)
    {
        uint64_t result = 0ul;

        for (int i = 0; i < 8; ++i)
        {
            result +=   (result << HSTA_FNV1_SHIFT_0)
                      + (result << HSTA_FNV1_SHIFT_1)
                      + (result << HSTA_FNV1_SHIFT_2)
                      + (result << HSTA_FNV1_SHIFT_3)
                      + (result << HSTA_FNV1_SHIFT_4)
                      + (result << HSTA_FNV1_SHIFT_5);

            result ^= (val0 & 0xff);

            val0 >>= HSTA_NBITS_IN_BYTE;
        }

        for (int i = 0; i < 8; ++i)
        {
            result +=   (result << HSTA_FNV1_SHIFT_0)
                      + (result << HSTA_FNV1_SHIFT_1)
                      + (result << HSTA_FNV1_SHIFT_2)
                      + (result << HSTA_FNV1_SHIFT_3)
                      + (result << HSTA_FNV1_SHIFT_4)
                      + (result << HSTA_FNV1_SHIFT_5);

            result ^= (val1 & 0xff);

            val1 >>= HSTA_NBITS_IN_BYTE;
        }

        return result;
    }

    static uint64_t hash_array(uint8_t *key, size_t size)
    {
        uint64_t result = 0ul;

        for (uint64_t i = 0; i < size; ++i)
        {
            result +=   (result << HSTA_FNV1_SHIFT_0)
                      + (result << HSTA_FNV1_SHIFT_1)
                      + (result << HSTA_FNV1_SHIFT_2)
                      + (result << HSTA_FNV1_SHIFT_3)
                      + (result << HSTA_FNV1_SHIFT_4)
                      + (result << HSTA_FNV1_SHIFT_5);

            result ^= (uint64_t) key[i];
        }

        return result;
    }

    static bool is_power_of_two(uint32_t val)
    {
        return (val & (val - 1)) == 0;
    }

    static uint32_t next_power_of_two(uint32_t val)
    {
        val--;
        val |= val >> 1;
        val |= val >> 2;
        val |= val >> 4;
        val |= val >> 8;
        val |= val >> 16;
        return 1u + val;
    }

    static bool flip_if(bool value_to_flip, bool cond)
    {
        return (value_to_flip != cond);
    }

    template <typename T>
    static T invalid()
    {
        return (T) -1;
    }

    static bool check_alloc_failure(dragonError_t err)
    {
        return    err == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE
               || err == DRAGON_INTERNAL_MALLOC_FAIL;
    }
};

extern Utils hsta_utils;

#endif // UTILS_HPP
