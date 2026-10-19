/*
 * ext_attach_client.cpp — Standalone C++ executable demonstrating the
 * dragon_ext_attach()/dragon_ext_detach() bootstrap API interoperating with
 * dragon.native.queue.Queue objects created by a Python process, via the
 * Local Services key-value store.
 *
 * This is NOT run under `dragon` -- it is a plain native binary, analogous
 * to test/ext_attach/test_ext_attach_client.c and
 * test/native/flimsgto.cpp/flimsgfrom.cpp.
 *
 * Usage:
 *     DRAGON_ATTACH_TOKEN=<token> ./ext_attach_client
 *
 * ...while ext_attach_publisher.py (run via `dragon -a ext_attach_publisher.py`
 * in another terminal, or by run_ext_attach_demo.sh) is running and blocked
 * on queue2.get().
 *
 * Protocol:
 *   1. dragon_ext_attach() to the running Dragon runtime.
 *   2. dragon_ls_get_kv() to fetch the two serialized (base64) Queue FLI
 *      descriptors published by ext_attach_publisher.py under the fixed
 *      keys below (these MUST match ext_attach_publisher.py's
 *      QUEUE1_KV_KEY / QUEUE2_KV_KEY verbatim).
 *   3. Base64-decode and dragon_fli_attach() to each.
 *   4. Receive one buffered item from queue1 via dragon_fli_recv_bytes()
 *      and print it.
 *   5. Send a reply on queue2 via dragon_fli_send_bytes(), tagging it with
 *      the same "direct bytes" wire-format hint that
 *      dragon.native.queue.Queue.put()/get() use for raw bytes payloads (see
 *      _direct_payload()/_DIRECT_BYTES_HINT/_DIRECT_PAYLOAD_HINT_SHIFT in
 *      src/dragon/native/queue.py) so the Python side's queue2.get() decodes
 *      it as raw bytes instead of attempting to unpickle it.
 *   6. dragon_ext_detach().
 */

#include <dragon/ext_attach.h>
#include <dragon/fli.h>
#include <dragon/utils.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

/* Namespaced LS key-value store keys used to publish the serialized queues.
   These MUST match, verbatim, QUEUE1_KV_KEY / QUEUE2_KV_KEY in
   ext_attach_publisher.py. */
static const char* QUEUE1_KV_KEY = "outerspace_proc.ext_attach.queue1";
static const char* QUEUE2_KV_KEY = "outerspace_proc.ext_attach.queue2";

/* Must match scroll.py's OPENING_SCROLL[1] verbatim -- this is what makes
   ext_attach_publisher.py's assertion on the queue2 round-trip pass. */
static const char* SCROLL_LINE_2 =
    "A lone process awakens beyond the dragon fold, seeking to join a runtime already in flight.";

/* Mirrors dragon.native.queue._DIRECT_BYTES_HINT / _DIRECT_PAYLOAD_HINT_SHIFT
   (src/dragon/native/queue.py). Queue.put() tags a raw bytes payload's wire
   "arg" with (payload_size << 2) | 1 so that Queue.get() recognizes it and
   returns raw bytes without attempting to unpickle. */
static const uint64_t DIRECT_BYTES_HINT = 1;
static const uint64_t DIRECT_PAYLOAD_HINT_SHIFT = 2;
static const uint64_t DIRECT_PAYLOAD_TYPE_MASK = (1ULL << DIRECT_PAYLOAD_HINT_SHIFT) - 1;

class ExtAttachGuard {
public:
    explicit ExtAttachGuard(dragonExtAttachDescr_t* handle) : handle_(handle), attached_(true) {}

    ~ExtAttachGuard() {
        if (attached_) {
            dragonExtDetachErr_t remote_err = DRAGON_EXT_DETACH_SUCCESS;
            dragonError_t err = dragon_ext_detach(handle_, nullptr, &remote_err);
            if (err != DRAGON_SUCCESS) {
                std::cerr << "ext_attach_client: cleanup detach failed: " << dragon_get_rc_string(err)
                          << " (remote_err=" << (int)remote_err << ")" << std::endl;
            }
        }
    }

    dragonError_t detach(dragonExtDetachErr_t* remote_err) {
        dragonError_t err = dragon_ext_detach(handle_, nullptr, remote_err);
        if (err == DRAGON_SUCCESS)
            attached_ = false;
        return err;
    }

private:
    ExtAttachGuard(const ExtAttachGuard&) = delete;
    ExtAttachGuard& operator=(const ExtAttachGuard&) = delete;

    dragonExtAttachDescr_t* handle_;
    bool attached_;
};

static dragonError_t
attach_queue_fli(const char* b64_serial, dragonFLIDescr_t* fli_out) {
    if (b64_serial == nullptr || fli_out == nullptr)
        return DRAGON_INVALID_ARGUMENT;

    dragonFLISerial_t ser = {};
    ser.data = dragon_base64_decode(b64_serial, &ser.len);
    if (ser.data == nullptr)
        return DRAGON_INVALID_ARGUMENT;

    dragonError_t err = dragon_fli_attach(&ser, nullptr, fli_out);
    dragonError_t free_err = dragon_fli_serial_free(&ser);
    if (err == DRAGON_SUCCESS && free_err != DRAGON_SUCCESS) {
        dragon_fli_detach(fli_out);
        return free_err;
    }
    return err;
}

static bool
is_srun_mode(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--srun") == 0)
            return true;
    }
    return false;
}

static std::string
build_srun_reply(double attach_ms) {
    const char* rank_env = std::getenv("SLURM_PROCID");
    const char* local_rank_env = std::getenv("SLURM_LOCALID");

    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0)
        std::snprintf(hostname, sizeof(hostname), "unknown-host");
    hostname[sizeof(hostname) - 1] = '\0';

    std::ostringstream msg;
    msg << "SRUN_ATTACH_TIMING"
        << " rank=" << (rank_env ? rank_env : "unknown")
        << " local_rank=" << (local_rank_env ? local_rank_env : "unknown")
        << " host=" << hostname
        << " attach_ms=" << std::fixed << std::setprecision(3) << attach_ms;
    return msg.str();
}

int
main(int argc, char** argv) {
    const bool srun_mode = is_srun_mode(argc, argv);

    dragonExtAttachDescr_t handle;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;

    auto attach_start = std::chrono::steady_clock::now();
    dragonError_t err = dragon_ext_attach(nullptr, nullptr, &reg_err, &handle);
    auto attach_end = std::chrono::steady_clock::now();
    const double attach_ms =
        std::chrono::duration<double, std::milli>(attach_end - attach_start).count();
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: dragon_ext_attach failed: " << dragon_get_rc_string(err);
        if (err == DRAGON_INVALID_OPERATION)
            std::cerr << " (remote_err=" << (int)reg_err << ")";
        char* error_stack = dragon_getlasterrstr();
        if (error_stack != nullptr && error_stack[0] != '\0')
            std::cerr << ": " << error_stack;
        std::cerr << std::endl;
        return 1;
    }
    ExtAttachGuard attach_guard(&handle);

    dragonP_UID_t puid = 0;
    err = dragon_ext_get_puid(&handle, &puid);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: dragon_ext_get_puid failed: " << dragon_get_rc_string(err) << std::endl;
        return 1;
    }
    std::cout << "ext_attach_client: attached, p_uid=" << puid << std::endl;
    std::cout << "ext_attach_client: attach_ms=" << std::fixed << std::setprecision(3) << attach_ms << std::endl;

    char* q1_ser_kv = nullptr;
    err = dragon_ls_get_kv((const unsigned char*)QUEUE1_KV_KEY, &q1_ser_kv, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not find key '" << QUEUE1_KV_KEY
                  << "' in the LS key-value store: " << dragon_get_rc_string(err) << std::endl;
        std::cerr << "ext_attach_client: did you start ext_attach_publisher.py first "
                     "(via `dragon -a ext_attach_publisher.py`)?" << std::endl;
        return 1;
    }

    char* q2_ser_kv = nullptr;
    err = dragon_ls_get_kv((const unsigned char*)QUEUE2_KV_KEY, &q2_ser_kv, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not find key '" << QUEUE2_KV_KEY
                  << "' in the LS key-value store: " << dragon_get_rc_string(err) << std::endl;
        std::cerr << "ext_attach_client: did you start ext_attach_publisher.py first "
                     "(via `dragon -a ext_attach_publisher.py`)?" << std::endl;
        free(q1_ser_kv);
        return 1;
    }

    dragonFLIDescr_t q1_fli;
    err = attach_queue_fli(q1_ser_kv, &q1_fli);
    free(q1_ser_kv);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not attach to queue1's FLI: " << dragon_get_rc_string(err) << std::endl;
        free(q2_ser_kv);
        return 1;
    }

    dragonFLIDescr_t q2_fli;
    err = attach_queue_fli(q2_ser_kv, &q2_fli);
    free(q2_ser_kv);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not attach to queue2's FLI: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_detach(&q1_fli);
        return 1;
    }

    /* Receive the first scroll line from queue1. */
    dragonFLIRecvHandleDescr_t recvh;
    err = dragon_fli_open_recv_handle(&q1_fli, &recvh, nullptr, nullptr, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not open recv handle on queue1: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_detach(&q1_fli);
        dragon_fli_detach(&q2_fli);
        return 1;
    }

    size_t received_size = 0;
    uint8_t* bytes_ptr = nullptr;
    uint64_t arg = 0;
    err = dragon_fli_recv_bytes(&recvh, 0, &received_size, &bytes_ptr, &arg, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not receive bytes from queue1: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_close_recv_handle(&recvh, nullptr);
        dragon_fli_detach(&q1_fli);
        dragon_fli_detach(&q2_fli);
        return 1;
    }

    const uint64_t payload_type = arg & DIRECT_PAYLOAD_TYPE_MASK;
    const uint64_t payload_size = arg >> DIRECT_PAYLOAD_HINT_SHIFT;
    if (payload_type != DIRECT_BYTES_HINT || payload_size != received_size) {
        std::cerr << "ext_attach_client: queue1 returned an incompatible payload hint" << std::endl;
        free(bytes_ptr);
        dragon_fli_close_recv_handle(&recvh, nullptr);
        dragon_fli_detach(&q1_fli);
        dragon_fli_detach(&q2_fli);
        return 1;
    }

    std::string first_line(reinterpret_cast<char*>(bytes_ptr), received_size);
    std::cout << "ext_attach_client: received from queue1: " << first_line << std::endl;
    free(bytes_ptr);

    err = dragon_fli_close_recv_handle(&recvh, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not close recv handle on queue1: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_detach(&q1_fli);
        dragon_fli_detach(&q2_fli);
        return 1;
    }
    dragon_fli_detach(&q1_fli);

    /* Send the reply on queue2. */
    dragonFLISendHandleDescr_t sendh;
    err = dragon_fli_open_send_handle(&q2_fli, &sendh, nullptr, nullptr, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not open send handle on queue2: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_detach(&q2_fli);
        return 1;
    }

    std::string reply = srun_mode ? build_srun_reply(attach_ms) : SCROLL_LINE_2;
    size_t reply_len = reply.size();
    uint64_t hint = (reply_len << DIRECT_PAYLOAD_HINT_SHIFT) | DIRECT_BYTES_HINT;
    err = dragon_fli_send_bytes(
        &sendh,
        reply_len,
        reinterpret_cast<const uint8_t*>(reply.data()),
        hint,
        /*buffer=*/true,
        nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not send bytes on queue2: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_close_send_handle(&sendh, nullptr);
        dragon_fli_detach(&q2_fli);
        return 1;
    }

    err = dragon_fli_close_send_handle(&sendh, nullptr);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: could not close send handle on queue2: " << dragon_get_rc_string(err) << std::endl;
        dragon_fli_detach(&q2_fli);
        return 1;
    }
    dragon_fli_detach(&q2_fli);
    std::cout << "ext_attach_client: sent reply on queue2: " << reply << std::endl;

    dragonExtDetachErr_t detach_err = DRAGON_EXT_DETACH_SUCCESS;
    err = attach_guard.detach(&detach_err);
    if (err != DRAGON_SUCCESS) {
        std::cerr << "ext_attach_client: dragon_ext_detach failed: " << dragon_get_rc_string(err)
                  << " (remote_err=" << (int)detach_err << ")" << std::endl;
        return 1;
    }
    std::cout << "ext_attach_client: detached cleanly." << std::endl;

    std::cout << "ext_attach_client: OK" << std::endl;
    return 0;
}
