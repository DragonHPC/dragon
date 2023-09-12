#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "dragon/channels.h"
#include "dragon/perf.h"
#include "dragon/utils.h"
#include "err.h"

static timespec_t
get_timespec(double time_in_sec)
{
    time_t secs = (time_t) time_in_sec;
    long nsecs = (long) ((time_in_sec - (double) secs) * (double) 1e9);
    return {secs, nsecs};
}

static double
get_time()
{
    struct timespec time;
    auto nsec_per_sec = (double) 1e9;

    clock_gettime(CLOCK_MONOTONIC, &time);

    return (double) time.tv_sec + ((double) time.tv_nsec / nsec_per_sec);
}

static void
check_err(dragonError_t err)
{
    if (err != DRAGON_SUCCESS) {
        abort();
    }
}

// forward declarations
class dragonChPerfSession;
class dragonChPerfKernel;

dragonChPerfSession *this_session = nullptr;

class dragonChPerfSession {
private:

    int num_channels;
    std::vector<dragonChannelDescr_t> chd;

public:

    dragonMemoryPoolDescr_t mpool;
    std::vector<dragonChPerfKernel *> kernels;
    std::vector<dragonChannelSendh_t> sendh; 
    std::vector<dragonChannelRecvh_t> recvh; 

    dragonChPerfSession(dragonChannelSerial_t *sdesc_array, int num_channels)
    {
        dragonError_t err;
        dragonChannelSendAttr_t sattr;

        err = dragon_channel_send_attr_init(&sattr);
        check_err(err);

        sattr.return_mode = DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY;

        this->num_channels = num_channels;
        this->chd.resize(num_channels);
        this->sendh.resize(num_channels);
        this->recvh.resize(num_channels);

        for (auto ch_idx = 0; ch_idx < num_channels; ++ch_idx) {
            auto sdesc = &sdesc_array[ch_idx];
            err = dragon_channel_attach(sdesc, &this->chd[ch_idx]);
            check_err(err);

            err = dragon_channel_sendh(&this->chd[ch_idx], &this->sendh[ch_idx], &sattr);
            check_err(err);

            err = dragon_chsend_open(&this->sendh[ch_idx]);
            check_err(err);

            err = dragon_channel_recvh(&this->chd[ch_idx], &this->recvh[ch_idx], nullptr);
            check_err(err);

            err = dragon_chrecv_open(&this->recvh[ch_idx]);
            check_err(err);
        }

        // attach to memory pool

        err = dragon_memory_pool_attach_from_env(&this->mpool, "DRAGON_INF_PD");
        check_err(err);
    }

    ~dragonChPerfSession()
    {
        auto err = DRAGON_SUCCESS;

        err = dragon_memory_pool_detach(&this->mpool);
        check_err(err);

        for (auto ch_idx = 0; ch_idx < this->num_channels; ++ch_idx) {
            err = dragon_chsend_close(&this->sendh[ch_idx]);
            check_err(err);

            err = dragon_chrecv_close(&this->recvh[ch_idx]);
            check_err(err);

            err = dragon_channel_detach(&this->chd[ch_idx]);
            check_err(err);
        }
    }
};

class dragonChPerfBytecodeOp {
private:

    dragonChPerfOpcode_t opcode;
    dragonChannelSendh_t sendh;
    dragonChannelRecvh_t recvh;
    size_t bytes;
    timespec_t timeout;

public:

    dragonError_t send_msg();
    dragonError_t get_msg();
    dragonError_t poll();

    dragonError_t
    new_msg(size_t bytes, bool alloc_mem, dragonMessage_t *msg);

    dragonChPerfBytecodeOp(dragonChannelSendh_t sendh,
                         size_t bytes,
                         timespec_t timeout)
    {
        this->opcode = DRAGON_PERF_OPCODE_SEND_MSG;
        this->sendh   = sendh;
        this->bytes   = bytes;
        this->timeout = timeout;
    }

    dragonChPerfBytecodeOp(dragonChannelRecvh_t recvh,
                         size_t bytes,
                         timespec_t timeout)
    {
        this->opcode = DRAGON_PERF_OPCODE_GET_MSG;
        this->recvh   = recvh;
        this->bytes   = bytes;
        this->timeout = timeout;
    }

    // TODO: add ctor for poll

    dragonError_t exec()
    {
        switch (this->opcode) {
            case DRAGON_PERF_OPCODE_SEND_MSG: return this->send_msg();
            case DRAGON_PERF_OPCODE_GET_MSG:  return this->get_msg();
            case DRAGON_PERF_OPCODE_POLL:     return this->poll();
            default: {
                if (this->opcode >= DRAGON_PERF_OPCODE_LAST) {
                    err_return(DRAGON_FAILURE, "invalid op code");
                } else {
                    err_return(DRAGON_NOT_IMPLEMENTED, "op code not implemented");
                }
            }
        }
    }
};

dragonError_t
dragonChPerfBytecodeOp::send_msg()
{
    auto err = DRAGON_SUCCESS;
    dragonMessage_t msg;

    err = this->new_msg(this->bytes, true, &msg);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to create a new message");
    }

    err = dragon_chsend_send_msg(&this->sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, &this->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send message");
    }

    err = dragon_channel_message_destroy(&msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy message");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonChPerfBytecodeOp::get_msg()
{
    auto err = DRAGON_SUCCESS;
    dragonMessage_t msg;

    err = this->new_msg(this->bytes, false, &msg);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to create a new message");
    }

    err = dragon_chrecv_get_msg_blocking(&this->recvh, &msg, &this->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get message");
    }

    // TODO: add data validation

    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy message");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonChPerfBytecodeOp::poll()
{
    // TODO: add support for poll
#if 0
    return dragon_channel_poll();
#endif
    err_return(DRAGON_NOT_IMPLEMENTED, "poll opcode not implemented");
}

dragonError_t
dragonChPerfBytecodeOp::new_msg(size_t bytes, bool alloc_mem, dragonMessage_t *msg)
{
    dragonMemoryDescr_t md;

    if (alloc_mem) {
        auto err = dragon_memory_alloc(&md, &this_session->mpool, bytes);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to allocate managed memory for communication buffer");
        }

        err = dragon_channel_message_init(msg, &md, nullptr);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to initialize message");
        }
    } else {
        auto err = dragon_channel_message_init(msg, nullptr, nullptr);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to initialize message");
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

class dragonChPerfKernel {
private:

    int idx;
    int ch_idx;
    int num_procs;
    bool first_run;
    std::vector<dragonChPerfBytecodeOp *> ops;
    std::vector<dragonChPerfBytecodeOp *> barrier_ops;

public:

    dragonError_t
    append(dragonChPerfOpcode_t opcode, int dst_ch_idx, size_t bytes, double timeout_in_sec, bool is_barrier);

    dragonError_t
    run(double *run_time);

    dragonChPerfKernel(int kernel_idx, int ch_idx, int num_procs)
    {
        this->idx = kernel_idx;
        this->ch_idx = ch_idx;
        this->num_procs = num_procs;
        this->first_run = true;
        this_session->kernels.push_back(this);

        // create barrier sub-kernel

        auto root = 0;
        auto timeout_in_sec = 30;
        auto num_bytes = 8;

        if (ch_idx == root) {
            // wait for messages from all leaf procs and send responses
            for (auto i = 1; i < num_procs; ++i) {
                this->append(DRAGON_PERF_OPCODE_GET_MSG, ch_idx, num_bytes, timeout_in_sec, true);
            }
            for (auto i = 1; i < num_procs; ++i) {
                this->append(DRAGON_PERF_OPCODE_SEND_MSG, i, num_bytes, timeout_in_sec, true);
            }
        } else {
            // send message to root and wait for response
            this->append(DRAGON_PERF_OPCODE_SEND_MSG, root, num_bytes, timeout_in_sec, true);
            this->append(DRAGON_PERF_OPCODE_GET_MSG, ch_idx, num_bytes, timeout_in_sec, true);
        }
    }

    ~dragonChPerfKernel()
    {
        for (auto *op : this->ops) {
            delete op;
        }
        for (auto *op : this->barrier_ops) {
            delete op;
        }
    }
};

dragonError_t
dragonChPerfKernel::append(dragonChPerfOpcode_t opcode, int dst_ch_idx, size_t bytes, double timeout_in_sec, bool is_barrier)
{
    switch (opcode) {
        case DRAGON_PERF_OPCODE_SEND_MSG: {
            auto bytecode_op = new dragonChPerfBytecodeOp(this_session->sendh[dst_ch_idx], bytes, get_timespec(timeout_in_sec));
            if (is_barrier) {
                this->barrier_ops.push_back(bytecode_op);
            } else {
                this->ops.push_back(bytecode_op);
            }

            break;
        } case DRAGON_PERF_OPCODE_GET_MSG: {
            auto bytecode_op = new dragonChPerfBytecodeOp(this_session->recvh[dst_ch_idx], bytes, get_timespec(timeout_in_sec));
            if (is_barrier) {
                this->barrier_ops.push_back(bytecode_op);
            } else {
                this->ops.push_back(bytecode_op);
            }

            break;
        } default: {
            err_return(DRAGON_NOT_IMPLEMENTED, "operation type not implemented");
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

// TODO: allow user to pass in num_iters
dragonError_t
dragonChPerfKernel::run(double *run_time)
{
    // do warm-up if necessary
    if (this->first_run) {
        for (auto *op : this->ops) {
            auto err = op->exec();
            if (err != DRAGON_SUCCESS) {
                append_err_return(err, "channel operation failed");
            }
        }

        this->first_run = false;
    }

    // run barrier
    for (auto *op : this->barrier_ops) {
        auto err = op->exec();
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "channel operation failed");
        }
    }

    auto begin_time = get_time();

    // run user's kernel
    int num_iters = 10;
    for (auto i = 0; i < num_iters; ++i) {
        for (auto *op : this->ops) {
            auto err = op->exec();
            if (err != DRAGON_SUCCESS) {
                append_err_return(err, "channel operation failed");
            }
        }
    }

    *run_time = (get_time() - begin_time) / (double) num_iters;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_chperf_session_new(dragonChannelSerial_t *sdesc_array, int num_channels)
{
    this_session = new dragonChPerfSession(sdesc_array, num_channels);
    if (this_session == nullptr) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocation new session object");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_chperf_session_cleanup()
{
    delete this_session;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_chperf_kernel_new(int kernel_idx, int ch_idx, int num_procs)
{
    dragonChPerfKernel *kernel = new dragonChPerfKernel(kernel_idx, ch_idx, num_procs);
    if (kernel == nullptr) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate new kernel object");
    }

    this_session->kernels.push_back(kernel);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_chperf_kernel_append_op(int kernel_idx, dragonChPerfOpcode_t opcode, int dst_ch_idx, size_t bytes, double timeout_in_sec)
{
    dragonChPerfKernel *kernel = this_session->kernels[kernel_idx];
    return kernel->append(opcode, dst_ch_idx, bytes, timeout_in_sec, false);
}

dragonError_t
dragon_chperf_kernel_run(int kernel_idx, double *run_time)
{
    dragonChPerfKernel *kernel = this_session->kernels[kernel_idx];
    return kernel->run(run_time);
}

