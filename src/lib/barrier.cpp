#include <dragon/barrier.hpp>
#include <dragon/exceptions.hpp>
#include <dragon/utils.h>
#include <functional>

namespace dragon {

BrokenBarrierError::BrokenBarrierError(const dragonError_t err, const char* err_str): DragonError(err, err_str) {}

Barrier::Barrier(dragonChannelDescr_t* barrier_channel, action_func action = nullptr) {
    dragonChannelSerial_t ser_chan;
    dragonError_t err;

    if (barrier_channel == nullptr)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "You must provide a valid barrier channel when using this Barrier constructor.");

    mInternallyManaged = false;
    mBarrierChannel = *barrier_channel;
    mAction = action;

    err = dragon_channel_capacity(barrier_channel, &mParties);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not get the capacity of the channel.");

    err = dragon_channel_serialize(&mBarrierChannel, &ser_chan);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the channel.");

    mSerBarrierChannel = dragon_base64_encode(ser_chan.data, ser_chan.len);

    err = dragon_channel_serial_free(&ser_chan);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not free the serialized channel descriptor.");
}

Barrier::Barrier(const char* serialized_barrier, action_func action = nullptr) {
    dragonChannelSerial_t ser_chan;
    dragonError_t err;

    if (serialized_barrier == nullptr)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "You must provide a valid serialized barrier when using this Barrier constructor.");

    mInternallyManaged = true;
    mSerBarrierChannel = serialized_barrier;
    mAction = action;

    ser_chan.data = dragon_base64_decode(serialized_barrier, &ser_chan.len);

    err = dragon_channel_attach(&ser_chan, &mBarrierChannel);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not attach to Dragon Semaphore channel.");

    err = dragon_channel_capacity(&mBarrierChannel, &mParties);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not get the capacity of the channel.");
}


Barrier::~Barrier() {
    if (mInternallyManaged) {
        // detach from the channel. Don't check return code since by definition it should work.
        dragon_channel_detach(&mBarrierChannel);
    }
}

size_t Barrier::wait(timespec_t* timeout) {
    dragonError_t err;
    dragonChannelRecvh_t recvh;
    dragonChannelRecvAttr_t attrs;
    timespec_t end_time_ptr;

    err = dragon_channel_recv_attr_init(&attrs);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not initialize channel recv handle attribute.");

    attrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    attrs.wait_mode = DRAGON_DEFAULT_WAIT_MODE;

    err = dragon_channel_recvh(&mBarrierChannel, &recvh, &attrs);
    if (err != DRAGON_SUCCESS) {
        abort();
        throw BrokenBarrierError(err, "Could not create channel receive handle.");
    }

    err = dragon_chrecv_open(&recvh);
    if (err != DRAGON_SUCCESS) {
        abort();
        throw BrokenBarrierError(err, "Could not open receive handle.");
    }

    err = dragon_timespec_deadline(timeout, &end_time_ptr);
    if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        abort();
        throw DragonError(err, "Could not compute deadline.");
    }

    err = dragon_channel_poll(&mBarrierChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLBARRIER_WAIT, timeout, nullptr);
    if (err == DRAGON_BARRIER_READY_TO_RELEASE) {
        try{
            if (mAction)
                (mAction)();
        } catch (...) {
            dragon_chrecv_close(&recvh);
            abort();
            throw BrokenBarrierError(DRAGON_FAILURE, "There was an error calling the action.");
        }

        timespec_t zero_timeout = {0, 0};
        err = dragon_channel_poll(&mBarrierChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLBARRIER_RELEASE, &zero_timeout, nullptr);
        if (err != DRAGON_SUCCESS) {
            dragon_chrecv_close(&recvh);
            abort();
            throw BrokenBarrierError(err, "Unexpected Error while releasing barrier.");
        }
    } else if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        abort();
        throw BrokenBarrierError(err, "Unexpected Error while polling channel.");
    }

    timespec_t rest_timeout;
    err = dragon_timespec_remaining(&end_time_ptr, &rest_timeout);
    if (err == DRAGON_TIMEOUT) {
        dragon_chrecv_close(&recvh);
        abort();
        throw TimeoutError(err, "Timeout.");
    } else if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        abort();
        throw DragonError(err, "Caught unexpected error when calculating remaining timeout.");
    }

    dragonMessage_t recv_msg;
    err = dragon_channel_message_init(&recv_msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        abort();
        throw BrokenBarrierError(err, "Failed to initialize channel message.");
    }

    err = dragon_chrecv_get_msg_blocking(&recvh, &recv_msg, &rest_timeout);
    if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        abort();
        throw DragonError(err, "Could not get message from channel.");
    }

    // Check the value of the received message.
    dragonMemoryDescr_t mem_descr;
    err = dragon_channel_message_get_mem(&recv_msg, &mem_descr);
    if (err != DRAGON_SUCCESS) {
        dragon_chrecv_close(&recvh);
        dragon_channel_message_destroy(&recv_msg, true);
        abort();
        throw DragonError(err, "Could not get the memory descriptor of the channel message.");
    }

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS) {
        dragon_channel_message_destroy(&recv_msg, true);
        throw DragonError(err, "Could not close receive handle.");
    }

    void * mem_ptr;
    err = dragon_memory_get_pointer(&mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS) {
        dragon_channel_message_destroy(&recv_msg, true);
        throw DragonError(err, "Could not get the pointer of the channel message.");
    }

    size_t * ret_val = static_cast<size_t*>(mem_ptr);
    size_t val = *ret_val;

    err = dragon_channel_message_destroy(&recv_msg, true);
    if (err != DRAGON_SUCCESS)
        throw BrokenBarrierError(err, "Could not destroy channel message.");

    if (val >= mParties) {
        char msg[200];
        snprintf(msg, 199, "Got an invalid value of %lu from waiting on the Barrier. This Dragon Barrier is broken.", val);
        throw BrokenBarrierError(DRAGON_BARRIER_BROKEN, msg);
    }

    return val;
}

void Barrier::reset() {
    dragonError_t err = dragon_channel_poll(&mBarrierChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLRESET, nullptr, nullptr);
    if (err != DRAGON_SUCCESS)
        throw BrokenBarrierError(err, "Failed to reset the barrier.");
}

void Barrier::abort() {
    timespec_t zero_timeout = {0, 0};
    dragonError_t err = dragon_channel_poll(&mBarrierChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLBARRIER_ABORT, &zero_timeout, nullptr);
    if (err != DRAGON_SUCCESS)
        throw BrokenBarrierError(err, "Failed to abort the barrier.");
}

size_t Barrier::parties() {
    return mParties;
}

size_t Barrier::n_waiting() {
    uint64_t count = 0;
    dragonError_t err = dragon_channel_barrier_waiters(&mBarrierChannel, &count);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Error getting the number of barrier waiters.");
    return count;
}

bool Barrier::broken() {
    bool result = dragon_channel_barrier_is_broken(&mBarrierChannel);
    return result;
}

const char* Barrier::serialize() {
    return mSerBarrierChannel.c_str();
}

} // end dragon namespace