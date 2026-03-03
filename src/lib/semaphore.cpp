#include <dragon/semaphore.hpp>
#include <dragon/exceptions.hpp>
#include <dragon/utils.h>

namespace dragon {

// Future Work: We'll support a Semaphore that will be totally life-cycle managed from C++.
// What that looks like is a little up in the air but might, for instance, have its
// lifecycle managed by the process that created it. Then a process local channel could be used.
// We'll also likely want to redo the SHCreateProcessLocalChannel message to include a b64 encoded
// attributes structure so all the arguments to dragon_create_process_local_channel could be replaced
// with an attributes structure. Other messages with similarly large number of arguments might be
// rewritten the same way to support an attributes structure being passed in to them as a b64
// encoded string which can the be decoded and referenced by the receiver.
// If trying to access a serialized attribute struction in Python, some Cython code will be necessary to
// get the values from the attributes. We'll construct an attributes structure object in Cython with accessor
// methods to make this possible.
//
// The same kind of design for a C++ lifecycle managed Barrier.
//
// Semaphore::Semaphore(timespec_t* timeout=nullptr) {
//         // Create stream channel. We need it to send request to orchestrator later to ask for a random manager.
//         err = dragon_create_process_local_channel(&mSemChannel, 0, 0, 0, timeout);
//         if (err != DRAGON_SUCCESS)
//             append_err_return(err, "Could not create semaphore channel.");

// }

Semaphore::Semaphore(dragonChannelDescr_t* sem_channel) {
    dragonChannelSerial_t ser_chan;
    dragonError_t err;

    if (sem_channel == nullptr)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "You must provide a valid semaphore channel when using this Semaphore constructor.");

    mInternallyManaged = false;
    mSemChannel = *sem_channel;

    err = dragon_channel_serialize(&mSemChannel, &ser_chan);

    mSerSemChannel = dragon_base64_encode(ser_chan.data, ser_chan.len);

    err = dragon_channel_serial_free(&ser_chan);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not free the serialized channel descriptor");
}


Semaphore::Semaphore(const char* serialized_sem) {
    dragonChannelSerial_t ser_chan;
    dragonError_t err;

    if (serialized_sem == nullptr)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "You must provide a valid serialized semaphore when using this Semaphore constructor.");

    mInternallyManaged = true;
    mSerSemChannel = serialized_sem;

    ser_chan.data = dragon_base64_decode(serialized_sem, &ser_chan.len);

    err = dragon_channel_attach(&ser_chan, &mSemChannel);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not attach to Dragon Semaphore channel.");
}

Semaphore::~Semaphore() {
    if (mInternallyManaged) {
        // detach from the channel. Don't check return code since by definition it should work.
        dragon_channel_detach(&mSemChannel);
    }
}

bool Semaphore::acquire(bool blocking, timespec_t* timeout) {
    dragonError_t err;
    timespec_t time_val;

    if (!blocking) {
        timeout = &time_val;
        time_val = {0,0};
    }

    err = dragon_channel_poll(&mSemChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_P, timeout, NULL);
    if (err == DRAGON_TIMEOUT)
        return false;

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not acquire the semaphore.");

    return true;
}


void Semaphore::release(int n, timespec_t* timeout) {
    dragonError_t err;
    int k;

    if (n<=0)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The value of n must be greater than 0.");

    for (k=0;k<n;k++) {
        err = dragon_channel_poll(&mSemChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_V, timeout, NULL);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not release the semaphore.");
    }
}


dragonULInt Semaphore::get_value() {
    dragonError_t err;
    dragonULInt value;

    err = dragon_channel_poll(&mSemChannel, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_PEEK, nullptr, &value);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not release the semaphore.");

    return value;
}


const char* Semaphore::serialize() {
    return mSerSemChannel.c_str();
}

} // end dragon namespace