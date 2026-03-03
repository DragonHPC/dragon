#ifndef HAVE_DRAGON_CPPQUEUE
#define HAVE_DRAGON_CPPQUEUE

#include <stdint.h>
#include <stdbool.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <cstring>
#include <vector>

#include <dragon/channels.h>
#include <dragon/fli.h>
#include <dragon/global_types.h>
#include <dragon/utils.h>
#include <dragon/messages.hpp>
#include <dragon/exceptions.hpp>
#include <dragon/serializable.hpp>

namespace dragon {

template  <class Serializable>
class Queue {
    public:

    /**
     * @brief Constructs a Queue object over a given FLI.
     *
     * @param fli A valid File Like Interface (FLI) descriptor object
     */
    Queue(dragonFLIDescr_t* fli) {
        dragonError_t err;
        dragonFLISerial_t serial;
        char * serialized = nullptr;

        if (fli == nullptr)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The fli argument cannot be null and must point to an attached fli descriptor.");

        mFLI = *fli;
        mDetachOnDestroy = false;

        err = dragon_fli_serialize(fli, &serial);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not serialize the FLI.");

        serialized = dragon_base64_encode(serial.data, serial.len);
        mSerialized = serialized;

        err = dragon_fli_serial_free(&serial);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not free the serialized FLI structure.");
    }

    /**
     * @brief Constructs a Queue from a base64 encoded Queue descriptor.
     *
     * The Queue should initially be created in Python code, but then may be passed
     * to C++ code which can be running anywhere within Dragon. The lifetime of
     * the Queue is managed by the Python code that created it. The Python code
     * should wait for the C++ code to complete before exiting itself since that
     * will cause the Queue to be destroyed.
     *
     * @param serialized A base64 encoded serialized descriptor of the Queue.
     * @param pool A Dragon managed memory pool from which the Queue should make
     * internal managed memory allocations. The pool must be on the same node where
     * the Queue is to be used. If NULL is provided the default pool will be used.
     */
    Queue(const char* serialized, const dragonMemoryPoolDescr_t* pool) {
        dragonError_t err;
        dragonFLISerial_t ser_fli;

        if (serialized == nullptr) {
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Cannot pass NULL serialized to Queue attach.");
        }

        mSerialized = serialized;
        ser_fli.data = dragon_base64_decode(serialized, &ser_fli.len);

        err = dragon_fli_attach(&ser_fli, pool, &mFLI);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not attach to serialized FLI.");

        err = dragon_fli_serial_free(&ser_fli);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not free the serialized FLI structure.");

        mDetachOnDestroy = true;
    }

    /**
     * @brief Destruct the Queue by detaching from it when created all in C/C++.
     *
     * The Queue will be detached when the FLI was not provided on the constructor. If
     * The FLI was attached internally, it will be detached when the Queue is destroyed.
     */
    ~Queue() {
        dragonError_t err;

        if (mDetachOnDestroy) {
            err = dragon_fli_detach(&mFLI);
            if (err != DRAGON_SUCCESS)
                cerr << "Error while destroying C++ Queue: " << dragon_get_rc_string(err) << " : " << dragon_getlasterrstr();
        }
    }

    /**
     * @brief Serialize a Queue and return its serialized descriptor.
     *
     * @return A base64 encoded serialized descriptor of the Queue.
     */
    const char* serialize() {
        return mSerialized.c_str();
    }

    /**
     * @brief Get a Serializable value from the queue.
     *
     * Performs a get operation on the Queue, possibly blocking while waiting for
     * a value to become available.
     *
     * @param strm_ch A stream channel to use in getting the value. If NULL is provided
     * then the Queue will provide its own stream channel. Providing a channel can be useful
     * or even necessary in some circumstances. Refer to the FLI documentation on stream
     * channels for further information.
     *
     * @param dest_pool A valid Dragon managed memory pool descriptor that will be the destination
     * of data received from the Queue object, at least temporarily. The Serializable value
     * implementation can receive memory directly, in which case the memory would reside in this
     * pool. If the Serializable implementation receives bytes to deserialize the object, then
     * the pool is used only as a temporary location during the get operation. Providing NULL
     * will result in using the default pool.
     *
     * @param arg A get operation will also return an extra uint64_t value that can be
     * user-specified. If NULL is provided, the argument is not returned.
     *
     * @param timeout A pointer to a timespec_t structure. The get operation will block for
     * at most the time specified. If a pointer to {0,0} is specified for the time, the get
     * operation will retrieve a value that is immediately available and return immediately
     * if there is no value waiting. If NULL is provided, the get operation will block
     * indefinitely until a value becomes available.
     *
     * @return A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws An EmptyError if the Queue is empty or if a timeout occurs. Otherwise it will
     * throw a DragonError.
     */
    Serializable get(dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool, uint64_t* arg, timespec_t* timeout) {
        dragonFLIRecvHandleDescr_t recvh;
        dragonFLIRecvAttr_t attrs;
        dragonError_t err ;

        err = dragon_fli_recv_attr_init(&attrs);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Couild not initialize attributes of the FLI receive handle.");
        attrs.dest_pool = dest_pool;

        err = dragon_fli_open_recv_handle(&mFLI, &recvh, strm_ch, &attrs, timeout);
        if (err == DRAGON_TIMEOUT)
            throw TimeoutError(err, "The receive handle open timed out.");

        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not open the FLI receive handle.");

        try {
            Serializable value = Serializable::deserialize(&recvh, arg, timeout);

            err = dragon_fli_close_recv_handle(&recvh, timeout);
            if (err == DRAGON_TIMEOUT)
                throw TimeoutError(err, "The receive handle close timed out.");

            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not close the FLI receive handle.");

            return value;

        } catch (const TimeoutError& e) {
            // Don't catch any error. Just get out.
            dragon_fli_close_recv_handle(&recvh, timeout);
            throw EmptyError(e.rc(), e.err_str());
        }
    }

    /**
     * @brief Convenience method for get.
     *
     * See the full get documentation for further details. Calling this waits indefinitely for a
     * value if NULL is provided and for timeout seconds if a non-zero timeout is provided. If {0,0}
     * is provided it does a try-once to see if there is a value available and returns otherwise.
     *
     * @param timeout A pointer to a timespec_t structure. The get operation will block for
     * at most the time specified. If a pointer to {0,0} is specified for the time, the get
     * operation will retrieve a value that is immediately available and return immediately
     * if there is no value waiting. If NULL is provided, the get operation will block
     * indefinitely until a value becomes available.
     *
     * @return A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws An EmptyError if the Queue is empty or if a timeout occurs. Otherwise it will
     * throw a DragonError.
     */
    Serializable get(timespec_t* timeout) {
        return get(nullptr, nullptr, nullptr, timeout);
    }

    /**
     * @brief Convenience method for get.
     *
     * See the full get documentation for further details. Calling this waits indefinitely for a
     * value.
     *
     * @return A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws DragonError if an unexpected error occurs.
     */
    Serializable get() {
        return get(nullptr, nullptr, nullptr, nullptr);
    }

    /**
     * @brief Convenience method for get.
     *
     * See the full get documentation for further details. This method only try one attempt
     * to acquire the resources to send the value.
     *
     * @param value A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws DragonError if an unexpected error occurs.
     */

    Serializable get_nowait() {
        timespec_t timeout = {0, 0};
        return get(nullptr, nullptr, nullptr, &timeout);
    }

    /**
     * @brief Puts a value into the Queue.
     *
     * This will put a Serializable value into the Queue by calling serialize on it and sending the result
     * over the internal FLI object of the Queue. If strm_ch is provided, sending can be further controlled. See
     * the documentation on stream channels used in sending the FLI. If dest_pool is provided, then it will be
     * put into the dest_pool by the sender when sent.
     *
     * @param value The Serializable value to be sent.
     *
     * @param strm_ch A stream channel to use in putting the value. If NULL is provided
     * then the Queue will provide its own stream channel. Providing a channel can be useful
     * or even necessary in some circumstances. Refer to the FLI documentation on stream
     * channels for further information. Of note, of strm_ch is provided the constant
     * STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION, then data will be streamed from the sender.
     * Additionally, if STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND is specified for the stream
     * channel, the a stream-based FLI will send the value using the main channel of the FLI.
     * Either of these two contant values eliminate a round-trip inside the FLI and while
     * using a Queue either of them will reliably work with multiple senders and multiple
     * receivers (in spite of the first constant name) since all data is buffered before being
     * sent on a put operation.
     *
     * @param dest_pool A valid Dragon managed memory pool descriptor that will be the destination
     * of data sent from the Queue object, at least temporarily. The Serializable value
     * implementation can receive memory directly, in which case, under the right conditions,
     * the serialized value would reside in this pool. If the Serializable implementation receives
     * bytes to deserialize the object, then the pool is used only as a temporary location during
     * the put operation. Providing NULL will result in using the default pool.
     *
     * @param arg A 64-bit value supplied by the user. It can be 0 if you don't need it, but
     * can be used for anything that the user might need to transmit in 64-bits of meta-data
     * about the object being sent. There are a few reserved values at the upper end of unsigned
     * 64-bit values. Refer to the FLI documentation for reserved values of arg.
     *
     * @param flush A boolean that guarantees that the put operation has deposited the value
     * in the queue, even off-node before the put operation completes. Normally this value should
     * be false and will result in faster put performance but an immediate poll operation on the
     * queue may show the queue is empty when it will not be moments later.
     *
     * @param timeout A pointer to a timespec_t structure. The put operation will block for
     * at most the time specified. If a pointer to {0,0} is specified for the time, the put
     * operation will put the value immediately and return immediately if it cannot complete
     * all operations immediately. If NULL is provided, the put operation will block
     * indefinitely until it is able to put the value. Instances where a put operation might
     * block are when the Queue has become full, or when the pool used for intermediate
     * communication has been depleted. Neither of these are likely to happen, but under heavy
     * loads the likelyhood increases as back-pressure/flow control becomes necessary.
     *
     * @return nothing
     * @throws DragonError if an unexpected error occurs or TimeoutError if a timeout occurs.
     */
    void put(const Serializable& value, dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool,
            uint64_t arg, bool flush, const timespec_t* timeout) {

        dragonError_t err;
        dragonFLISendHandleDescr_t sendh;
        dragonFLISendAttr_t attrs;

        err = dragon_fli_send_attr_init(&attrs);
        attrs.dest_pool = dest_pool;
        attrs.allow_strm_term = false;
        attrs.turbo_mode = false;
        attrs.flush = flush;

        err = dragon_fli_open_send_handle(&mFLI, &sendh, strm_ch, &attrs, timeout);
        if (err == DRAGON_TIMEOUT)
            throw TimeoutError(err, "The send handle open timed out.");

        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not open the FLI send handle.");

        value.serialize(&sendh, arg, true, timeout);

        err = dragon_fli_close_send_handle(&sendh, timeout);
        if (err == DRAGON_TIMEOUT)
            throw TimeoutError(err, "The send handle close timed out.");

        if (err == DRAGON_CHANNEL_FULL)
            throw FullError(err, "Queue is full.");

        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not close the FLI send handle.");

        err = dragon_fli_new_task(&mFLI, timeout);
        if (err != DRAGON_SUCCESS && err != DRAGON_SEMAPHORE_NOT_FOUND)
            throw DragonError(err, "Could not add new task to the FLI.");
    }

    /**
     * @brief Convenience method for put.
     *
     * See the full put documentation for further details.
     *
     * @param value A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @param timeout A timeout that could expire if memory resources are low.
     *
     * @throws DragonError if an unexpected error occurs.
     */

    void put(const Serializable& value, const timespec_t* timeout) {
        put(value, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, nullptr, 0, false, timeout);
    }

    /**
     * @brief Convenience method for put.
     *
     * See the full put documentation for further details. This method waits forever
     * for enough resources to send the value should sufficient system resources not
     * be available including shared memory.
     *
     * @param value A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws DragonError if an unexpected error occurs.
     */

    void put(Serializable& value) {
        put(value, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, nullptr, 0, false, nullptr);
    }

    /**
     * @brief Convenience method for put.
     *
     * See the full put documentation for further details. This method only try one attempt
     * to acquire the resources to send the value.
     *
     * @param value A Serializable value. The type of Serializable value was specified when the
     * the Queue was instantiated from this template.
     *
     * @throws DragonError if an unexpected error occurs.
     */

    void put_nowait(const Serializable& value) {
        timespec_t timeout = {0, 0};
        put(value, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, nullptr, 0, false, &timeout);
    }

    /**
     * @brief Check for an item Checks for an item in the queue.
     *
     * Returns immediately with true or false. The timeout is because it is a
     * potentially remote operation, but it should return quickly in most
     * circumstances. If a timeout occurs, then a DragonError is thrown. A
     * nullptr for timeout will result in waiting forever which should
     * never happen. A {0,0} timeout is a try-once operation.
     *
     * @returns true when an item is present and false if not.
     * @throws DragonError if a timeout should occur.
     */
    bool poll(timespec_t* timeout) {
        dragonError_t err;
        err = dragon_fli_poll(&mFLI, timeout);
        if (err == DRAGON_EMPTY)
            return false;

        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not poll the queue.");

        return true;
    }

    /**
     * @brief Find if the queue is full or not.
     *
     * Returns true when the queue is full and false otherwise.
     * The Queue is full if there are the same number of items in the
     * queue as the capacity of the queue. However, since puts and gets
     * are potentially blocking operations, you generally do not need to be
     * concerned about whether a queue is full or not. It will just block for
     * a put operation when it is.
     *
     * @returns true when it is full and false otherwise.
     */
    bool full() {
        dragonError_t err;
        err = dragon_fli_full(&mFLI);
        if (err == DRAGON_NOT_FULL)
            return false;
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not check if queue is full or not.");

        return true;
    }

    /**
     * @brief Check the queue for empty
     *
     * If the queue has no items in it this will return true. However,
     * since gets are potentially blocking operations, an empty queue
     * will just cause a get operation to block, waiting for an item to
     * be available.
     *
     * @returns true if the queue is empty, false otherwise.
     */
    bool empty(timespec_t* timeout) {
        return !poll(timeout);
    }

    /**
     * @brief Return the number of items in the queue
     *
     * Returns the number of messages in the queue. By
     * the time this method returns to the user, the
     * value could have changed since it is a multiprocessing
     * queue. While a timeout an be specified, it should
     * never timeout in normal processing.
     *
     * @param timeout The amount of time to wait for the
     * information. A {0,0} timeout will try-once. A nullptr
     * timeout will wait forever (if needed).
     * @returns The number of items in the queue.
     * @throws DragonError if it could not get the size.
     */
    size_t size(timespec_t* timeout) {
        dragonError_t err;
        size_t num_msgs = 0;
        err = dragon_fli_num_msgs(&mFLI, &num_msgs, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the size of the queue");
        return num_msgs;
    }

    /**
     * @brief Mark a task done for a task queue
     *
     * A queue can be created as a task queue. If this queue was,
     * then you an mark tasks done using this method. The number of
     * tasks is incremented every time an item is put to the queue.
     * Think of items on the queue as tasks. Tasks can be marked done
     * using this method when they are completed.
     *
     * @param timeout The amount of time to wait for the
     * task to be marked done. A {0,0} timeout will try-once. A nullptr
     * timeout will wait forever (if needed). This should be a fast
     * operation.
     *
     * @throws DragonError if an unexpected error occurred.
     */

    void task_done(timespec_t* timeout) {
        dragonError_t err;

        err = dragon_fli_task_done(&mFLI, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not perform task done.");
    }


    /**
     * @brief Block until the number of tasks has reached zero.
     *
     * Calling this blocks when the number of tasks is not zero.
     *
     * @param timeout The amount of time to wait for all
     * tasks to be marked done. A {0,0} timeout will try-once. A nullptr
     * timeout will wait forever (if needed).
     *
     * @throws DragonError if an unexpected error occurred.
     */
    void join(timespec_t* timeout) {
        dragonError_t err = dragon_fli_join(&mFLI, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not join queue.");
    }

    private:
    bool mDetachOnDestroy; ///< @brief The internal integer value of the class.
    dragonFLIDescr_t mFLI;
    std::string mSerialized;

};

} // end dragon namespace

#endif