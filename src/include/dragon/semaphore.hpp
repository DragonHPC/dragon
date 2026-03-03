#ifndef HAVE_DRAGON_CPPSEMAPHORE
#define HAVE_DRAGON_CPPSEMAPHORE

#include <stdint.h>
#include <stdbool.h>
#include <string>
#include <cstring>

#include <dragon/channels.h>

namespace dragon {

class Semaphore {
    public:
    /**
     * @brief Construct a Semaphore object from a Semaphore channel.
     *
     * Given a channel that was created as a semaphore channel, construct
     * a C++ Semaphore object over it. Typically this constructor would only
     * be used if there was a need to customize the channel in some way outside
     * of the Semaphore support.
     *
     * @param sem_channel Is a channel descriptor for a semaphore channel.
     */
    Semaphore(dragonChannelDescr_t* sem_channel);

    /**
     * @brief Attach to an existing Semaphore object. Given a serialized
     * descriptor for a Semaphore, create a Semaphore object over it. The
     * Semaphore would have been created elsewhere, either in Python or
     * C++. If created in Python, the Python code controls the lifetime of
     * the Semaphore. See the Dragon Native Python Semaphore documentation
     * for a description of how its lifetime is managed.
     *
     * @param serialized_sem A serialized descriptor of the semaphore object.
     */
    Semaphore(const char* serialized_sem);

    /**
     * @brief Destructor for a Semaphore.
     *
     * Calling delete on a Semaphore pointer or allowing a stack allocated Semaphore
     * to fall out of scope calls this destructor which automatically takes care
     * of any cleanup.
     */
    ~Semaphore();

    /**
     * @brief Acquire the Semaphore, decrementing the counter, blocking other
     * processes when the counter is decremented to 0.
     *
     * @param blocking Block the process if the Semaphore cannot be acquired immediately.
     * Defaults to true. If false, then the timeout is ignored.
     *
     * @param timeout Time to wait, defaults to nullptr (i.e. no timeout). A non-blocking
     * acquire can be achieved by setting blocking to false or by setting timeout to {0,0}.
     *
     * @returns True if the semaphore was acquired, false otherwise. It does not raise
     * an exception if a timeout occurs.
     */
    bool acquire(bool blocking=true, timespec_t* timeout=nullptr);

    /**
     * @brief Release the Semaphore, incrementing the internal value by n,
     * thus unblocking up to n processes.
     *
     * @param n The number of waiters to unblock on this semaphore. Defaults to
     * 1.
     *
     * @param timeout The amount of time to wait for the operation to complete.
     * A value of nullptr means to wait until completion. A value of {0,0} means
     * to try once and give up if waiting would be required. Normally nullptr, the
     * default, should be specified.
     *
     * @returns nothing.
     */
    void release(int n=1, timespec_t* timeout=nullptr);

    /**
     * @brief Returns the value of the semaphore. Note that due to the intended parallel
     * use of the semaphore, the value may have changed by the time the calling process
     * gets the value. It cannot be counted on to still be the value returned without
     * additional synchronization of the parallel code.
     *
     * @returns An integer with the current value of the semaphore when polled.
     */
    dragonULInt get_value();

    /**
     * @brief Returns a serialized representation of the semaphore that may be
     * shared with other processes in a parallel program.
     *
     * @returns A string representing the serialized Semaphore which may be
     * shared with other processes.
    */
    const char* serialize();


    private:
    bool mInternallyManaged;
    dragonChannelDescr_t mSemChannel;
    std::string mSerSemChannel;
};

} // end dragon namespace

#endif