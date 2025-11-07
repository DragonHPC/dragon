#ifndef HAVE_DRAGON_CPPBARRIER
#define HAVE_DRAGON_CPPBARRIER

#include <stdint.h>
#include <stdbool.h>
#include <string>
#include <cstring>

#include <dragon/channels.h>
#include <dragon/exceptions.hpp>

namespace dragon {

/**
 * @brief Exception thrown by Barrier when required.
 *
 * This exception will be thrown when required per the definition
 * of Barrier functionality.
 */
class BrokenBarrierError: public DragonError {
    public:
    BrokenBarrierError(const dragonError_t err, const char* err_str);
};

typedef void (*action_func)();

class Barrier {

    public:
    /**
     * @brief Construct a Barrier object from a Barrier channel.
     *
     * Given a channel that was created as a barrier channel, construct
     * a C++ Barrier object over it. Typically this constructor would only
     * be used if there was a need to customize the channel in some way outside
     * of the Barrier support.
     *
     * @param barrier_channel Is a channel descriptor for a barrier channel.
     */
    Barrier(dragonChannelDescr_t* barrier_channel, action_func action);

    /**
     * @brief Attach to an existing Barrier object. Given a serialized
     * descriptor for a Barrier, create a Barrier object over it. The
     * Barrier would have been created elsewhere, either in Python or
     * C++. If created in Python, the Python code controls the lifetime of
     * the Barrier. See the Dragon Native Python Barrier documentation
     * for a description of how its lifetime is managed.
     *
     * @param serialized_sem A serialized descriptor of the barrier object.
     */
    Barrier(const char* serialized_barrier, action_func action);

    /**
     * @brief Destructor for a Barrier.
     *
     * Calling delete on a Barrier pointer or allowing a stack allocated Barrier
     * to fall out of scope calls this destructor which automatically takes care
     * of any cleanup.
     */
    ~Barrier();

    /**
     * @brief Wait on the barrier.
     *
     * Wait on the barrier until all parties are waiting or the timeout
     * expires.
     *
     * @returns an unique integer ranging from 0 to number of parties-1 for the
     * barrier.
     */
    size_t wait(timespec_t* timeout=nullptr);

    /**
     * @brief Reset the barrier.
     *
     * Set the barrier to the empty state. Any waiting processes
     * receive the BrokenBarrierError.
     */
    void reset();

    /**
     * @brief abort the barrier.
     *
     * After calling this the barrier is broken. Any waiters that are in wait state will
     * receive BrokenBarrierError.
     */
    void abort();

    /**
     * @brief Return the specified number of parties to the barrier.
     *
     * @returns the number of parties of the barrier.
     */
    size_t parties();

    /**
     * @brief Return the number of parties currently in the barrier.
    */
    size_t n_waiting();

    /**
     * @brief Return whether barrier is broken.
     *
     * @returns true or false
     */
    bool broken();

     /**
     * @brief Returns a serialized representation of the barrier that may be
     * shared with other processes in a parallel program.
     *
     * @returns A string representing the serialized Barrier which may be
     * shared with other processes.
    */
    const char* serialize();


    private:
    bool mInternallyManaged;
    dragonChannelDescr_t mBarrierChannel;
    std::string mSerBarrierChannel;
    action_func mAction;
    size_t mParties;

};

} // end dragon namespace


#endif