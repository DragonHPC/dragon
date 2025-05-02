#ifndef HAVE_DRAGON_CPPQUEUE
#define HAVE_DRAGON_CPPQUEUE

#include <stdint.h>
#include <stdbool.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <dragon/channels.h>
#include <dragon/global_types.h>

class DragonException : public std::exception {
    public:
    DragonException(dragonError_t err, std::string msg);
    virtual ~DragonException();

    protected:
    std::string msg;
    dragonError_t err;
};

/**
 *  @brief This provides a Managed Queue Stream Buffer
 *
 *  This class provides a managed queue as a stream for sending
 *  and receiving data via a managed queue in the Dragon run-time.
 *  This class can be used to construct a stream, either an ostream
 *  or an istream for reading and writing data to/from a Dragon queue.
 **/

class DragonManagedQueueBuf: public std::streambuf {
    public:
    DragonManagedQueueBuf(std::string name, size_t buffer_size, size_t maxsize, bool joinable, dragonQueueAttr_t * queue_attr,
                                            dragonPolicy_t * policy);
    virtual ~DragonManagedQueueBuf();

    protected:
    // methods to be determined
};

/**
 *  @brief This provides an Unmanaged Queue Stream Buffer
 *
 *  This class provides a unmanaged queue as a stream for sending
 *  and receiving data via an unmanaged queue in the Dragon run-time.
 *  This class can be used to construct a stream, either an ostream
 *  or an istream for reading and writing data to/from a Dragon queue.
 **/

class DragonUnmanagedQueueBuf: public std::streambuf {
    public:
    DragonUnmanagedQueueBuf(size_t buffer_size, dragonMemoryPoolDescr_t * pool, size_t maxsize, bool joinable,
                                    dragonQueueAttr_t * queue_attr, dragonPolicy_t * policy);
    DragonUnmanagedQueueBuf(std::string ch_ser, size_t buffer_size);
    std::string serialize();

    virtual ~DragonUnmanagedQueueBuf();

    protected:
    // methods to be determined
};


#endif