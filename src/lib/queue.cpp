#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <dragon/queue.hpp>
#include <dragon/queue.h>

/**
 * @brief Encapsulates Dragon error and message.
 *
 * Objects of this class are constructed and thrown when error conditions
 * with Dragon Occur. Subclasses of this exception identify errors on a
 * more granular level so they may be caught and dealt with using
 * specific exception handling.
 **/
DragonException::DragonException(dragonError_t err, std::string msg) {}

/**
 * @brief The DragonException destructor.
 *
 * Insures proper cleanup of the exception when it is no longer needed.
 **/
DragonException::~DragonException() {}

/**
 * @brief Construct a Dragon Managed Queue streambuf
 *
 * This creates the buffer and the underlying managed queue instance.
 *
 * @param name A unique name to be assigned to this stream buffer in the user's application.
 *
 * @param buffer_size The size of the buffer to be allocated for reading/writing data.
 *
 * @param maxsize is the total capacity of the Queue.
 *
 * @param joinable is a flag whether or not the dragon_queue_task_done() and dragon_queue_join() calls function (these calls are not shown below yet).
 *
 * @param queue_attr is a pointer to Queue attributes or NULL to use default values.
 *
 * @param policy is a pointer to the Policy structure to use.
 *
 **/
DragonManagedQueueBuf::DragonManagedQueueBuf(std::string name, size_t buffer_size, size_t maxsize, bool joinable, dragonQueueAttr_t * queue_attr,
                                            dragonPolicy_t * policy) {}

/**
 * @brief Destructor for a DragonManagedQueueBuf.
 *
 * This destroys/detaches from the underlying managed queue object.
 *
 **/
DragonManagedQueueBuf::~DragonManagedQueueBuf() {}

/**
 * @brief Construct a Dragon Unmanaged Queue streambuf
 *
 * This creates the buffer and the underlying unmanaged queue instance.
 *
 * @param buffer_size The size of the buffer to be allocated for reading/writing data.
 *
 * @param pool is the Managed Memory Pool to allocate space for the Queue from.
 *
 * @param maxsize is the total capacity of the Queue.
 *
 * @param joinable is a flag whether or not the dragon_queue_task_done() and dragon_queue_join() calls function.
 *
 * @param queue_attr is a pointer to Queue attributes or NULL to use default values.
 *
 * @param policy is a pointer to the Policy structure to use, of which affinity and reference counting will be ignored.
 *
 **/
DragonUnmanagedQueueBuf::DragonUnmanagedQueueBuf(size_t buffer_size, dragonMemoryPoolDescr_t * pool, size_t maxsize, bool joinable,
                                    dragonQueueAttr_t * queue_attr, dragonPolicy_t * policy) {}

/**
 * @brief Construct a Dragon Unmanaged Queue streambuf
 *
 * This creates the buffer and the underlying unmanaged queue instance.
 *
 * @param ch_ser A Dragon base64 encoded serialized descriptor to a channel.
 * @param buffer_size The size of the buffer to be allocated for reading/writing data.
 *
 **/
DragonUnmanagedQueueBuf::DragonUnmanagedQueueBuf(std::string ch_ser, size_t buffer_size) {}

/**
 * @brief Destructor for a DragonManagedQueueBuf.
 *
 * This destroys/detaches from the underlying managed queue object.
 *
 **/
DragonUnmanagedQueueBuf::~DragonUnmanagedQueueBuf() {}


/**
 * @brief Obtain a serialized descriptor for the unmanaged queue buffer.
 *
 * This returns a string which may be sent to another process for use
 * in attaching to this unmanaged queue via the appropriate
 * DragonUnmanagedQueueBuf constructor.
 *
 **/
std::string DragonUnmanagedQueueBuf::serialize() {}

