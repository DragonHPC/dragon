#include <stdlib.h>
#include "_queue.h"
#include "_utils.h"
#include "umap.h"
#include "err.h"
#include <dragon/queue.h>

/* dragon globals */
DRAGON_GLOBAL_MAP(queues);

static dragonError_t
_add_umap_queue_entry(const dragonQueueDescr_t * qdesc, const dragonQueue_t * newq)
{
    dragonError_t err;

    if (*dg_queues == NULL) {
        *dg_queues = malloc(sizeof(dragonMap_t));
        if (*dg_queues == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for channels");

        err = dragon_umap_create(dg_queues, DRAGON_QUEUE_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for queues");
    }

    err = dragon_umap_additem(dg_queues, qdesc->_idx, newq);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into queues umap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_queue_descr_from_q_uid(const dragonQ_UID_t q_uid, dragonQueueDescr_t * q_desc)
{
    if (q_desc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid queue descriptor");

    dragonQueue_t * queue;
    dragonError_t err = dragon_umap_getitem(dg_queues, q_uid, (void *)&queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find queue in umap");

    q_desc->_idx = q_uid;
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_queue_from_descr(const dragonQueueDescr_t * qdesc, dragonQueue_t ** queue)
{
    if (qdesc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid queue descriptor");

    dragonError_t err = dragon_umap_getitem(dg_queues, qdesc->_idx, (void*)queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in queues umap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_init_channel_handles(dragonQueue_t * queue)
{
    if (queue == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Logging handle cannot be NULL");

    dragonError_t err = dragon_channel_sendh(&queue->ch, &queue->csend, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create send handle");

    err = dragon_chsend_open(&queue->csend);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle");

    err = dragon_channel_recvh(&queue->ch, &queue->crecv, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create recv handle");

    err = dragon_chrecv_open(&queue->crecv);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open recv handle");

    no_err_return(DRAGON_SUCCESS);
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void
_set_thread_local_mode_queues(bool set_thread_local)
{
    if (set_thread_local) {
        dg_queues = &_dg_thread_queues;
    } else {
        dg_queues = &_dg_proc_queues;
    }
}

// BEGIN USER API

/** @brief Initialize a Policy structure with default values.
*
*  Set all fields of a Policy structure to their default values. This allows users to only specify the ones they
*  want modified from the defaults.
*
*  @param policy is a pointer to the dragonPolicy_t structure to initialize.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_policy_init(dragonPolicy_t * policy)
{
    return DRAGON_NOT_IMPLEMENTED;
}

/** @brief Initialize a Queue attributes structure with default values.
*
*  Set all fields of a Queue attributes structure to their default values. This allows users to only specify the ones
*  they want modified from the defaults.
*
*  @param queue_attr is a pointer to the dragonQueueAttr_t structure to initialize.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_attr_init(dragonQueueAttr_t * queue_attr)
{
    if (queue_attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Attributes cannot be NULL");

    queue_attr->max_blocks = DRAGON_QUEUE_DEFAULT_MAXSIZE;
    queue_attr->bytes_per_msg_block = DRAGON_QUEUE_DEFAULT_BLOCKSIZE;

    return DRAGON_SUCCESS;
}


/** @brief Create a new unmanaged Queue resource
*
*  Create a new unmanaged Queue resource accessible to any process within the Dragon runtime context.
*  Because the Queue is unmanaged, it will not be discoverable by name by other processes. The values for affinity
*  and reference counting in the given Policy will be ignored. Instead the Queue will be cerated on the same
*  node as the calling process, and cleanup of the Queue must be done explicitly by a user process. There are no
*  interactions with Dragon Global Services in this call. This call is used by Dragon services to create a managed
*  Queue a requests through dragon_managed_queue_create().
*
*  @param pool is the Managed Memory Pool to allocate space for the Queue from.
*
*  @param maxsize is the total capacity of the Queue.
*
*  @param q_uid is the unique identifier for the Queue.
*
*  @param joinable is a flag whether or not the dragon_queue_task_done() and dragon_queue_join() calls function.
*
*  @param queue_attr is a pointer to Queue attributes or NULL to use default values.
*
*  @param policy is a pointer to the Policy structure to use, of which affinity and reference counting will be ignored.
*
*  @param queue is a pointer to the Queue descriptor to update.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_create(dragonMemoryPoolDescr_t * pool, size_t maxsize, dragonQ_UID_t q_uid,
                                  bool joinable, dragonQueueAttr_t * queue_attr, dragonPolicy_t * policy,
                                  dragonQueueDescr_t * queue)
{
    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Pool cannot be NULL");

    if (queue == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Queue descriptor cannot be NULL");

    if (dragon_memory_pool_is_local(pool) != true)
        err_return(DRAGON_INVALID_ARGUMENT, "Pool must be local");

    dragonQueueAttr_t def_attr;
    dragonError_t err;
    if (queue_attr == NULL) {
        err = dragon_queue_attr_init(&def_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not init Queue attributes");

        def_attr.max_blocks = maxsize;
        queue_attr = &def_attr;
    } else {
        //err = _validate_attr(queue_attr);
    }

    dragonQueue_t * newq = malloc(sizeof(dragonQueue_t));
    err = dragon_memory_pool_descr_clone(&newq->pool, pool);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Cannot clone pool descriptor");
        goto q_fail;
    }

    dragonChannelAttr_t cattr;
    dragon_channel_attr_init(&cattr);

    cattr.bytes_per_msg_block = queue_attr->bytes_per_msg_block;
    cattr.capacity = queue_attr->max_blocks;

    err = dragon_channel_create(&newq->ch, q_uid, pool, &cattr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to create queue channel");
        goto q_fail;
    }

    err = _init_channel_handles(newq);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("error opening queue channel handles");
        goto q_ch_fail;
    }

    queue->_idx = q_uid;
    newq->q_uid = q_uid;
    err = _add_umap_queue_entry(queue, newq);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("cannot insert queue into umap");
        goto q_ch_fail;
    }

    no_err_return(DRAGON_SUCCESS);

 q_ch_fail:
    dragon_channel_destroy(&newq->ch);
 q_fail:
    if (newq != NULL)
        free(newq);

    no_err_return(err);
}


/** @brief Create a new managed Queue resource
*
*  Create a new unmanaged Queue resource accessible to any process within the Dragon runtime context.
*  Because the Queue is managed, this call will include interactions with Dragon Global Services to make it
*  discoverable and potentially reference counted depending on the given Policy.
*
*  @param name is a null-terminated name to give to the Queue in the Dragon namespace.
*
*  @param maxsize is the total capacity of the Queue.
*
*  @param joinable is a flag whether or not the dragon_queue_task_done() and dragon_queue_join() calls function (these calls are not shown below yet).
*
*  @param queue_attr is a pointer to Queue attributes or NULL to use default values.
*
*  @param policy is a pointer to the Policy structure to use.
*
*  @param queue is a pointer to the Queue descriptor to update.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_managed_queue_create(char * name, size_t maxsize, bool joinable, dragonQueueAttr_t * queue_attr,
                                            dragonPolicy_t * policy, dragonQueueDescr_t * queue)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Create a serialized descriptor of a Queue
*
*  Create a serialized descriptor of a Queue that can be shared with other processes. A process typical communicates
*  the data member of queue_serial by some means to another process. That process can then attach to the Queue
*  and get a valid dragonQueueDescr_t from it.
*
*  @param queue_descr is a pointer to the Queue descriptor.
*
*  @param queue_serial is pointer to the serialized descriptor to update.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_serialize(dragonQueueDescr_t * queue_descr, dragonQueueSerial_t * queue_serial)
{
    if (queue_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Queue descriptor cannot be NULL");

    if (queue_serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Serial descriptor cannot be NULL");

    /* @MCB NOTE: We could just cast the QueueSerial_t to a ChannelSer and just use that
           but this will make it easier later if we choose to add more bits and bobs to the queue
    */
    queue_serial->len = 0;
    queue_serial->data = NULL;

    dragonQueue_t * queue;
    dragonError_t err = _queue_from_descr(queue_descr, &queue);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Invalid queue descriptor");

    dragonChannelSerial_t ch_ser;
    err = dragon_channel_serialize(&queue->ch, &ch_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to serialize queue channel");

    queue_serial->len = ch_ser.len + sizeof(dragonULInt);
    queue_serial->data = malloc(queue_serial->len);
    dragonULInt * sptr = (dragonULInt *)queue_serial->data;
    *sptr = (dragonULInt)queue_descr->_idx;
    sptr++;

    memcpy(sptr, ch_ser.data, ch_ser.len);

    dragon_channel_serial_free(&ch_ser);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Query for a managed Queue
 *
 * Query Dragon for a managed Queue by the given name and then attach to it.
 *
 * @param name is a null-terminated name for the Queue to query for.
 *
 * @param queue is a pointer to the Queue descriptor to update.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t dragon_managed_queue_attach(char * name, dragonQueueDescr_t * queue)
{
    return DRAGON_NOT_IMPLEMENTED;
}

/**
 * @brief Attach to a Queue
 *
 * Attach to the Queue given by the serialized descriptor. Once attach, the process can call operations on the
 * Queue.
 *
 * @param queue_serial is pointer to the serialized descriptor of the Queue.
 *
 * @param queue_descr is a pointer to the Queue descriptor to update.
 *
 * @return
 *     DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t dragon_queue_attach(dragonQueueSerial_t * queue_serial, dragonQueueDescr_t * queue_descr)
{
    if (queue_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Queue descriptor cannot be NULL");

    if (queue_serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Serial descriptor cannot be NULL");

    dragonULInt * sptr = (dragonULInt *)queue_serial->data;
    dragonQ_UID_t q_uid = (dragonQ_UID_t)*sptr;
    sptr++;

    dragonError_t err = _queue_descr_from_q_uid(q_uid, queue_descr);
    if (err == DRAGON_SUCCESS)
        no_err_return(DRAGON_SUCCESS);

    dragonQueue_t * queue = malloc(sizeof(dragonQueue_t));

    dragonChannelSerial_t ch_ser;
    ch_ser.data = (uint8_t *)sptr;
    ch_ser.len = queue_serial->len;

    err = dragon_channel_attach(&ch_ser, &queue->ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to queue channel");

    err = dragon_channel_get_pool(&queue->ch, &queue->pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve queue pool");

    err = _init_channel_handles(queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error opening queue handles");

    queue_descr->_idx = q_uid;
    err = _add_umap_queue_entry(queue_descr, queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not add queue to umap");

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Destroy an unmanaged Queue
*
*  Directly destroy an unmanaged Queue, cleaning up all underlying resources. This can only be called by processes
*  on the same node as the Queue itself, with direct access to the memory it is contained in. This function will
*  return an error if the call is made on a remote Queue.
*
*  @param queue_descr is a pointer to the Queue descriptor of the Queue to destroy, which will not be usable upon return.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_destroy(dragonQueueDescr_t * queue_descr)
{
    if (queue_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Queue descriptor cannot be null");

    dragonQueue_t * queue;
    dragonError_t err = _queue_from_descr(queue_descr, &queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid queue descriptor");

    err = dragon_channel_destroy(&queue->ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not release underlying channel");

    err = dragon_umap_delitem(dg_queues, queue_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete queue from umap");

    free(queue);

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Destroy a managed Queue
*
*  Make a request to Dragon Global Services to destroy the Queue and all underlying resources. This can be called
*  by any process with a valid descriptor of the Queue, no matter the process's location on the system relative to
*  the Queue.
*
*  @param queue is a pointer to the Queue descriptor of the Queue to destroy, which will not be usable upon return.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_managed_queue_destroy(dragonQueueDescr_t * queue)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Detach from a Queue
*
*  Detach from the Queue. If the Queue is managed and reference counted, the count will not be decremented by
*  by this call. This can result in resource leaks, but can also be used to allow objects to persist outside the
*  lifetime of a set of processes. The Queue descriptor will not be usable after this call.
*
*  @param queue_descr is a pointer to the Queue descriptor to detach from.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_detach(dragonQueueDescr_t * queue_descr)
{
    if (queue_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid queue descriptor");

    dragonQueue_t * queue;
    dragonError_t err = _queue_from_descr(queue_descr, &queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Invalid queue descriptor");

    err = dragon_channel_detach(&queue->ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error detaching from queue channel");

    dragon_umap_delitem(dg_queues, queue_descr->_idx);

    free(queue);

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Detach from a managed Queue, decrementing the reference count if needed
*
*  Detach from a managed Queue. If the Queue is reference counted, the count will be decremented by
*  by this call. If the Queue is not managed, an error will be returned. The Queue descriptor will not be usable
*  after this call.
*
*  @param queue is a pointer to the Queue descriptor to detach from.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_managed_queue_detach(dragonQueueDescr_t * queue)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Put a data item to the Queue
*
*  Put an item with the given data to the Queue. A timeout can be specified. Zero timeout will try once to put the
*  the item and retur a timeout error if it fails. Specifying NULL means an infinite wait to
*  complete the operation.
*
*  @param queue is a pointer to the Queue descriptor to put data to.
*
*  @param ptr is a pointer to the data to put.
*
*  @param nbytes is the number of bytes to put.
*
*  @param timeout is a pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_put(dragonQueueDescr_t * queue_desc, void * ptr, size_t nbytes, const timespec_t * timeout)
{

    // Streams should be a queue of channels, each channel being opened and filled until closed and then "sent" to be read
    // The main queue should hold each of these "stream" channels as an item

    if (queue_desc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "queue cannot be NULL");

    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "item pointer cannot be NULL");

    dragonQueue_t * queue;
    dragonError_t err = _queue_from_descr(queue_desc, &queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get queue from descriptor");

    // @TODO: Keep a buffer around for small messages

    // Alloc space
    dragonMemoryDescr_t mem_buf;
    err = dragon_memory_alloc(&mem_buf, &queue->pool, nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to allocated memory");

    // Copy item
    void * mem_ptr;
    err = dragon_memory_get_pointer(&mem_buf, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get memory pointer");

    memcpy(mem_ptr, ptr, nbytes);

    dragonMessage_t msg;
    err = dragon_channel_message_init(&msg, &mem_buf, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to init message");

    //TODO: if timeout
    const timespec_t no_blocking = {0,0};
    err = dragon_chsend_send_msg(&queue->csend, &msg, NULL, &no_blocking);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to send message");

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Get a data item to the Queue
*
*  Get an item from the Queue. A timeout can be specified. Zero timeout will try once to put the
*  the item and retur a timeout error if it fails. Specifying NULL means an infinite wait to
*  complete the operation.
*
*  @param queue is a pointer to the Queue descriptor to put data to.
*
*  @param ptr is a pointer that will be updated to memory allocated on the heap and contains the data.
*
*  @param nbytes is the number of bytes in the returned data.
*
*  @param timeout is a pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_get(dragonQueueDescr_t * queue_desc, void ** ptr, size_t * nbytes, const timespec_t * timeout)
{
    if (queue_desc == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Queue descriptor cannot be NULL");

    dragonQueue_t * queue;
    dragonError_t err = _queue_from_descr(queue_desc, &queue);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find queue from descriptor");


    // Should there be a short buffer here for small messages?

    dragonMessage_t msg;
    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize message");

    // TODO: timeout
    const timespec_t no_blocking = {0,0};
    err = dragon_chrecv_get_msg_blocking(&queue->crecv, &msg, &no_blocking);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve next item from queue");

    dragonMemoryDescr_t mem_descr;
    err = dragon_channel_message_get_mem(&msg, &mem_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not access queue item memory");

    void * mem_ptr;
    err = dragon_memory_get_pointer(&mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve queue item memory pointer");

    err = dragon_memory_get_size(&mem_descr, nbytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve queue item size");

    // Allocate on the heap and copy payload over
    *ptr = malloc(*nbytes);
    memcpy(*ptr, mem_ptr, *nbytes);

    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy queue message");

    no_err_return(DRAGON_SUCCESS);
}


/** @brief Open a new stream to Put an item to a Queue
*
*  For non-contiguous items or items that cannot fit in memory to be placed on a Queue, the file-like interface to a
*  Queue allows a process to write an item across multiple operations. The operations between open and close of the
*  put stream make up a single logical put operation. This interface can be used with any serialization/encoder
*  strategy for complex data structures (e.g., Pickle, JSON encoding).
*
*  @param queue is a pointer to the Queue descriptor to put data to.
*
*  @param put_str is a pointer to the put stream to open.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_put_open(dragonQueueDescr_t * queue, dragonQueuePutStream_t * put_str)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Write data to a put stream on a Queue
*
*  Write a block of data into a stream encapsulating a put operation. Multiple write operations can be done on the
*  stream, which are logically assembled as a single item in the Queue. Specifying NULL for the
*  timeout value means an infinite wait to complete the operation.
*
*  @param put_str is a pointer to the opened put stream.
*
*  @param ptr is a pointer to the data to write.
*
*  @param nbytes is the number of bytes to write.
*
*  @param timeout is a pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_put_write(dragonQueuePutStream_t * put_str, void * ptr, size_t nbytes, const timespec_t * timeout)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Close a put stream on a Queue
*
*  This operation closes a put stream and completes the process of enqueuing the item. The put stream is not usable
*  after this call completes except for using it on another call to dragon_queue_put_open().
*
*  @param put_str is a pointer to the put stream to close.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_put_close(dragonQueuePutStream_t * put_str)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Open a new stream to Get an item from a Queue
*
*  For non-contiguous items or items that cannot fit in memory to be placed on a Queue, the file-like interface to a
*  Queue allows a process to write an item across multiple operations. The operations between open and close of the
*  put stream make up a single logical put operation. This interface can be used with any serialization/encoder
*  strategy for complex data structures (e.g., Pickle, JSON encoding). The get stream interface allows process pulling
*  an item from the Queue to deserialize or perform other processing on data sent with a write-stream.
*
*  @param queue is a pointer to the Queue descriptor to get data from.
*
*  @param get_str is a pointer to the get stream to open.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_get_open(dragonQueueDescr_t * queue, dragonQueueGetStream_t * get_str)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Get data from a Queue through a stream
*
*  Update the given pointer with to a block of memory read from the get stream. The caller can process this block
*  of memory before making another call to dragon_queue_get_read() to get more data. If there is not more data to be
*  read an error code is returned indicating end of file. The caller is responsible for freeing the memory returned
*  by this call.
*
*  @param get_str is a pointer to the opened get stream.
*
*  @param ptr is a pointer to update with to a block of memory read from the stream.
*
*  @param nbytes is a pointer to update with the number of bytes read.
*
*  @param timeout is a pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_get_read(dragonQueueGetStream_t * get_str, void ** ptr, size_t * nbytes, const timespec_t * timeout)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Get data from a Queue through a stream into a buffer
*
*  This call is the same as dragon_queue_get_read() except this call expects an existing buffer to be provided to
*  write data from the Queue. The caller provides the maximum size to read from the stream and is given the actual
*  number of bytes read.
*
*  @param get_str is a pointer to the opened get stream.
*
*  @param max_bytes is the maximum number of bytes to read from the stream as to not overflow ptr.
*
*  @param ptr is a pointer to memory to update with the next bock of bytes from the stream.
*
*  @param nbytes is a pointer to update with the number of bytes read.
*
*  @param timeout is a pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_get_readinto(dragonQueueGetStream_t * get_str, size_t max_bytes, void * ptr, size_t * nbytes, const timespec_t * timeout)
{
    return DRAGON_NOT_IMPLEMENTED;
}


/** @brief Close a get stream on a Queue
*
*  This operation closes a get stream and completes the process of enqueuing the item. The get stream is not usable
*  after this call completes except for using it on another call to dragon_queue_get_open().
*
*  @param get_str is a pointer to the get stream to close.
*
*  @return
*      DRAGON_SUCCESS or a return code to indicate what problem occurred.
*/
dragonError_t dragon_queue_get_close(dragonQueueGetStream_t * get_str)
{
    return DRAGON_NOT_IMPLEMENTED;
}
