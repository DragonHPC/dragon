#include <stdlib.h>
#include <stdio.h>

#include "err.h"
#include "_utils.h"
#include "logging.h"

// Useful for translating priority values into strings
static const char * dg_log_levels[] = {
    "[NOTSET]",
    "[DEBUG]",
    "[INFO]",
    "[WARN]",
    "[ERROR]",
    "[CRITICAL]"
};

static int
_lookup_priority_idx(const dragonLogPriority_t priority)
{
    // Translate priority enum to string table index
    // Enums are currently assigned to python logging level values, this may change in the future
    // @MCB: Consider this a workaround for now instead of building a macro table
    switch(priority) {
        case DG_NOTSET:   return 0;
        case DG_DEBUG:    return 1;
        case DG_INFO:     return 2;
        case DG_WARNING:  return 3;
        case DG_ERROR:    return 4;
        case DG_CRITICAL: return 5;

        default: return 0;
    }
}

static dragonError_t
_init_channel_handles(dragonLoggingDescr_t * logger)
{
    dragonChannelSendAttr_t send_attr;
    dragonChannelRecvAttr_t recv_attr;

    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Logging handle cannot be NULL");

    // Use idle wait policy for both send and recv handles
    dragonError_t err = dragon_channel_send_attr_init(&send_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create send attributes");

    err = dragon_channel_recv_attr_init(&recv_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create recv attributes");

    send_attr.wait_mode = DRAGON_IDLE_WAIT;
    recv_attr.wait_mode = DRAGON_IDLE_WAIT;

    // Create send and receive handles
    err = dragon_channel_sendh(&logger->ch, &logger->csend, &send_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create send handle");

    err = dragon_chsend_open(&logger->csend);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle");

    err = dragon_channel_recvh(&logger->ch, &logger->crecv, &recv_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create recv handle");

    err = dragon_chrecv_open(&logger->crecv);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open recv handle");

    no_err_return(DRAGON_SUCCESS);
}

// Internal function to retrieve the next log of given priority
static dragonError_t
_get_log(const dragonLoggingDescr_t * logger, const dragonLogPriority_t priority, dragonMessage_t * msg, const timespec_t * timeout)
{
    if (logger == NULL)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Logging handle cannot be NULL");

    if (msg == NULL)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Message cannot be NULL");

    // Init the message here to allow for better cleanup on error
    dragonError_t err = dragon_channel_message_init(msg, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to initialize message container");

    // If timeout is NULL, channels will set the default_timeout value
    // which is DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.
    err = dragon_chrecv_get_msg_blocking(&logger->crecv, msg, timeout);

    if (err == DRAGON_CHANNEL_EMPTY || err == DRAGON_TIMEOUT) {
        append_err_noreturn("No logs to retrieve");
        goto jmp_msg_cleanup;
    }

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not retrieve log");
        goto jmp_msg_cleanup;
    }

    dragonMemoryDescr_t mem_descr;
    err = dragon_channel_message_get_mem(msg, &mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Error retrieving message memory");
        goto jmp_msg_cleanup;
    }

    void * msg_ptr;
    err = dragon_memory_get_pointer(&mem_descr, &msg_ptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Error retrieving memory pointer for log entry");
        goto jmp_msg_cleanup;
    }

    dragonLogPriority_t log_priority = *(dragonLogPriority_t*)msg_ptr;
    if (log_priority < priority) {
        // Release message if not valid priority
        err = dragon_channel_message_destroy(msg, true);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not release memory on low priority message");

        // Return low priority error code
        no_err_return(DRAGON_LOGGING_LOW_PRIORITY_MSG);
    }

    no_err_return(DRAGON_SUCCESS);

jmp_msg_cleanup:
    dragon_channel_message_destroy(msg, true);
    no_err_return(err);
}


// Internal function to do pointer arithmetic and extract priority and string from log message
static dragonError_t
_unpack_priority_and_log_bytes(void* mem_ptr, size_t mem_size, dragonLogPriority_t *priority, void** log, size_t *log_len)
{
    if (mem_ptr == NULL) {
        err_return(DRAGON_INVALID_ARGUMENT, "mem_ptr cannot be NULL");
    }

    if (log == NULL) {
        err_return(DRAGON_INVALID_ARGUMENT, "log cannot be NULL");
    }

    // optionally assign the priority
    if (priority != NULL) {
        *priority = *(dragonLogPriority_t*)mem_ptr;
    }

    // calculate the log message location and copy it out
    void* log_ptr = mem_ptr + sizeof(dragonLogPriority_t);
    *log_len = mem_size - sizeof(dragonLogPriority_t);

    *log = malloc(*log_len);
    memcpy(*log, log_ptr, *log_len);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_logging_attr_init(dragonLoggingAttr_t * lattrs)
{
    if (lattrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Attributes struct cannot be NULL");

    dragonError_t err = dragon_channel_attr_init(&(lattrs->ch_attr));
    if (err != DRAGON_SUCCESS)
        err_return(err, "Unable to initialize logging channel attributes");

    lattrs->ch_attr.capacity = DRAGON_LOGGING_DEFAULT_CAPACITY;
    lattrs->mode = DRAGON_LOGGING_LOSSLESS;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Initialize the Logging channel to send/collect messages from varying processes/libraries
 *
 * @param mpool Pointer to the memory pool the channel should use
 * @param l_uid Unique identifier for the channel to be assigned
 * @param lattrs (Optional) Custom attributes for the Logger *NOT IMPLEMENTED*
 * @param logger Handle to the logger to be initialized
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_init(dragonMemoryPoolDescr_t * mpool, const dragonC_UID_t l_uid, dragonLoggingAttr_t * lattrs, dragonLoggingDescr_t * logger)
{
    if (mpool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "MemoryPool cannot be NULL");
    // @TODO: Optionally if mpool == NULL, we could create a default pool size based on the logger attributes?

    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Logging descriptor cannot be NULL");

    dragonError_t err;
    dragonLoggingAttr_t def_attr;
    if (lattrs == NULL) {
        err = dragon_logging_attr_init(&def_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Error initializing default logging attributes");

        lattrs = &def_attr;
    }

    logger->mode = lattrs->mode;

    if (logger->mode == DRAGON_LOGGING_LOSSLESS) {
        size_t lock_size = dragon_lock_size(DRAGON_LOGGING_DEFAULT_LOCK_TYPE);
        dragonMemoryDescr_t lock_mem;
        err = dragon_memory_alloc(&lock_mem, mpool, lock_size);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to allocate memory for lock");

        void * lock_ptr;
        err = dragon_memory_get_pointer(&lock_mem, &lock_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to get memory pointer for lock");

        err = dragon_lock_init(&logger->dlock, lock_ptr, DRAGON_LOGGING_DEFAULT_LOCK_TYPE);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to initialize logging lock");
    }

    err = dragon_channel_create(&logger->ch, l_uid, mpool, &(lattrs->ch_attr));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create channel");

    err = _init_channel_handles(logger);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error opening send and receive handles");

    err = dragon_memory_pool_descr_clone(&logger->mpool, mpool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not clone pool descriptor");

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Destroy logging channel and memory pool
 *
 * @param logger Handle to the logger to be destroyed
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_destroy(dragonLoggingDescr_t * logger, bool destroy_pool)
{
    // TODO: Destroy channel, destroy pool
    dragonError_t err = dragon_channel_destroy(&logger->ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy logger channel");

    // TODO: Do we need to clear the lock memory since it's allocated in this pool?
    if (destroy_pool) {
        err = dragon_memory_pool_destroy(&logger->mpool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not destroy logger channel");
    }
    // @MCB TODO: Anything else in the logger that needs clearing?
    return DRAGON_SUCCESS;
}

/**
 * @brief Serialize logging channel information for other processes to attach to
 *
 * @param logger Handle to the logger to be serialized
 * @param log_ser Serialized data output
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_serialize(const dragonLoggingDescr_t * logger, dragonLoggingSerial_t * log_ser)
{
    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "LoggingDescr cannot be NULL");

    if (log_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "LoggingSerial cannot be NULL");


    dragonChannelSerial_t ch_ser;
    dragonError_t err = dragon_channel_serialize(&logger->ch, &ch_ser);;
    if (err != DRAGON_SUCCESS)
        err_return(err, "Unable to serialize logging channel");


    log_ser->len = sizeof(size_t) + ch_ser.len + dragon_lock_size(DRAGON_LOGGING_DEFAULT_LOCK_TYPE) + sizeof(dragonLoggingMode_t);
    log_ser->data = malloc(log_ser->len);
    dragonULInt * sptr = (dragonULInt *)log_ser->data;

    // Copy in serialized channel data
    *sptr = ch_ser.len;
    sptr = (dragonULInt *) ((char *)sptr + sizeof(size_t));
    memcpy(sptr, ch_ser.data, ch_ser.len);
    sptr = (dragonULInt *) ((char *)sptr + ch_ser.len);

    // Release malloc since we have a copy now
    err = dragon_channel_serial_free(&ch_ser);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Unable to free serialized channel data");

    *sptr = (dragonULInt)logger->mode;
    sptr++;

    // get pointer to lock mem
    // store lock mem
    sptr += dragon_lock_size(DRAGON_LOGGING_DEFAULT_LOCK_TYPE);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Release local allocations for serialized data
 *
 * @param log_ser Serialized data to free
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_serial_free(dragonLoggingSerial_t * log_ser)
{
    if (log_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempting to free NULL serial descriptor");

    if (log_ser->data != NULL)
        free(log_ser->data);

    log_ser->data = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Attach to serialized logging channel
 *
 * @param log_ser Serialized logging channel data to attach
 * @param logger Handle to the logger
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_attach(const dragonLoggingSerial_t * log_ser, dragonLoggingDescr_t * logger,
                      dragonMemoryPoolDescr_t *mpool)
{
    if (log_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "serial data cannot be NULL");

    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "logging handle cannot be NULL");

    dragonULInt * sptr = (dragonULInt*)log_ser->data;
    dragonChannelSerial_t ch_ser;
    ch_ser.len = *(size_t *)sptr;
    sptr = (dragonULInt *) ((char *)sptr + sizeof(size_t));
    ch_ser.data = (uint8_t *)sptr;
    sptr = (dragonULInt *) ((char *)sptr + ch_ser.len);

    dragonError_t err = dragon_channel_attach(&ch_ser, &(logger->ch));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to logging channel");

    logger->mode = *(dragonLoggingMode_t*)sptr;
    sptr++;

    if (mpool == NULL) {
        err = dragon_channel_get_pool(&logger->ch, &logger->mpool);
    }
    else {
        err = dragon_memory_pool_descr_clone(&logger->mpool, mpool);
    }

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to retrieve logging memory pool");

    // Confirm the pool is local. If not, raise an error
    if (!dragon_memory_pool_is_local(&logger->mpool)) {
        append_err_return(DRAGON_CHANNEL_MEMORY_POOL_NONLOCAL, "Logging channel requires a local memory pool");
    }

    err = _init_channel_handles(logger);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error opening send and receive handles");

    return DRAGON_SUCCESS;
}

dragonError_t
dragon_logging_detach(dragonLoggingDescr_t * logger)
{
    // TODO: Is this even necessary to have?
    return DRAGON_SUCCESS;
}

/**
 * @brief Insert a message with a given priority and a string to the logging channel.
 *
 *  Allocates necessary managed memory to insert the message and its priority level.
 *  If the logging channel is at capacity, will retrieve the next available message, discard it, and try again.
 *  If the second send attempt fails, returns with an error.
 *  Currently does not block.
 *
 * @param logger Handle to the logger
 * @param priority Priority level of the message
 * @param log Message log as a string
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_put(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, void* log, size_t log_len)
{
    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Logging descriptor cannot be NULL");

    if (log == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Message cannot be NULL");

    dragonMemoryDescr_t msg_buf;
    dragonError_t err = dragon_memory_alloc(&msg_buf, &logger->mpool, sizeof(dragonLogPriority_t) + log_len);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not allocate message");

    void * msg_ptr;
    err = dragon_memory_get_pointer(&msg_buf, &msg_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pointer to memory.");

    *(dragonLogPriority_t*)msg_ptr = priority;
    msg_ptr += sizeof(dragonLogPriority_t);

    memcpy(msg_ptr, log, log_len);

    dragonMessage_t msg;
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not init message.");

    if (logger->mode == DRAGON_LOGGING_LOSSLESS) {

        // Send with blocking
        err = dragon_chsend_send_msg(&logger->csend, &msg, NULL, NULL);

    } else {

        /* @MCB:
           Changed to throw away newest message on full
           This retains the original error instead of flooding the log messages with
           endless errors, losing the root cause.
           Overall we need to revisit this, and potentially have some side-channel for
           "logging on logging" errors to let us know exactly when and where we got an overflow
           issue.
        */

        /*
        // Lock beforehand, this prevents race conditions for the "pop/push" pattern on full
        dragonError_t lock_err = dragon_lock(&logger->dlock);
        if (lock_err != DRAGON_SUCCESS)
            append_err_return(lock_err, "Failed to obtain logging lock");
        */

        // Send with no timeout
        // If channel is full, discard message and return error (checked down below)
        const timespec_t no_blocking = {0,0};
        err = dragon_chsend_send_msg(&logger->csend, &msg, NULL, &no_blocking);

        /*
        if (err == DRAGON_CHANNEL_FULL) {

            err = _get_log(logger, priority, &msg, NULL); // Pop a message and retry
            if (err != DRAGON_SUCCESS) {
                dragon_unlock(&logger->dlock);
                append_err_return(err, "Failed to get message from full channel");
            }

            err = dragon_channel_message_destroy(&msg, true); // Release that message
            if (err != DRAGON_SUCCESS) {
                dragon_unlock(&logger->dlock);
                append_err_return(err, "Could not release message memory");
            }

            // Re-init message with the buffer contents
            err = dragon_channel_message_init(&msg, &msg_buf, NULL);
            if (err != DRAGON_SUCCESS) {
                dragon_unlock(&logger->dlock);
                append_err_return(err, "Unable to re-initialize message");
            }

            // This err gets checked down below, no need to check it directly here
            err = dragon_chsend_send_msg(&logger->csend, &msg, NULL, &no_blocking);
        }

        lock_err = dragon_unlock(&logger->dlock);
        if (lock_err != DRAGON_SUCCESS)
            append_err_return(lock_err, "Failed to unlock");
        */

    }

    if (err != DRAGON_SUCCESS) {
        dragonError_t ret_err = err; // Store original error to return after destroying message
        err = dragon_channel_message_destroy(&msg, true);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not destroy message");

        append_err_return(ret_err, "Could not insert log into channel");
    }

    // if (msg_len > logger.attrs.bytes_per_block)
    err = dragon_channel_message_destroy(&msg, false); // Release local allocations, but leave message in block.
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not release memory");
    // TODO: if msg_buf size can fit into the channel block then free the allocation

    no_err_return(DRAGON_SUCCESS);
}

// Make this internal and have separate "log_get" and "log_get_blocking" calls so a thread can sit and wait for an entry to show up if desired?
// NOTE: USER IS EXPECTED TO HANDLE FREEING INTERNAL MALLOC ON MSG_OUT POINTER
/**
 * @brief Retrieve the next available message if it is at least *priority* level
 *
 *  Retrieve the next message if it is at least *priority*.  Otherwise return *DRAGON_LOGGING_LOW_PRIORITY_MSG*
 *  If successful, will allocate memory in *msg_out* to copy data into.
 *  User is responsbile for freeing this.
 *  *timeout* can be specified to use blocking behavior, otherwise pass NULL for non-blocking
 *
 * @param logger Handle to the logger
 * @param priority Minimum priority that the next message needs to meet to be returned
 * @param msg_out Pointer to allocate and copy message data into
 * @param timeout (Optional) How long to wait for the next message
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_get(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, void ** log, size_t* log_len, timespec_t * timeout)
{

    dragonMessage_t msg;
    size_t mem_size;
    void * mem_ptr;

    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "msg_out arg cannot be NULL");

    if (log == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "log arg cannot be NULL");

    if (log_len == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "log_len arg cannot be NULL");

    // Message is init'd and cleaned on fail inside _get_log
    dragonError_t err = _get_log(logger, priority, &msg, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve log message");

    err = dragon_memory_get_pointer(msg._mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error retrieving memory for log entry");

    err = dragon_memory_get_size(msg._mem_descr, &mem_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error retrieving memory size");

    err = _unpack_priority_and_log_bytes(mem_ptr, mem_size, NULL, log, log_len);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error getting log from message.");

    // Release dragon channel memory holding the message
    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to free log after retrieval");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Retrieve the next available message of at least *priority* level
 *
 *  Retrieve the next message of at least *priority*.  Will continue to retrieve messages until *priority* is satisfied or no further logs are available.
 *  If successful, will allocate memory in *msg_out* to copy data into.
 *  User is responsbile for freeing this.
 *  *timeout* can be specified to use blocking behavior, otherwise pass NULL for non-blocking
 *
 * @param logger Handle to the logger
 * @param priority Minimum priority that the next message needs to meet to be returned
 * @param log Pointer to allocate and copy message data into
 * @param log_len The log's length stored at location pointed to by log_len
 * @param timeout (Optional) How long to wait for the next message
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_get_priority(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, dragonLogPriority_t *actual_priority, void ** log, size_t* log_len, timespec_t * timeout)
{

    dragonMessage_t msg;
    void * mem_ptr;
    size_t mem_size;
    dragonError_t err;

    if (logger == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "msg_out arg cannot be NULL");

    if (log == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "log arg cannot be NULL");

    if (log_len == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "log_len arg cannot be NULL");

    // Loop until the channel is either empty, fails entirely, or we get a message
    do {
        err = _get_log(logger, priority, &msg, timeout);
    } while (err == DRAGON_LOGGING_LOW_PRIORITY_MSG);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve log message");

    err = dragon_memory_get_pointer(msg._mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error retrieving memory for log entry");

    err = dragon_memory_get_size(msg._mem_descr, &mem_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error retrieving memory size");

    err = _unpack_priority_and_log_bytes(mem_ptr, mem_size, actual_priority, log, log_len);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error getting log");

    // Release dragon channel memory holding the message
    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to free log after retrieval");

    return DRAGON_SUCCESS;
}


/**
 * @brief Retrieve the next available message of at least *priority* level and print it
 *
 *  Retrieve the next message of at least *priority*.
 *  Will continue to retrieve messages until *priority* is satisfied or no further logs are available.
 *  Upon success, print message to stdout.
 *  *timeout* can be specified to use blocking behavior, otherwise pass NULL for non-blocking
 *
 * @param logger Handle to the logger
 * @param priority Minimum priority that the next message needs to meet to be returned
 * @param msg_out Pointer to allocate and copy message data into
 * @param timeout (Optional) How long to wait for the next message
 *
 * @return DRAGON_SUCCESS or a Dragon Error code on failure
 **/
dragonError_t
dragon_logging_print(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, timespec_t * timeout)
{
    // @TODO: Change the signature to include a FILE pointer.  Just spits out to stdout for now.
    void * log;
    size_t log_len;
    dragonLogPriority_t log_priority;
    dragonError_t err = dragon_logging_get_priority(logger, priority, &log_priority, &log, &log_len, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to retrieve logs");

    int log_table = _lookup_priority_idx(log_priority);
    // TODO: Replace stdout with whatever file pointer was passed in.  If NULL default to stdout
    fprintf(stdout, "%s\t| ", dg_log_levels[log_table]);
    fwrite(log, 1, log_len, stdout);
    fprintf(stdout, "\n");

    // Release allocated log data
    free(log);
    return DRAGON_SUCCESS;
}


dragonError_t
dragon_logging_count(const dragonLoggingDescr_t * logger, uint64_t * count)
{
    return dragon_channel_message_count(&(logger->ch), count);
}
