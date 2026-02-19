#ifndef HAVE_DRAGON_LOGGING_H
#define HAVE_DRAGON_LOGGING_H

#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/global_types.h>
#include <dragon/return_codes.h>
#include "shared_lock.h"
#include <stdbool.h>


#define DRAGON_LOGGING_DEFAULT_CAPACITY 3000
#define DRAGON_LOGGING_DEFAULT_LOCK_TYPE DRAGON_LOCK_FIFO_LITE

typedef enum {
    DRAGON_LOGGING_LOSSLESS, // Block on full send
    DRAGON_LOGGING_FIRST, // Drop new messages when full
    DRAGON_LOGGING_LAST // Drop old messages when full /* NOT IMPLEMENTED */
} dragonLoggingMode_t;

typedef struct dragonLoggingDescr_st {
    dragonMemoryPoolDescr_t mpool;
    dragonChannelDescr_t ch;
    dragonChannelSendh_t csend;
    dragonChannelRecvh_t crecv;
    dragonLock_t dlock;
    dragonLoggingMode_t mode;
} dragonLoggingDescr_t;

// NOTE: Currently unused, just an idea
/*
typedef struct dragonLog_st {
    int priority; // Should be an enum probably
    char * msg;
} dragonLog_t;
*/

// These levels should map to Python's logging module
typedef enum {
    DG_NOTSET = 0,
    DG_DEBUG = 10,
    DG_INFO = 20,
    DG_WARNING = 30,
    DG_ERROR = 40,
    DG_CRITICAL = 50
} dragonLogPriority_t;


// What kind of attributes would be useful?
// Max log size?  Number of logs to hold onto?  Minimum priority setting?
typedef struct dragonLoggingAttr_st {
    dragonChannelAttr_t ch_attr;
    dragonLoggingMode_t mode;
} dragonLoggingAttr_t;

// This needs to be a proper structure due to locks and etc, this is no longer just a channel wrap
typedef struct dragonLoggingSerial_st {
    size_t len;
    uint8_t * data; // Contains channel serializer, lock pointer, and logging mode
} dragonLoggingSerial_t;

// Creates the underlying channel and opens the send/recv handles
// Should eventually take an optional `dragonLoggingAttr_t` param to allow tuning
dragonError_t
dragon_logging_init(dragonMemoryPoolDescr_t * mpool, const dragonC_UID_t l_uid, dragonLoggingAttr_t * lattrs, dragonLoggingDescr_t * logger);

// Serialize a binary blob for other processes to attach to
dragonError_t
dragon_logging_serialize(const dragonLoggingDescr_t * logger, dragonLoggingSerial_t * log_ser);

dragonError_t
dragon_logging_serial_free(dragonLoggingSerial_t * log_ser);

dragonError_t
dragon_logging_destroy(dragonLoggingDescr_t * logger, bool destroy_pool);

// Extract and attach to the channel and pool, populate logger struct
dragonError_t
dragon_logging_attach(const dragonLoggingSerial_t * log_ser, dragonLoggingDescr_t * logger, dragonMemoryPoolDescr_t *pool_descr);

dragonError_t
dragon_logging_attr_init(dragonLoggingAttr_t * lattr);

dragonError_t
dragon_logging_put(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, void* log, size_t log_len);

dragonError_t
dragon_logging_get(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, void ** log, size_t* log_len, timespec_t * timeout);

dragonError_t
dragon_logging_get_priority(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, dragonLogPriority_t *actual_priority, void ** log, size_t* log_len, timespec_t * timeout);

// Should later have a FILE * argument to print to arbitrary locations.  Current just stdout
dragonError_t
dragon_logging_print(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, timespec_t * timeout);

dragonError_t
dragon_logging_count(const dragonLoggingDescr_t * logger, uint64_t * count);

/* TODOs */
// Rename priority to msg_type (or severity level, etc)
// Possibly have one large logging channel for most components, but allow for individual components to have their own logging
// Add a log_flush() function to retrieve all logs out of a given channel (add a simple lock to the logger for this?)
// Possibly have a separate log_filter function vs a simple priority filter in log_get
// How to handle performance data logging?  Should likely be distinct from regular logging messages somehow


#endif
