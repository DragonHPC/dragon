/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_GLOBAL_TYPES_H
#define HAVE_DRAGON_GLOBAL_TYPES_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_UUID_NELEM 16
#define DRAGON_UUID_SIZE (DRAGON_UUID_NELEM * sizeof(uint8_t))

/* standard types for use across the Dragon stack */
typedef uint64_t dragonRT_UID_t;
typedef uint64_t dragonC_UID_t;
typedef uint64_t dragonP_UID_t;
typedef uint64_t dragonM_UID_t;
typedef uint64_t dragonQ_UID_t;
typedef uint64_t dragonULInt;
typedef uint32_t dragonUInt;
typedef uint8_t dragonUUID[DRAGON_UUID_NELEM];
typedef struct timespec timespec_t;
typedef struct timeval timeval_t;

/* a few global constants */
/* This following strings must be identical in facts.py */
#define DRAGON_NUM_GW_ENV_VAR "DRAGON_NUM_GW_CHANNELS_PER_NODE"
#define DRAGON_DEFAULT_PD_VAR "DRAGON_DEFAULT_PD"
#define DRAGON_INF_PD_VAR "DRAGON_INF_PD"

/**
 * @brief Wait Mode constants
 *
*  The mode of operation to use when waiting for an event on a resource.
*  Idle waiting is appropriate for long waiting periods, lower power consumption,
*  and when processor cores are over-subscribed. Spin waiting is appropriate for
*  shorter duration waits and when latency and throughput is more important.
**/

typedef enum dragonWaitMode_st {
    DRAGON_IDLE_WAIT = 0, /*!< Lower power consumption with a litte more wakeup overhead. */
    DRAGON_SPIN_WAIT = 1, /*!< Minimal wakeup overhead. More power consumption and contention. */
    DRAGON_ADAPTIVE_WAIT = 2 /*!< Perhaps the best of both worlds. */
} dragonWaitMode_t;

static const dragonWaitMode_t DRAGON_DEFAULT_WAIT_MODE = DRAGON_ADAPTIVE_WAIT;

/** Return From Send When Constants
 *
 *  These constants provide the return behavior of sending messages.
 *  For clarity, the differnt behaviors are described in more detail
 *  here.
 *
 *  DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY: return as soon as possible.
 *  The source buffer can not be reused safely until some other
 *  user-coded sychronization ensures the message was received by another
 *  process. The source buffer can never be reused by the sender when
 *  transfer-of-ownership is specified on the send.
 *
 *  DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED: return as soon as the
 *  source buffer can be reused by the sender, except when
 *  transfer-of-ownership is specified on the send.
 *
 *  DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED: return as soon as the
 *  message is deposited into the destination Channel.  The source
 *  buffer can be reused by the sender except when transfer-of-ownership
 *  is specified on the send.
 *
 *  DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED: return as soon as the
 *  message is received by some process out of the target Channel.
 *  The source buffer can be reused by the sender except when
 *  transfer-of-ownership is specified on the send.
 **/

typedef enum dragonChannelSendReturnWhen_st {
    DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY,
    DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED,
    DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED,
    DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED,
    DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE
} dragonChannelSendReturnWhen_t;

/**
 * @brief Constants indicating a type of channel operation.
 *
 * Constants for send_msg, get_msg and poll channels operations. Currently,
 * these are used to help selet a gateway index for a channel operation,
 * with the constant's value specifying an offset into a gateway group.
 **/

typedef enum dragonChannelOpType_st {
    DRAGON_OP_TYPE_SEND_MSG = 0,
    DRAGON_OP_TYPE_GET_MSG,
    DRAGON_OP_TYPE_POLL
} dragonChannelOpType_t;

/**
 * @brief This is the type of the release function for dragon waiting.
 *
 * This function type is the type of a function that may be supplied to certain
 * api calls when waiting either directly or indirectly on a bcast object. The
 * purpose of a function implementing this prototype is to allow the release of a
 * locking mechanism when a process becomes a waiter. In most cases, without this
 * mechanism there would be a window between checking some value and becoming a waiter
 * that cannot be avoided without some additional synchronization primitive. This
 * release function can be used in those circumstances to allow the synchronization
 * mechanism to remain locked until the process has become an official waiter of the bcast object.
 * For instance, a dragon lock may be released once the bcast object is officially waiting.
 * The only restriction is the function must be a function of one argument. However, that
 * argument could be a pointer to a struct if multiple args were needed.
 */

typedef void (*dragonReleaseFun)(void* unlock_arg);

#define DRAGON_CHANNEL_DEFAULT_GW_ENV_PREFIX "DRAGON_GW"

#ifdef __cplusplus
}
#endif

#endif
