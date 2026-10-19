/*
 * Dragon Posix Message Queue communication API

    This creates a rendezvous style message queue
    where either the sender or receiver may create the
    queue first and the other will get the same queue
    regardless of order.

 * Copyright Cray HPE 2021
*/
#ifndef _DRAGON_PMSGQUEUE_H
#define _DRAGON_PMSGQUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <mqueue.h>
#include <stdbool.h>
#include <dragon/return_codes.h>

const size_t DRAGON_PMSGQUEUE_MAX_MSG_SIZE = 2048;
const size_t DRAGON_PMSGQUEUE_MAX_MSGS = 10;

typedef enum dragonPMsgQueueState_st {
    DRAGON_PMSGQUEUE_CLOSED = 0,
    DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING,
    DRAGON_PMSGQUEUE_OPEN_FOR_SENDING,
    DRAGON_PMSGQUEUE_ERROR,
    DRAGON_PMSGQUEUE_HANDSHAKE_ERROR
} dragonPMsgQueueState_t;

typedef struct mq_attr dragonPMsgQueueAttrs_t;

typedef struct dragonPMsgQueueHandle_st {
    mqd_t pmq_handle;
    char* qname;
    dragonPMsgQueueState_t state;
    char* last_error_msg;
} dragonPMsgQueueHandle_t;

dragonError_t
dragon_pmsgqueue_init_handle(dragonPMsgQueueHandle_t* handle);

dragonError_t
dragon_pmsgqueue_reset(dragonPMsgQueueHandle_t* handle, size_t timeout);

dragonError_t
dragon_pmsgqueue_create_recvq(dragonPMsgQueueHandle_t* handle, const char* name);

dragonError_t
dragon_pmsgqueue_create_sendq(dragonPMsgQueueHandle_t* handle, const char* name);

dragonError_t
dragon_pmsgqueue_close(dragonPMsgQueueHandle_t* handle, bool destroy);

dragonError_t
dragon_pmsgqueue_send(dragonPMsgQueueHandle_t* handle, const char* msg);

dragonError_t
dragon_pmsgqueue_recv(dragonPMsgQueueHandle_t* handle, char** msg, size_t timeout);

dragonError_t
dragon_pmsgqueue_stats(dragonPMsgQueueHandle_t* handle, dragonPMsgQueueAttrs_t* attrs);

dragonError_t
dragon_pmsgqueue_get_last_err_msg(dragonPMsgQueueHandle_t* handle, char** msg);

dragonError_t
dragon_pmsgqueue_get_queue_name(dragonPMsgQueueHandle_t* handle, char** name_ptr);

#ifdef __cplusplus
}
#endif

#endif
