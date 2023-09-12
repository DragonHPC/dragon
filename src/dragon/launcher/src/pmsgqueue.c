/*
 * Dragon Posix Message Queue communication API
*/
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include <sys/types.h>

#include "_pmsgqueue.h"
#include "err.h"

#define HANDSHAKE "PMsgQueueHandShake11235"

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

void
_set_error(dragonPMsgQueueHandle_t* handle, const char* msg)
{
    int len = strlen(msg);
    handle->last_error_msg = malloc(len+1);
    handle->state = DRAGON_PMSGQUEUE_ERROR;
    strcpy(handle->last_error_msg, msg);
}

dragonError_t
_open(dragonPMsgQueueHandle_t* handle, const char* name, int oflag)
{
    char buffer[100];

    if (handle->state != DRAGON_PMSGQUEUE_CLOSED) {
        if (handle->last_error_msg != NULL) {
            no_err_return(DRAGON_PMSGQUEUE_CLOSE_MUST_BE_CALLED_FIRST);
        }

        if (handle->qname != NULL) {
            _set_error(handle, "Close must be called on handle before re-opening.");
            no_err_return(DRAGON_PMSGQUEUE_CLOSE_MUST_BE_CALLED_FIRST);
        }
    }

    // initial state only. It will change once opened successfully.
    handle->state = DRAGON_PMSGQUEUE_CLOSED;
    handle->qname = NULL;
    handle->last_error_msg = NULL;

    if (name == NULL) {
        _set_error(handle, "The name argument cannot be NULL");
        no_err_return(DRAGON_PMSGQUEUE_INVALID_NAME);
    }

    if (name[0] != '/') {
        _set_error(handle, "The name argument must begin with a / to be a valid name.");
        no_err_return(DRAGON_PMSGQUEUE_INVALID_NAME);
    }

    int msg_max_size;
    FILE* f = fopen("/proc/sys/fs/mqueue/msgsize_max","r");
    int found = fscanf(f, "%d", &msg_max_size);
    if (found == 0) {
        _set_error(handle, "Failed to read maximum mqueue message size from /proc/sys/fs/mqueue/msgsize_max.");
        no_err_return(DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR);
    }

    if (msg_max_size < DRAGON_PMSGQUEUE_MAX_MSG_SIZE) {
        _set_error(handle, "System configured with too small mqueue maximum message size.");
        no_err_return(DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR);
    }

    fclose(f);

    int max_msgs;
    f = fopen("/proc/sys/fs/mqueue/msg_max","r");
    found = fscanf(f, "%d", &max_msgs);
    if (found == 0) {
        _set_error(handle, "Failed to read maximum mqueue number of messages from /proc/sys/fs/mqueue/msg_max.");
        no_err_return(DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR);
    }

    if (max_msgs < DRAGON_PMSGQUEUE_MAX_MSGS) {
        _set_error(handle, "System configured with too small mqueue maximum number of messages.");
        no_err_return(DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR);
    }

    fclose(f);

    struct mq_attr attrs;
    attrs.mq_msgsize = DRAGON_PMSGQUEUE_MAX_MSG_SIZE;
    attrs.mq_maxmsg = DRAGON_PMSGQUEUE_MAX_MSGS;
    attrs.mq_flags = 0;
    attrs.mq_curmsgs = 0;

    errno = 0;
    handle->pmq_handle = mq_open(name, oflag, S_IRUSR|S_IWUSR, &attrs);

    if (errno != 0) {

        handle->state = DRAGON_PMSGQUEUE_ERROR;

        if (errno == EEXIST) {
            _set_error(handle, "There was a permission violation on the open.");
            no_err_return(DRAGON_PMSGQUEUE_PERMISSION_VIOLATION);
        }

        if (errno == ENAMETOOLONG) {
            _set_error(handle, "The given name was too long.");
            no_err_return(DRAGON_PMSGQUEUE_INVALID_NAME);
        }

        sprintf(buffer,"The PMsgQueue creation failed with ERRNO=%d",errno);
        _set_error(handle, buffer);

        no_err_return(DRAGON_PMSGQUEUE_UNSPECIFIED_ERROR);
    }

    handle->qname = malloc(strlen(name)+1);
    strcpy(handle->qname, name);

    handle->last_error_msg = NULL;
    no_err_return(DRAGON_SUCCESS);
}

/******************************
    BEGIN USER API
*******************************/

/** @brief Initialize the handle
 *
 *  This API provides a rendezvous style message queue based on the POSIX mqueue. It
 *  requires that the mqueue server has been started (normally is) and that the system
 *  support POSIX message queues.
 *
 *  This function is called to initialize a handle. It only needs (should) be called
 *  once per handle.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_init_handle(dragonPMsgQueueHandle_t* handle)
{
    handle->state = DRAGON_PMSGQUEUE_CLOSED;
    handle->qname = NULL;
    handle->last_error_msg = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Reset the queue
 *
 *  Calling this intializes the queue to empty. This must be called by both a
 *  receiver and a sender in any order. The receiver will block on its call to
 *  init until the reset is complete. Only one sender/receiver pair should call
 *  this at any given time. The result is to empty the queue of any extra messages.
 *
 *  This is needed because a PMsgQueue may have left-over messages in it from
 *  a previous use and the pair of reset calls by sender and receiver executes
 *  a handshake to remove any left-overs from the queue.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param timeout Only relevent for the receiver. The timeout is the
 *  number of seconds to wait. The sender does not block, so timeout is
 *  ignored for senders.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_reset(dragonPMsgQueueHandle_t* handle, size_t timeout)
{
    dragonError_t rc;

    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_SENDING) {
        rc = dragon_pmsgqueue_send(handle, HANDSHAKE);

        if (rc != DRAGON_SUCCESS)
            handle->state = DRAGON_PMSGQUEUE_HANDSHAKE_ERROR;

        no_err_return(DRAGON_SUCCESS);
    }

    if (handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING) {
        char * msg = "";
        bool handshaking = true;
        while (handshaking) {
            /* while technically, we may wait for longer than the
               timeout, the queue will have some left-over messages
               which will be read first without waiting, then it will
               wait once on the timeout for the handshake if it is not
               there immediately */
            rc = dragon_pmsgqueue_recv(handle, &msg, timeout);
            if (rc == DRAGON_TIMEOUT)
                err_return(rc, "PMsgQueue receiver timed out on handshake during reset.");

            if (rc != DRAGON_SUCCESS)
                err_return(rc, "Error while resetting PMsgQueue");

            if (!strcmp(HANDSHAKE, msg))
                handshaking = false;
        }

        no_err_return(DRAGON_SUCCESS);
    }

    err_return(DRAGON_INVALID_OPERATION, "The PMsgQueue has not been opened for sending or receiving.");
}

/** @brief Create a Receive Queue
 *
 *  Creates a receive queue with the given name. The name must begin with a /
 *  and does NOT include the /dev/mqueue prefix used by POSIX. This call sets
 *  up the handle to be in receive mode for the given queue name.
 *
 *  The handle must be in the proper state to call this function. Either close
 *  or init_handle must have just been called. If the handle was used earlier
 *  in the program, call close to put it in the proper state. If this is the first
 *  use of the handle, call init_handle.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param name A pointer to a null-terminated string used for the queue name.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_create_recvq(dragonPMsgQueueHandle_t* handle, const char* name)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    dragonError_t rc = _open(handle, name, O_RDONLY | O_CREAT);

    if (rc == DRAGON_SUCCESS)
        handle->state = DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING;

    // else invalid state is already set.

    return rc;
}

/** @brief Create a Send Queue
 *
 *  Creates a send queue with the given name. The name must begin with a /
 *  and does NOT include the /dev/mqueue prefix used by POSIX. This call sets
 *  up the handle to be in send mode for the given queue name.
 *
 *  The handle must be in the proper state to call this function. Either close
 *  or init_handle must have just been called. If the handle was used earlier
 *  in the program, call close to put it in the proper state. If this is the first
 *  use of the handle, call init_handle.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param name A pointer to a null-terminated string used for the queue name.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_create_sendq(dragonPMsgQueueHandle_t* handle, const char* name)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    dragonError_t rc = _open(handle, name, O_WRONLY | O_CREAT);

    if (rc == DRAGON_SUCCESS) {
        handle->state = DRAGON_PMSGQUEUE_OPEN_FOR_SENDING;
    }

    return rc;
}

/** @brief Close a PMsgQueue
 *
 *  Closes a PMSgQueue and puts the handle in the proper state to open a queue
 *  later.
 *
 *  NOTE: The API does malloc data and this close function cleans up by freeing
 *  any remaining allocated memory. So to prevent memory leaks, call close after
 *  calling any other API calls except init_handle and get_last_error_msg.
 *
 *  NOTE: Closing a queue does not remove it unless destroy is true.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param destroy A boolean indicating whether the queue should be removed
 *  from the mqueue server.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_close(dragonPMsgQueueHandle_t* handle, bool destroy)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    // close works by definition. So errors while closing are ignored.
    if (handle->state != DRAGON_PMSGQUEUE_CLOSED) {
        mq_close(handle->pmq_handle);
        handle->state = DRAGON_PMSGQUEUE_CLOSED;
        if (destroy) {
            if (handle->qname != NULL) {
                mq_unlink(handle->qname);
            }
        }
    }

    if (handle->qname != NULL) {
            free(handle->qname);
            handle->qname = NULL;
    }

    if (handle->last_error_msg != NULL) {
            free(handle->last_error_msg);
            handle->last_error_msg = NULL;
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Send a message
 *
 *  Deposit a message (i.e. null-terminated string) into the queue.
 *  This function can block if the maximum number of messages were
 *  exceeded.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param msg A pointer to a null-terminated string.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_send(dragonPMsgQueueHandle_t* handle, const char* msg)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The message cannot be NULL");

    char buffer[100];
    if (handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING) {
        _set_error(handle, "Attempting to send using a receive handle.");
        no_err_return(DRAGON_PMSGQUEUE_SEND_TO_RECVQ);
    }

    if (handle->state != DRAGON_PMSGQUEUE_OPEN_FOR_SENDING) {
        _set_error(handle, "The PMsgQueue is not open.");
        no_err_return(DRAGON_PMSGQUEUE_NOT_OPEN);
    }

    size_t len = strlen(msg);

    if (len > DRAGON_PMSGQUEUE_MAX_MSG_SIZE) {
        _set_error(handle, "The maximum message size is exceeded.");
        no_err_return(DRAGON_PMSGQUEUE_EXCEEDS_MAX_MSG_SIZE);
    }

    errno = 0;

    int rc = mq_send(handle->pmq_handle, msg, len+1, 0);

    if (rc != 0) {
        sprintf(buffer, "Cannot send. ERRNO was %d", errno);
        _set_error(handle,buffer);
        no_err_return(DRAGON_PMSGQUEUE_CANNOT_SEND);
    }

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Receive a message
 *
 *  Retrieve a null-terminated string from the queue. This function
 *  will block if no message is currently available.
 *
 *  The space msg points to after this call is dynamically allocated
 *  and must be freed by the user code.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param msg A pointer to a pointer variable that will be set to the
 *  retrieved message.
 *  @param timeout The number of seconds to wait before timing out. If
 *  timeout=0 then it will wait indefinitely for a message.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_recv(dragonPMsgQueueHandle_t* handle, char** msg, size_t timeout)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The message pointer cannot be NULL");

    char buffer[max(DRAGON_PMSGQUEUE_MAX_MSG_SIZE+1, 100)];

    *msg = NULL; // start with it null.

    if (handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_SENDING) {
        _set_error(handle, "Attempting to recv using a send handle.");
        no_err_return(DRAGON_PMSGQUEUE_RECV_FROM_SENDQ);
    }

    if (handle->state != DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING) {
        _set_error(handle, "The PMsgQueue is not open.");
        no_err_return(DRAGON_PMSGQUEUE_NOT_OPEN);
    }

    errno = 0;
    int len = 0;

    if (timeout!=0) {
        struct timespec time;
        /* We must use CLOCK_REALTIME here since that is required below */
        clock_gettime(CLOCK_REALTIME, &time);
        time.tv_sec += timeout;
        time.tv_nsec = 0;

        len = mq_timedreceive(handle->pmq_handle, buffer, DRAGON_PMSGQUEUE_MAX_MSG_SIZE, 0, &time);

        if (errno == ETIMEDOUT) {
            _set_error(handle, "Receive timeout");
            no_err_return(DRAGON_TIMEOUT);
        }

    } else {
        len = mq_receive(handle->pmq_handle, buffer, DRAGON_PMSGQUEUE_MAX_MSG_SIZE, 0);
    }

    if (len < 0) {
        sprintf(buffer, "Cannot recv. ERRNO was %d", errno);
        _set_error(handle,buffer);
        no_err_return(DRAGON_PMSGQUEUE_CANNOT_RECV);
    }

    // make certain it is null-terminated. This is likely unnecessary (see send).
    if (buffer[len-1] != '\0') {
        buffer[len] = '\0';
        len += 1;
    }

    *msg = malloc(len);
    strcpy(*msg, buffer);

    handle->last_error_msg = NULL;
    no_err_return(DRAGON_SUCCESS);

}

/** @brief Retrieve statistics from the queue
 *
 *  This call gets statustics from the queue and stores them in the
 *  attrs structure. The attrs structure has the format of mq_attr
 *  from POSIX. See mq_attr documentation for a description of fields.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param attrs A pointer to a dragonPMsgQueueAttrs_t variable. This
 *  type is a typedef for the mq_attr structure of POSIX.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_stats(dragonPMsgQueueHandle_t* handle, dragonPMsgQueueAttrs_t* attrs)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attrs cannot be NULL");

    char buffer[100];

    if (handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_RECEIVING ||
        handle->state == DRAGON_PMSGQUEUE_OPEN_FOR_SENDING) {
            errno = 0;
            int rc = mq_getattr(handle->pmq_handle, attrs);

            if (rc != 0) {
                sprintf(buffer, "Cannot get stats. ERRNO was %d", errno);
                _set_error(handle,buffer);
                no_err_return(DRAGON_PMSGQUEUE_UNSPECIFIED_ERROR);
            }

            handle->last_error_msg = NULL;
            no_err_return(DRAGON_SUCCESS);
        }

    _set_error(handle, "PMsgQueue is not open so cannot get stats\n");
    no_err_return(DRAGON_PMSGQUEUE_NOT_OPEN);
}

// Upon getting the last error message, it is the responsibility of the
// client to free it.

/** @brief Get the most recent error message
 *
 *  This call returns the most recent error message from the queue.
 *  If called and the return type is DRAGON_SUCCESS, the user
 *  is responsible for freeing the message which was dynamically allocated
 *  when created.
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param msg A pointer to a pointer variable that will point to the
 *  retrieved message. If there was no message, then it will point to NULL.
 *
 *  @return A dragonError_t return code.
 */
dragonError_t
dragon_pmsgqueue_get_last_err_msg(dragonPMsgQueueHandle_t* handle, char** msg)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (msg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The message pointer cannot be NULL");

    if (handle->last_error_msg == NULL) {
        *msg = NULL;
        no_err_return(DRAGON_PMSGQUEUE_NO_ERR_MSG_AVAILABLE);
    }

    *msg = handle->last_error_msg;
    handle->last_error_msg = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Get the queue name from the handle
 *
 *  Returns the queue name from the handle
 *
 *  @param handle A pointer to struct of type dragonPMsgQueueHandle_t.
 *  @param name_ptr A pointer to a pointer variable that will point to the
 *  retrieved queue name.
 *
 *  @return A dragonError_t return code.
 */

dragonError_t
dragon_pmsgqueue_get_queue_name(dragonPMsgQueueHandle_t* handle, char** name_ptr)
{
    if (handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle cannot be NULL");

    if (name_ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The name pointer address cannot be NULL");

    *name_ptr = handle->qname;

    no_err_return(DRAGON_SUCCESS);
}
