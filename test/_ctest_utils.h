#ifndef __CTEST_UTILS_H
#define __CTEST_UTILS_H


// Header file so C tests all use the same return codes and boilerplate functions

#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/return_codes.h>

#include <unistd.h> // for getpid()

/* If your test needs to use something other than the default defines here, undef them after including this header
   e.g.
   #include "../_ctest_utils.h"
   #undef NMSGS
   #define NMSGS 100000
*/

#define SUCCESS DRAGON_SUCCESS
#define FAILED DRAGON_FAILURE

/* Default values for channels */
#define NMSGS 100
#define MSGWORDS 4096
#define B_PER_MBLOCK 1024

/* Misc used frequently */
#define BILL 1000000000L

// General TEST_STATUS value, used in main_err_fail so declare here even if unused in the actual test
static dragonError_t TEST_STATUS = SUCCESS;

// Print latest error and return FAILED
#define err_fail(derr, msg) ({                                          \
            char * errstr = dragon_getlasterrstr();                     \
            const char * errcode = dragon_get_rc_string(derr);          \
            printf("TEST: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
            return FAILED;                                              \
        })

// Same as err_fail but set TEST_STATUS to FAILED and jump to a cleanup goto
#define main_err_fail(derr, msg, jmp) ({                                \
            char * errstr = dragon_getlasterrstr();                     \
            const char * errcode = dragon_get_rc_string(derr);          \
            printf("TEST_MAIN: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
            TEST_STATUS = FAILED;                                       \
            goto jmp;                                                   \
        })

// For generic fails that need cleanup but do not have a dragon error code
#define jmp_fail(msg, jmp) ({                   \
            printf("%s\n", msg);                \
            TEST_STATUS = FAILED;               \
            goto jmp;                           \
        })

/* ++++++++++++ Utility Functions ++++++++++++ */
char *
util_salt_filename(char * fname)
{
    char * buf = (char*)malloc(64); // Arbitrary length but 64 should be enough for filenames
    snprintf(buf, 64, "%s_%d", fname, getpid());
    return buf;
}

dragonError_t
util_create_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch, int c_uid, int n_msgs, int b_per_mblock)
{
    /* Create a Channel attributes structure so we can tune the Channel */
    dragonChannelAttr_t cattr;
    dragon_channel_attr_init(&cattr);

    /* Set the Channel capacity to our message limit for this test */
    cattr.capacity = (n_msgs > 0) ? n_msgs : NMSGS;
    cattr.bytes_per_msg_block = (b_per_mblock > 0) ? b_per_mblock : B_PER_MBLOCK;

    /* Create the Channel in the memory pool */
    dragonError_t derr = dragon_channel_create(ch, c_uid, mpool, &cattr);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Could not create channel");

    return DRAGON_SUCCESS;
}

dragonError_t
util_create_open_send_recv_handles(dragonChannelDescr_t * ch,
                                   dragonChannelSendh_t * csend, dragonChannelSendAttr_t * sattr,
                                   dragonChannelRecvh_t * crecv, dragonChannelRecvAttr_t * rattr)
{
    /*
      ---- NOTE ----
      Provide NULL for Send/Recv attrs to use default values
    */

    dragonError_t derr;
    if (csend != NULL) {
        /* Create a send handle from the Channel */
        derr = dragon_channel_sendh(ch, csend, sattr);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not create send handle");

        /* Open the send handle for writing */
        derr = dragon_chsend_open(csend);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not open send handle");
    }

    if (crecv != NULL) {
        /* Create a receive handle from the Channel */
        derr = dragon_channel_recvh(ch, crecv, rattr);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not create recv handle");

        /* Open the receive handle */
        derr = dragon_chrecv_open(crecv);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not open recv handle");
    }

    return DRAGON_SUCCESS;
}

dragonError_t
util_close_send_recv_handles(dragonChannelSendh_t * csend, dragonChannelRecvh_t * crecv)
{
    dragonError_t derr;
    if (csend != NULL) {
        derr = dragon_chsend_close(csend);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to close send handle");
    }

    if (crecv != NULL) {
        derr = dragon_chrecv_close(crecv);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to close recv handle");
    }

    return DRAGON_SUCCESS;
}


#endif // __CTEST_UTIlS_H
