#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "../_ctest_utils.h"

#define MFILE "channels_test"
#define M_UID 0
#define MSGSIZE 64

// In lieu of adding the header for host id:
dragonULInt dragon_host_id();

dragonError_t
create_pool(dragonMemoryPoolDescr_t * mpool)
{
    size_t mem_size = 1UL<<31;

    char * fname = util_salt_filename(MFILE);
    dragonError_t err = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create memory pool");
    free(fname);

    return SUCCESS;
}

dragonError_t
destroy_pool(dragonMemoryPoolDescr_t * mpool)
{
    dragonError_t err = dragon_memory_pool_destroy(mpool);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy memory pool");

    return SUCCESS;
}

dragonError_t
create_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    /* Create the Channel in the memory pool */
    dragonError_t err = dragon_channel_create(ch, 42, mpool, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create channel");

    return SUCCESS;
}

dragonError_t
destroy_channel(dragonChannelDescr_t * ch)
{
    dragonError_t err = dragon_channel_destroy(ch);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy channel");

    return SUCCESS;
}

dragonError_t
create_open_send_handle(dragonChannelDescr_t * ch, dragonChannelSendh_t * ch_sh)
{
    /* Create a send handle from the Channel */
    dragonError_t err = dragon_channel_sendh(ch, ch_sh, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create send handle");

    /* Open the send handle for writing */
    err = dragon_chsend_open(ch_sh);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open send handle");

    return SUCCESS;
}

dragonError_t
get_send_attr(dragonChannelSendh_t * ch_sh, dragonChannelSendAttr_t * attr)
{
    dragonError_t err = dragon_chsend_get_attr(ch_sh, attr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not retrieve send attributes");

    return SUCCESS;
}

dragonError_t
create_message(dragonMemoryPoolDescr_t * mpool, dragonMessage_t * msg)
{
    dragonMemoryDescr_t msg_buf;
    size_t dsize = MSGSIZE;

    dragonError_t err = dragon_memory_alloc(&msg_buf, mpool, dsize);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to allocate memory from pool");

    int * dptr;
    err = dragon_memory_get_pointer(&msg_buf, (void **)&dptr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to get pointer to message memory");

    for (int i = 0; i < dsize/sizeof(int); i++) {
        dptr[i] = i;
    }

    err = dragon_channel_message_init(msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create new message");

    return SUCCESS;
}

dragonError_t
destroy_message(dragonMessage_t * msg)
{
    dragonError_t err = dragon_channel_message_destroy(msg, true);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy message");

    return SUCCESS;
}

dragonError_t
check_send_gateway_message_structure(dragonGatewayMessage_t * gmsg, dragonChannelDescr_t * ch,
                                     dragonChannelSendAttr_t * attr, dragonULInt msg_hints, dragonULInt clientid)
{
    if (gmsg->msg_kind != DRAGON_GATEWAY_MESSAGE_SEND) {
        printf("Validation of GatewayMessage as send-type failed (got %i)\n", gmsg->msg_kind);
        return FAILED;
    }

    if (gmsg->target_hostid != dragon_host_id()) {
        printf("Validation of GatewayMessage hostid failed (got %lu, expected %lu)\n", gmsg->target_hostid,
               dragon_host_id());
        return FAILED;
    }

    if (gmsg->send_return_mode != attr->return_mode) {
        printf("Validation of GatewayMessage return mode failed (got %i, expected %i)\n", gmsg->send_return_mode,
               attr->return_mode);
        return FAILED;
    }

    // check deadline

    // check dest mem ser descr

    dragonMessageAttr_t mattr;
    dragonError_t err = dragon_channel_message_getattr(&gmsg->send_payload_message, &mattr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to get attributes from GatewayMessage message member");

    if (strncmp((char *)mattr.sendhid, (char *)attr->sendhid, 16) != 0) {
        printf("Validation of GatewayMessage sendhid failed (got %s, expected %s)\n", (char *)mattr.sendhid,
               (char *)attr->sendhid);
        return FAILED;
    }

    if (mattr.clientid != clientid) {
        printf("Validation of GatewayMessage clientid failed (got %lu, expected %lu)\n", mattr.clientid, clientid);
        return FAILED;
    }

    if (mattr.hints != msg_hints) {
        printf("Validation of GatewayMessage msg_hints failed (got %lu, expected %lu)\n", mattr.hints, msg_hints);
        return FAILED;
    }

    dragonMemoryDescr_t msg_mem;
    err = dragon_channel_message_get_mem(&gmsg->send_payload_message, &msg_mem);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to get memory from GatewayMessage message member");

    int * dptr;
    err = dragon_memory_get_pointer(&msg_mem, (void **)&dptr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to get pointer from GatewayMessage message member");

    size_t dsize = MSGSIZE;
    for (int i = 0; i < dsize/sizeof(int); i++) {
        if (i != dptr[i]) {
            printf("Validation of GatewayMessage message buffer failed at pos=%i (got %i, expected %i)\n", i, i,
                   dptr[i]);
            return FAILED;
        }
    }

    /* detach and reattach to the Channel */

    return SUCCESS;
}

int
main(int argc, char *argv[])
{
    dragonMemoryPoolDescr_t mpool;
    dragonChannelDescr_t channel;
    dragonChannelSendh_t ch_sh;
    dragonChannelSendAttr_t attr;
    dragonMessage_t msg;
    dragonGatewayMessage_t gmsg;
    dragonGatewayMessageSerial_t gmsg_ser;
    dragonGatewayMessage_t gmsg2;

    dragonError_t err = create_pool(&mpool);
    if (err != SUCCESS)
        return FAILED;

    err = create_channel(&mpool, &channel);
    if (err != SUCCESS)
        main_err_fail(err, "Error creating channel", pool_cleanup);

    err = create_open_send_handle(&channel, &ch_sh);
    if (err != SUCCESS)
        main_err_fail(err, "Error opening send handle", channel_cleanup);

    err = get_send_attr(&ch_sh, &attr);
    if (err != SUCCESS)
        main_err_fail(err, "Error acquiring send attributes", channel_cleanup);

    err = create_message(&mpool, &msg);
    if (err != SUCCESS)
        main_err_fail(err, "Error creating message", channel_cleanup);

    err = dragon_channel_gatewaymessage_send_create(&mpool, &msg, NULL, &channel, &attr, NULL, &gmsg);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to create GatewayMessage", message_cleanup);

    err = check_send_gateway_message_structure(&gmsg, &channel, &attr, 0UL, 0UL);
    if (err != SUCCESS)
        main_err_fail(err, "Error with gateway message structure", message_cleanup);

    err = dragon_channel_gatewaymessage_serialize(&gmsg, &gmsg_ser);
    if (err != SUCCESS)
        main_err_fail(err, "Error serializing the gateway message", message_cleanup);

    err = dragon_channel_gatewaymessage_attach(&gmsg_ser, &gmsg2);
    if (err != SUCCESS)
        main_err_fail(err, "Error attaching to serialized gateway message", message_cleanup);

    err = dragon_channel_gatewaymessage_serial_free(&gmsg_ser);
    if (err != SUCCESS)
        main_err_fail(err, "Error freeing serialized gateway message", message_cleanup);

    err = dragon_channel_gatewaymessage_detach(&gmsg2);
    if (err != SUCCESS)
        main_err_fail(err, "Error detaching from gateway message", message_cleanup);

    err = dragon_channel_gatewaymessage_destroy(&gmsg);
    if (err != SUCCESS)
        main_err_fail(err, "Error destroying the gateway message", message_cleanup);

message_cleanup:
    destroy_message(&msg);
channel_cleanup:
    destroy_channel(&channel);
pool_cleanup:
    destroy_pool(&mpool);

    if (err == SUCCESS)
        printf("GatewayMessage Test Complete - all tests passed.\n");
    else
        printf("GatewayMessage Test Complete - failed test (err = %i).\n", err);

    return TEST_STATUS;
}
