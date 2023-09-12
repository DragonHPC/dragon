#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <dragon/utils.h>
#include <unistd.h>
#include <sys/wait.h>

#include "../_ctest_utils.h"

#define MFILE "channels_test"
#define M_UID 0
#define CH_CAPACITY 10
#define B_PER_MBLOCK 1024
#define CHANNELS_COUNT 1
#define FAKE_MUID 9999999
#define FAKE_CUID 9999999
#define FAKE_HOSTID 1

#undef MSGWORDS
#define MSGWORDS 256


void
alter_channel_ser_descriptor(size_t* ser_data_arr, size_t m_uid, size_t c_uid, size_t hostid)
{
    /* The following is sensitive to the serialization of channels, memory, and
       memory pools. If anything changes, this code must be fixed. */
    ser_data_arr[0] = c_uid; /* c_uid of channel */
    /* ser_data_arr[1] is the length of the memory serialized data */
    ser_data_arr[2] = m_uid; /* m_uid of pool */
    ser_data_arr[3] = hostid; /* hostid of pool, channel, memory */
}

dragonError_t
create_pool(dragonMemoryPoolDescr_t* mpool)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    printf("Allocate pool %lu B\n", mem_size);


    char * fname = util_salt_filename(MFILE);
    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create pool");

    free(fname);

    return SUCCESS;
}

void
fill_msg(int* msg_ptr, int num_words)
{
    for (int i = 0; i < num_words; i++) {
        msg_ptr[i] = i;
    }
}

void
validate_msg(int* msg_ptr, int num_words)
{
    for (int i = 0; i < num_words; i++) {
        if (msg_ptr[i] != i) {
            printf("FAILURE: Message verification failed at index=%d\n", i);
            return;
        }
    }

    printf("Message Validation Passed\n");
}



dragonError_t
create_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size)
{
    int k;

    for (k=0;k<arr_size;k++) {
        //printf("Creating Channel %d in ring\n",k);
        /* Create a Channel attributes structure so we can tune the Channel */
        dragonChannelAttr_t cattr;
        dragonError_t cerr = dragon_channel_attr_init(&cattr);

        /* Set the Channel capacity to our message limit for this test */
        cattr.capacity = CH_CAPACITY;
        cattr.bytes_per_msg_block = B_PER_MBLOCK;
        //printf("Channel capacity set to %li\n", cattr.capacity);

        /* Create the Channel in the memory pool */
        cerr = dragon_channel_create(&channel[k], k, mpool, &cattr);
        if (cerr != DRAGON_SUCCESS)
            err_fail(cerr, "Failed to create channel");
    }

    return SUCCESS;
}


dragonError_t
init_channels_serial_desc(dragonChannelDescr_t channel[], dragonChannelSerial_t serialized_channels[], int arr_size)
{
    int k;

    for (k=0; k<arr_size; k++) {
        dragonError_t cerr = dragon_channel_serialize(&channel[k], &serialized_channels[k]);
        if (cerr != DRAGON_SUCCESS)
            err_fail(cerr, "Failed to serialize channel");
    }

    return SUCCESS;
}



dragonError_t
fake_transport(const dragonChannelSerial_t* gw_ser)
{
    dragonChannelDescr_t gw_ch;
    dragonMessage_t msg;
    dragonError_t err;
    dragonMemoryDescr_t mem;
    void* ptr;
    size_t nbytes;
    dragonGatewayMessageSerial_t gw_msg_ser;
    dragonGatewayMessage_t gw_msg;
    dragonMemoryPoolDescr_t mpool;

    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != SUCCESS)
        err_fail(err, "Failed to initialize message structure in fake_transport");

    err = dragon_channel_attach(gw_ser, &gw_ch);
    if (err != SUCCESS)
        err_fail(err, "Failed to attach to gateway channel");

    err = dragon_channel_get_pool(&gw_ch, &mpool);
    if (err != SUCCESS)
        err_fail(err, "Failed to get pool from gateway channel");

    printf("Attached to gateway channel\n");

    dragonChannelRecvh_t gw_recvh;
    err = dragon_channel_recvh(&gw_ch, &gw_recvh, NULL);
    if (err != SUCCESS)
        err_fail(err, "Failed to create recv handle for gw channel");

    printf("Created gateway receive handle\n");

    err = dragon_chrecv_open(&gw_recvh);
    if (err != SUCCESS)
        err_fail(err, "Failed to open recv handle for gw channel");

    printf("Opened gateway receive handle\n");
    int number_of_send_requests = 0;
    int number_of_receive_requests = 0;

    for (int k=0;k<7;k++) {

        err = dragon_chrecv_get_msg_blocking(&gw_recvh, &msg, NULL);
        if (err != SUCCESS) {
            printf("Error: Could not recv message from gw channel.\n");
            err_fail(err, "Failed to create message");
        }

        err = dragon_channel_message_get_mem(&msg, &mem);
        if (err != SUCCESS)
            err_fail(err, "Failed to get memory for gateway message");

        err = dragon_memory_get_pointer(&mem, &ptr);
        if (err != SUCCESS)
            err_fail(err, "Failed to get pointer to memory for gateway message");

        err = dragon_memory_get_size(&mem, &nbytes);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Failed to get size of memory for gateway message");

        gw_msg_ser.data = ptr;
        gw_msg_ser.len = nbytes;

        err = dragon_channel_gatewaymessage_attach(&gw_msg_ser, &gw_msg);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Failed to attach to gateway message");

        if (gw_msg.msg_kind == DRAGON_GATEWAY_MESSAGE_SEND) {
            printf("Got send request message from gateway channel\n");

            number_of_send_requests++;

            dragonMemoryDescr_t payload_mem;
            err = dragon_channel_message_get_mem(&gw_msg.send_payload_message, &payload_mem);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to get message from send request in fake transport");

            int* msg_ptr;
            size_t nbytes;
            size_t nwords;

            err = dragon_memory_get_pointer(&payload_mem, (void*)&msg_ptr);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to get message pointer from send request in fake transport");

            err = dragon_memory_get_size(&payload_mem, &nbytes);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to get message size from send request in fake transport");

            nwords = nbytes / sizeof(int);

            validate_msg(msg_ptr, nwords);

            err = DRAGON_SUCCESS;

            /* On the second send request the fake transport returns a timeout error */

            if (number_of_send_requests == 2)
                err = DRAGON_TIMEOUT;

            err = dragon_channel_gatewaymessage_transport_send_cmplt(&gw_msg, err);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to successfully do the transport send completion");
        }

        if (gw_msg.msg_kind == DRAGON_GATEWAY_MESSAGE_GET) {
            dragonMessage_t msg;
            dragonMemoryDescr_t msg_buf;

            printf("Got get request message from gateway channel\n");
            fflush(stdout);

            number_of_receive_requests++;

            if (number_of_receive_requests == 2) {
                err = dragon_channel_gatewaymessage_transport_get_cmplt(&gw_msg, NULL, DRAGON_TIMEOUT);
                if (err != DRAGON_SUCCESS) {
                    err_fail(err, "Failed to successfully do the transport get completion");
                }
            } else {
                dragonError_t err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to allocate message buffer from pool");

                printf("Got space for a message\n");

                /* Get a pointer to that memory and fill up a message payload */
                int * msg_ptr;
                err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to get pointer to memory");

                printf("Filling message\n");

                fill_msg(msg_ptr, MSGWORDS);

                /* Create a message using that memory */

                err = dragon_channel_message_init(&msg, &msg_buf, NULL);
                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to create message");

                printf("Completing the receive of a message and supplying it to remote client.\n");

                err = dragon_channel_gatewaymessage_transport_get_cmplt(&gw_msg, &msg, DRAGON_SUCCESS);
                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to successfully do the transport get completion");
            }

        }

        if (gw_msg.msg_kind == DRAGON_GATEWAY_MESSAGE_EVENT) {
            printf("Got event request message from gateway channel\n");

            printf("Completing the event and supplying it to remote client.\n");
            fflush(stdout);

            err = dragon_channel_gatewaymessage_transport_event_cmplt(&gw_msg, DRAGON_CHANNEL_POLLOUT, DRAGON_SUCCESS);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to successfully do the transport get completion");
        }

    }
    printf("Exiting fake_transport successfully\n");

    return SUCCESS;
}

int
main(int argc, char *argv[])
{
    dragonMemoryPoolDescr_t mpool;
    dragonChannelDescr_t channels[CHANNELS_COUNT];
    dragonChannelDescr_t remote_channel;
    dragonChannelSerial_t gw_ser;

    dragonError_t err = create_pool(&mpool);
    if (err != SUCCESS)
        err_fail(err, "Failed to create gateway channel test pool");

    /* Creating only one channel, the gateway channel */

    err = create_channels(&mpool, channels, CHANNELS_COUNT);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to create gateway channel test channels", jmp_destroy_pool);

    err = dragon_channel_register_gateway(&channels[0]);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to register gateway channel", jmp_destroy_pool);

    err = dragon_channel_serialize(&channels[0], &gw_ser);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to serialize gateway channel", jmp_destroy_pool);

    if (fork()==0) {
        fake_transport(&gw_ser);
        return 0;
    }

    dragonChannelDescr_t rem_ch;
    dragonChannelSerial_t ser_ch;
    /* Create a Channel attributes structure so we can tune the Channel */
    dragonChannelAttr_t cattr;
    dragonChannelSendAttr_t sattr;
    err = dragon_channel_attr_init(&cattr);

    /* Set the Channel capacity to our message limit for this test */
    cattr.capacity = CH_CAPACITY;
    cattr.bytes_per_msg_block = B_PER_MBLOCK;

    err = dragon_channel_send_attr_init(&sattr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to init send handle attributes", jmp_destroy_pool);

    sattr.return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED;

    /* Creating a test channel but only attaching and detaching here */

    err = dragon_channel_create(&rem_ch, 9999, &mpool, &cattr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create Channel", jmp_destroy_pool);

    err = dragon_channel_serialize(&rem_ch, &ser_ch);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to seralize a channel", jmp_destroy_pool);

    err = dragon_channel_attach(&ser_ch, &remote_channel);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to attach to local channel", jmp_destroy_pool);

    err = dragon_channel_detach(&remote_channel);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to detach from local channel", jmp_destroy_pool);

    err = dragon_channel_destroy(&rem_ch);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to detach from local channel", jmp_destroy_pool);

    err = dragon_channel_create(&rem_ch, 9999, &mpool, &cattr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create Channel", jmp_destroy_pool);

    err = dragon_channel_serialize(&rem_ch, &ser_ch);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to serialize a channel", jmp_destroy_pool);

    /* The following changes the channel serialized descriptor to make it look
       like a remote channel. Another way to do this would be to create a masquerading
       channel, but for now we use this approach */

    alter_channel_ser_descriptor((size_t *)ser_ch.data, FAKE_MUID, FAKE_CUID, FAKE_HOSTID);


    printf("Attaching to remote channel\n");

    err = dragon_channel_attach(&ser_ch, &remote_channel);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to attach to remote channel", jmp_destroy_pool);

    /* Now send message to remote channel */
    /* Allocate managed memory as buffer space for both send and receive */
    dragonChannelSendh_t sendh;
    err = util_create_open_send_recv_handles(&remote_channel, &sendh, &sattr, NULL, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to create send or open handle for remote channel (check full error message for which)", jmp_destroy_pool);

    dragonMemoryDescr_t msg_buf;
    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

    /* Get a pointer to that memory and fill up a message payload */
    int * msg_ptr;
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get pointer to memory", jmp_destroy_pool);

    fill_msg(msg_ptr, MSGWORDS);

    /* Create a message using that memory */
    dragonMessage_t msg;
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create message", jmp_destroy_pool);

    printf("Sending to remote channel.\n");

    err = dragon_chsend_send_msg(&sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to send message", jmp_destroy_pool);

    printf("Sent message to remote channel.\n");

    /* Here we are going to send a message and expect a timeout to test that path. */

    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

    /* Get a pointer to that memory and fill up a message payload */
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get pointer to memory", jmp_destroy_pool);

    fill_msg(msg_ptr, MSGWORDS);

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create message", jmp_destroy_pool);

    printf("Sending to remote channel.\n");
    timespec_t timeout;
    timeout.tv_nsec = 0;
    timeout.tv_sec = 2; /* We won't really wait that long. The fake_transport will return right away with a
                           fake timeout */

    err = dragon_chsend_send_msg(&sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, &timeout);
    if (err != DRAGON_TIMEOUT)
        main_err_fail(err, "Failed to time out on send message", jmp_destroy_pool);

    printf("We got a timeout on send to remote channel as expected.\n");

    /* Here we are going to send a small message that is buffered into the gateway message. */

    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*10);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

    /* Get a pointer to that memory and fill up a message payload */
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get pointer to memory", jmp_destroy_pool);

    fill_msg(msg_ptr, 10);

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create message", jmp_destroy_pool);

    printf("Sending small message to remote channel.\n");

    err = dragon_chsend_send_msg(&sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to time out on send message", jmp_destroy_pool);

    printf("We sent it successfully.\n");

    /* Here we send the message to the remote channel with return when buffered and small enough to fit
       in the gateway message itself. */

    err = dragon_chsend_close(&sendh);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to close send handle");

    err = dragon_channel_send_attr_init(&sattr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to init send handle attrs");

    sattr.return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED; /* this is default, but setting explicitly anyway */

    err = util_create_open_send_recv_handles(&remote_channel, &sendh, &sattr, NULL, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to create or open send handle for remote channel", jmp_destroy_pool);

    printf("Opened send handle to remote channel\n");


    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*2);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

    /* Get a pointer to that memory and fill up a message payload */
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get pointer to memory", jmp_destroy_pool);

    fill_msg(msg_ptr, 2);

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to create message", jmp_destroy_pool);

    printf("Sending to remote channel.\n");

    err = dragon_chsend_send_msg(&sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);

    if (err != SUCCESS)
        main_err_fail(err, "Failed to send message", jmp_destroy_pool);

    printf("Sent message to remote channel.\n");


    dragonChannelRecvh_t recvh;
    dragonMessage_t rem_msg;

    err = dragon_channel_message_init(&rem_msg, NULL, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Could not initialize empty message for remote receive", jmp_destroy_pool);

    err = util_create_open_send_recv_handles(&remote_channel, NULL, NULL, &recvh, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Could not create or open receive handle for remote channel (check full message for which)", jmp_destroy_pool);


    printf("Created and opened Receive handle to remote channel\n");

    err = dragon_chrecv_get_msg_blocking(&recvh, &rem_msg, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get message from remote channel", jmp_destroy_pool);

    printf("Received remote message in main function.\n");

    dragonMemoryDescr_t payload_mem;
    err = dragon_channel_message_get_mem(&rem_msg, &payload_mem);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get message from received message in main function", jmp_destroy_pool);

    err = dragon_memory_get_pointer(&payload_mem, (void*)&msg_ptr);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get message pointer from received message in main function", jmp_destroy_pool);

    validate_msg(msg_ptr, MSGWORDS);

    printf("The message payload was validated.\n");

    printf("Now doing a remote receive with expected timeout\n");

    err = dragon_channel_message_init(&rem_msg, NULL, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Could not initialize empty message for remote receive", jmp_destroy_pool);

    err = dragon_chrecv_get_msg_blocking(&recvh, &rem_msg, &timeout);
    if (err != DRAGON_TIMEOUT)
        main_err_fail(err, "Failed to get timeout on receive of message from remote channel", jmp_destroy_pool);

    printf("We got the expected timeout return code\n");

    printf("Now doing remote poll\n");
    err = dragon_channel_poll(&remote_channel, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLIN | DRAGON_CHANNEL_POLLOUT, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Failed to get successful remote poll return code", jmp_destroy_pool);

    /* Now waiting for the fake_transport to exit */

    int status;
    int corpse = wait(&status);

    if (corpse < 0)
        jmp_fail("Failed to wait for exit of fake transport", jmp_destroy_pool);

    if (status != SUCCESS)
        jmp_fail("Fake transport exited with non-zero return code", jmp_destroy_pool);

    /* Unregistering gateway channels */

    err = dragon_channel_unregister_gateway(&channels[0]);
    if (err != SUCCESS)
        main_err_fail(err, "dragon_channel_unregister_gateway of gateway channel failed", jmp_destroy_pool);

    err = dragon_channel_unregister_gateway(NULL);
    if (err == SUCCESS)
        main_err_fail(err, "Unexpected DRAGON_SUCCESS on dragon_channel_unregister_gateway with NULL", jmp_destroy_pool);

    printf("Gateway Test Complete - all tests passed.\n");
jmp_destroy_pool:
    /* destroy pool used for all messaging */
    dragon_memory_pool_destroy(&mpool);

    return TEST_STATUS;
}
