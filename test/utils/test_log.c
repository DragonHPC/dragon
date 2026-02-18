#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include "../../src/lib/logging.h"
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../_ctest_utils.h"

int main(int argc, char **argv)
{

    size_t mem_size = 1UL<<30;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    dragonC_UID_t l_uid = 90210;
    char * fname = util_salt_filename("test_log");
    void * result = NULL;

    printf("Pool Create\n");
    dragonError_t derr = dragon_memory_pool_create(&mpool, mem_size, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");


    dragonLoggingAttr_t lattr;
    derr = dragon_logging_attr_init(&lattr);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to init logging attrs", jmp_destroy_pool);

    lattr.ch_attr.capacity = NMSGS; // defined in _ctest_utils
    lattr.mode = DRAGON_LOGGING_FIRST;

    dragonLoggingDescr_t logger;
    derr = dragon_logging_init(&mpool, l_uid, &lattr, &logger);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to initialize logger", jmp_destroy_pool);

    /* @MCB old TODO note, not sure if we'll refactor to make use of this route later
    // Logging priority should be some set of flags to allow bitwise OR on get calls
    // e.g. dragon_logging_get(&logger, LOG_DEBUG | LOG_ERROR, NULL);
    */

    char log_msg[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    size_t log_len;

    // Test putting in different priority messages
    derr = dragon_logging_put(&logger, DG_DEBUG, log_msg, strlen(log_msg));
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message A", jmp_destroy_pool);
    printf("Inserted message ABCDEFGHIJKLMNOPQRSTUVWXYZ\n");

    derr = dragon_logging_get(&logger, DG_DEBUG, &result, &log_len, NULL);
    printf("%s and length is %ld and strlen is %ld\n", (char *)result, log_len, strlen(result));
    free(result);
    result = NULL;

    derr = dragon_logging_put(&logger, DG_DEBUG, "ABCDE", 5);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message A", jmp_destroy_pool);
    printf("Inserted message ABCDE\n");

    derr = dragon_logging_put(&logger, DG_INFO, "B", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message B", jmp_destroy_pool);
    printf("Inserted message B\n");

    derr = dragon_logging_put(&logger, DG_WARNING, "C", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message C", jmp_destroy_pool);
    printf("Inserted message C\n");

    derr = dragon_logging_put(&logger, DG_ERROR, "D", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message D", jmp_destroy_pool);
    printf("Inserted message D\n");

    printf("Inserted all messages\n");
    // Retrieve all logs
    // TODO: This will later be a flush() call
    for (int i = 0; i < 4; i++) {
        derr = dragon_logging_print(&logger, DG_DEBUG, NULL);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to get test message %d\n", i);
            main_err_fail(derr, "Failed to retrieve expected message", jmp_destroy_pool);
        }
    }

    // Test only retrieving certain levels
    derr = dragon_logging_put(&logger, DG_INFO, "E", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message E", jmp_destroy_pool);
    derr = dragon_logging_put(&logger, DG_WARNING, "F", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message F", jmp_destroy_pool);
    derr = dragon_logging_put(&logger, DG_ERROR, "G", 1);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message G", jmp_destroy_pool);

    // Test priority will skip over undesired messages
    derr = dragon_logging_print(&logger, DG_WARNING, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get test message (priority 2, first)", jmp_destroy_pool);
    derr = dragon_logging_print(&logger, DG_WARNING, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get test message (priority 2, second)", jmp_destroy_pool);

    // Test timeout
    void * msg_out = NULL;
    size_t msg_len;
    timespec_t timeout = {1, 0};
    printf("Waiting one second with an empty log queue...\n");
    derr = dragon_logging_get(&logger, DG_DEBUG, &msg_out, &msg_len, &timeout);
    if (derr == DRAGON_SUCCESS)
        jmp_fail("Expected error code on timeout, got success", jmp_destroy_pool);
    if (msg_out != NULL)
        jmp_fail("Expected NULL msg_out on timeout", jmp_destroy_pool);

    // Test log overflow
    printf("Testing log overflow...\n");
    for (int i = 0; i < (NMSGS + 1); i++) {
        char buf[4];
        sprintf(buf, "%d", i);
        derr = dragon_logging_put(&logger, DG_DEBUG, buf, strlen(buf));
        // Assert the log drops messages on full
        if (derr == DRAGON_CHANNEL_FULL) {
            if (i == 100)
                break;
            else {
                printf("Got CHANNEL_FULL at unexpected index %d, should be 100", i);
                main_err_fail(derr, "Unexpected CHANNEL_FULL", jmp_destroy_pool);
            }
        }

        if (derr != DRAGON_SUCCESS) {
            printf("Failed at index %d!\n", i);
            main_err_fail(derr, "Failed to insert message in log overflow", jmp_destroy_pool);
        }
    }

    /* Iterate over an empty channel a bajillion times with blocking mode enabled to test for memory leaks
       Check memory footprint from a separate terminal
       This isn't automatic so be sure to check manually now and then
    */

    printf("Testing serialize...\n");
    dragonLoggingSerial_t log_ser;
    derr = dragon_logging_serialize(&logger, &log_ser);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Could not serialize logging", jmp_destroy_pool);

    printf("Testing attach...\n");
    dragonLoggingDescr_t logger2;
    derr = dragon_logging_attach(&log_ser, &logger2, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Could not attach to serialized descriptor", jmp_destroy_pool);

    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
    printf("All tests passed\n");

jmp_destroy_pool:
    printf("Goto Destroy\n");
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }

    return TEST_STATUS;
}
