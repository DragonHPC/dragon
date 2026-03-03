#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/logging.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>

#define SUCCESS 0
#define FAILED 1

#define err_fail(derr, msg) ({                                          \
            char * errstr = dragon_getlasterrstr();                     \
            const char * errcode = dragon_get_rc_string(derr);          \
            printf("TEST: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
            return FAILED;                                              \
        })

#define main_err_fail(derr, msg, jmp) ({                                \
            char * errstr = dragon_getlasterrstr();                     \
            const char * errcode = dragon_get_rc_string(derr);          \
            printf("TEST_MAIN: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
            TEST_STATUS = FAILED;                                       \
            goto jmp;                                                   \
        })


int TEST_STATUS = SUCCESS;

int main(int argc, char **argv)
{

    size_t mem_size = 1UL<<30;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = "log_test";

    printf("Pool Create\n");
    dragonError_t derr = dragon_memory_pool_create(&mpool, mem_size, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");


    dragonLoggingDescr_t logger;
    derr = dragon_logging_init(&mpool, NULL, &logger);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to initialize logger", jmp_destroy_pool);

    // Test putting in different priority messages
    derr = dragon_logging_put(&logger, DG_DEBUG, "A");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message A", jmp_destroy_pool);
    printf("Inserted message A\n");

    derr = dragon_logging_put(&logger, DG_INFO, "B");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message B", jmp_destroy_pool);
    printf("Inserted message B\n");

    derr = dragon_logging_put(&logger, DG_WARNING, "C");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message C", jmp_destroy_pool);
    printf("Inserted message C\n");

    derr = dragon_logging_put(&logger, DG_ERROR, "D");
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
    derr = dragon_logging_put(&logger, DG_INFO, "E");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message E", jmp_destroy_pool);
    derr = dragon_logging_put(&logger, DG_WARNING, "F");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message F", jmp_destroy_pool);
    derr = dragon_logging_put(&logger, DG_ERROR, "G");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message G", jmp_destroy_pool);

    // Test priority will skip over undesired messages
    derr = dragon_logging_print(&logger, DG_WARNING, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get test message (priority 2, first)", jmp_destroy_pool);
    derr = dragon_logging_print(&logger, DG_WARNING, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get test message (priority 2, second)", jmp_destroy_pool);

    derr = dragon_logging_put(&logger, DG_DEBUG, "A string!");
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put test message 'A string!'", jmp_destroy_pool);

    // Test grabbing the string back
    char * out_str = NULL;
    derr = dragon_logging_get_str(&logger, DG_DEBUG, &out_str, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to retrieve stringified message", jmp_destroy_pool);
    printf("%s\n", out_str);
    // get_str locally allocates the string to free the internal logging memory, release user-side
    free(out_str);

    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
jmp_destroy_pool:
    printf("Goto Destroy\n");
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }

    return TEST_STATUS;
}
