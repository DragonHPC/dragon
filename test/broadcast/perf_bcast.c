#include <dragon/bcast.h>
#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <dragon/utils.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>

#define SERFILE "bcast_serialized.dat"
#define MFILE "bcast_test"
#define M_UID 0
#define BILL 1000000000L
#define MILL 1000000L

/* These two constants used below to choose type of waiting */
#define IDLE_WAIT 1
#define SPIN_WAIT 2


#define FAILED 1
#define SUCCESS 0

#define num_iterations 20

static int processes[] = {1, 10, 20, 30, 40, 50};
static int processes_count = sizeof(processes) / sizeof(int);

#define max_payload_sz 2097152
static int payloads[] = {0, 512, 2048, max_payload_sz};
static int payloads_count = sizeof(payloads) /  sizeof(int);


void
fill_payload(char payload[], size_t payload_sz)
{
    for (size_t i = 0; i < payload_sz; i++)
        payload[i] = i % 128;
}

bool
payload_ok(char payload[], size_t payload_sz)
{
    for (size_t i = 0; i < payload_sz; i++) {
        unsigned int x = payload[i];
        if (i%128 != x) {
            printf("The value at %lu was %u\n",i,x);
            return false;
        }
    }

    return true;
}

int
proc_idle_waiter(dragonMemoryPoolDescr_t* pd, dragonBCastDescr_t* bd, dragonChannelSendh_t* sendh, size_t expected_payload_sz, const timespec_t* timeout, int iterations)
{

    double total_time = 0;
    timespec_t t1, t2;
    dragonMessage_t msg;
    dragonError_t err;

    for (int k=0; k<iterations; k++) {
        dragonError_t err;
        char* payload;
        size_t payload_sz;

        clock_gettime(CLOCK_MONOTONIC, &t1);
        err = dragon_bcast_wait(bd, DRAGON_IDLE_WAIT, timeout, (void**)&payload, &payload_sz, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &t2);

        double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
        total_time += etime;

        if (err == DRAGON_TIMEOUT) {
            printf("Error on dragon_bcast_wait with EC=%s\n", dragon_get_rc_string(err));
            printf("%s\n", dragon_getlasterrstr());
            return err;
        }

        if (err != DRAGON_SUCCESS) {
            printf("Error on dragon_bcast_wait with EC=%s\n", dragon_get_rc_string(err));
            printf("%s\n", dragon_getlasterrstr());
            return err;
        }

        if (payload_sz != expected_payload_sz) {
            printf("Payload size in idle waiter did not match expected payload size.\n");
            printf("expected=%lu and actual=%lu\n", expected_payload_sz, payload_sz);
            return FAILED;
        }

        //if (!payload_ok(payload, payload_sz)) {
        //    printf("Error on payload verification\n");
        //    return FAILED;
        //}

        if (err == DRAGON_SUCCESS && payload_sz > 0)
            free(payload);
    }

    /* Allocate managed memory as buffer space for both send and receive */
    dragonMemoryDescr_t msg_buf;
    err = dragon_memory_alloc(&msg_buf, pd, sizeof(double));
    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to allocate message buffer from pool.  Got EC=%i\nERRSTR = \n%s\n",err, errstr);
        return FAILED;
    }

    /* Get a pointer to that memory and fill up a message payload */
    double * msg_ptr;
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to get pointer to message buffer.  Got EC=%i\nERRSTR = \n%s\n",err, errstr);
        return FAILED;
    }

    *msg_ptr = total_time;

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to init message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    err = dragon_chsend_send_msg(sendh, &msg, NULL, NULL);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to send message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    err = dragon_channel_message_destroy(&msg, false);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to destroy message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    return SUCCESS;
}

int
proc_spin_waiter(dragonMemoryPoolDescr_t* pd, dragonBCastDescr_t* bd, dragonChannelSendh_t* sendh, size_t expected_payload_sz, const timespec_t* timeout, int iterations)
{

    double total_time = 0;
    timespec_t t1, t2;
    dragonMessage_t msg;
    dragonError_t err;

    for (int k=0; k<iterations; k++) {
        dragonError_t err;
        char* payload;
        size_t payload_sz;

        clock_gettime(CLOCK_MONOTONIC, &t1);
        err = dragon_bcast_wait(bd, DRAGON_SPIN_WAIT, timeout, (void**)&payload, &payload_sz, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &t2);

        double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
        total_time += etime;

        if (err != DRAGON_SUCCESS) {
            printf("Error on dragon_bcast_wait with EC=%s\n", dragon_get_rc_string(err));
            printf("%s\n", dragon_getlasterrstr());
            return err;
        }

        if (payload_sz != expected_payload_sz) {
            printf("Payload size in spin waiter did not match expected payload size.\n");
            printf("expected=%lu and actual=%lu\n", expected_payload_sz, payload_sz);
            return FAILED;
        }

        //if (!payload_ok(payload, payload_sz)) {
        //    printf("Error on payload verification\n");
        //    return FAILED;
        //}

        if (err == DRAGON_SUCCESS && payload_sz > 0)
            free(payload);
    }

    /* Allocate managed memory as buffer space for both send and receive */
    dragonMemoryDescr_t msg_buf;
    err = dragon_memory_alloc(&msg_buf, pd, sizeof(double));
    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to allocate message buffer from pool.  Got EC=%i\nERRSTR = \n%s\n",err, errstr);
        return FAILED;
    }

    /* Get a pointer to that memory and fill up a message payload */
    double * msg_ptr;
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to get pointer to message buffer.  Got EC=%i\nERRSTR = \n%s\n",err, errstr);
        return FAILED;
    }

    *msg_ptr = total_time;

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to init message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    err = dragon_chsend_send_msg(sendh, &msg, NULL, NULL);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to send message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    err = dragon_channel_message_destroy(&msg, false);

    if (err != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to destroy message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    return SUCCESS;
}

void * global_payload;
size_t global_payload_sz;
dragonError_t global_derr;
char* global_err_str;

void trigger_hdler(int sig) {
    signal(sig, SIG_IGN);

    if (!payload_ok(global_payload, global_payload_sz))
        printf("Error on payload verification in callback\n");

    signal(SIGUSR1, trigger_hdler);
}

void
callback(dragonBCastDescr_t* bd, void* payload, size_t payload_sz, dragonError_t derr, char* err_str)
{
    if (!payload_ok(payload, payload_sz)) {
        printf("Error on payload verification in callback\n");
    }
}

int
create_pool(dragonMemoryPoolDescr_t* mpool)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    //printf("Allocating pool of size %lu bytes.\n", mem_size);

    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, MFILE, M_UID, NULL);
    if (derr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create the memory pool.  Got EC=%i\nERRSTR = \n%s\n",derr, errstr);
        return FAILED;
    }

    return SUCCESS;
}

void
check_result(dragonError_t err, dragonError_t expected_err, int* tests_passed, int* tests_attempted)
{
    (*tests_attempted)++;

    if (err != expected_err) {
        printf("Test %d Failed with error code %s\n", *tests_attempted, dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
    }
    else
        (*tests_passed)++;
}

dragonError_t
run_test_trigger_all(int wait_mode, dragonMemoryPoolDescr_t* pool, dragonBCastDescr_t* bd, dragonChannelSendh_t* sendh, dragonChannelRecvh_t* recvh, int* tests_passed, int* tests_attempted)
{
    timespec_t t1, t2;
    char payload[max_payload_sz];
    fill_payload(payload, max_payload_sz);


    for (int sz_idx = 0; sz_idx < payloads_count; sz_idx++) {
        int payload_sz = payloads[sz_idx];

        for (int pidx = 0; pidx < processes_count; pidx++) {
            double total_wait_time = 0;
            int waiters_count = processes[pidx];
            dragonError_t err;
            dragonMessage_t msg;
            dragonMemoryDescr_t mem;
            double total_time = 0;
            int k;
            int status;
            int count;


            /* fork the right number of waiters */
            for (k=0;k<waiters_count;k++) {
                if (fork()==0) {
                    if (wait_mode == IDLE_WAIT)
                        exit(proc_idle_waiter(pool, bd, sendh, payload_sz, NULL, num_iterations));
                    else
                        exit(proc_spin_waiter(pool, bd, sendh, payload_sz, NULL, num_iterations));
                }
            }

            /* Call trigger_all the correct number of times for the waiters to complete. When
               doing this, make sure each time trigger_all is called that all the waiters
               are actually waiting before calling trigger_all. */

            for (k=0; k<num_iterations;k++) {

                err = dragon_bcast_num_waiting(bd, &count);
                check_result(err, DRAGON_SUCCESS, tests_passed, tests_attempted);

                while (count < waiters_count) {
                    err = dragon_bcast_num_waiting(bd, &count);
                    check_result(err, DRAGON_SUCCESS, tests_passed, tests_attempted);
                }

                /* Pause here to let idle waiters get to their futex. */
                if (wait_mode == IDLE_WAIT)
                    usleep(100000);

                clock_gettime(CLOCK_MONOTONIC, &t1);
                err = dragon_bcast_trigger_all(bd, NULL, &payload, payload_sz);
                clock_gettime(CLOCK_MONOTONIC, &t2);
                if (err != DRAGON_SUCCESS) {
                    printf("Got error code on trigger all: %s\n", dragon_get_rc_string(err));
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to trigger all. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    fflush(stdout);
                    return FAILED;
                }

                double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
                total_time += etime;
            }

            /* Wait for the waiters to exit with the status code */
            count = 0;
            for (k=0;k<waiters_count;k++) {
                wait(&status);
                if (status!=SUCCESS) {
                    printf("There was an error on an idle waiter being triggered.\n");
                } else {
                    count += 1;
                }
            }

            if (count != waiters_count) {
                printf("We did not get all processes exiting succefully.\n");
                return FAILED;
            }

            /* Now get the total time each child spent waiting */
            for (k=0;k<waiters_count; k++) {
                double* child_time;

                err = dragon_channel_message_init(&msg, NULL, NULL);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to init message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_chrecv_get_msg(recvh, &msg);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to receive message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_channel_message_get_mem(&msg, &mem);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to get memory. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_memory_get_pointer(&mem, (void**) &child_time);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to get memory pointer. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                total_wait_time += *child_time;

                err = dragon_channel_message_destroy(&msg, true);

                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to destroy message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }
            }

            double avg_wait_time;

            avg_wait_time = (total_wait_time / (num_iterations * waiters_count)) * MILL;

            /* We subtract one tenth of a second because the trigger was delayed
            by one tenth of a second above (usleep) to get all idle waiters to
            use the futex */

            if (wait_mode == IDLE_WAIT)
                avg_wait_time = avg_wait_time - MILL/10;

            char* wait_str;
            if (wait_mode == IDLE_WAIT)
                wait_str = "Idle Wait";
            else
                wait_str = "Spin Wait";

            double avg_trigger_time = (total_time / num_iterations) * MILL;

            printf("%s  %13d   %18d   %21.6f   %24.6f\n", wait_str, waiters_count, payload_sz, avg_wait_time, avg_trigger_time);

        }
    }

    return DRAGON_SUCCESS;
}

dragonError_t
run_test_trigger_one(int wait_mode, dragonMemoryPoolDescr_t* pool, dragonBCastDescr_t* bd, dragonChannelSendh_t* sendh, dragonChannelRecvh_t* recvh, int* tests_passed, int* tests_attempted)
{
    timespec_t t1, t2;
    char payload[max_payload_sz];
    fill_payload(payload, max_payload_sz);

    for (int sz_idx = 0; sz_idx < payloads_count; sz_idx++) {
        int payload_sz = payloads[sz_idx];

        for (int pidx = 0; pidx < processes_count; pidx++) {
            double total_wait_time = 0;
            int waiters_count = processes[pidx];
            dragonError_t err;
            dragonMessage_t msg;
            dragonMemoryDescr_t mem;
            double total_time = 0;
            int k;
            int status;
            int count;

            /* fork the right number of waiters */
            for (k=0;k<waiters_count;k++) {
                if (fork()==0) {
                    if (wait_mode == IDLE_WAIT)
                        exit(proc_idle_waiter(pool, bd, sendh, payload_sz, NULL, num_iterations));
                    else
                        exit(proc_spin_waiter(pool, bd, sendh, payload_sz, NULL, num_iterations));
                }
            }

            /* Call trigger_all the correct number of times for the waiters to complete. When
               doing this, make sure each time trigger_all is called that all the waiters
               are actually waiting before calling trigger_all. */

            int num_triggers = num_iterations * waiters_count;

            for (k=0; k<num_triggers;k++) {

                err = dragon_bcast_num_waiting(bd, &count);
                check_result(err, DRAGON_SUCCESS, tests_passed, tests_attempted);

                while (count == 0) {
                    err = dragon_bcast_num_waiting(bd, &count);
                    check_result(err, DRAGON_SUCCESS, tests_passed, tests_attempted);
                }

                clock_gettime(CLOCK_MONOTONIC, &t1);
                err = dragon_bcast_trigger_one(bd, NULL, &payload, payload_sz);
                clock_gettime(CLOCK_MONOTONIC, &t2);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to trigger one. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
                total_time += etime;
            }

            /* Wait for the waiters to exit with the status code */
            count = 0;
            for (k=0;k<waiters_count;k++) {
                wait(&status);
                if (status!=SUCCESS) {
                    printf("There was an error on an idle waiter being triggered.\n");
                } else {
                    count += 1;
                }
            }

            if (count != waiters_count) {
                printf("We did not get all processes exiting succefully.\n");
                return FAILED;
            }

            /* Now get the total time each child spent waiting */
            for (k=0;k<waiters_count; k++) {
                double* child_time;

                err = dragon_channel_message_init(&msg, NULL, NULL);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to init message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_chrecv_get_msg(recvh, &msg);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to receive message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_channel_message_get_mem(&msg, &mem);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to get memory. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                err = dragon_memory_get_pointer(&mem, (void**) &child_time);
                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to get memory pointer. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }

                total_wait_time += *child_time;

                err = dragon_channel_message_destroy(&msg, true);

                if (err != DRAGON_SUCCESS) {
                    char * errstr = dragon_getlasterrstr();
                    printf("Failed to destroy message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
                    return FAILED;
                }
            }

            double avg_wait_time = (total_wait_time / (num_iterations * waiters_count)) * MILL;

            char* wait_str;
            if (wait_mode == IDLE_WAIT)
                wait_str = "Idle Wait";
            else
                wait_str = "Spin Wait";
            double avg_trigger_time = (total_time / num_triggers) * MILL;

            printf("%s  %13d   %18d   %21.6f   %24.6f\n", wait_str, waiters_count, payload_sz, avg_wait_time, avg_trigger_time);
        }
    }

    return DRAGON_SUCCESS;
}

int
main(int argc, char* argv[])
{
    int tests_passed = 0;
    int tests_attempted = 0;
    dragonMemoryPoolDescr_t pool;

    dragonBCastDescr_t bd;
    dragonError_t err;
    dragonChannelDescr_t channel;
    dragonChannelRecvh_t recvh;
    dragonChannelSendh_t sendh;


    if (create_pool(&pool) != SUCCESS) {
        printf("Could not create memory pool for bcast tests.\n");
        return FAILED;
    }

    err = dragon_channel_create(&channel, 42, &pool, NULL);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_recvh(&channel, &recvh, NULL);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_sendh(&channel, &sendh, NULL);

    err = dragon_chrecv_open(&recvh);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chsend_open(&sendh);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    // create in memory pool
    err = dragon_bcast_create(&pool, max_payload_sz, 60, NULL, &bd);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    printf("BCast Performance Test\n");
    printf("*****************************************************************************************************\n");
    printf("Information about the test: The TRIGGER ALL tests below artificially inflate the average wait time\n");
    printf("of idle waiters by sleeping a bit to get all idle waiters to sleep on their futex. This provides a \n");
    printf("'worst case' idle wait. In reality, an idle wait is much faster if the futex is being triggered quickly.\n");
    printf("For comparison, look at the idle wait times in the TRIGGER ONE tests below which don't do any sleeping\n");
    printf("to get the processes to sleep on their futexes.\n\n");

    printf("TRIGGER ALL TESTS\n");
    printf("Operation  Num Processes   Payload Sz (bytes)   Avg Wait Time (usecs)   Avg Trigger Time (usecs)\n");
    printf("*********  *************   ******************   *********************   ************************\n");

    err = run_test_trigger_all(IDLE_WAIT, &pool, &bd, &sendh, &recvh, &tests_passed, &tests_attempted);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = run_test_trigger_all(SPIN_WAIT, &pool, &bd, &sendh, &recvh, &tests_passed, &tests_attempted);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    printf("\n\nTRIGGER ONE TESTS\n");
    printf("Operation  Num Processes   Payload Sz (bytes)   Avg Wait Time (usecs)   Avg Trigger Time (usecs)\n");
    printf("*********  *************   ******************   *********************   ************************\n");

    err = run_test_trigger_one(IDLE_WAIT, &pool, &bd, &sendh, &recvh, &tests_passed, &tests_attempted);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = run_test_trigger_one(SPIN_WAIT, &pool, &bd, &sendh, &recvh, &tests_passed, &tests_attempted);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_destroy(&bd);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    dragon_memory_pool_destroy(&pool);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    return 0;
}
