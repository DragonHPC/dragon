//#include <dragon/shared_lock.h>
//#include <dragon/return_codes.h>
#include "../../src/lib/shared_lock.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>
#include <signal.h>

#include "../_ctest_utils.h"

#define NUM_LOCKS 3
#define NVARS_FOR_CORRECTNESS 2
#define VERBOSE
#define LOCKPTR_IDX 0
#define CHECKVAL_IDX_1 1
#define CHECKVAL_IDX_2 2

int ierr;
dragonLockKind_t lkinds[3] = {DRAGON_LOCK_FIFO, DRAGON_LOCK_FIFO_LITE, DRAGON_LOCK_GREEDY};
char * SHM_fname;

static void
test_lock(dragonLock_t * dlock, int tid, void ** lock_ptr)
{
    int * slastv = (int *)lock_ptr[CHECKVAL_IDX_1];
    int * snextv = (int *)lock_ptr[CHECKVAL_IDX_2];

    for (int i = 0; i < 100; i++) {

        dragonError_t derr = dragon_lock(dlock);
        if (derr != DRAGON_SUCCESS) {
#ifdef VERBOSE
            printf("ERROR: thread %i failed to obtain lock (kind = %i, err = %i)\n", tid, dlock->kind, derr);
#endif
            ierr = 1;
        }

        /* perform some reads and stores spaced out that fails if the lock was not working */
        int mylastv = *slastv;
        (*slastv)++;

        usleep(500);

        int mynextv = *snextv;
        (*snextv)++;

        if (mylastv != mynextv) {
#ifdef VERBOSE
            printf("LAST = %i, NEXT = %i\n", mylastv, mynextv);
            printf("(PID %i) ERROR: thread %i failed lock protection (kind = %i)\n", getpid(), tid, dlock->kind);
#endif
            ierr = 1;
        }

        derr = dragon_unlock(dlock);
        if (derr != DRAGON_SUCCESS) {
#ifdef VERBOSE
            printf("(PID %i) ERROR: thread %i failed to release lock (kind = %i, err = %i)\n", getpid(),
                   tid, dlock->kind, derr);
#endif
            ierr = 1;
        }

    }

}

void
run_tests(dragonLockKind_t lkind, dragonLock_t * dlock, void ** lock_ptr)
{
    /* first test with all threads using the same handle */
    ierr = 0;
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        test_lock(dlock, tid, lock_ptr);
    }

    if (ierr == 0) {
        printf("(PID %i) Lock kind %i :: shared handle test :: Success\n", getpid(), lkind);
    } else {
        printf("(PID %i) Lock kind %i :: shared handle test :: Failed\n", getpid(), lkind);
    }

    /* now test with unique handles for each thread */
    ierr = 0;
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();

        dragonLock_t ldlock;
        dragonError_t derr = dragon_lock_attach(&ldlock, lock_ptr[LOCKPTR_IDX]);
        if (derr != DRAGON_SUCCESS) {
            printf("(PID %i) ERROR: thread %i failed to attach lock (kind = %i, err = %i)\n", getpid(),
                   tid, lkind, derr);
        }

        test_lock(&ldlock, tid, lock_ptr);

        derr = dragon_lock_detach(&ldlock);
        if (derr != DRAGON_SUCCESS) {
            printf("(PID %i) ERROR: thread %i failed to detach lock (kind = %i, err = %i)\n", getpid(),
                   tid, lkind, derr);
        }
    }

    if (ierr == 0) {
        printf("(PID %i) Lock kind %i :: unique handle test :: Success\n", getpid(), lkind);
    } else {
        printf("(PID %i) Lock kind %i :: unique handle test :: Failed\n", getpid(), lkind);
    }
}

/* create SHM space for all of the locks and validation variables */
static int
get_lock_ptrs(dragonLockKind_t * lkinds, size_t * needed_space, void *** ptr)
{
    /* determine space needed for the lock types and validation variables */
    *needed_space = 0;

    for (int i = 0; i < NUM_LOCKS; i++)
        *needed_space += (dragon_lock_size(lkinds[i]) + sizeof(int) * NVARS_FOR_CORRECTNESS);

    void * lptr;

    /* now create SHM */
    SHM_fname = util_salt_filename("dragon_lock_test");
    int sfd = shm_open(SHM_fname, O_RDWR | O_CREAT | O_EXCL , S_IRUSR | S_IWUSR);
    if (sfd == -1) {
        printf("ERROR: unable to shm_open space for lock\n");
        return FAILED;
    }

    int serr = ftruncate(sfd, *needed_space);
    if (serr == -1) {
        printf("ERROR: unable to ftruncate space for lock\n");
        return FAILED;
    }

    lptr = mmap(NULL, *needed_space, PROT_READ | PROT_WRITE, MAP_SHARED, sfd, 0);
    if (lptr == NULL) {

        printf("ERROR: mmap space for lock\n");
        return FAILED;

    }

    /* now map on the lock locations and validation variable locations */
    for (int i = 0; i < NUM_LOCKS; i++) {

        ptr[i][LOCKPTR_IDX] = lptr;
        lptr += dragon_lock_size(lkinds[i]);

        for (int j = 0; j < NVARS_FOR_CORRECTNESS; j++) {
            ptr[i][CHECKVAL_IDX_1+j] = lptr;
            lptr += sizeof(int);
        }

    }

    return SUCCESS;
}

static int
free_lock_ptrs(size_t needed_space, void *** ptr)
{
    if (ptr == NULL)
        return FAILED;

    if (ptr[0] == NULL)
        return FAILED;

    if (ptr[0][0] == NULL)
        return FAILED;

    int serr = munmap(ptr[0][0], needed_space);
    if (serr == -1) {
        printf("ERROR: unable to munmap space for lock\n");
        return FAILED;
    }

    int sfd = shm_unlink(SHM_fname);
    if (sfd == -1) {
        printf("ERROR: unable to shm_unlink space for lock\n");
        return FAILED;
    }

    return SUCCESS;
}

/* block on receiving SIGUSR1 */
static int
block_on_sig()
{
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &set, NULL);

    int sig;
    int ret_val = sigwait(&set, &sig);
    if (ret_val == -1)
        return FAILED;

    return SUCCESS;
}

int
main(int argc, char *argv[])
{
    printf("Usage: test_threaded_lock [nchildren] [nthreads_per_child]\n");
    printf("If not specified default is 2 children and 2 threads\n");
    int nchildren = 2;
    int nthreads = 2;

    if (argc > 1) {
        nchildren = atoi(argv[1]);
    }
    if (argc > 2) {
        nthreads = atoi(argv[2]);
    }

    printf("Testing with %i children\n", nchildren);
    printf("Testing with %i threads\n", nthreads);

    /* get space for all of the lock types we'll test */
    void *** lock_ptrs = malloc(sizeof(void **) * NUM_LOCKS);
    if (lock_ptrs == NULL) {
        printf("ERROR: unable to allocate lock pointers\n");
        return FAILED;
    }
    for (int i = 0; i < NUM_LOCKS; i++) {
        lock_ptrs[i] = malloc(sizeof(void *) * (1 + NVARS_FOR_CORRECTNESS));
        if (lock_ptrs[i] == NULL) {
            printf("ERROR: unable to allocate lock pointers for %i\n", i);
            return FAILED;
        }
    }

    /* create the SHM and wire up the pointers to it */
    size_t needed_space;
    if (get_lock_ptrs(lkinds, &needed_space, lock_ptrs)) {
        printf("ERROR: unable to allocate SHM for lock pointers\n");
        return FAILED;
    }

    /* create the base lock handles for each lock and initialize the validation variables */
    dragonLock_t * base_locks = malloc(sizeof(dragonLock_t) * NUM_LOCKS);
    if (base_locks == NULL) {
        printf("ERROR: unable to allocate space for lock handles\n");
        return FAILED;
    }
    for (int i = 0; i < NUM_LOCKS; i++) {

        dragonError_t derr = dragon_lock_init(&base_locks[i], lock_ptrs[i][LOCKPTR_IDX], lkinds[i]);
        if (derr != DRAGON_SUCCESS) {
            printf("ERROR: failed to create lock (kind = %i, err = %i)\n", lkinds[i], derr);
            return FAILED;
        }

        for (int j = 0; j < NVARS_FOR_CORRECTNESS; j++) {
            *(int *)(lock_ptrs[i][CHECKVAL_IDX_1+j]) = 0;
        }

    }

    /* fork any children we need to */
    int * child_pids = NULL;
    int am_parent = 1;
    if (nchildren > 0) {

        child_pids = malloc(sizeof(int) * nchildren);
        if (child_pids == NULL) {
            printf("ERROR: unable to allocate space for child PIDs\n");
            return FAILED;
        }

        for (int i = 0; i < nchildren; i++) {
            if (am_parent) {
                child_pids[i] = fork();
                if (child_pids[i] == 0)
                    am_parent = 0;
            }
        }

    }

    /* set the number of threads we'll use (note OMP calls delayed until after fork as required) */
    omp_set_num_threads(nthreads);

    /* if we are a child, sit waiting for a signal from the parent */
    if (am_parent == 0) {

        if (block_on_sig()) {
            printf("ERROR: failed blocking on signal\n");
            return FAILED;
        }

    } else {

        /* parent alone tests with threads */
        printf("+------------ Parent Test with Threads -----------+\n");
        for (int i = 0; i < NUM_LOCKS; i++)
            run_tests(lkinds[i], &base_locks[i], lock_ptrs[i]);

        /* release the children to test */
        if (nchildren > 0) {

            for (int i = 0; i < nchildren; i++)
                kill(child_pids[i], SIGUSR1);

            printf("+------------ Test with Children and Threads -----------+\n");

        }

    }

    /* everyone runs tests */
    if (nchildren > 0) {

        for (int i = 0; i < NUM_LOCKS; i++)
            run_tests(lkinds[i], &base_locks[i], lock_ptrs[i]);

    }

    /* if we are a child, just return */
    if (am_parent == 0) {
        return SUCCESS;
    }

    /* all tests done, wait on children and cleanup */
    if (nchildren > 0) {

        for (int i = 0; i < nchildren; i++)
            wait(NULL);
        free(child_pids);

    }

    for (int i = 0; i < NUM_LOCKS; i++) {

        dragonError_t derr = dragon_lock_destroy(&base_locks[i]);
        if (derr != DRAGON_SUCCESS) {
            printf("ERROR: failed to destroy lock (kind = %i, err = %i)\n", lkinds[i], derr);
        }

    }
    free(base_locks);

    if (free_lock_ptrs(needed_space, lock_ptrs)) {
        printf("ERROR: failed to cleanup SHM\n");
        return FAILED;
    }
    for (int i = 0; i < NUM_LOCKS; i++)
        free(lock_ptrs[i]);
    free(lock_ptrs);

    return SUCCESS;
}
