#define _GNU_SOURCE
#include "../../src/lib/shared_lock.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <sched.h>
#include "../../src/include/dragon/global_types.h"

#define NITERS 100000
#define WARMUP 1000
//#define VERBOSE
//#define USE_GREEDY
//#define USEFILE
//#define LITEFIFO

uint64_t dupt;
int lastt;
dragonLockKind_t dkind;

const char * lock_type[] = {"FIFO", "FIFOLite", "Greedy"};

int bind_thread_to_core(pthread_t thread, int core_id)
{
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

typedef struct tdata_st {
    int tid;
    void *ptr;
    double timer;
} tdata_t;

void * api_bench(void * tdatav)
{
    tdata_t * tdata = (tdata_t *)tdatav;

    dragonError_t derr;
    dragonLock_t dlock;

    derr = dragon_lock_attach(&dlock, tdata->ptr);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to init lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    timespec_t t1, t2;
    uint64_t iter;
    for (iter = 0UL; iter < NITERS+WARMUP; iter++) {

        if (iter == WARMUP)
            clock_gettime(CLOCK_MONOTONIC, &t1);

        derr = dragon_lock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to obtain lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }

        /* track for fairness */
        if (iter >= WARMUP) {
            if (tdata->tid == lastt)
                dupt++;
            lastt = tdata->tid;
        }

        derr = dragon_unlock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to release lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    tdata->timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    derr = dragon_lock_detach(&dlock);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to detach from lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    return NULL;
}


void * fifo_bench(void * tdatav)
{
    tdata_t * tdata = (tdata_t *)tdatav;

    dragonError_t derr;
    dragonFIFOLock_t dlock;

    derr = dragon_fifo_lock_attach(&dlock, tdata->ptr);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to init fifo lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    timespec_t t1, t2;

    uint64_t iter;
    for (iter = 0UL; iter < NITERS+WARMUP; iter++) {

        if (iter == WARMUP) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
        }

        derr = dragon_fifo_lock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to obtain fifo lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }

        /* track for fairness */
        if (iter >= WARMUP) {
            if (tdata->tid == lastt)
                dupt++;
            lastt = tdata->tid;
        }

        derr = dragon_fifo_unlock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to release fifo lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    tdata->timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    derr = dragon_fifo_lock_detach(&dlock);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to detach from fifo lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    return NULL;
}

void * fifolite_bench(void * tdatav)
{
    tdata_t * tdata = (tdata_t *)tdatav;

    dragonError_t derr;
    dragonFIFOLiteLock_t dlock;

    derr = dragon_fifolite_lock_attach(&dlock, tdata->ptr);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to init fifolite lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    timespec_t t1, t2;
    uint64_t iter;

    for (iter = 0UL; iter < NITERS+WARMUP; iter++) {

        if (iter == WARMUP) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
        }

        derr = dragon_fifolite_lock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to obtain fifolite lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }

        /* track for fairness */
        if (iter >= WARMUP) {
            if (tdata->tid == lastt)
                dupt++;
            lastt = tdata->tid;
        }

        derr = dragon_fifolite_unlock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to release fifolite lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    tdata->timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    derr = dragon_fifolite_lock_detach(&dlock);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to detach from fifolite lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    return NULL;
}

void * greedy_bench(void * tdatav)
{
    tdata_t * tdata = (tdata_t *)tdatav;

    dragonError_t derr;
    dragonGreedyLock_t dlock;

    derr = dragon_greedy_lock_attach(&dlock, tdata->ptr);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to init greedy lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    timespec_t t1, t2;
    uint64_t iter;

    for (iter = 0UL; iter < NITERS+WARMUP; iter++) {

        if (iter == WARMUP) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
        }

        derr = dragon_greedy_lock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to obtain greedy lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }

        /* track for fairness */
        if (iter >= WARMUP) {
            if (tdata->tid == lastt)
                dupt++;
            lastt = tdata->tid;
        }

        derr = dragon_greedy_unlock(&dlock);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to release greedy lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    tdata->timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    derr = dragon_greedy_lock_detach(&dlock);
    if (derr != DRAGON_SUCCESS) {
        printf("Failed to detach from greedy lock (err = %i)\n", derr);
        pthread_exit(NULL);
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    int fd, serr;
    int api_fd, api_serr;
    void * shmptr = NULL;
    void * api_shmptr = NULL;

    if (argc != 2) {
        printf("Error, give me the number of threads!\n");
        return 1;
    }

    int nthreads = atoi(argv[1]);
    printf("Running with %i threads\n", nthreads);

    dragonError_t derr;
    dragonError_t api_derr;
    dragonError_t lerr;

    dragonLock_t api_lock;
    /* Re-order these to change lock bench ordering
       0 -- FIFO
       1 -- FIFOLite
       2 -- Greedy
    */
    int lock_order[3] = {0, 1, 2};

    dragonFIFOLock_t fifo_lock;
    dragonFIFOLiteLock_t fifolite_lock;
    dragonGreedyLock_t greedy_lock;

    int num_locks = 3;
    for (int itr = 0; itr < num_locks; itr++)
    {
        int cur_lock = lock_order[itr];
        printf("\nBenching Lock Type: %s\n", lock_type[cur_lock]);
        switch(cur_lock) {
        case 0:
            dkind = DRAGON_LOCK_FIFO;
            break;

        case 1:
            dkind = DRAGON_LOCK_FIFO_LITE;
            break;

        case 2:
            dkind = DRAGON_LOCK_GREEDY;
            break;
        }

        size_t needed_space = dragon_lock_size(dkind);
        needed_space = 4096 * ((size_t)((float)needed_space / 4096.) + 1);
#ifdef VERBOSE
        printf("Creating file mapping of %lu bytes\n", needed_space);
#endif

        /* open up space for the lock */
#ifndef USEFILE
        fd = shm_open("/dragon_lock_test", O_RDWR | O_CREAT | O_EXCL , S_IRUSR | S_IWUSR);
        api_fd = shm_open("/api_dragon_lock_test", O_RDWR | O_CREAT | O_EXCL , S_IRUSR | S_IWUSR);
#else
        fd = open("./dragon_lock_test", O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
        api_fd = open("./api_dragon_lock_test", O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
#endif
        if (fd == -1 || api_fd == -1) {
            printf("Failed to create shared memory segment (File already exists?)\n");
            munmap(shmptr, needed_space);
            munmap(api_shmptr, needed_space);
#ifdef USEFILE
            fd = shm_unlink("/dragon_lock_test");
            api_fd = shm_unlink("/api_dragon_lock_test");
#else
            close(fd);
            close(api_fd);
#endif

            return 1;
        }

        /* make it the right size */
        serr = ftruncate(fd, needed_space);
        api_serr = ftruncate(api_fd, needed_space);
        if (serr == -1 || api_serr == -1) {
            printf("Failed to resize shm to %li bytes\n", needed_space);
            return 1;
        }

        shmptr = mmap(NULL, needed_space, PROT_WRITE, MAP_SHARED, fd, 0);
        api_shmptr = mmap(NULL, needed_space, PROT_WRITE, MAP_SHARED, api_fd, 0);
        if (shmptr == MAP_FAILED || api_shmptr == MAP_FAILED) {
            printf("Failed to map shared memory\n");
            return 1;
        }

        dragonLockState_t state;

        api_derr = dragon_lock_init(&api_lock, api_shmptr, dkind);
        switch(cur_lock) {
        case 0:
            derr = dragon_fifo_lock_init(&fifo_lock, shmptr);
            break;

        case 1:
            derr = dragon_fifolite_lock_init(&fifolite_lock, shmptr);
            break;

        case 2:
            derr = dragon_greedy_lock_init(&greedy_lock, shmptr);
            break;
        }

        if (derr != DRAGON_SUCCESS || api_derr != DRAGON_SUCCESS) {
            printf("Failed to init lock (err = %i | %i)\n", derr, api_derr);
            pthread_exit(NULL);
        }

        api_derr = dragon_lock(&api_lock);

        lerr = dragon_lock_state(&api_lock, &state);

        if (lerr != DRAGON_SUCCESS) {
            printf("Failed to get Lock State for lock (err = %i)\n", lerr);
            pthread_exit(NULL);
        }

        if (state != DRAGON_LOCK_STATE_LOCKED) {
            printf("State of lock should be locked and got %d instead\n", state);
            pthread_exit(NULL);
        }

        switch(cur_lock) {
        case 0:
            derr = dragon_fifo_lock(&fifo_lock);
            lerr = dragon_fifo_lock_state(&fifo_lock, &state);

            if (lerr != DRAGON_SUCCESS) {
                printf("Failed to get Lock State for fifo lock (err = %i)\n", lerr);
                pthread_exit(NULL);
            }

            if (state != DRAGON_LOCK_STATE_LOCKED) {
                printf("State of fifo lock should be locked and got %d instead\n", state);
                pthread_exit(NULL);
            }
            break;

        case 1:
            derr = dragon_fifolite_lock(&fifolite_lock);
            lerr = dragon_fifolite_lock_state(&fifolite_lock, &state);

            if (lerr != DRAGON_SUCCESS) {
                printf("Failed to get Lock State for fifolite lock (err = %i)\n", lerr);
                pthread_exit(NULL);
            }

            if (state != DRAGON_LOCK_STATE_LOCKED) {
                printf("State of fifolite lock should be locked and got %d instead\n", state);
                pthread_exit(NULL);
            }
            break;

        case 2:
            derr = dragon_greedy_lock(&greedy_lock);
            lerr = dragon_greedy_lock_state(&greedy_lock, &state);

            if (lerr != DRAGON_SUCCESS) {
                printf("Failed to get Lock State for greedy lock (err = %i)\n", lerr);
                pthread_exit(NULL);
            }

            if (state != DRAGON_LOCK_STATE_LOCKED) {
                printf("State of greedy lock should be locked and got %d instead\n", state);
                pthread_exit(NULL);
            }
            break;
        }

        if (derr != DRAGON_SUCCESS || api_derr != DRAGON_SUCCESS) {
            printf("Failed to obtain lock (err = %i)\n", derr);
            pthread_exit(NULL);
        }

        for (int jtr = 0; jtr < 2; jtr++) {
            /* setup our fairness trackers */
            dupt = 0UL;
            lastt = -1;

            /* create threads to contend with */
            int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
            int i;
            pthread_t * pts;
            tdata_t * tdata;
            pts = malloc(sizeof(pthread_t) * nthreads);
            tdata = malloc(sizeof(tdata_t) * nthreads);
            pthread_attr_t attr;
            pthread_attr_init(&attr);

            printf("%s %s: ", lock_type[cur_lock], (jtr == 0 ? "Direct" : "API"));

            for (i = 0; i < nthreads; i++) {

                if (jtr != 0) {
                    tdata[i].ptr = api_shmptr;
                } else {
                    tdata[i].ptr = shmptr;
                }
                tdata[i].tid = i;

                int terr;
                if (jtr != 0) {
                    terr = pthread_create(&pts[i], &attr, api_bench, (void *)&tdata[i]);
                } else {
                    switch(cur_lock) {
                    case 0:
                        terr = pthread_create(&pts[i], &attr, fifo_bench, (void *)&tdata[i]);
                        break;

                    case 1:
                        terr = pthread_create(&pts[i], &attr, fifolite_bench, (void *)&tdata[i]);
                        break;

                    case 2:
                        terr = pthread_create(&pts[i], &attr, greedy_bench, (void *)&tdata[i]);
                        break;

                    }
                }

                if (terr != 0) {
                    printf("Failed to start pthread in loop\n");
                    exit(1);
                }


                int target_core;
                target_core = i;
                if (target_core >= num_cores) {
                    target_core -= num_cores;
                }
                bind_thread_to_core(pts[i], i+1);
            }

            printf("Starting... ");
            sleep(2);

            if (jtr != 0) {
                derr = dragon_unlock(&api_lock);

            } else {
                switch (cur_lock) {
                case 0:
                    derr = dragon_fifo_unlock(&fifo_lock);
                    break;

                case 1:
                    derr = dragon_fifolite_unlock(&fifolite_lock);
                    break;

                case 2:
                    derr = dragon_greedy_unlock(&greedy_lock);
                    break;
                }
            }
            if (derr != DRAGON_SUCCESS) {
                printf("Failed to release lock (err = %i)\n", derr);
                pthread_exit(NULL);
            }

            double avg_time = 0.0;
            double avg_ops = 0.0;
            for (i = 0; i < nthreads; i++) {
                pthread_join(pts[i], NULL);
#ifdef VERBOSE
                printf("Thread %i: %f seconds (%f ops/s)\n", i, tdata[i].timer,
                       (double)NITERS/tdata[i].timer);
#endif
                avg_time += tdata[i].timer;
                avg_ops += ((double)NITERS)/tdata[i].timer;
            }
            avg_time = avg_time / nthreads;
            avg_ops = avg_ops / nthreads;

            printf("Average for %i threads: %f seconds (%f ops/s)\n", nthreads, avg_time, avg_ops);

            free(pts);
            free(tdata);

            if (jtr != 0) {
                dragon_lock_destroy(&api_lock);
            } else {
                switch (cur_lock) {
                case 0:
                    dragon_fifo_lock_destroy(&fifo_lock);
                    break;

                case 1:
                    dragon_fifolite_lock_destroy(&fifolite_lock);
                    break;

                case 2:
                    dragon_greedy_lock_destroy(&greedy_lock);
                    break;
                }
            }

            if (jtr != 0) {
                serr = munmap(api_shmptr, needed_space);
            } else {
                serr = munmap(shmptr, needed_space);
            }
            if (serr == -1) {
                printf("Failed to unmap shm\n");
                return 1;
            }

#ifndef USEFILE
            if (jtr != 0) {
                api_fd = shm_unlink("/api_dragon_lock_test");
            } else {
                fd = shm_unlink("/dragon_lock_test");
            }
#else
            if (jtr != 0) {
                close(api_fd);
            } else {
                close(fd);
            }
#endif
            if (fd == -1) {
                printf("Failed to unlink shm\n");
                return 1;
            }

            double unfairness;
            unfairness = (double)dupt / ((double)nthreads * (double)NITERS);
            printf("Unfairness = %f (0 == FIFO)\n", unfairness);
        }

    }

    return 0;
}
