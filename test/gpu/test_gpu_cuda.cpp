#include "../../src/lib/gpu/gpu.hpp"

#include <atomic>
#include <stdint.h>
#include <sys/wait.h>
#include <unistd.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h> // For fork()
#include <iostream>

#define SHM_KEY "/SHARED_IPC_HANDLE" // A unique key for the shared memory segment


#include "../_ctest_utils.h"

#define NULL_BYTE 0x00
#define NULL_VAL  0x0ul

#define EXP_DST_BYTE 0xff
#define EXP_DST_VAL  0xfffffffffffffffful

static void
check_result(dragonGPUHandle_t *gpuh, dragonError_t err, dragonError_t expected_err, int& tests_passed, int& tests_attempted, const char *file, int line)
{
    ++tests_attempted;

    if (err != expected_err) {
        const int strlen = 256;
        char errstr[strlen];
        dragon_gpu_get_errstr(gpuh, "GPU operation failed", err, errstr, strlen);
        fprintf(
            stderr,
            "Test %d failed with error code %s in file %s at line %d\n",
            tests_attempted,
            dragon_get_rc_string(err),
            file, line
        );
        fprintf(stderr, "%s\n", errstr);
        abort();
    } else {
        ++tests_passed;
    }
}


int
main(int argc, char **argv)
{
    auto derr = DRAGON_SUCCESS;
    auto ntests_passed = 0;
    auto ntests_attempted = 0;
    auto gpu_backend_type = DRAGON_GPU_BACKEND_CUDA;

    if (argc > 1) {
        auto tmp_argstr = argv[1];
        if (0 == strcmp(tmp_argstr, "cuda")) {
            gpu_backend_type = DRAGON_GPU_BACKEND_CUDA;
        } else if (0 == strcmp(tmp_argstr, "hip")) {
            gpu_backend_type = DRAGON_GPU_BACKEND_HIP;
        } else if (0 == strcmp(tmp_argstr, "ze")) {
            gpu_backend_type = DRAGON_GPU_BACKEND_ZE;
        }
    }

    auto sem_rc = 0;
    // Allocate shared memory for the semaphore
    sem_t *sem = (sem_t *)mmap(NULL, sizeof(sem_t),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sem == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    sem_rc = sem_init(sem, 1, 0); // pshared=1 for inter-process
    if (sem_rc != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }

    auto dst_addr = (void *)nullptr;
    auto size = 8ul;

    if (auto pid = fork()) { // parent

        dragonGPUHandle_t gpuh;

        derr = dragon_gpu_setup(gpu_backend_type, &gpuh);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        size_t handle_size = dragon_gpu_get_ipc_handle_size(&gpuh);

        // set up shared memory to be used later for IPC handle

        auto fd = shm_open(SHM_KEY, O_CREAT | O_RDWR, 0666);
        if (fd == -1) {
            perror("shm_open failed");
            abort();
        }

        auto ftrun_error = ftruncate(fd, handle_size);
        if (ftrun_error == -1) {
            perror("ftruncate failed");
            shm_unlink(SHM_KEY);
            close(fd);
            abort();
        }

        void *shared_ipc_handle = mmap(NULL, handle_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        derr = dragon_gpu_mem_alloc(&gpuh, &dst_addr, size);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        // start with a sanity test

        volatile uint64_t *dst_val = (volatile uint64_t *) malloc(sizeof(uint64_t));
        assert(dst_val != nullptr);

        *dst_val = 0ul;

        derr = dragon_gpu_memset(&gpuh, dst_addr, EXP_DST_BYTE, size);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_copy(&gpuh, (void *) dst_val, dst_addr, size, DRAGON_GPU_D2H);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        assert(*dst_val == EXP_DST_VAL);
        *dst_val = 0ul;

        // now clear dst_addr for the main test

        derr = dragon_gpu_memset(&gpuh, dst_addr, NULL_BYTE, size);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        // get ipc handle to be used by child
        dragonIPCHandle_t ipc_handle;

        derr = dragon_gpu_get_ipc_handle(&gpuh, dst_addr, &ipc_handle);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        // copy ipc handle to shared memory
        memcpy(shared_ipc_handle, ipc_handle.data.data(), ipc_handle.data.size());

        // let child know it can get the ipc handle
        sem_rc = sem_post(sem);
        if (sem_rc != 0) {
            fprintf(stderr, "Failed to post to semaphore: %s\n", strerror(errno));
            sem_destroy(sem);
            return EXIT_FAILURE;
        }

        // wait for flag to be set by child

        while (*dst_val == NULL_VAL) {
            derr = dragon_gpu_copy(&gpuh, (void *) dst_val, dst_addr, size, DRAGON_GPU_D2H);
            check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);
        }
        assert(*dst_val == EXP_DST_VAL);

        // let the child detach from dst_addr before we clean up
        int wstatus;
        wait(&wstatus);

        derr = dragon_gpu_free_ipc_handle(&gpuh, &ipc_handle);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_mem_free(&gpuh, dst_addr);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_cleanup(&gpuh);
        check_result(&gpuh, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        auto munmap_rc = munmap(shared_ipc_handle, handle_size);
        if (munmap_rc == -1) {
            perror("munmap failed");
            abort();
        }

        auto close_rc = close(fd);
        if (close_rc == -1) {
            perror("close failed");
            shm_unlink(SHM_KEY);
            abort();
        }

        auto shm_unlink_rc = shm_unlink(SHM_KEY);
        if (shm_unlink_rc == -1) {
            perror("shm_unlink failed");
            abort();
        }

    } else { // child
        // attach to dst_addr and write flag for parent


        // setup gpu
        dragonGPUHandle_t gpuh_child;

        derr = dragon_gpu_setup(gpu_backend_type, &gpuh_child);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        // wait for ipc handle to be made available by parent
        sem_rc = sem_wait(sem);
        if (sem_rc != 0) {
            fprintf(stderr, "Failed to wait on semaphore: %s\n", strerror(errno));
            sem_destroy(sem);
            return EXIT_FAILURE;
        }

        size_t handle_size = dragon_gpu_get_ipc_handle_size(&gpuh_child);


        auto fd = shm_open(SHM_KEY, O_CREAT | O_RDWR, 0666);
        if (fd == -1) {
            perror("shm_open failed");
            abort();
        }

        auto ftrun_error = ftruncate(fd, handle_size);
        if (ftrun_error == -1) {
            perror("ftruncate failed");
            shm_unlink(SHM_KEY);
            close(fd);
            abort();
        }

        void *shared_ipc_handle = mmap(NULL, handle_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);


        // get ipc handle
        dragonIPCHandle_t ipc_handle;
        ipc_handle.data.resize(handle_size); // Set to the correct size
        memcpy(ipc_handle.data.data(), shared_ipc_handle, handle_size);

        // attach to ipc handle
        derr = dragon_gpu_attach(&gpuh_child, &ipc_handle, &dst_addr);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        auto src_addr = (void *)nullptr;

        derr = dragon_gpu_mem_alloc(&gpuh_child, &src_addr, size);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_memset(&gpuh_child, src_addr, EXP_DST_BYTE, size);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_copy(&gpuh_child, dst_addr, src_addr, size, DRAGON_GPU_D2D);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_detach(&gpuh_child, dst_addr);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_mem_free(&gpuh_child, src_addr);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        derr = dragon_gpu_cleanup(&gpuh_child);
        check_result(&gpuh_child, derr, DRAGON_SUCCESS, ntests_passed, ntests_attempted, __FILE__, __LINE__);

        auto munmap_rc = munmap(shared_ipc_handle, handle_size);
        if (munmap_rc == -1) {
            perror("munmap failed");
            abort();
        }

        auto close_rc = close(fd);
        if (close_rc == -1) {
            perror("close failed");
            shm_unlink(SHM_KEY);
            abort();
        }

	exit(EXIT_SUCCESS);
    }

    sem_destroy(sem);
    munmap(sem, sizeof(sem_t));

    fprintf(stdout, "%d out of %d tests passsed\n", ntests_passed, ntests_attempted);
    fflush(stdout);
}

