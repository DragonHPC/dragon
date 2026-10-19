#include "dnuma.h"
#include "err.h"
#include <dlfcn.h>
#include <stdlib.h>

/* Define numa API functions to allow dlopen of said libraries */
static int _numa_load_tried = 0;
static int _numa_pointers_set = 0;

int (*numa_available_p)(void);
struct bitmask* (*numa_allocate_nodemask_p)(void);
struct bitmask* (*numa_bitmask_setall_p)(struct bitmask*);
struct bitmask* (*numa_bitmask_setbit_p)(struct bitmask*, unsigned int);
void (*numa_interleave_memory_p)(void*, size_t, struct bitmask*);
void (*numa_free_nodemask_p)(struct bitmask*);
void (*numa_bitmask_free_p)(struct bitmask*);
int (*numa_sched_getaffinity_p)(pid_t, struct bitmask*);
int (*numa_sched_setaffinity_p)(pid_t, struct bitmask*);
int (*numa_num_configured_nodes_p)(void);
int (*numa_node_to_cpus_p)(int, struct bitmask*);
int (*numa_bitmask_isbitset_p)(struct bitmask*, unsigned int);
void (*numa_free_cpumask_p)(struct bitmask*);
struct bitmask* (*numa_allocate_cpumask_p)(void);

#define CHECK_NUMA_AVAILABLE()                   \
    dragonError_t err = dragon_numa_available(); \
    if (err != DRAGON_SUCCESS)                   \
        append_err_return(err, "numa not found");

/* Open numactl function pointers */
static int
_set_numa_function_pointers()
{
    if (!_numa_load_tried) {
        if (!_numa_pointers_set) {
            void* lib_numa_handle = dlopen("libnuma.so.1", RTLD_LAZY | RTLD_GLOBAL);

            if (lib_numa_handle != NULL) {

                numa_available_p = dlsym(lib_numa_handle, "numa_available");
                numa_allocate_nodemask_p = dlsym(lib_numa_handle, "numa_allocate_nodemask");
                numa_free_nodemask_p = dlsym(lib_numa_handle, "numa_free_nodemask");
                numa_num_configured_nodes_p = dlsym(lib_numa_handle, "numa_num_configured_nodes");
                numa_node_to_cpus_p = dlsym(lib_numa_handle, "numa_node_to_cpus");
                numa_allocate_cpumask_p = dlsym(lib_numa_handle, "numa_allocate_cpumask");
                numa_free_cpumask_p = dlsym(lib_numa_handle, "numa_free_cpumask");
                numa_bitmask_setall_p = dlsym(lib_numa_handle, "numa_bitmask_setall");
                numa_bitmask_setbit_p = dlsym(lib_numa_handle, "numa_bitmask_setbit");
                numa_bitmask_free_p = dlsym(lib_numa_handle, "numa_bitmask_free");
                numa_bitmask_isbitset_p = dlsym(lib_numa_handle, "numa_bitmask_isbitset");
                numa_interleave_memory_p = dlsym(lib_numa_handle, "numa_interleave_memory");
                numa_sched_getaffinity_p = dlsym(lib_numa_handle, "numa_sched_getaffinity");
                numa_sched_setaffinity_p = dlsym(lib_numa_handle, "numa_sched_setaffinity");

                _numa_pointers_set = numa_available_p != NULL && numa_allocate_nodemask_p != NULL &&
                    numa_num_configured_nodes_p != NULL && numa_node_to_cpus_p != NULL &&
                    numa_allocate_cpumask_p != NULL && numa_bitmask_setall_p != NULL &&
                    numa_bitmask_setbit_p != NULL && numa_bitmask_free_p != NULL &&
                    numa_bitmask_isbitset_p != NULL && numa_interleave_memory_p != NULL &&
                    numa_sched_getaffinity_p != NULL && numa_sched_setaffinity_p != NULL;
                if (!_numa_pointers_set)
                    dlclose(lib_numa_handle);
            }
        }
        _numa_load_tried = 1;
    }

    return _numa_pointers_set;
}

dragonError_t
dragon_numa_found()
{
    if (!_set_numa_function_pointers())
        err_return(DRAGON_NUMA_NOT_FOUND, "libnuma not found or could not load symbols");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_available()
{
    dragonError_t err = dragon_numa_found();
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "numa not found");

    int ret = numa_available_p();
    if (ret < 0)
        err_return(DRAGON_NUMA_NOT_AVAILABLE, "numa not available");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_allocate_nodemask(struct bitmask** bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask output pointer is NULL");

    *bm = numa_allocate_nodemask_p();
    if (*bm == NULL)
        err_return(DRAGON_NUMA_ERROR, "failed to allocate nodemask");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_bitmask_setbit(struct bitmask* bm, unsigned int n)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    struct bitmask* ret = numa_bitmask_setbit_p(bm, n);
    if (ret == NULL)
        err_return(DRAGON_NUMA_ERROR, "failed to set bit in bitmask");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_bitmask_setall(struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    struct bitmask* ret = numa_bitmask_setall_p(bm);
    if (ret == NULL)
        err_return(DRAGON_NUMA_ERROR, "failed to set all bits in bitmask");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_free_nodemask(struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    // In some headers numa_free_nodesmask is replaced with an inline
    // def of numa_bitmask_free. Respect that if numa_free_nodemask comes
    // back NULL
    if (numa_free_nodemask_p == NULL) {
        numa_bitmask_free_p(bm);
    } else {
        numa_free_nodemask_p(bm);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_interleave_memory(void* addr, size_t size, struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (addr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "addr is NULL");

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    numa_interleave_memory_p(addr, size, bm);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_sched_getaffinity(pid_t pid, struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    int ret = numa_sched_getaffinity_p(pid, bm);
    if (ret != 0)
        err_return(DRAGON_NUMA_ERROR, "failed to get affinity");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_sched_setaffinity(pid_t pid, struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    int ret = numa_sched_setaffinity_p(pid, bm);
    if (ret != 0)
        err_return(DRAGON_NUMA_ERROR, "failed to set affinity");
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_num_configured_nodes(int* num_nodes)
{
    CHECK_NUMA_AVAILABLE();

    if (num_nodes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "num_nodes is NULL");

    *num_nodes = numa_num_configured_nodes_p();
    if (*num_nodes < 0)
        err_return(DRAGON_NUMA_ERROR, "failed to get number of configured nodes");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_node_to_cpus(int node, struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    if (node < 0)
        err_return(DRAGON_INVALID_ARGUMENT, "NUMA node is negative");

    int ret = numa_node_to_cpus_p(node, bm);
    if (ret != 0)
        err_return(DRAGON_NUMA_ERROR, "failed to get cpus for node");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_allocate_cpumask(struct bitmask** bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask output pointer is NULL");
    *bm = numa_allocate_cpumask_p();
    if (*bm == NULL)
        err_return(DRAGON_NUMA_ERROR, "failed to allocate cpumask");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_free_cpumask(struct bitmask* bm)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    // In some headers numa_free_cpumask is replaced with an inline
    // def of numa_bitmask_free. Respect that if numa_free_cpumask comes
    // back NULL
    if (numa_free_cpumask_p == NULL) {
        numa_bitmask_free_p(bm);
    } else {
        numa_free_cpumask_p(bm);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_bitmask_isbitset(struct bitmask* bm, unsigned int n, int* bitset)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    if (bitset == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitset output pointer is NULL");

    int ret = numa_bitmask_isbitset_p(bm, n);
    *bitset = ret;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_numa_bitmask_size(struct bitmask* bm, unsigned long* size)
{
    CHECK_NUMA_AVAILABLE();

    if (bm == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask is NULL");

    if (size == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "bitmask size output pointer is NULL");

#ifdef __APPLE__
    err_return(DRAGON_NUMA_NOT_AVAILABLE, "NUMA is not available on macOS");
#else
    *size = bm->size;
    no_err_return(DRAGON_SUCCESS);
#endif
}
