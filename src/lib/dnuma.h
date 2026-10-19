#ifndef HAVE_DRAGON_NUMA_H
#define HAVE_DRAGON_NUMA_H

#include <sys/types.h>

#ifndef __APPLE__
#include <numa.h>
#else
struct bitmask;
#endif //__APPLE__

#include <dragon/return_codes.h>

#ifdef __cplusplus
extern "C" {
#endif

dragonError_t
dragon_numa_found();

dragonError_t
dragon_numa_available();

dragonError_t
dragon_numa_allocate_nodemask(struct bitmask** bm);

dragonError_t
dragon_numa_bitmask_setbit(struct bitmask* bm, unsigned int n);

dragonError_t
dragon_numa_bitmask_setall(struct bitmask* bm);

dragonError_t
dragon_numa_free_nodemask(struct bitmask* bm);

dragonError_t
dragon_numa_interleave_memory(void* addr, size_t size, struct bitmask* bm);

dragonError_t
dragon_numa_sched_getaffinity(pid_t pid, struct bitmask* bm);

dragonError_t
dragon_numa_sched_setaffinity(pid_t pid, struct bitmask* bm);

dragonError_t
dragon_numa_num_configured_nodes(int* num_nodes);

dragonError_t
dragon_numa_node_to_cpus(int node, struct bitmask* bm);

dragonError_t
dragon_numa_allocate_cpumask(struct bitmask** bm);

dragonError_t
dragon_numa_free_cpumask(struct bitmask* bm);

dragonError_t
dragon_numa_bitmask_isbitset(struct bitmask* bm, unsigned int n, int* bitset);

dragonError_t
dragon_numa_bitmask_size(struct bitmask* bm, unsigned long* size);

#ifdef __cplusplus
}
#endif

#endif
