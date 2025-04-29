#include <numa.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdatomic.h>
#include "_managed_memory.h"
#include "_utils.h"
#include "hostid.h"
#include "_heap_manager.h"
#include <dragon/channels.h>
#include <dragon/utils.h>

/* Define numa API functions to allow dlopen of said libraries */
static int _numa_pointers_set = 0;

int (*numa_available_p)(void);
struct bitmask* (*numa_allocate_nodemask_p)(void);
struct bitmask* (*numa_bitmask_setall_p)(struct bitmask*);
void (*numa_interleave_memory_p)(void*, size_t, struct bitmask*);
void (*numa_free_nodemask_p)(struct bitmask*);


/* dragon globals */
DRAGON_GLOBAL_MAP(pools);
DRAGON_GLOBAL_MAP(mallocs);

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define _obtain_manifest_lock(pool) ({\
    if (pool == NULL) {\
        dragonError_t lerr = DRAGON_INVALID_ARGUMENT;\
        char * err_str = _errstr_with_code("manifest lock error code. pool is null", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return lerr;\
    }\
    dragonError_t err = dragon_lock(&pool->mlock);\
    if (err != DRAGON_SUCCESS) {\
        char * err_str = _errstr_with_code("manifest lock error code", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return err;\
    }\
})

#define _release_manifest_lock(pool) ({\
    if (pool == NULL) {\
        dragonError_t lerr = DRAGON_INVALID_ARGUMENT;\
        char * err_str = _errstr_with_code("manifest lock error code. pool is null", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return lerr;\
    }\
    dragonError_t err = dragon_unlock(&pool->mlock);\
    if (err != DRAGON_SUCCESS) {\
        char * err_str = _errstr_with_code("manifest unlock error code", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return err;\
    }\
})

#define _maybe_obtain_manifest_lock(pool) ({\
    dragonError_t err = dragon_lock(&pool->mlock);\
    if (err != DRAGON_SUCCESS && err != DRAGON_OBJECT_DESTROYED) {\
        char * err_str = _errstr_with_code("manifest lock error code", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return err;\
    }\
})

#define _maybe_release_manifest_lock(pool) ({\
    dragonError_t err = dragon_unlock(&pool->mlock);\
    if (err != DRAGON_SUCCESS && err != DRAGON_OBJECT_DESTROYED) {\
        char * err_str = _errstr_with_code("manifest unlock error code", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return err;\
    }\
})


/* Open numactl function pointers */
int _set_numa_function_pointers() {

    if (!_numa_pointers_set) {
        void *lib_numa_handle = dlopen("libnuma.so.1", RTLD_LAZY | RTLD_GLOBAL);

        if (lib_numa_handle != NULL) {
            numa_available_p = dlsym(lib_numa_handle, "numa_available");
            numa_allocate_nodemask_p = dlsym(lib_numa_handle, "numa_allocate_nodemask");
            numa_bitmask_setall_p = dlsym(lib_numa_handle, "numa_bitmask_setall");
            numa_interleave_memory_p =dlsym(lib_numa_handle, "numa_interleave_memory");
            numa_free_nodemask_p = dlsym(lib_numa_handle, "numa_free_nodemask");

            _numa_pointers_set = 1;
        }
    }

    return _numa_pointers_set;
}


/* This converts from a data pointer to an internal heap manager pointer. */
static void*
_to_hoffset(dragonMemoryPool_t* pool, void* dptr) {
    if (pool==NULL)
        return NULL;

    /* When the pool is destroyed, the lock is destroyed. If
       this happens, then something is trying to use managed mem (or free it)
       after the pool is destroyed. */
    if (!dragon_lock_is_valid(&pool->heap.mgrs[0].dlock))
        return NULL;

    uint64_t hptr = (uint64_t)dptr - (uint64_t)pool->local_dptr;

    return (void*)hptr;
}

/* This converts from an internal heap manager pointer to a data pointer. */
static void*
_to_dptr(dragonMemoryPool_t* pool,  void* hoffset) {
    if (pool==NULL)
        return NULL;

    /* When the pool is destroyed, the lock is destroyed. If
       this happens, then something is trying to use managed mem (or free it)
       after the pool is destroyed. */
    if (!dragon_lock_is_valid(&pool->heap.mgrs[0].dlock))
        return NULL;

    void* dptr = (void*)((uint64_t)hoffset + (uint64_t)pool->local_dptr);

    return dptr;
}

static void
_find_pow2_requirement(size_t v, size_t * nv, uint32_t * power)
{
    uint32_t pow = 0;
    size_t sz = 1;

    if (v == 0) {
        *power = 0;
        *nv = 1;
        return;
    }

    v = v - 1; /* just in case v is an exact power of 2 */

    while (v > 0) {
        v = v >> 1;
        pow += 1;
        sz = sz << 1;
    }

    *power = pow;
    *nv = sz;
}

static uint64_t
_round_up(uint64_t val, uint64_t multiple)
{
    if (multiple == 0UL) {
        return val;
    }

    uint64_t remainder = val % multiple;
    if (remainder == 0UL) {
        return val;
    }

    return val + multiple - remainder;
}

static dragonError_t
_pool_from_descr(const dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPool_t ** pool)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem_multikey(dg_pools, pool_descr->_rt_idx, pool_descr->_idx, (void *)pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in pools umap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
 _pool_descr_from_uids(const dragonRT_UID_t rt_uid, const dragonM_UID_t m_uid, dragonMemoryPoolDescr_t * pool_descr)
 {
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonMemoryPool_t * pool;
    dragonError_t err = dragon_umap_getitem_multikey(dg_pools, rt_uid, m_uid, (void *)&pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in pools umap");

    /* update the descriptor with the m_uid key and note this cannot be original */
    pool_descr->_rt_idx = rt_uid;
    pool_descr->_idx = m_uid;
    pool_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
 }

static dragonError_t
_mem_from_descr(const dragonMemoryDescr_t * mem_descr, dragonMemory_t ** mem)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor of NULL provided.");

    if (mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor of constant DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP provided.");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_mallocs, mem_descr->_idx, (void *)mem);

    if (err != DRAGON_SUCCESS) {
        char err_str[100];
        snprintf(err_str, 99, "failed to find item in dg_mallocs umap with value %lu", mem_descr->_idx);
        append_err_return(err, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_add_pool_umap_entry(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPool_t * pool,
                     dragonRT_UID_t rt_uid, dragonM_UID_t m_uid)
{
    dragonError_t err;

    if (*dg_pools == NULL) {
        *dg_pools = malloc(sizeof(dragonMap_t));
        if (*dg_pools == NULL) {
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for pools");
        }

        err = dragon_umap_create(dg_pools, DRAGON_MEMORY_POOL_UMAP_SEED);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to create umap for pools");
        }
    }

    err = dragon_umap_additem_multikey(dg_pools, rt_uid, m_uid, pool);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to insert item into pools umap");
    }

    /* store the m_uid as the key in the descriptor */
    pool_descr->_rt_idx = rt_uid;
    pool_descr->_idx = m_uid;

    /* Default _original to 0 */
    pool_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_add_alloc_umap_entry(dragonMemory_t * mem, dragonMemoryDescr_t * mem_descr)
{
    dragonError_t err;

    if (*dg_mallocs == NULL) {
        *dg_mallocs = malloc(sizeof(dragonMap_t));
        if (*dg_mallocs == NULL) {
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for allocs");
        }

        err = dragon_umap_create(dg_mallocs, DRAGON_MEMORY_MEM_UMAP_SEED);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to create umap for dg_mallocs");
        }
    }

    err = dragon_umap_additem_genkey(dg_mallocs, mem, &mem_descr->_idx);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to insert item into dg_mallocs umap");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_determine_heap_size(size_t requested_size, dragonMemoryPoolAttr_t * attr, size_t * required_size, uint32_t * min_block_power, uint32_t * max_block_power)
{
    uint32_t max_power, min_power;
    size_t req_size;
    dragonError_t err;

    _find_pow2_requirement(attr->data_min_block_size, &req_size, &min_power);

    _find_pow2_requirement(requested_size, &req_size, &max_power);

    err = dragon_heap_size(max_power, min_power, attr->lock_type, &req_size);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the heap size.");

    if (min_block_power != NULL)
        *min_block_power = min_power;

    if (max_block_power != NULL)
        *max_block_power = max_power;

    if (required_size != NULL)
        *required_size = req_size;

    no_err_return(DRAGON_SUCCESS);
}

/* determine the size of allocations required to fullfill the request data file size and manifest size */
static dragonError_t
_determine_pool_allocation_size(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    /* find the required size for the manifest. The requested_size is the number of entries in the
       manifest (max number of records it can hold) and the allocated_size is the number of bytes including
       the manifest header.
    */
    size_t blocks_size;
    dragonError_t err = dragon_blocks_size(pool->manifest_requested_size, sizeof(dragonULInt)*3, &blocks_size);

    if (err != DRAGON_SUCCESS) {
        char * err_str = _errstr_with_code("manifest table error code", (int)err);
        err_noreturn(err_str);
        free(err_str);
        return err;
    }

    /* for the requested allocation size, determine
       the actual allocation size needed to embed a heap manager into
       each while also adjusting to the POW2 it requires
    */

    uint32_t max_block_power, min_block_power;
    uint64_t manifest_heap_size;
    size_t lock_size;
    size_t bcast_size;

    lock_size = dragon_lock_size(attr->lock_type);

    err = _determine_heap_size(pool->data_requested_size, attr, &manifest_heap_size,
                               &min_block_power, &max_block_power);
    if (err != DRAGON_SUCCESS)
        append_err_return(err,"Could not get heap size in determining pool allocation size.");

    err = dragon_bcast_size(0, DRAGON_MEMORY_MANIFEST_SPIN_WAITERS, NULL, &bcast_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the bcast_size for the manifest.");

    /* For the fixed header size we take the size of the structure but subtract
       off the size of five fields: the manifest_bcast_space, the heap,
       the pre_allocs, the filenames, and the manifest_table. All fields
       within the header are 8 byte fields so it will have the same size
       as pointers to each value in the dragonMemoryPoolHeader_t
       structure. */

    size_t fixed_header_size = sizeof(dragonMemoryPoolHeader_t) - 5*sizeof(void*);

    attr->manifest_allocated_size =
        lock_size +
        bcast_size +
        fixed_header_size +
        manifest_heap_size +
        attr->npre_allocs * sizeof(size_t) +
        (attr->n_segments + 1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH +
        blocks_size;

    attr->data_min_block_size = 1UL << min_block_power;
    attr->allocatable_data_size = 1UL << max_block_power;
    attr->manifest_table_size = blocks_size;
    attr->manifest_heap_size = manifest_heap_size;
    attr->total_data_size = _round_up(attr->allocatable_data_size, DRAGON_TWO_MEG); /* No meta-data stored in data segment. */

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unlink_data_file(const char * file, dragonMemoryPoolAttr_t * attr)
{
    /* Because this is used during error recovery, don't use err_return macros.
       It would reset the backtrace string and we want the original problem. */

    if (file == NULL)
        return DRAGON_INVALID_ARGUMENT;

    int ierr;

    if (attr->mem_type == DRAGON_MEMORY_TYPE_HUGEPAGE) {
        ierr = unlink(file);
    }
    else if (attr->mem_type == DRAGON_MEMORY_TYPE_SHM) {
        ierr = shm_unlink(file);
    }
    else {
        err_return(DRAGON_MEMORY_ILLEGAL_MEMTYPE, "invalid memory type");
    }

    if (ierr == -1)
        return DRAGON_MEMORY_ERRNO;


    return DRAGON_SUCCESS;
}

static dragonError_t
_unlink_manifest_file(const char * file)
{
    /* Because this is used during error recovery, don't use err_return macros.
       It would reset the backtrace string and we want the original problem. */

    if (file == NULL)
        return DRAGON_INVALID_ARGUMENT;

    int ierr = shm_unlink(file);

    if (ierr == -1)
        return DRAGON_MEMORY_ERRNO;

    return DRAGON_SUCCESS;
}

static dragonError_t
_open_map_manifest_shm(dragonMemoryPool_t * pool, const char * mfile, size_t file_size)
{
    pool->mfd = shm_open(mfile, O_RDWR, 0);
    if (pool->mfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() manifest file (file exist?)");

    pool->mptr = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->mfd, 0);
    if (pool->mptr == MAP_FAILED)
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() manifest file");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_open_map_data(dragonMemoryPool_t * pool, const char * dfile, dragonMemoryPoolAttr_t * attr)
{
    if (attr->mem_type == DRAGON_MEMORY_TYPE_HUGEPAGE) {
        pool->dfd = open(dfile, O_RDWR, 0);
    }
    else if (attr->mem_type == DRAGON_MEMORY_TYPE_SHM) {
        pool->dfd = shm_open(dfile, O_RDWR, 0);
    }
    else {
        err_return(DRAGON_MEMORY_ILLEGAL_MEMTYPE, "invalid memory type");
    }

    if (pool->dfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() data file");

    pool->local_dptr = mmap(NULL, attr->total_data_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->dfd, 0);
    if (pool->local_dptr == MAP_FAILED)
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() data file");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_create_map_manifest_shm(dragonMemoryPool_t * pool, const char * mfile, dragonMemoryPoolAttr_t * attr)
{
    pool->mfd = shm_open(mfile, O_RDWR | O_CREAT | O_EXCL , attr->mode);
    if (pool->mfd == -1) {
        char err_str[400];
        snprintf(err_str, 399, "Failed to shm_open() and create the manifest file, ERRNO=%s: %s", strerror(errno), mfile);
        err_return(DRAGON_MEMORY_ERRNO, err_str);
    }

    int ierr = ftruncate(pool->mfd, attr->manifest_allocated_size);
    if (ierr == -1) {
        _unlink_manifest_file(mfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to ftruncate() manifest file");
    }

    pool->mptr = mmap(NULL, attr->manifest_allocated_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->mfd, 0);
    if (pool->mptr == MAP_FAILED) {
        _unlink_manifest_file(mfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() manifest file");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_create_map_data(dragonMemoryPool_t * pool, const char * dfile, dragonMemoryPoolAttr_t * attr)
{
    /* create the data file and map it in */
    if (attr->mem_type == DRAGON_MEMORY_TYPE_HUGEPAGE) {
        pool->dfd = open(dfile, O_RDWR | O_CREAT | O_EXCL, attr->mode);
    }
    else if (attr->mem_type == DRAGON_MEMORY_TYPE_SHM) {
        pool->dfd = shm_open(dfile, O_RDWR | O_CREAT | O_EXCL, attr->mode);
    }
    else {
        err_return(DRAGON_MEMORY_ILLEGAL_MEMTYPE, "invalid memory type");
    }

    if (pool->mfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to open and create data file (file exist?)");

    int ierr = ftruncate(pool->dfd, attr->total_data_size);
    if (ierr == -1) {
        _unlink_data_file(dfile, attr);
        err_return(DRAGON_MEMORY_ERRNO, "failed to ftruncate() data file");
    }

    pool->local_dptr = mmap(NULL, attr->total_data_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->dfd, 0);
    if (pool->local_dptr == MAP_FAILED) {
        _unlink_data_file(dfile, attr);
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() data file");
    }

    if (_numa_pointers_set) {
        if (numa_available_p() != -1) {
            struct bitmask *mask = numa_allocate_nodemask_p();
            numa_bitmask_setall_p(mask);
            numa_interleave_memory_p(pool->local_dptr, attr->total_data_size, mask);
            numa_free_nodemask_p(mask);
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unmap_manifest_shm(dragonMemoryPool_t * pool)
{
    if (pool->mptr == NULL)
        err_return(DRAGON_MEMORY_ERRNO, "cannot munmap() NULL manifest pointer");

    int ierr = munmap(pool->mptr, *(pool->header.manifest_allocated_size));
    if (ierr == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to munmap() manifest file");

    ierr = close(pool->mfd);
    if (ierr == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to close manifest file descriptor");

    /* Because this is used during error recovery, don't use no_err_return.
       It would reset the backtrace string and we want the original problem. */
    return DRAGON_SUCCESS;
}

static dragonError_t
_unmap_data(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    if (pool->local_dptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "cannot munmp() NULL data pointer");

    int ierr = munmap(pool->local_dptr, attr->total_data_size);
    if (ierr == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to munmap() data file");

    ierr = close(pool->dfd);
    if (ierr == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to close data file descriptor");

    /* Because this is used during error recovery, don't use no_err_return.
       It would reset the backtrace string and we want the original problem. */
    return DRAGON_SUCCESS;
}

static dragonError_t
_alloc_pool(dragonMemoryPool_t * pool, const char * base_name, dragonMemoryPoolAttr_t * attr)
{
    /* determine the allocation sizes we really need */
    dragonError_t err = _determine_pool_allocation_size(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not determine pool allocation size");


    /* create the file name for the manifest */
    char * mfile = malloc(sizeof(char) * (DRAGON_MEMORY_MAX_FILE_NAME_LENGTH));
    if (mfile == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate manifest file name string");

    int nchars = snprintf(mfile, DRAGON_MEMORY_MAX_FILE_NAME_LENGTH, "/%s%s_manifest",
                          DRAGON_MEMORY_DEFAULT_FILE_PREFIX, base_name);

    if (nchars == -1) {
        free(mfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "encoding error generating manifest filename");
    }

    if (nchars > DRAGON_MEMORY_MAX_FILE_NAME_LENGTH) {
        free(mfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "The filename for the pool manifest was too long.");
    }

    /* create the manifest and map it in */
    err = _create_map_manifest_shm(pool, mfile, attr);
    if (err != DRAGON_SUCCESS) {
        free(mfile);
        append_err_return(err, "failed to create manifest");
    }

    if (nchars == -1) {
        _unmap_manifest_shm(pool);
        _unlink_manifest_file(mfile);
        free(mfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "encoding error generating data filename");
    }

    if (nchars > DRAGON_MEMORY_MAX_FILE_NAME_LENGTH) {
        _unmap_manifest_shm(pool);
        _unlink_manifest_file(mfile);
        free(mfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "The filename for the pool data segment was too long.");
    }

    /* create the filename for the data file */
    char * dfile = malloc(sizeof(char) * (DRAGON_MEMORY_MAX_FILE_NAME_LENGTH));
    if (dfile == NULL) {
        _unmap_manifest_shm(pool);
        _unlink_manifest_file(mfile);
        free(mfile);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate data file name string");
    }

    /* create the data file and map it in */
    bool fallback_dev_shm = false;
    char *mount_dir = NULL;

    if (dragon_get_hugepage_mount(&mount_dir) == DRAGON_SUCCESS) {
        nchars = snprintf(
            dfile, DRAGON_MEMORY_MAX_FILE_NAME_LENGTH, "%s/%s%s_part%i",
            mount_dir, DRAGON_MEMORY_DEFAULT_FILE_PREFIX, base_name, 0
        );

        attr->mem_type = DRAGON_MEMORY_TYPE_HUGEPAGE;
        err = _create_map_data(pool, dfile, attr);
        if (err == DRAGON_MEMORY_ERRNO) {
            fallback_dev_shm = true;
        }
    }
    else {
        fallback_dev_shm = true;
    }

    /* mmapping a hugepage-backed buffer didn't work, so try /dev/shm */
    if (fallback_dev_shm) {
        nchars = snprintf(dfile, DRAGON_MEMORY_MAX_FILE_NAME_LENGTH, "/%s%s_part%i",
                          DRAGON_MEMORY_DEFAULT_FILE_PREFIX, base_name, 0);

        attr->mem_type = DRAGON_MEMORY_TYPE_SHM;
        err = _create_map_data(pool, dfile, attr);
    }

    /* if we can't allocate memory using either hugepages or /dev/shm, then declare defeat */
    if (err != DRAGON_SUCCESS) {
        _unmap_manifest_shm(pool);
        _unlink_manifest_file(mfile);
        free(mfile);
        free(dfile);
        append_err_return(err, "failed to create data file");
    }

    /* add in filename to the attrs */
    attr->names = malloc(sizeof(char *));
    if (attr->names == NULL) {
        _unmap_manifest_shm(pool);
        _unmap_data(pool, attr);
        _unlink_manifest_file(mfile);
        _unlink_data_file(dfile, attr);
        free(mfile);
        free(dfile);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate attr filename list");
    }
    pool->mname    = strdup(mfile);
    attr->mname    = strdup(mfile);
    attr->names[0] = strdup(dfile);
    attr->n_segments   = 0;

    free(mfile);
    free(dfile);

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_free_pool(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    /* Because this is used during error recovery, don't use err_return macros.
       It would reset the backtrace string and we want the original problem.
       By definition this should always work, so return SUCCESS no matter what. */

    _unmap_manifest_shm(pool);
    _unmap_data(pool, attr);
    _unlink_manifest_file(attr->mname);

    for (int i = 0; i < attr->n_segments + 1; i++)
        _unlink_data_file(attr->names[i], attr);

    return DRAGON_SUCCESS;
}

static dragonError_t
_instantiate_heap_managers(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    dragonError_t err;
    /* we'll embed a single heap manager for now into the managed memory
       later we can get fancier and add multiple for different ranges of allocs */
    pool->heap.nmgrs = 1;
    pool->heap.mgrs  = malloc(sizeof(dragonDynHeap_t) * pool->heap.nmgrs);
    if (pool->heap.mgrs == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate heap manager array");

    pool->heap.mgrs_dptrs = malloc(sizeof(void *) * pool->heap.nmgrs);
    if (pool->heap.mgrs_dptrs == NULL) {
        free(pool->heap.mgrs);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate heap manager dptr array");
    }

    /* compute the power of 2 value we're allocating and the block size */
    size_t required_size;
    uint32_t max_block_power, min_block_power;
    _find_pow2_requirement(pool->data_requested_size, &required_size, &max_block_power);
    _find_pow2_requirement(attr->data_min_block_size, &required_size, &min_block_power);

    /* The heap is allocated in the manifest segment. The data segment is the
       actual data handed back to users. The heap in the manifest is
       allocated as small as possible while mirroring what is handed back
       from the data segment. */

    int nsizes = max_block_power - min_block_power + 1;
    size_t * preallocated = malloc(sizeof(size_t) * nsizes);
    for (int i = 0; i < nsizes; i++)
        preallocated[i] = 0;
    int npre_allocs = nsizes;
    if (attr->npre_allocs < npre_allocs)
        npre_allocs = attr->npre_allocs;
    if (attr->pre_allocs != NULL && npre_allocs > 0) {
        for (int i = 0; i < npre_allocs; i++)
            preallocated[i] = attr->pre_allocs[i];
    }
    /* Actual allocated data is at pool->local_dptr, but the heap is managed at
       pool->header.heap Translation to correct addresses will be done when pointers
       are handed back to users. */
    pool->heap.mgrs_dptrs[0] = pool->header.heap;
    err = dragon_heap_init(pool->heap.mgrs_dptrs[0], &pool->heap.mgrs[0], max_block_power,
                min_block_power, attr->lock_type, preallocated);

    free(preallocated);
    if (err != DRAGON_SUCCESS) {
        free(pool->heap.mgrs);
        free(pool->heap.mgrs_dptrs);
        append_err_return(err, "failed to instantiate heap manager into memory");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_attach_heap_managers(dragonMemoryPool_t * const pool)
{
    dragonError_t err;
    /*
      Malloc heapmgr manager (structures)
      Malloc heapmgr data pointers (memory blobs)
      Assign heapmgr pointer to pool data pointer
      Attach heapmgr to pool heapmgr data pointer
      Init number of manifest datapointers
      Attach manifest heapmgr
    */

    dragonMemoryPoolHeap_t * heap = &(pool->heap);
    heap->nmgrs = 1;
    heap->mgrs = malloc(sizeof(dragonDynHeap_t) * heap->nmgrs);
    if (heap->mgrs == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate heap manager array");

    heap->mgrs_dptrs = malloc(sizeof(void*) * heap->nmgrs);
    if (heap->mgrs_dptrs == NULL) {
        free(heap->mgrs);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate heap manager dptr array");
    }

    /* Actual allocated data is at pool->local_dptr, but the heap is managed at
       pool->header.heap Translation to correct addresses will be done when pointers
       are handed back to users. */
    heap->mgrs_dptrs[0] = pool->header.heap;
    err = dragon_heap_attach(heap->mgrs_dptrs[0], &(heap->mgrs[0]));
    if (err != DRAGON_SUCCESS) {
        free(heap->mgrs);
        free(heap->mgrs_dptrs);
        append_err_return(err, "failed to attach heap manager to memory");
    }

    err = dragon_blocks_attach(pool->header.manifest_table, &(heap->mfstmgr));
    if (err != DRAGON_SUCCESS) {
        free(heap->mgrs);
        free(heap->mgrs_dptrs);
        append_err_return(err, "could not attach heap manager to manifest memory");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_detach_heap_managers(dragonMemoryPool_t * pool)
{
    dragonError_t err;
    dragonMemoryPoolHeap_t * heap = &(pool->heap);
    for (int idx=0; idx < heap->nmgrs; idx++) {
        err = dragon_heap_detach(&(heap->mgrs[idx]));
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to destroy a heap manager");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_destroy_heap_managers(dragonMemoryPool_t * pool)
{
    dragonError_t err;
    dragonMemoryPoolHeap_t * heap = &(pool->heap);
    for (int idx=0; idx < heap->nmgrs; idx++) {
        err = dragon_heap_destroy(&(heap->mgrs[idx]));
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to destroy a heap manager");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_assign_filenames(dragonMemoryPool_t * pool, char ** names, dragonUInt nnames)
{
    if (names == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid filename list");

    if (nnames == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "empty filename list");

    char * fname_ptr = pool->header.filenames;

    for (int i=0; i < nnames; i++) {
        strncpy(fname_ptr, names[i], DRAGON_MEMORY_MAX_FILE_NAME_LENGTH);
        fname_ptr += DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_obtain_filenames(dragonMemoryPool_t * pool, char *** names)
{
    dragonUInt n_filenames = *(pool->header.n_segments) + 1;
    /* allocate space to the names list */
    *names = malloc(sizeof(char *) * n_filenames);
    if (*names == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate list of filenames");

    char* fname_ptr = pool->header.filenames;

    for (int i=0; i<n_filenames; i++) {
        (*names)[i] = strdup(fname_ptr);
        if (*names[i] == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not copy data segment filename.");
        fname_ptr += DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_lookup_allocation(dragonMemoryPool_t * pool, uint64_t id, dragonMemory_t * out_mem)
{
    /* this routine is used both for positive and negative lookups, don't append an error string */
    dragonError_t err;
    out_mem->mfst_record.id = id;

    err = dragon_blocks_get(&(pool->heap.mfstmgr), out_mem->mfst_record.id, &(out_mem->mfst_record.type));

    return err;
}

static dragonError_t
_generate_manifest_record(dragonMemory_t * mem, dragonMemoryPool_t * pool,
                          const dragonMemoryAllocationType_t type, const timespec_t* timeout)
{
    dragonError_t err;
    timespec_t deadline;
    timespec_t remaining;

    /* generate a record and put it in the manifest */
    mem->mfst_record.offset        = (dragonULInt)((char *)mem->local_dptr - (char *)pool->local_dptr);
    mem->mfst_record.size          = (dragonULInt)mem->bytes;
    mem->mfst_record.type          = type;

    err = dragon_timespec_deadline(timeout, &deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not compute deadline for timeout.");

    err = dragon_lock(&pool->mlock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not obtain manifest lock while generating manifest record");

    err = dragon_blocks_alloc(&(pool->heap.mfstmgr), &(mem->mfst_record.type), &(mem->mfst_record.id));

    /* if a zero timeout is supplied, then don't block. This is useful
       for the managed memory code */
    if (deadline.tv_sec == 0 && deadline.tv_nsec == 0) {
        // release the lock. Don't check err and don't return since alloc_err is the
        // more important error to return.
        char* msg = dragon_getrawerrstr();
        dragon_unlock(&(pool->mlock));
        dragon_setrawerrstr(msg);
        free(msg);
        append_err_return(err, "Could not get space in manifest to store allocation record.");
    }

    while (err == DRAGON_OUT_OF_SPACE) {
        err = dragon_timespec_remaining(&deadline, &remaining);
        if (err != DRAGON_SUCCESS) {
            dragon_unlock(&(pool->mlock));
            append_err_return(err, "Could not compute time remaining.");
        }

        /* The bcast wait will unlock regardless of the return code here
           so no need to call unlock in case of error. */
        err = dragon_bcast_wait(&pool->manifest_bcast, DRAGON_ADAPTIVE_WAIT, &remaining, NULL, 0, (dragonReleaseFun)dragon_unlock, &pool->mlock);
        if (err == DRAGON_TIMEOUT)
            append_err_return(DRAGON_OUT_OF_SPACE, "We timed out waiting for a manifest table entry. The manifest is full.");

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Timeout or could not wait for manifest record in memory pool.");

        err = dragon_lock(&pool->mlock);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not obtain manifest lock while generating manifest record");

        err = dragon_blocks_alloc(&(pool->heap.mfstmgr), &(mem->mfst_record.type), &(mem->mfst_record.id));
    }

    if (err != DRAGON_OBJECT_DESTROYED)
        // release the lock. Don't check err and don't return since alloc_err is the
        // more important error to return.
        dragon_unlock(&(pool->mlock));

    if (err != DRAGON_SUCCESS) {
        char err_str[100];
        snprintf(err_str, 99, "Cannot add manifest record type=%lu and id=%lu to pool m_uid=%lu\n", mem->mfst_record.type, mem->mfst_record.id, *pool->header.m_uid);
        append_err_return(err, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_map_manifest_header(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t* attr)
{
    dragonError_t err;

    /* if we are given the lock kind, determine the size and skip over that
        for where to embed the rest */
    size_t lock_size;
    size_t bcast_size;
    if (attr != NULL) {

        lock_size = dragon_lock_size(attr->lock_type);

        /* initialize a lock into this region */
        err = dragon_lock_init(&pool->mlock, pool->mptr, attr->lock_type);
        if (err != DRAGON_SUCCESS) {
            char * err_str = _errstr_with_code("lock init error code in manifest", (int)err);
            err_noreturn(err_str);
            free(err_str);
            return err;
        }

    } else {

        /* attach a lock at the beginning.  we can then determine type and size to skip over */
        err = dragon_lock_attach(&pool->mlock, pool->mptr);
        if (err != DRAGON_SUCCESS) {
            char * err_str = _errstr_with_code("lock attach error code in manifest", (int)err);
            err_noreturn(err_str);
            free(err_str);
            return err;
        }

        lock_size = dragon_lock_size(pool->mlock.kind);

    }

    err = dragon_bcast_size(0, DRAGON_MEMORY_MANIFEST_SPIN_WAITERS, NULL, &bcast_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the bcast_size for the manifest.");


    /* Skip over the area for the lock and any extra for alignment. The header
       The header itself does not need alignment. But it is easier
       to align both the header and heap because otherwise another header value
       would be needed. */
    void * ptr = pool->mptr;
    ptr = ptr + lock_size;

    dragonULInt * hptr = (dragonULInt *)ptr;

    pool->header.m_uid                   = &hptr[0];
    pool->header.hostid                  = &hptr[1];
    pool->header.allocatable_data_size   = &hptr[2];
    pool->header.total_data_size         = &hptr[3];
    pool->header.data_min_block_size     = &hptr[4];
    pool->header.manifest_allocated_size = &hptr[5];
    pool->header.manifest_heap_size      = &hptr[6];
    pool->header.manifest_table_size     = &hptr[7];
    pool->header.segment_size            = &hptr[8];
    pool->header.max_size                = &hptr[9];
    pool->header.n_segments              = &hptr[10];
    pool->header.mem_type                = &hptr[11];
    pool->header.lock_type               = &hptr[12];
    pool->header.growth_type             = &hptr[13];
    pool->header.mode                    = (dragonUInt *)&hptr[14];
    pool->header.npre_allocs             = &hptr[15];
    pool->header.manifest_bcast_space    = (void*) &hptr[16];

    pool->header.heap                    = (void*) pool->header.manifest_bcast_space + bcast_size;

    size_t def_npre_allocs;
    size_t heap_size;

    if (attr != NULL) {
        /* If attr is not NULL then the pool is being created. So get these values from attributes. */
        def_npre_allocs = attr->npre_allocs;
        heap_size = attr->manifest_heap_size;
    } else {
        /* If attr is NULL then pool is being attached. Get these values from the created pool. */
        def_npre_allocs = *(pool->header.npre_allocs);
        heap_size = *(pool->header.manifest_heap_size);
    }

    pool->header.pre_allocs = pool->header.heap + heap_size;

    pool->header.filenames = ((char*) pool->header.pre_allocs) + sizeof(dragonULInt*) * def_npre_allocs;

    if (attr != NULL) {
        pool->header.manifest_table = (void*)pool->header.filenames +
                        sizeof(char) * (attr->n_segments+1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;

    } else {
        pool->header.manifest_table = (void*)pool->header.filenames +
                        sizeof(char) * (*pool->header.n_segments+1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;
    }

    if (attr != NULL) {
        // attr is not NULL when the pool is being created. It is NULL when the pool is being attached.
        // The code is nearly the same, so only do this sanity check when it is created.
        dragonULInt actual_size = (size_t)((void*)pool->header.manifest_table - pool->mptr) + attr->manifest_table_size;

        if (actual_size != attr->manifest_allocated_size) {
            char err_str[200];
            snprintf(err_str, 199, "The managed memory manifest actual size does not match the reserved size. Reserved bytes are %lu and the required bytes are %lu", attr->manifest_allocated_size, actual_size);
            err_return(DRAGON_FAILURE, err_str);
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_initialize_manifest_header(dragonMemoryPool_t * pool, dragonM_UID_t m_uid, dragonMemoryPoolAttr_t * attr)
{
    dragonError_t err = _map_manifest_header(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot map header region");

    /* assign all of the values into the header */
    *(pool->header.m_uid)                   = m_uid;
    *(pool->header.hostid)                  = dragon_host_id();
    *(pool->header.allocatable_data_size)   = (dragonULInt)attr->allocatable_data_size;
    *(pool->header.total_data_size)         = (dragonULInt)attr->total_data_size;
    *(pool->header.data_min_block_size)     = (dragonULInt)attr->data_min_block_size;
    *(pool->header.manifest_allocated_size) = (dragonULInt)attr->manifest_allocated_size;
    *(pool->header.manifest_heap_size)      = (dragonULInt)attr->manifest_heap_size;
    *(pool->header.manifest_table_size)     = (dragonULInt)attr->manifest_table_size;
    *(pool->header.segment_size)            = (dragonULInt)attr->segment_size;
    *(pool->header.max_size)                = (dragonULInt)attr->max_size;
    *(pool->header.n_segments)              = (dragonULInt)attr->n_segments;
    *(pool->header.mem_type)                = (dragonULInt)attr->mem_type;
    *(pool->header.lock_type)               = (dragonULInt)attr->lock_type;
    *(pool->header.growth_type)             = (dragonULInt)attr->growth_type;
    *(pool->header.mode)                    = (dragonUInt)attr->mode;
    *(pool->header.npre_allocs)             = (dragonULInt)attr->npre_allocs;

    for (int i = 0; i < attr->npre_allocs; i ++) {
        pool->header.pre_allocs[i] = attr->pre_allocs[i];
    }

    err = _assign_filenames(pool, attr->names, attr->n_segments+1);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate filenames into manifest header");

    size_t bcast_size;
    err = dragon_bcast_size(0, DRAGON_MEMORY_MANIFEST_SPIN_WAITERS, NULL, &bcast_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the bcast_size for the manifest.");

    err = dragon_bcast_create_at(pool->header.manifest_bcast_space, bcast_size, 0, DRAGON_MEMORY_MANIFEST_SPIN_WAITERS, NULL, &pool->manifest_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize the manifest bcast.");

    /* now instantiate the blocks for keeping track of manifest records */
    err = dragon_blocks_init(pool->header.manifest_table, &pool->heap.mfstmgr,
                                pool->manifest_requested_size, sizeof(dragonULInt)*3);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to instantiate hash table into manifest memory");

    /* instantiate heap managers into the manifest */
    err = _instantiate_heap_managers(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate heap managers");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_attrs_from_header(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    attr->allocatable_data_size   = *(pool->header.allocatable_data_size);
    attr->total_data_size         = *(pool->header.total_data_size);
    attr->data_min_block_size     = *(pool->header.data_min_block_size);
    attr->manifest_allocated_size = *(pool->header.manifest_allocated_size);
    attr->segment_size            = *(pool->header.segment_size);
    attr->max_size                = *(pool->header.max_size);
    attr->n_segments              = *(pool->header.n_segments);
    attr->lock_type               = *(pool->header.lock_type);
    attr->mem_type                = *(pool->header.mem_type);
    attr->growth_type             = *(pool->header.growth_type);
    attr->mode                    = *(pool->header.mode);
    attr->npre_allocs             = *(pool->header.npre_allocs);
    attr->pre_allocs              = malloc(sizeof(size_t) * attr->npre_allocs);
    if (attr->pre_allocs == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate space for pre-allocated block list");
    for (int i = 0; i < attr->npre_allocs; i++) {
        attr->pre_allocs[i] = pool->header.pre_allocs[i];
    }

    attr->mname = strndup(pool->mname, DRAGON_MEMORY_MAX_FILE_NAME_LENGTH);

    dragonError_t err = _obtain_filenames(pool, &attr->names);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot assign filename into attributes");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_validate_attr(const dragonMemoryPoolAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attr argument was NULL as provided to the _validate_attr function.");

    if (attr->segment_size < attr->data_min_block_size)
        err_return(DRAGON_INVALID_ARGUMENT, "The segment size must be at least as big as the minimum block size.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_copy_attr_nonames(dragonMemoryPoolAttr_t * new_attr, const dragonMemoryPoolAttr_t * attr)
{
    new_attr->data_min_block_size     = attr->data_min_block_size;
    new_attr->max_allocations         = attr->max_allocations;
    new_attr->waiters_for_manifest    = attr->waiters_for_manifest;
    new_attr->manifest_entries        = attr->manifest_entries;
    new_attr->allocatable_data_size   = attr->allocatable_data_size;
    new_attr->total_data_size         = attr->total_data_size;
    new_attr->manifest_allocated_size = attr->manifest_allocated_size;
    new_attr->segment_size            = attr->segment_size;
    new_attr->max_size                = attr->max_size;
    new_attr->n_segments              = attr->n_segments;
    new_attr->lock_type               = attr->lock_type;
    new_attr->mem_type                = attr->mem_type;
    new_attr->growth_type             = attr->growth_type;
    new_attr->mode                    = attr->mode;
    new_attr->npre_allocs             = attr->npre_allocs;
    if (attr->npre_allocs > 0 && attr->pre_allocs != NULL) {
        new_attr->pre_allocs = malloc(sizeof(size_t) * attr->npre_allocs);
        if (new_attr->pre_allocs == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate space for pre-allocated block list");
        for (int i = 0; i < attr->npre_allocs; i++)
            new_attr->pre_allocs[i] = attr->pre_allocs[i];
    } else {
        new_attr->npre_allocs = 0;
        new_attr->pre_allocs = NULL;
    }

    no_err_return(DRAGON_SUCCESS);
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void
_set_thread_local_mode_managed_memory(bool set_thread_local)
{
    if (set_thread_local) {
        dg_pools = &_dg_thread_pools;
        dg_mallocs = &_dg_thread_mallocs;
    } else {
        dg_pools = &_dg_proc_pools;
        dg_mallocs = &_dg_proc_mallocs;
    }
}

dragonError_t
dragon_memory_manifest_info(dragonMemoryDescr_t * mem_descr, dragonULInt* type, dragonULInt* id)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "memory descriptor is NULL");

    /* Retrieve memory from descriptor, then free local memory struct. */
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    *type = mem->mfst_record.type;
    *id = mem->mfst_record.id;

    no_err_return(DRAGON_SUCCESS);
}

// NOT FOR USE BY USERS. DEVELOPER USE ONLY.
// We can set this flag so that a condition is true at some point in an execution
// in a process. Then we can write an if statement that won't be true until after
// this flag is set which can be useful in debugging.
static int debug_flag = 0;

void
dragon_memory_set_debug_flag(int the_flag) {
    debug_flag = the_flag;
}

/* --------------------------------------------------------------------------------
    BEGIN USER API
   ----------------------------------------------------------------------------- */

/**
 * @brief Initialze memory pool attributes.
 *
 * Memory pools have attributes that can be specified to customize the behavior
 * and characteristics of the memory pool. Call this method first to initialize
 * all attributes to default values, before customizing the values for your
 * application's specifics.
 *
 * @param attr is a attribute structue and may result in mallocing space, so
 * destroy should be called when done with the structure.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_attr_init(dragonMemoryPoolAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool attribute");

    attr->allocatable_data_size      = DRAGON_MEMORY_DEFAULT_MAX_SIZE;
    attr->data_min_block_size        = DRAGON_MEMORY_DEFAULT_MIN_BLK_SIZE;
    attr->max_allocations            = 0; // 0 Indicates not set by user.
    attr->waiters_for_manifest       = 0; // read-only
    attr->manifest_entries           = 0; // read-only
    attr->max_allocatable_block_size = DRAGON_MEMORY_DEFAULT_MAX_SIZE;
    attr->manifest_allocated_size    = 0;
    attr->max_size                   = DRAGON_MEMORY_DEFAULT_MAX_SIZE;
    attr->n_segments                 = 0;
    attr->segment_size               = DRAGON_MEMORY_DEFAULT_SEG_SIZE;
    attr->lock_type                  = DRAGON_MEMORY_DEFAULT_LOCK_TYPE;
    attr->mem_type                   = DRAGON_MEMORY_DEFAULT_MEM_TYPE;
    attr->growth_type                = DRAGON_MEMORY_DEFAULT_GROWTH_TYPE;
    attr->mode                       = DRAGON_MEMORY_DEFAULT_MODE;
    attr->npre_allocs                = 0;
    attr->pre_allocs                 = NULL;
    attr->mname                      = NULL;
    attr->names                      = NULL;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Destroy the memory pool attribute structure.
 *
 * Any mallocs that were performed by dragon_memory_attr_init are
 * freed by calling this destroy function. Any mallocs that were
 * executed done by a caller of this function are the responsibility
 * of the code calling this. After user code is done initig the pool, be sure
 * to set any pointer field to NULL or this function will return an
 * error code to try and catch unfreed memory that was mistakenly left
 * around.
 *
 * @param attr is the attribute structure to clean up.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t
dragon_memory_attr_destroy(dragonMemoryPoolAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool attribute");

    if (attr->mname != NULL)
        free(attr->mname);

    if (attr->names != NULL) {
        for (size_t i = 0; i < attr->n_segments + 1; i++) {
            if (attr->names[i] != NULL)
                free(attr->names[i]);
        }
        free(attr->names);
    }

    if (attr->pre_allocs != NULL)
        free(attr->pre_allocs);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_memory_get_attr(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAttr_t * attr) {
    dragonMemoryPool_t * pool;
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attr pointer cannot be NULL when getting attributes from the pool.");

    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    if (pool->local_dptr == NULL) // Not Local
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "Cannot get attributes from non-local pool.");

    err = dragon_memory_attr_init(attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize the attributes structure when getting pool attributes.");

    err = _attrs_from_header(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the attributes from the pool header.");

    dragonBlocksStats_t stats;
    err = dragon_blocks_stats(&(pool->heap.mfstmgr), &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the attributes from the manifest.");

    attr->manifest_entries = stats.current_count;
    attr->max_manifest_entries = stats.max_count;
    attr->max_allocations = stats.num_blocks;
    dragon_memory_pool_get_utilization_pct(pool_descr, &attr->utilization_pct);
    dragon_memory_pool_get_free_size(pool_descr, &attr->free_space);

    int num_waiters;

    err = dragon_bcast_num_waiting(&pool->manifest_bcast, &num_waiters);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the number waiting on the manifest bcast.");

    attr->waiters_for_manifest = num_waiters;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Creates a memory pool of size *bytes*.
 *
 * Creates a memory pool of usable size *bytes*. *base_name* is a null-terminated
 * character string to use if the memory is backed by a file or shared memory.
 * The attributes *attr* specifies customizations of the memory or is ignored
 * if set to ``NULL``. If *attr* is provided and the value of *mem_type* in it
 * set to ``DRAGON_MEMORY_TYPE_PRIVATE``, then *base_name* is ignored.
 *
 * The memory pool descriptor *pool_descr* is returned once the allocation is
 * satisfied. *pool_descr* can then be used for memory allocations.
 * *pool_descr* can be serialized and shared with other processes that want to
 * allocate memory from it.
 *
 * Note that depending on the value of *nodemask* in *attr*, the memory for the
 * pool may or may not be faulted in.
 *
 * @param pool_descr is a structure pointer provided by the user, pointing at space that can be initialized.
 * @param bytes is the size of the memory pool in bytes.
 * @param base_name is the base of the name of the shared memory segement which is to be mapped into memory.
 * @param m_uid is the memory pool identifer to uniquely identify this pool.
 * @param attr are the attributes to be applied when creating this pool. Providing NULL for this argument will
 * use default attributes for the pool.
 *
 * @return DRAGON_SUCCESS or an error code.
 */
dragonError_t
dragon_memory_pool_create(dragonMemoryPoolDescr_t * pool_descr, const size_t bytes, const char * base_name,
                          const dragonM_UID_t m_uid, const dragonMemoryPoolAttr_t * attr)
{
    dragonError_t err;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    /* The _idx should never be zero. It is set below if successully initialized. We'll use
       the special value 0 to help detect SOME failures to check the return code in user code.
       It is not possible to catch all failures. */
    pool_descr->_idx = 0UL;
    pool_descr->_rt_idx = 0UL;

    if (base_name == NULL || (strlen(base_name) == 0))
        err_return(DRAGON_INVALID_ARGUMENT, "invalid base_name");

    /* if the attrs are NULL populate a default one */
    dragonMemoryPoolAttr_t  def_attr;
    err = dragon_memory_attr_init(&def_attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not initialize pool attr");

    if (attr != NULL) {
        err = _validate_attr(attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "invalid pool attr");
        err = _copy_attr_nonames(&def_attr, attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "could not copy pool attr");
    }

    if (bytes < def_attr.data_min_block_size)
        err_return(DRAGON_INVALID_ARGUMENT, "The requested size must be at least as big as the minimum block size.");

    if (def_attr.n_segments != 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Only n_segments==0 is supported in dragonMemoryPoolAttr_t for now.");

    /* @KDL - The following line is temporary. When/if we start to support multiple segments, we will
       not need this line */
    def_attr.max_size = bytes; /* This is the maximum amount the pool can grow to. Not suppied by user for now */

    if (def_attr.max_size < bytes)
        err_return(DRAGON_INVALID_ARGUMENT, "Maximum pool size must be greater than or equal to initial size.");

    if (def_attr.segment_size != 0)
        def_attr.n_segments = (def_attr.max_size - bytes) / def_attr.segment_size;
    else
        def_attr.n_segments = 0;

    /* create the pool structure */
    dragonMemoryPool_t * pool;
    pool = malloc(sizeof(dragonMemoryPool_t));
    if (pool == NULL) {
        dragon_memory_attr_destroy(&def_attr);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new pool object");
    }

    /* set flag indicating that this pool is hosted by the current runtime */
    pool->runtime_is_local = true;

    /* determine size of the pool based on the requested number of bytes */
    uint32_t max_block_power, min_block_power, segment_max_block_power;
    size_t max_block_size;
    size_t min_block_size;
    size_t required_size;

    /* segments are not currently used. A segment is a self-contained heap and managed
       memory pools are designed to be segmented, but for now only one segment exists. */
    _find_pow2_requirement(def_attr.segment_size, &required_size, &segment_max_block_power);
    _find_pow2_requirement(def_attr.data_min_block_size, &min_block_size, &min_block_power);
    _find_pow2_requirement(bytes, &max_block_size, &max_block_power);

    if (max_block_power == min_block_power) {
        char err_msg[200];
        snprintf(err_msg, 199, "The max and min block power are both %u which is not valid.", max_block_power);
        err_return(DRAGON_INVALID_ARGUMENT, err_msg);
    }

    /* This is what the user requested but the allocatable_data_size in the header will
       contain the actually allocated size since they may not have requested a power of 2 */
    pool->data_requested_size = bytes;
    pool->num_blocks = 1UL << (max_block_power - min_block_power);
    /* This is copied into the pool structure for quicker access - it is a constant after
       pool is created - and for safety since if the pool is destroyed the pointer in
       the header (where it is eventually stored) may no longer be valid. */
    pool->min_block_size = def_attr.data_min_block_size;

    if (pool->num_blocks < 5) {
        char err_msg[200];
        snprintf(err_msg, 199, "Number of blocks is %lu which is invalid. The max block power is %u and min block power is %u which is not valid.", pool->num_blocks, max_block_power, min_block_power);
        err_return(DRAGON_INVALID_ARGUMENT, err_msg);
    }

    /* The user may not have chosen a power of 2 for the segment size, so we'll set it to
       the right value which is the smallest power of 2 that is bigger than their original
       request. */
    def_attr.segment_size = 1UL << segment_max_block_power;
    def_attr.allocatable_data_size = max_block_size;
    /* this is supposed to be the maximum size the pool can grow to by adding segments -
       but we only have one segment for now and n_segments is 0 */
    def_attr.max_size = max_block_size + def_attr.segment_size * def_attr.n_segments;

    /* The maximum allocations is a restriction on how many concurrent allocations there
       can be in the memory pool at one time. This is needed because the manifest contains
       a static blocks mapping for all allocations and there must be space for them
       when the pool is created. To exceed this number, the pool would need to be filled
       with many, many small allocations. This is not likely to happen, but should it
       occur, the user can override the default by providing attributes. It just increases
       the space for the manifest in memory. */
    size_t max_allocations = 0;
    if (def_attr.max_allocations == 0) {
        if (pool->num_blocks > DRAGON_CONCURRENT_MEM_ALLOCATIONS_THRESHOLD) {
            // user has not provided a maximum number of concurrent allocations
            // and the number of blocks in the heap is above this
            // threshold. We will use a heuristic. 2097152 is
            // double the number of blocks in a 4GB heap with a
            // minimum block_size of 4K. 1048576 was was used as
            // our default memory pool for quite a while, but was
            // are now 4x more efficient in storage for the
            // manifest and much more efficient in the heap
            // manager. We'll take the total size/4GB*2097152 to
            // give us a percentage of 2097152 proportional to
            // this number of entries. If there are fewer segments
            // (i.e. called blocks here) in the heap, then we'll
            // use that since we would never need more entries
            // than blocks. This 2097152 number of entries equates
            // to about 64MB of space in shared memory. We'll
            // keep it proportional to that. For instance, an 8GB
            // pool would require about 128MB of space in the
            // manifest using this heuristic.
            dragonULInt multiple = max_block_size / 4294967296; // 4GB
            dragonULInt allocs = multiple * DRAGON_CONCURRENT_MEM_ALLOCATIONS_THRESHOLD;
            if (allocs > DRAGON_CONCURRENT_MEM_ALLOCATIONS_THRESHOLD)
                max_allocations = allocs;
            else
                max_allocations = DRAGON_CONCURRENT_MEM_ALLOCATIONS_THRESHOLD;
        } else
            // Below the threshold of 128MB for the manifest blocks
            // we'll allocate enough block entries to hold all possible
            // allocations.
            max_allocations = pool->num_blocks;
    } else
        // User-provided, so use it.
        max_allocations = def_attr.max_allocations;

    if (max_allocations < 5) {
        char err_msg[200];
        snprintf(err_msg, 199, "The max allocations was %lu which is too small.", max_allocations);
        err_return(DRAGON_INVALID_ARGUMENT, err_msg);
    }

    // This adjusts max allocations if it is bigger than it would ever need to be. We
    // would never use more than the number of blocks in the heap.
    max_allocations = MIN(pool->num_blocks, max_allocations);

    /* This computes the max number of entries that will be needed in the blocks for the
       manifest. This is needed to be able to establish the entire needed size for the manifest
       given that it could grow by adding additional segments.

       Adding segments is not yet implemented, but this allows for it. The
       n_segments value is 0. */

    pool->manifest_requested_size = max_allocations +
            def_attr.n_segments * max_allocations;

    // Creating a pool should count as an explicit attach
    // Possibly unnecessary atomic operation since the pool only exists here right now
    atomic_store(&(pool->ref_cnt), 1);

    /* perform the allocation */
    err = _alloc_pool(pool, base_name, &def_attr);
    if (err != DRAGON_SUCCESS) {
        char* err_str = dragon_getrawerrstr();
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        dragon_setrawerrstr(err_str);
        free(err_str);
        append_err_return(err, "cannot allocate memory pool");
    }

    /* manifest header init should be done before much of anything else
       because error handling below (like calls to _free_pool) rely
       on the manifest header being initialized first */
    err = _initialize_manifest_header(pool, m_uid, &def_attr);
    if (err != DRAGON_SUCCESS) {
        char* err_str = dragon_getrawerrstr();
        _free_pool(pool, &def_attr);
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        dragon_setrawerrstr(err_str);
        free(err_str);
        append_err_return(err, "Could not initialize the manifest header.");
    }

    dragonRT_UID_t rt_uid = dragon_get_local_rt_uid();

    /* create the umap entry for the descriptor */
    err = _add_pool_umap_entry(pool_descr, pool, rt_uid, m_uid);
    if (err != DRAGON_SUCCESS) {
        _free_pool(pool, &def_attr);
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        return err;
    }
    /* Set _original to 1 inside create */
    pool_descr->_original = 1;

    /* we no longer need the local attributes as they are all now embedded into the header */
    dragon_memory_attr_destroy(&def_attr);

    no_err_return(DRAGON_SUCCESS);
}

/** Destroys the memory pool described by *pool_descr*.
 *  Any outstanding allocations from the memory pool become invalid.
 *
 * @return DRAGON_SUCCESS or an error code.
 */

dragonError_t
dragon_memory_pool_destroy(dragonMemoryPoolDescr_t * pool_descr)
{
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    if (pool->local_dptr == NULL) // Not Local
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "Cannot destroy non-local pool");

    /* We do this here to make sure no other processes start an operation while pool is being destroyed. */
    dragon_lock_destroy(&pool->mlock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to release heap manager lock");

    /* get an attributes structure for use in the destruction */
    dragonMemoryPoolAttr_t attrs;
    err = _attrs_from_header(pool, &attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot construct pool attributes from pool");

    err = dragon_bcast_destroy(&pool->manifest_bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy the manifest bcast");

    err = _destroy_heap_managers(pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy the heap manager");

    /* actually cleanup the resources the pool is using */
    err = _free_pool(pool, &attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to release pool resources");

    err = dragon_memory_attr_destroy(&attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to destroy the attributes for this pool");

    /* delete the entry from the umap */
    err = dragon_umap_delitem_multikey(dg_pools, pool_descr->_rt_idx, pool_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in pools umap");
    pool_descr->_rt_idx = 0UL;
    pool_descr->_idx = 0UL;
    pool_descr->_original = 0;

    /* Free the heap manager */
    free(pool->heap.mgrs);
    free(pool->heap.mgrs_dptrs);

    /* finally free the base object */
    free(pool->mname);
    free(pool);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the hostid of the host where this pool resides.
 *
 * The hostid of the pool can be used for identifying the location of this pool
 * within the Dragon run-time nodes. It identifies the node where this pool
 * exists.
 *
 * @param pool_descr is a valid pool descriptor for the pool in question.
 * @param hostid is a pointer to the location where the hostid will be copied by this
 * function call.
 *
 * @return DRAGON_SUCCESS or an error code.
 */
dragonError_t
dragon_memory_pool_get_hostid(dragonMemoryPoolDescr_t * pool_descr, dragonULInt * hostid)
{
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    if (hostid == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The host_id pointer cannot be null");

    if (pool->local_dptr != NULL)
        /* local pool */
        *hostid = *pool->header.hostid;
    else
        *hostid = pool->remote.hostid;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Determine if a pool is hosted in the local runtime.
 *
 * @param pool_descr is a valid pool descriptor for the pool in question.
 * @param runtime_is_local is a boolean indicating if the pool is hosted in the local runtime.
 *
 * @return DRAGON_SUCCESS or an error code.
 */
dragonError_t
dragon_memory_pool_runtime_is_local(dragonMemoryPoolDescr_t *pool_descr, bool *runtime_is_local)
{
    dragonMemoryPool_t *pool = NULL;

    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    *runtime_is_local = pool->runtime_is_local;

    return err;
}

/**
 * @brief Get the runtime unique id (rt_uid) for a pool.
 *
 * @param pool_descr is a valid pool descriptor for the pool in question.
 * @param rt_uid is the unique value used to identify a runtime. The value is composed
 * of two IP addresses: the internet IP address of the login node of the system hosting
 * the runtime, and the intranet IP address of the head node for the runtime.
 *
 * @return DRAGON_SUCCESS or an error code.
 */
dragonError_t
dragon_memory_pool_get_rt_uid(dragonMemoryPoolDescr_t *pool_descr, dragonRT_UID_t *rt_uid)
{
    dragonMemoryPool_t *pool = NULL;

    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    if (pool->runtime_is_local) {
        *rt_uid = dragon_get_local_rt_uid();
    } else {
        *rt_uid = pool->remote.rt_uid;
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get meta information about a memory pool.
 *
 * Retrieve memory pool m_uid and optionally the filename of the pool without needing to attach
 * to the pool. If *m_uid* is not NULL it will copy the m_uid of the pool into the space it points
 * to. If *filename* is not NULL it will copy the filename of the pool using :c:func:`strdup()`:.
 * It is up to the caller to free the space *filename* points to in this case.
 *
 * @param m_uid is a pointer to space where the m_uid can be copied or NULL if it is not needed.
 * @param filename is a pointer to a character pointer which will be initialized to point to to the
 * pool's filename unless NULL was provided in which case the filename copy is not performed.
 *
 * @return DRAGON_SUCCESS or error code
 */
dragonError_t
dragon_memory_pool_get_uid_fname(const dragonMemoryPoolSerial_t * pool_ser, dragonULInt * m_uid, char ** filename)
{
    if (pool_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool serializer is NULL");

    if (m_uid != NULL) {
        *m_uid = *(dragonULInt*)pool_ser->data;
    }

    if (filename != NULL) {
        dragonULInt * ptr = (dragonULInt*)pool_ser->data;
        ptr += 5; // skip id, host_id, runtime ip addrs, mem_type, manifest_len
        *filename = strdup((char*)ptr);
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Discover whether a pool is local or not.
 *
 * Return true if the memory pool is on the current node and false, otherwise.
 * Occassionally, it is necessary to know if a memory pool is local to a node
 * when serialized descriptors are passed around.
 *
 * @return true if is is local and false otherwise.
 */
bool
dragon_memory_pool_is_local(dragonMemoryPoolDescr_t * pool_descr)
{
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    /* check if the pool is local */
    if (pool->local_dptr == NULL)
        return false;

    return true;
}

/** @brief Make a copy of a memory pool descriptor.
 *
 * Does a copy from *oldpool_descr* into *newpool_descr* providing a
 * clone of the original.
 *
 * @param newpool_descr is the newly cloned descriptor.
 * @param oldpool_descr is the original pool descriptor.
 *
 * @returns DRAGON_SUCCESS or an error code.
 */
dragonError_t
dragon_memory_pool_descr_clone(dragonMemoryPoolDescr_t * newpool_descr, const dragonMemoryPoolDescr_t * oldpool_descr)
{
    /* Check that the given descriptor points to a valid pool */
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(oldpool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    /* update the new one */
    newpool_descr->_rt_idx = oldpool_descr->_rt_idx;
    newpool_descr->_idx = oldpool_descr->_idx;
    newpool_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Provide a max size for a serialized pool descriptor.
 *
 * returns the maximum size that will be needed to hold a serialized
 * pool descriptor.
 *
 * @returns The maximum required size for a serialized pool descriptor.
 */
size_t
dragon_memory_pool_max_serialized_len() {
    return DRAGON_MEMORY_POOL_MAX_SERIALIZED_LEN;
}

/**
 * @brief Serialize a pool descriptor to be shared with other processes.
 *
 * When sharing a pool, the pool descriptor may be serialized by the user to
 * share with other processes which would use this to attach to the pool. Note
 * that only local pools can be used to allocate memory. Non-local pool
 * descriptors cannot be used for allocating memory on a separate node. Once
 * the pool has been attached or the serialized data has been passed on to
 * another process, it will be necessary to destroy the serialized pool
 * descriptor since it contains malloced data.
 *
 * @param pool_ser is a pointer to a structure that will hold the serialized
 * pool descriptor.
 *
 * @param pool_descr is the pool descriptor that is being serialized.
 *
 * @return DRAGON_SUCCESS or an error code.
 */

dragonError_t
dragon_memory_pool_serialize(dragonMemoryPoolSerial_t * pool_ser, const dragonMemoryPoolDescr_t * pool_descr)
{
    /* Check args are not null. */
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (pool_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool serializer is NULL");

    /* We'll initialize variables in serialized descriptor here. They will be reset below when
       serialization is successful */
    pool_ser->len = 0;
    pool_ser->data = NULL;

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    bool local_pool=true;
    if (pool->local_dptr == NULL)
        local_pool = false;

    /* Include null terminator */
    size_t mname_len = strlen(pool->mname) + 1;
    if (mname_len > DRAGON_MEMORY_MAX_FILE_NAME_LENGTH)
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "manifest filename length exceeds maximum");

    /* Store total size of memory blob */
    pool_ser->len = (DRAGON_MEMORY_POOLSER_NULINTS * sizeof(dragonULInt)) + mname_len;
    /* @MCB TODO: Where should this get freed? */
    pool_ser->data = (uint8_t*)malloc(pool_ser->len);
    if (pool_ser->data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate data pool");

    /* Grab local pointer for assigning */
    dragonULInt * ptr = (dragonULInt*)pool_ser->data;

    /* put in the m_uid */
    if (local_pool)
        *ptr = *pool->header.m_uid;
    else
        *ptr = pool->remote.m_uid;
    ptr++;

    /* Get current host ID */
    /* Store at front for easier attach logic */
    if (local_pool)
        *ptr = dragon_host_id();
    else
        *ptr = pool->remote.hostid;
    ptr++;

    /* Copy the runtime unique ID */
    dragonULInt rt_uid;
    if (pool->runtime_is_local)
        *ptr = rt_uid = dragon_get_local_rt_uid();
    else
        *ptr = rt_uid = pool->remote.rt_uid;
    ptr++;

    /* Copy memory type (SHM, file, etc) */
    if (local_pool)
        *ptr = *pool->header.mem_type;
    else
        *ptr = pool->remote.mem_type;
    ptr++;

    /* Copy manifest size */
    if (local_pool)
        *ptr = *pool->header.manifest_allocated_size;
    else
        *ptr = pool->remote.manifest_len;
    ptr++;

    /* Copy in manifest filename last */
    strncpy((char*)(ptr), pool->mname, mname_len);

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Attach to a pool using a serialied descriptor.
 *
 * A process that is given a serialized pool descriptor can use it to attach
 * to an existing pool by calling this function. Once the pool has been attached
 * or the serialized data has been passed on to another process, it will be necessary
 * to destroy the serialized pool descriptor since it contains malloced data.
 *
 * @param pool_descr will be initialized if DRAGON_SUCCESS is returned.
 * @param pool_ser is the given serialized pool descriptor for this pool.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_attach(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolSerial_t * pool_ser)
{
    bool local_pool = true;
    bool runtime_is_local;

    /* Check args are not null. */
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool descriptor argument cannot be NULL.");

    if (pool_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized pool argument cannot be NULL.");

    if ((pool_ser->data) == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized pool data field cannot be NULL.");

    if ((pool_ser->len) <= DRAGON_MEMORY_POOLSER_NULINTS*sizeof(dragonULInt))
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized pool length field value is too small to be valid.");

    /* Grab pointer for local movement */
    dragonULInt * ptr = (dragonULInt*)pool_ser->data;

    /* pull out the m_uid */
    dragonM_UID_t m_uid = *ptr;
    ptr++;

    /* Check if the local dragon_host_id and runtime ip addrs match the pool's values */
    dragonULInt local_host_id = dragon_host_id();
    dragonULInt host_id = *ptr;
    ptr++;

    dragonULInt local_rt_uid = dragon_get_local_rt_uid();
    dragonULInt rt_uid = *ptr;
    ptr++;

    /* check if we already have attached to this pool, if so we will just use that */
    dragonError_t err = _pool_descr_from_uids(rt_uid, m_uid, pool_descr);
    if (err == DRAGON_SUCCESS) {
        dragonMemoryPool_t * pool;
        _pool_from_descr(pool_descr, &pool);
        atomic_fetch_add_explicit(&(pool->ref_cnt), 1, memory_order_acq_rel);
        no_err_return(DRAGON_SUCCESS);
    }

    /* Allocate a new pool, open the manifest file, and map it into the pool header */
    dragonMemoryPool_t * pool = (dragonMemoryPool_t*)malloc(sizeof(dragonMemoryPool_t));
    if (pool == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate internal pool structure.");

    if (local_rt_uid != rt_uid)
        runtime_is_local = false;
    else
        runtime_is_local = true;

    /* If this is a non-local pool we are attaching to, then we set a flag and return. */
    if (local_host_id != host_id || !runtime_is_local)
        local_pool = false;
    else
        local_pool = true;

    pool->runtime_is_local = runtime_is_local;

    /* Grab the memory storage type */
    dragonULInt mem_type = *ptr;
    ptr++;

    /* Get the size of the manifest */
    dragonULInt manifest_len = *ptr;
    ptr++;

    /* Set the filepath for the pool */
    size_t path_len = strlen((char*)ptr);
    if (path_len > DRAGON_MEMORY_MAX_FILE_NAME_LENGTH-1) {
        free(pool);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "filepath length too long");
    }
    pool->mname = strdup((char*)ptr);

    /* check the size to see that it matches what is being interpreted */
    size_t actual_len = ((void*)ptr - (void*)pool_ser->data) + path_len + 1; // null terminator accounts for 1

    if (actual_len > pool_ser->len) {
        char err_str[200];
        snprintf(err_str, 199, "The serialized descriptor prematurely ended. The expected size was %lu and the given size was %lu", actual_len, pool_ser->len);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    if (actual_len < pool_ser->len) {
        char err_str[200];
        snprintf(err_str, 199, "The serialized descriptor was longer than anticipated. The expected size was %lu and the given size was %lu", actual_len, pool_ser->len);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    if (local_pool) {
        err = _open_map_manifest_shm(pool, (char*)ptr, manifest_len);
        if (err != DRAGON_SUCCESS) {
            free(pool);
            append_err_return(err, "failed to attach map manifest");
        }

        err = _map_manifest_header(pool, NULL);
        if (err != DRAGON_SUCCESS) {
            free(pool);
            append_err_return(err, "failed to map manifest header");
        }

        /* Load attributes out of the header */
        _obtain_manifest_lock(pool);
        dragonMemoryPoolAttr_t mattr;
        err = _attrs_from_header(pool, &mattr);
        _release_manifest_lock(pool);
        if (err != DRAGON_SUCCESS) {
            dragon_memory_attr_destroy(&mattr);
            if (pool != NULL)
                free(pool);

            append_err_return(err, "failed to get attributes from header");
        }

        /* Load the data file descriptor (by default the first filepath entry in the attributes) */
        err = _open_map_data(pool, mattr.names[0], &mattr);
        if (err != DRAGON_SUCCESS) {
            dragon_memory_attr_destroy(&mattr);
            if (pool != NULL)
                free(pool);

            append_err_return(err, "failed to open data SHM");
        }

        /* Free malloc'd attribute struct, does not need to be long-lived */
        dragon_memory_attr_destroy(&mattr);

        /* Attach required heap managers */
        err = _attach_heap_managers(pool);
        if (err != DRAGON_SUCCESS) {
            free(pool);

            append_err_return(err, "failed to attach heap managers");
        }

        /* Attach the manifest bcast */
        err = dragon_bcast_attach_at(pool->header.manifest_bcast_space, &pool->manifest_bcast);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to manifest bcast");


        /* This is copied into the pool structure for quicker access - it is a constant after
           pool is created - and for safety since if the pool is destroyed the pointer in
           the header (where it is eventually stored) may no longer be valid. */
        pool->min_block_size = *(pool->header.data_min_block_size);
        pool->num_blocks = (*pool->header.total_data_size) / pool->min_block_size;


    } else {
        pool->local_dptr = NULL;
        pool->remote.hostid = host_id;
        pool->remote.rt_uid = rt_uid;
        pool->remote.m_uid = m_uid;
        pool->remote.mem_type = mem_type;
        pool->remote.manifest_len = manifest_len;
    }

    /* Add entry into pool umap updating pool descriptor's idx */
    err = _add_pool_umap_entry(pool_descr, pool, rt_uid, m_uid);

    if (err != DRAGON_SUCCESS) {
        free(pool);
        append_err_return(err, "failed to add umap entry");
    }

    // This was our first attach, otherwise we would have returned when retrieving the pool
    // Set counters appropriately
    atomic_store(&(pool->ref_cnt), 1);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Given an environment variable name, use it to attach to a memory pool
 *
 * Certain environment variables contain serialized descriptors to resources, including
 * memory pools created by Dragon's run-time services. This function will attach
 * to a pool descriptor whose serialized descriptor is found in an environment variable.
 * Once a process no longer needs access to a memory pool, it should detach from it.
 *
 * @param pool_descr is a pointer to a pool descriptor that will be initialized upon
 * successful completion of this function.
 *
 * @param env_var is a pointer to a null-terminated environment variable string which
 * maps to a base64 encoded serialized memory pool descriptor.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_attach_from_env(dragonMemoryPoolDescr_t * pool_descr, const char * env_var)
{
    dragonMemoryPoolSerial_t pool_ser;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a valid pool descriptor variable.");

    char *encoded_pool_str = getenv(env_var);
    if (encoded_pool_str == NULL) {
        char err_str[200];
        snprintf(err_str, 199, "serialized descriptor for requested pool (\"%s\") cannot be found in environment", env_var);
        err_return(DRAGON_INVALID_ARGUMENT, err_str);
    }

    pool_ser.data = dragon_base64_decode(encoded_pool_str, &pool_ser.len);

    dragonError_t err = dragon_memory_pool_attach(pool_descr, &pool_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to attach to memory pool");

    dragon_memory_pool_serial_free(&pool_ser);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Attach to Default Pool
 *
 * Processes created by Dragon have access to a default pool, one per node.
 * The default pool on a node can be used when no other managed memory pools
 * are available or have been provided. This function attaches to the
 * default pool on its current node.
 *
 * @param pool is a pointer to a pool descriptor to be initialized by this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_memory_pool_attach_default(dragonMemoryPoolDescr_t* pool)
{
    dragonError_t err;

    err = dragon_memory_pool_attach_from_env(pool, DRAGON_DEFAULT_PD_VAR);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to default memory pool.");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Detach from a memory pool
 *
 * Once a process no longer needs access to a memory pool, it should
 * detach from it.
 *
 * @param pool_descr is the memory pool to detach from.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_detach(dragonMemoryPoolDescr_t * pool_descr)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    /* Get the pool from descriptor */
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    // Remove a reference
    long int ref_cnt = atomic_fetch_sub_explicit(&(pool->ref_cnt), 1, memory_order_acq_rel) - 1;

    if (ref_cnt > 0)
        no_err_return(DRAGON_SUCCESS); // Other refs exist, do nothing.

    /* If this is a non-local pool, then there is less to do */
    if (pool->local_dptr != NULL) {
        /* Get pool attributes to free them */
        dragonMemoryPoolAttr_t attrs;
        _maybe_obtain_manifest_lock(pool);

        err = _attrs_from_header(pool, &attrs);
        _maybe_release_manifest_lock(pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot construct pool attributes from pool");

        err = dragon_bcast_detach(&pool->manifest_bcast);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to detach the manifest bcast");

        err = _detach_heap_managers(pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach heap manager in pool detach.");

        /* @MCB TODO: Need control flow to manage different memory types */
        /* Unmap manifest and data pointers */
        err = _unmap_manifest_shm(pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to unmap manifest");

        err = _unmap_data(pool, &attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to unmap data");

        /* Clear attributes */
        err = dragon_memory_attr_destroy(&attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to destroy pool attributes");
    }

    /* Remove from umap */
    err = dragon_umap_delitem_multikey(dg_pools, pool_descr->_rt_idx, pool_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in pools umap");

    pool_descr->_rt_idx = 0UL;
    pool_descr->_idx = 0UL;
    pool_descr->_original = 0;

    /* Free pool allocs */
    free(pool->mname);
    free(pool);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Free the space used by a serialied pool descriptor.
 *
 * When a pool descriptor is serialized, there is space that is
 * malloced to hold the serialized pool descriptor. To prevent a
 * memory leak in the current process, a serialized pool desctiptor
 * should be destroyed once it is no longer needed, which might
 * be after the process has attached to it.
 *
 * @param pool_ser is the serialized pool descriptor that should
 * be cleaned up by freeing internal storage.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_serial_free(dragonMemoryPoolSerial_t * pool_ser)
{
    if (pool_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool serial is NULL");

    if (pool_ser->data != NULL)
        free(pool_ser->data);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the muid from a pool.
 *
 * Especially when attaching a pool, the muid isn't always known
 * apriori. This call can be used to discover the muid of any
 * pool.
 *
 * @param pool is a pool descriptor
 *
 * @param muid is a pointer to space to receive the muid
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_muid(dragonMemoryPoolDescr_t* pool_descr, dragonULInt* muid)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (muid == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "muid is NULL");

    /* Get the pool from descriptor */
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    if (pool->local_dptr != NULL)
        /* It is local */
        *muid = *pool->header.m_uid;
    else /* non-local */
        *muid = pool->remote.m_uid;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the free space in the pool.
 *
 * Return the amount of free space.
 *
 * @param pool is a pool descriptor
 *
 * @param free_size is a pointer to space to receive the free size in bytes.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_get_free_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* free_size) {

    dragonMemoryPool_t * pool;
    dragonError_t err;
    dragonHeapStats_t stats;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (free_size == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "free_size is NULL");

    /* Get the pool from descriptor */
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    err = dragon_heap_get_stats(&pool->heap.mgrs[0], &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pool stats.");

    *free_size = stats.total_free_space;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the total space for the pool.
 *
 * Return the amount of space for the whole pool, whether
 * currently allocated or not.
 *
 * @param pool is a pool descriptor
 *
 * @param free_size is a pointer to space to receive the total size in bytes.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_pool_get_total_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* total_size) {

    dragonMemoryPool_t * pool;
    dragonError_t err;
    dragonHeapStats_t stats;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (total_size == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "total_size is NULL");

    /* Get the pool from descriptor */
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    err = dragon_heap_get_stats(&pool->heap.mgrs[0], &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pool stats.");

    *total_size = stats.total_size;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the free utilization percent in the pool.
 *
 * Return the percentage of free space.
 *
 * @param pool is a pool descriptor
 *
 * @param utilization_pct is a pointer to space to receive the pool usage in percentage.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_utilization_pct(dragonMemoryPoolDescr_t* pool_descr, double* utilization_pct) {

    dragonMemoryPool_t * pool;
    dragonError_t err;
    dragonHeapStats_t stats;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (utilization_pct == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "utilization_pct is NULL");

    /* Get the pool from descriptor */
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    err = dragon_heap_get_stats(&pool->heap.mgrs[0], &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pool stats.");

    *utilization_pct = stats.utilization_pct;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the number of free blocks with particular block_size in the pool.
 *
 * Return the percentage of free space.
 *
 * @param pool is a pool descriptor
 *
 * @param free_blocks is a pointer to space to receive the number of free blocks with different block sizes as an array of dragonHeapStatsAllocationItem_t.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_free_blocks(dragonMemoryPoolDescr_t* pool_descr, dragonHeapStatsAllocationItem_t* free_blocks) {

    dragonMemoryPool_t * pool;
    dragonError_t err;
    dragonHeapStats_t stats;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (free_blocks == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "free_blocks is NULL");

    /* Get the pool from descriptor */
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    err = dragon_heap_get_stats(&pool->heap.mgrs[0], &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pool stats.");

    memcpy(free_blocks, &stats.free_blocks, stats.num_block_sizes*sizeof(dragonHeapStatsAllocationItem_t));

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Get the number of free blocks with particular block_size in the pool.
 *
 * Return the percentage of free space.
 *
 * @param pool is a pool descriptor
 *
 * @param num_block_sizes is a pointer to space to receive the number of block sizes as a size_t value.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_num_block_sizes(dragonMemoryPoolDescr_t* pool_descr, size_t* num_block_sizes) {

    dragonMemoryPool_t * pool;
    dragonError_t err;
    dragonHeapStats_t stats;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (num_block_sizes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "num_block_sizes is NULL");

    /* Get the pool from descriptor */
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    err = dragon_heap_get_stats(&pool->heap.mgrs[0], &stats);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pool stats.");

    *num_block_sizes = stats.num_block_sizes;

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Get the maximum length of a serialized memory descriptor
 *
 * Memory allocations may be serialized and shared with other processes.
 * In some situations it may be necessary to preallocate some space
 * to be used to store this serialized data. This function returns the
 * maximum amount of space that would be required in bytes.
 *
 * @returns the size in bytes that is needed for the maximum sized
 * serialized descriptor.
*/
size_t
dragon_memory_max_serialized_len() {
    return DRAGON_MEMORY_MAX_SERIALIZED_LEN;
}

/**
 * @brief Serialize a memory descriptor
 *
 * Memory descriptors, allocated from memory pools, may be serialized
 * to share with other processes. This function returns a serialized
 * descriptor for the memory allocation.
 *
 * @param mem_ser is a pointer to a serialized memory descriptor structure.
 * @param mem_descr is the memory descriptor being serialized.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_serialize(dragonMemorySerial_t * mem_ser, const dragonMemoryDescr_t * mem_descr)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor. It cannot be NULL");

    if (mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempting to serialize memory with constant value DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP.");

    if (mem_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory serializer");

    /* We'll initialize variables in serialized descriptor here. They will be reset below when
       serialization is successful */
    mem_ser->len = 0;
    mem_ser->data = NULL;

    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    dragonMemoryPoolSerial_t pool_ser;
    err = dragon_memory_pool_serialize(&pool_ser, &(mem->pool_descr));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to serialize memory pool");

    mem_ser->len = 0;
    /* Size of serialized pool descriptor data
       Make sure to include the size of the size_t for the pool serialized length
    */
    mem_ser->len += (pool_ser.len + sizeof(size_t));

    /* Size of MemoryManifestRec_t member values */
    mem_ser->len += (DRAGON_MEMORY_MEMSER_NULINTS * (sizeof(dragonULInt)));

    /* This allocation is released in dragon_memory_serial_free() */
    mem_ser->data = malloc(mem_ser->len);
    if ((mem_ser->data == NULL))
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocated data length");

    uint8_t * ptr = mem_ser->data;

    /* Store size of serialized pool descriptor first */
    *(size_t*)ptr = pool_ser.len;

    ptr += sizeof(size_t);

    /* Store the serialized pool descriptor */
    memcpy(ptr, pool_ser.data, pool_ser.len);
    ptr += pool_ser.len;

    /* Free our temporary pool serialization after memcpy */
    err = dragon_memory_pool_serial_free(&pool_ser);
    if (err != DRAGON_SUCCESS)
        return err;

    /* Alloc allocation ID */
    *(dragonULInt*)ptr = mem->mfst_record.id;
    ptr += sizeof(dragonULInt);

    /* offset is included for remote mem descriptors and for clone information
       when clone has different offset */
    *(dragonULInt*)ptr = mem->offset;
    ptr += sizeof(dragonULInt);

    /* bytes is included for remote mem descriptors and verification for
       subsequent clones. */
    *(dragonULInt*)ptr = mem->bytes;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Free the serialized descriptor
 *
 * A serialized memory descriptor, created with the serialize function call,
 * will contain malloced space that will need to be freed when the serialized
 * descriptor is no longer needed by the current process. At that time it can
 * be freed. This would be after it has passed the serialized descriptor to a new
 * process or when it is longer being used in the current process.
 *
 * @param mem_ser is a pointer to the serialized memory descriptor to free. The
 * descriptor itself is not freed, but its internal structures are freed if needed.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_serial_free(dragonMemorySerial_t * mem_ser)
{
    if (mem_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "memory serializer is NULL");

    if (mem_ser->data != NULL)
        free(mem_ser->data);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Attach to an existing memory allocation.
 *
 * A serialized memory descriptor, provided from another process, is
 * attached to the current process by making this call. Upon successful
 * completion, the memory descriptor is initialized. Not that you can
 * attach to remote, non-local memory descriptors but you cannot get a
 * pointer into a non-local memory allocation.
 *
 * @param mem_descr is initialized after this call.
 * @param mem_ser is the serialized memory descriptor provided to this process.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/
dragonError_t
dragon_memory_attach(dragonMemoryDescr_t * mem_descr, const dragonMemorySerial_t * mem_ser)
{
    /* @TODO: This is in the hot loop, keep it optimized */

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The memory descriptor argument cannot be NULL.");

    if (mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempting to attach to memory with constant value DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP.");

    if (mem_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The serialized memory argument cannot be NULL.");

    if (mem_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The memory serialized data field cannot be NULL.");

    if ((mem_ser->len) <= DRAGON_MEMORY_MEMSER_NULINTS*sizeof(dragonULInt))
        err_return(DRAGON_INVALID_ARGUMENT, "The memory serialized length field value is too small to be valid.");

    uint8_t * ptr = mem_ser->data;

    /* Grab our serialized pool info */
    dragonMemoryPoolSerial_t pool_ser;
    size_t pool_len = *(size_t*)ptr;
    ptr += sizeof(size_t);
    pool_ser.len = pool_len;
    pool_ser.data = ptr;

    /* Move our pointer past all the serialized pool info */
    ptr += pool_len;

    /* Get our serialized pool */
    dragonMemoryPoolDescr_t pool_descr;

    dragonError_t err = dragon_memory_pool_attach(&pool_descr, &pool_ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not attach to memory pool");

    dragonULInt id = *(dragonULInt*)ptr;
    ptr += sizeof(dragonULInt);
    dragonULInt offset = *(dragonULInt*)ptr;
    ptr += sizeof(dragonULInt);
    dragonULInt bytes = *(dragonULInt*)ptr;

    err = dragon_memory_get_alloc_memdescr(mem_descr, &pool_descr, id, offset, &bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not obtain allocation memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Detach from a memory allocation
 *
 * Once a memory allocation is no longer needed, a process can either
 * detach from it or destroy it. Detach leaves the memory allocation
 * in existence, but cleans up any process local references to it.
 *
 * @param mem_descr is the memory descriptor to be detached.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_detach(dragonMemoryDescr_t * mem_descr)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "memory descriptor is NULL");

    if (mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempting to detach from memory with constant value DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP.");

    /* @MCB: Attaching to memory without having the pool will cause a call to dragon_memory_pool_attach.
       This call has its own set of malloc's, but does not return a pool descriptor handle to call detach on later.
       In order to properly clean everything up, call dragon_memory_get_pool() before detaching the last attached memory descriptor.
       Call dragon_memory_detach(), then call dragon_memory_pool_detach() on the resulting pool handle.
     */

    /* @MCB TODO: Assert the mem_descr->_original is set to 0? This would prevent detach calls on original allocations
    */

    /* Retrieve memory from descriptor, then free local memory struct. */
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    free(mem);

    /* Remove index entry from umap */
    err = dragon_umap_delitem(dg_mallocs, mem_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in mallocs umap");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the unique id for this memory descriptor in its pool.
 *
 * This function retrieves the identifier assigned to this memory allocation.
 * It is unique for this instance of its pool only. It is unique for this
 * memory allocation in its pool.
 *
 * @param mem_descr The given memory descriptor
 * @param id The memory allocation's unique identifier
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t dragon_memory_id(dragonMemoryDescr_t * mem_descr, uint64_t* id)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The mem_descr argument cannot be NULL.");

    if (id == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The id argument cannot be NULL.");

    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    *id = mem->mfst_record.id;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the the memory descriptor associated with a pool given its unique id.
 *
 * This function initializes a memory descriptor given its unique id in a pool.
 *
 * @param pool_descr The pool in which to look for the memory.
 * @param id The memory allocation's unique identifier
 * @param mem_descr The memory descriptor to be initialized.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t
dragon_memory_from_id(const dragonMemoryPoolDescr_t * pool_descr, uint64_t id, dragonMemoryDescr_t * mem_descr)
{
    dragonError_t err;

    err = dragon_memory_get_alloc_memdescr(mem_descr, pool_descr, id, 0, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not obtain allocation memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Allocate memory from a pool, waiting if necessary
 *
 * Allocates memory of the given size in bytes from a memory pool. The memory
 * will be block aligned. The minimum block size of the pool determines its
 * alignment. Pools are a finite sized resource. If the desired memory
 * allocation size is not currently available, this call will block the
 * current process for timeout until it is available. The effect of using this
 * is that back pressure can be applied to a process or processes once they
 * have consumed all the local resources they are allowed.
 *
 * @param mem_descr is a pointer to the memory descriptor to be initialized.
 *
 * @param pool_descr is a pointer to a pool descriptor for the pool in which to
 * make the allocation.
 *
 * @param bytes is the size of the requested allocation in bytes.
 *
 * @param timeout a pointer to the timeout that is to be used. Providing NULL
 * means to wait indefinitely. Providing a zero timeout (both seconds and nanoseconds)
 * means to try once and not block if the space is not available.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t
dragon_memory_alloc_blocking(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes, const timespec_t* timeout)
{
    char err_str[400];
    void* hptr = NULL;

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    /* The _idx should never be zero. It is set below if successully initialized. We'll use
       the special value 0 to help detect SOME failures to check the return code in user code.
       It is not possible to catch all failures. */
    mem_descr->_idx = 0;

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid pool descriptor");

    /* check if the pool is addressable.  if not, this is an off-node pool, and we cannot
        fulfill the request here */
    if (pool->local_dptr == NULL && bytes > 0)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL,
                   "Cannot allocate memory for non-local pool.");

    /* create the new memory object we'll hook the new descriptor to */
    dragonMemory_t * mem;
    mem = malloc(sizeof(dragonMemory_t));
    if (mem == NULL) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory object");
    }

    /* _generate_manifest_record assumes these fields are set */
    mem->local_dptr = NULL;
    mem->bytes = bytes;
    mem->offset = 0;

    if (pool->local_dptr != NULL && bytes > 0) {
        err = dragon_heap_malloc_blocking(&pool->heap.mgrs[0], bytes, &hptr, timeout);
        if (err != DRAGON_SUCCESS) {
            mem->bytes = 0;
            /* Don't use append_err_return. In hot path */
            return err;
        }

        mem->local_dptr = _to_dptr(pool, hptr);
        void* end_ptr = ((void*)pool->local_dptr) + *pool->header.total_data_size;

        if (mem->local_dptr < pool->local_dptr || mem->local_dptr >= end_ptr)
            err_return(DRAGON_FAILURE, "Pointer out of bounds");

        err = _generate_manifest_record(mem, pool, DRAGON_MEMORY_ALLOC_DATA, timeout);
        if (err != DRAGON_SUCCESS) {
            char* msg = dragon_getrawerrstr();
            dragon_heap_free(&pool->heap.mgrs[0], hptr, bytes);
            free(mem);
            dragon_setrawerrstr(msg);
            free(msg);
            snprintf(err_str, 399, "Cannot create manifest record.\nThis is frequently caused by too many concurrent allocations in a pool. Pools can be configured\nto allow for more concurrent allocations by specifying the max_allocations attribute when creating the pool.\nThe current max_allocations is set to %lu which requires %lu bytes in shared memory.", pool->manifest_requested_size, *pool->header.manifest_table_size);
            append_err_return(err, err_str);
        }
    }

    /* insert the new item into our umap */
    err = _add_alloc_umap_entry(mem, mem_descr);
    if (err != DRAGON_SUCCESS) {
        if (bytes > 0)
            dragon_heap_free(&pool->heap.mgrs[0], hptr, bytes);
        free(mem);
        append_err_return(err, "Could not add umap entry");
    }

    /* store the id from the pool descriptor for this allocation so we can later reverse map
        back to the pool to get the heap managers for memory frees */
    mem->pool_descr._rt_idx = pool_descr->_rt_idx;
    mem->pool_descr._idx = pool_descr->_idx;
    mem->pool_descr._original = 1;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Allocate memory from a pool
 *
 * This function allocates memory from a memory pool if the requested size is
 * avialable. Otherwise, it returns a return code indicating that the
 * requested size was not available.
 *
 * @param mem_descr is a pointer to a memory descriptor that will be initialied
 * by the call.
 *
 * @param pool_descr is a pointer to a pool descriptor which will be used for the
 * allocation.
 *
 * @param bytes is the size of the requested allocation.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code. If the requested
 * size is not then DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE is returned.
 */
dragonError_t
dragon_memory_alloc(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes)
{
    timespec_t timeout = {0,0}; /* a zero timeout tells dragon_heap_malloc_blocking not to block */
    return dragon_memory_alloc_blocking(mem_descr, pool_descr, bytes, &timeout);
}

/**
 * @brief Allocate memory from a pool, waiting if necessary
 *
 * Allocate memory for specific type with specific ID Use for allocating
 * channels, channel buffers, and other non-general-purpose data allocations.
 * Allocates memory of the given size in bytes from a memory pool. The memory
 * will be block aligned. The minimum block size of the pool determines its
 * alignment. Pools are a finite sized resource. If the desired memory
 * allocation size is not currently available, this call will block the
 * current process for timeout until it is available. The effect of using this
 * is that back pressure can be applied to a process or processes once they
 * have consumed all the local resources they are allowed.
 *
 * @param mem_descr is a pointer to the memory descriptor to be initialized.
 *
 * @param pool_descr is a pointer to a pool descriptor for the pool in which to
 * make the allocation.
 *
 * @param bytes is the size of the requested allocation in bytes.
 *
 * @param type is constant value associated with the type of memory to be allocated.
 *
 * @param timeout a pointer to the timeout that is to be used. Providing NULL
 * means to wait indefinitely. Providing a zero timeout (both seconds and nanoseconds)
 * means to try once and not block if the space is not available.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t
dragon_memory_alloc_type_blocking(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type, const timespec_t* timeout)
{
    char err_str[400];
    void* hptr = NULL;

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "could not retrieve pool from descriptor");
    }

    /* The _idx should never be zero. It is set below if successully initialized. We'll use
       the special value 0 to help detect SOME failures to check the return code in user code.
       It is not possible to catch all failures. */
    mem_descr->_idx = 0;

    /* check if the pool is addressable.  if not, this is an off-node pool, and we cannot
        fulfill the request here */
    if (pool->local_dptr == NULL && bytes > 0)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL,
                   "Cannot allocate memory for non-local pool.");

    /* create the new memory object we'll hook the new descriptor to */
    dragonMemory_t * mem;
    mem = malloc(sizeof(dragonMemory_t));
    if (mem == NULL) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory object");
    }

    /*
      Allocate in the manifest for a record
      Insert that record
      Release the lock
     */

    /* _generate_manifest_record assumes these fields are set */
    mem->local_dptr = NULL;
    mem->bytes = bytes;
    mem->offset = 0;

    if (pool->local_dptr != NULL && bytes > 0) {
        err = dragon_heap_malloc_blocking(&pool->heap.mgrs[0], bytes, &hptr, timeout);

        if (err != DRAGON_SUCCESS) {
            free(mem);
            /* Don't use append_err_return. In hot path */
            return err;
        }

        mem->local_dptr = _to_dptr(pool, hptr);
        void* end_ptr = ((void*)pool->local_dptr) + *pool->header.total_data_size;

        if (mem->local_dptr < pool->local_dptr || mem->local_dptr >= end_ptr)
            err_return(DRAGON_FAILURE, "Pointer out of bounds");

        err = _generate_manifest_record(mem, pool, type, timeout);
        if (err != DRAGON_SUCCESS) {
            char* msg = dragon_getrawerrstr();
            dragon_heap_free(&pool->heap.mgrs[0], hptr, bytes);
            free(mem);
            dragon_setrawerrstr(msg);
            free(msg);
            snprintf(err_str, 399, "Cannot create manifest record.\nThis is frequently caused by too many concurrent allocations in a pool. Pools can be configured\nto allow for more concurrent allocations by specifying the max_allocations attribute when creating the pool.\nThe current max_allocations is set to %lu which requires %lu bytes in shared memory.", pool->manifest_requested_size, *pool->header.manifest_table_size);
            append_err_return(err, err_str);
        }
    }

    err = _add_alloc_umap_entry(mem, mem_descr);
    if (err != DRAGON_SUCCESS) {
        if (bytes > 0)
            dragon_heap_free(&pool->heap.mgrs[0], hptr, bytes);
        free(mem);
        append_err_return(err, "failed to insert item into dg_mallocs umap");
    }

    /* store the id from the pool descriptor for this allocation so we can later reverse map
        back to the pool to get the heap managers for memory frees */
    mem->pool_descr._rt_idx = pool_descr->_rt_idx;
    mem->pool_descr._idx = pool_descr->_idx;
    mem->pool_descr._original = 1;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Allocate memory from a pool
 *
 * Allocate memory for specific type with specific ID Use for allocating
 * channels, channel buffers, and other non-general-purpose data allocations.
 * Allocates memory of the given size in bytes from a memory pool. The memory
 * will be block aligned. The minimum block size of the pool determines its
 * alignment. Pools are a finite sized resource. If the desired memory
 * allocation size is not currently available, this call will block the
 * current process for timeout until it is available. The effect of using this
 * is that back pressure can be applied to a process or processes once they
 * have consumed all the local resources they are allowed.
 *
 * @param mem_descr is a pointer to the memory descriptor to be initialized.
 *
 * @param pool_descr is a pointer to a pool descriptor for the pool in which to
 * make the allocation.
 *
 * @param bytes is the size of the requested allocation in bytes.
 *
 * @param type is constant value associated with the type of memory to be allocated.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code. If the requested
 * size is not then DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE is returned.
 */
dragonError_t
dragon_memory_alloc_type(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type)
{
    timespec_t timeout = {0,0}; /* a zero timeout tells dragon_heap_malloc_blocking not to block */
    return dragon_memory_alloc_type_blocking(mem_descr, pool_descr, bytes, type, &timeout);
}

/**
 * @brief Free a memory allocation
 *
 * Call this to free a memory allocation in a pool. Once done, all pointers into
 * the memory allocation are invalid and should not be used again. Doing so
 * would have unpredictable results. Calling this on a clone of a memory descriptor
 * will result in all the memory in the allocation being freed, not just the cloned
 * region. This operation only works when the memory descriptor is local to the node
 * where the process calling it is running.
 *
 * @param mem_descr is the memory descriptor to free.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_free(dragonMemoryDescr_t * mem_descr)
{
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    dragonMemory_t * mem;
    char* err_str;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    if (mem->local_dptr == NULL && mem->bytes > 0) // it is non-local
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot free memory remotely. You must free where the pool is located.");

    dragonMemoryPool_t * pool;
    err = _pool_from_descr(&mem->pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain pool from memory descriptor");

    /* Before freeing everything, delete the manifest record. Zero byte allocations
       are not recorded in the heap or manifest */
    if (mem->bytes > 0) {
        _obtain_manifest_lock(pool);
        err = dragon_blocks_free(&(pool->heap.mfstmgr), mem->mfst_record.id);
        if (err != DRAGON_SUCCESS)
            err_str = dragon_getlasterrstr();
        _release_manifest_lock(pool);

        if (err != DRAGON_SUCCESS) {
            err_noreturn(err_str);
            free(err_str);
            append_err_return(err, "cannot remove from manifest");
        }

        /* This will not timeout or hang because there is no payload. */
        dragon_bcast_trigger_one(&pool->manifest_bcast, NULL, NULL, 0);

        void* hptr = _to_hoffset(pool, mem->local_dptr);

        /* free the data */
        err = dragon_heap_free(&pool->heap.mgrs[0], hptr, mem->mfst_record.size);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot release memory back to data pool");

        mem->local_dptr = NULL;
    }

    /* delete the entry from the umap */
    err = dragon_umap_delitem(dg_mallocs, mem_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in dg_mallocs umap");

    free(mem);
    mem_descr->_idx = 0UL;
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check whether a memory pool allocation exists
 *
 * Given a memory descriptor for a particular memory allocation in a pool, return
 * true or false that the allocation exists via the flag argument. This method only works locally
 * on the node where the memory pool exists. The process calling this function and the memory pool must
 * be co-located on the same node. This method checks to see if memory was deleted after being attached.
 *
 * @param mem_descr is a user supplied memory descriptor for the memory descriptor being queried.
 *
 * @param flag is a pointer to an integer where 1 will be stored if the memory exists and 0 otherwise.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_allocation_exists(dragonMemoryDescr_t * mem_descr, int * flag)
{
    dragonError_t err;

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    if (flag == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid flag pointer");

    // Default result to 0
    *flag = 0;

    dragonMemory_t* mem;
    err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        err_return(err, "allocation could not be found.");

    // Get Pool
    dragonMemoryPool_t * pool;
    err = _pool_from_descr(&mem->pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain pool from memory descriptor");

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot check an allocation exists on a non-local pool.");

    _obtain_manifest_lock(pool);
    err = _lookup_allocation(pool, mem->mfst_record.id, mem);
    _release_manifest_lock(pool);
    if (err != DRAGON_SUCCESS) {
        err_return(err, "allocation does not exist");
    }

    // Allocation found
    *flag = 1;

    no_err_return(DRAGON_SUCCESS);
}


/**
 * @brief Free local the allocations structure
 *
 * Calling this function frees internal mallocs that are done with the
 * allocation sructure. This does not free the underlying memory allocations.
 * This function should be called when the result of calling either of the
 * get_allocations functions is no longer needed.
 *
 * @param allocs is the allocations structure that was returned from getting
 * the allocations.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_allocations_destroy(dragonMemoryPoolAllocations_t * allocs)
{
    if (allocs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "attempting to destroy NULL pointer");

    if (allocs->types != NULL)
        free(allocs->types);

    if (allocs->ids != NULL)
        free(allocs->ids);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get all allocations from a memory pool
 *
 * This function returns all current allocations from the given memory pool.
 * This function only works for pools that are co-located on the same node
 * as the process calling this function.
 *
 * @param pool_descr is a pointer to the descriptor to the memory pool.
 *
 * @param allocs is a pointer to an allocations structure that will be initialized
 * by this function.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_allocations(const dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAllocations_t * allocs)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (allocs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "allocation struct is NULL");

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve pool from descriptor");

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot get allocations from a non-local pool.");

    dragonMemoryManifestRec_t rec;

    /* obtain the manifest lock */
    _obtain_manifest_lock(pool);

    uint64_t num_items;

    err = dragon_blocks_count(&pool->heap.mfstmgr, NULL, 0, 0, &num_items);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        append_err_return(err, "could not retrieve manifest stats");
    }

    allocs->nallocs = num_items;

    if (num_items == 0) {
        allocs->types = NULL;
        allocs->ids = NULL;
        no_err_return(DRAGON_SUCCESS);
    }

    allocs->types = malloc(sizeof(dragonULInt) * allocs->nallocs);
    if (allocs->types == NULL) {
        _release_manifest_lock(pool);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for types");
    }

    allocs->ids = malloc(sizeof(dragonULInt) * allocs->nallocs);
    if (allocs->ids == NULL) {
        _release_manifest_lock(pool);
        free(allocs->types);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for ids");
    }

    dragonULInt * type_ptr = allocs->types;
    dragonULInt * id_ptr = allocs->ids;

    err = dragon_blocks_first(&pool->heap.mfstmgr, NULL, 0, 0, &rec.id);

    while (err == DRAGON_SUCCESS) {
        err = dragon_blocks_get(&pool->heap.mfstmgr, rec.id, &rec.type);
        if (err != DRAGON_SUCCESS) {
            _release_manifest_lock(pool);
            free(allocs->types);
            free(allocs->ids);
            append_err_return(err, "could not get value for type in iteration.");
        }

        *(type_ptr) = rec.type;
        *(id_ptr) = rec.id;
        type_ptr++;
        id_ptr++;

        err = dragon_blocks_next(&pool->heap.mfstmgr, NULL, 0, 0, &rec.id);
    }

    if (err != DRAGON_BLOCKS_ITERATION_COMPLETE) {
        _release_manifest_lock(pool);
        free(allocs->types);
        free(allocs->ids);
        append_err_return(err, "Could not iterate over manifest.");
    }

    /* release the manifest lock */
    _release_manifest_lock(pool);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get all allocations of a particular type from a memory pool
 *
 * This function returns all current allocations of a particular type from the
 * given memory pool. This function only works for pools that are co-located
 * on the same node as the process calling this function.
 *
 * @param pool_descr is a pointer to the descriptor to the memory pool.
 *
 * @param type is the type of the allocations to retrieve from the memory pool.
 *
 * @param allocs is a pointer to an allocations structure that will be initialized
 * by this function.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_type_allocations(const dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryAllocationType_t type,
                                        dragonMemoryPoolAllocations_t * allocs)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "pool descriptor is NULL");

    if (allocs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "allocation struct is NULL");

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve pool from descriptor");

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot get allocations from a non-local pool.");

    dragonMemoryManifestRec_t rec;
    rec.type = type;

    /* obtain the manifest lock */
    _obtain_manifest_lock(pool);

    uint64_t num_items;

    err = dragon_blocks_count(&pool->heap.mfstmgr, &rec.type, 0, sizeof(rec.type), &num_items);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        append_err_return(err, "could not retrieve manifest stats");
    }

    allocs->nallocs = num_items;

    allocs->types = malloc(sizeof(dragonULInt) * allocs->nallocs);
    if (allocs->types == NULL) {
        _release_manifest_lock(pool);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for types");
    }

    allocs->ids = malloc(sizeof(dragonULInt) * allocs->nallocs);
    if (allocs->ids == NULL) {
        _release_manifest_lock(pool);
        free(allocs->types);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for ids");
    }

    dragonULInt * type_ptr = allocs->types;
    dragonULInt * id_ptr = allocs->ids;

    err = dragon_blocks_first(&pool->heap.mfstmgr, &rec.type, 0, sizeof(rec.type), &rec.id);

    while (err == DRAGON_SUCCESS) {
        err = dragon_blocks_get(&pool->heap.mfstmgr, rec.id, &rec.type);
        if (err != DRAGON_SUCCESS) {
            _release_manifest_lock(pool);
            free(allocs->types);
            free(allocs->ids);
            append_err_return(err, "could not get value for type in iteration.");
        }

        *(type_ptr) = rec.type;
        *(id_ptr) = rec.id;
        type_ptr++;
        id_ptr++;

        err = dragon_blocks_next(&pool->heap.mfstmgr, &rec.type, 0, sizeof(rec.type), &rec.id);
    }

    if (err != DRAGON_BLOCKS_ITERATION_COMPLETE) {
        _release_manifest_lock(pool);
        free(allocs->types);
        free(allocs->ids);
        append_err_return(err, "Could not iterate over manifest.");
    }

    /* release the manifest lock */
    _release_manifest_lock(pool);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the base address for a memory pool
 *
 * This function returns the base address for a memory pool.
 *
 * @param pool_descr is a pointer to the descriptor to the memory pool.
 *
 * @param base_ptr is a pointer to the returned base address.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_pointer(const dragonMemoryPoolDescr_t * pool_descr, void **base_ptr)
{
    dragonMemoryPool_t * pool = NULL;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve pool from descriptor");

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot get a base pointer for a non-local pool.");

    *base_ptr = pool->local_dptr;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the total size of a memory pool
 *
 * This function returns the total size of a memory pool.
 *
 * @param pool_descr is a pointer to the descriptor to the memory pool.
 *
 * @param size is a pointer to the returned size of the pool.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_get_size(const dragonMemoryPoolDescr_t * pool_descr, size_t *size)
{
    dragonMemoryPool_t * pool = NULL;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve pool from descriptor");

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot get the size of a non-local pool.");

    *size = *pool->header.total_data_size;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get a pointer into a memory descriptor
 *
 * Calling this function gives the caller a pointer into a memory allocation starting at
 * offset 0 if it is the original memory descriptor or at the cloned offset if this is a
 * clone. This function only works on the node where the memory descriptor's pool resides.
 *
 * @param mem_descr is a pointer to a memory descriptor into which a pointer is returned.
 *
 * @param ptr is a pointer to a pointer where the pointer into the memory descriptor is
 * copied.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_get_pointer(const dragonMemoryDescr_t * mem_descr, void ** ptr)
{
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem->bytes == 0) {
        *ptr = NULL;
        no_err_return(DRAGON_SUCCESS);
    }

    if (mem->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot get a pointer to a non-local memory allocation.");

    *ptr = mem->local_dptr + mem->offset;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get a memory descriptor size
 *
 * This function returns the size of the given memory descriptor. If this
 * is an original memory descriptor and not a clone, then the original requested
 * size will be returned. If it is a clone, it will be the size minus whatever
 * offset was provided when the clone was made. This function works both on-node
 * and off-node.
 *
 * @param mem_descr is a pointer to a valid, initialized memory descriptor
 *
 * @param bytes is a pointer to where to store the number of bytes for the
 * given memory descriptor.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_get_size(const dragonMemoryDescr_t * mem_descr, size_t * bytes)
{
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid bytes argument");

    *bytes = mem->bytes;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get the pool where this memory descriptor resides
 *
 * Get the pool where this memory descriptor resides. This can be called
 * both on-node and off-node.
 *
 * @param mem_descr is a pointer to a valid memory descriptor.
 *
 * @param pool_descr is a pointer to a pool descriptor which will be initialized
 * by this call.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_get_pool(const dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr)
{
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    err = dragon_memory_pool_descr_clone(pool_descr, &mem->pool_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "unable to produce pool descriptor from memory descriptor");

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Get a memory descriptor from it (type, type id) identifier
 *
 * Given a (type, type id) pair for a given pool, the manifest is used to
 * find the identified memory descriptor and return it. This only works on
 * the node where the pool actually resides. It does not work off-node. A
 * clone of the memory descriptor is intialized by this call.
 *
 * @param mem_descr is the memory descriptor to initialize with the located memory.
 *
 * @param pool_descr is the memory pool that is being queried.
 *
 * @param id is the id part of the memory descriptor identifier
 *
 * @param offset is an offset into the memory descriptor used to provide a cloned offset
 * into the returned memory descriptor.
 *
 * @param bytes_size is a pointer to an unsigned integer where the size of the
 * memory descriptor clone is stored.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_get_alloc_memdescr(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr,
                                 const dragonULInt id, const dragonULInt offset, const dragonULInt* bytes_size)
{
    dragonError_t err;
    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "NULL memory descriptor");

    if (mem_descr == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP)
        err_return(DRAGON_INVALID_ARGUMENT, "Attempting to get memory allocations with constant value DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP.");

    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "NULL pool descriptor");

    dragonMemoryPool_t * pool;
    err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not retrieve pool from descriptor");

    dragonMemory_t * mem = malloc(sizeof(dragonMemory_t));
    if (mem == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory structure");

    if (pool->local_dptr != NULL && ((bytes_size == NULL) || (bytes_size != NULL && *bytes_size > 0))) { /* It is a local pool allocation of non-zero size */
        /* Acquire manifest info for the allocation */
        _obtain_manifest_lock(pool);
        err = _lookup_allocation(pool, id, mem);
        _release_manifest_lock(pool);

        if (err != DRAGON_SUCCESS) {
            char err_str[100];
            free(mem);
            snprintf(err_str, 99, "could not find matching id=%lu allocation", id);
            append_err_return(err, err_str);
        }

        /* Fill out rest of mem_struct */
        if (bytes_size != NULL) {
            if (*bytes_size + offset > mem->mfst_record.size) {
                free(mem);
                err_return(DRAGON_INVALID_ARGUMENT, "You cannot request a size/offset combination that is larger than the original memory pool allocation.");
            }
            mem->bytes = *bytes_size;
        } else
            mem->bytes = mem->mfst_record.size - offset;
        mem->local_dptr = mem->mfst_record.offset + pool->local_dptr;
    } else {
        /* non-local (or zero size) so use the provided bytes_size, but don't
           lookup allocation. When the allocation was serialized
           originally, the bytes was included in the serialization. So
           even remotely this will be valid when used via a clone. If
           a user mistakenly were to call this remotely with their own
           id, offset, and bytes_size, it will be verified
           before any pointer is handed out on a local node */

        if (bytes_size == NULL) {
            free(mem);
            err_return(DRAGON_INVALID_ARGUMENT, "A non-local memory descriptor cannot be looked up. The bytes_size argument must point to a valid size");
        }
        mem->local_dptr = NULL;
        mem->bytes = *bytes_size;
        mem->mfst_record.id = id;
        mem->mfst_record.type = 0; // not known remotely, but not required remotely.
        mem->mfst_record.offset = offset;
        mem->mfst_record.size = *bytes_size;
    }

    mem->offset = offset;

    err = dragon_memory_pool_descr_clone(&(mem->pool_descr), pool_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not clone pool descriptor");

    /* Add umap entry to dg_mallocs (out_mem_descr) */
    err = _add_alloc_umap_entry(mem, mem_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "could not add umap entry");

    mem_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Clone a memory descriptor with a custom offset
 *
 * Given a memory descriptor, construct a clone with a new offset
 * and possible new length.
 *
 * @param newmem_descr is the memory descriptor to hold the clone.
 *
 * @param oldmem_descr is the memory descriptor from which the clone is made.
 *
 * @param offset is the new offset to be used into the oldmem_descr.
 *
 * @param custom_length is a pointer to the new length to be used for the clone
 * which is useful when you want to carve up a larger allocation. Providing NULL
 * will use the original length minus the offset.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_descr_clone(dragonMemoryDescr_t * newmem_descr, const dragonMemoryDescr_t * oldmem_descr,
                          ptrdiff_t offset, size_t * custom_length)
{
    /* Check that the given descriptor points to valid memory */
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(oldmem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (offset > mem->bytes)
        err_return(DRAGON_INVALID_ARGUMENT, "offset too big for allocation");

    if (custom_length != NULL) {
        if ((offset + *custom_length) > mem->bytes)
            err_return(DRAGON_INVALID_ARGUMENT, "offset plus custom length too big for allocation");
    }

    /* create the new memory object we'll hook the new descriptor to */
    dragonMemory_t * mem_clone;
    mem_clone = malloc(sizeof(dragonMemory_t));
    if (mem_clone == NULL) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate clone of memory object");
    }

    mem_clone->bytes = mem->bytes - offset;
    if (custom_length != NULL)
        mem_clone->bytes = *custom_length;
    mem_clone->offset = mem->offset + offset;
    mem_clone->local_dptr = mem->local_dptr;
    mem_clone->pool_descr = mem->pool_descr;
    mem_clone->mfst_record = mem->mfst_record;

    /* insert the new item into our umap */
    err = dragon_umap_additem_genkey(dg_mallocs, mem_clone, &newmem_descr->_idx);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to insert item into dg_mallocs umap");
    }

    // The new one is not the original.
    newmem_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Change the size of a given memory descriptor
 *
 * Without making a clone, change the size of a given memory descriptor.
 *
 * @param mem_descr is the memory descriptor to modify.
 *
 * @param new_size is the new size. The new size cannot cause the
 * memory descriptor to go beyond the original allocation's size unless
 * it is a zero-byte allocation in which case a new allocation is made which
 * will be big enough to hold the given size.
 *
 * @param timeout is the timeout to use when a zero-byte allocation
 * has its size modified to a non-zero byte allocation. Otherwise not used.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_modify_size(dragonMemoryDescr_t * mem_descr, const size_t new_size, const timespec_t* timeout)
{
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    if (mem->bytes == 0) {
        err = dragon_memory_alloc_blocking(mem_descr, &mem->pool_descr, new_size, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not allocate memory to resize zero-byte allocaiton.");

        no_err_return(DRAGON_SUCCESS);
    }

    if (new_size + mem->offset > mem->mfst_record.size)
        err_return(DRAGON_INVALID_ARGUMENT, "The new size+offset is bigger than the allocated size.");

    mem->bytes = new_size;

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Compute a hash value for a memory allocation
 *
 * Use the Dragon hash function to compute a hash value
 *
 * @param mem_descr is the memory descriptor to hash.
 *
 * @param hash_value is the hash function's value
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_hash(dragonMemoryDescr_t* mem_descr, dragonULInt* hash_value)
{
    /* Check that the given descriptor points to valid memory */
    dragonMemory_t* mem;
    dragonError_t err;

    if (hash_value == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must pass a pointer to a hash_value location to store the result.");

    err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem->bytes == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot compute a hash for a zero bytes allocation.");


    if (mem->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot hash a non-local memory allocation.");

    *hash_value = dragon_hash(mem->local_dptr+mem->offset, mem->bytes);

    no_err_return(DRAGON_SUCCESS);

}

/**
 * @brief Check for equal contents
 *
 * Check that two memory allocations have equal contents.
 *
 * @param mem_descr1 One memory allocation.
 * @param mem_descr2 Other memory allocation.
 *
 * @param result is true if equal and false otherwise.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_equal(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result)
{
    /* Check that the given descriptor points to valid memory */
    dragonMemory_t* mem1;
    dragonMemory_t* mem2;
    dragonError_t err;

    if (result == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must pass a pointer to a result location to store the result.");

    *result = false;

    err = _mem_from_descr(mem_descr1, &mem1);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem1->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot hash a non-local memory allocation.");

    err = _mem_from_descr(mem_descr2, &mem2);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem2->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot hash a non-local memory allocation.");

    *result = dragon_bytes_equal(mem1->local_dptr + mem1->offset, mem2->local_dptr + mem2->offset, mem1->bytes, mem2->bytes);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check for to memory allocations are actually the same.
 *
 * Check that two memory allocations are the same allocation.
 *
 * @param mem_descr1 One memory allocation.
 * @param mem_descr2 Other memory allocation.
 *
 * @param result is true if equal and false otherwise.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_is(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result)
{
    /* Check that the given descriptor points to valid memory */
    dragonMemory_t* mem1;
    dragonMemory_t* mem2;
    dragonError_t err;

    if (result == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must pass a pointer to a result location to store the result.");

    *result = false;

    err = _mem_from_descr(mem_descr1, &mem1);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem1->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot hash a non-local memory allocation.");

    err = _mem_from_descr(mem_descr2, &mem2);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem2->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot hash a non-local memory allocation.");

    *result = (mem1->mfst_record.id == mem2->mfst_record.id);

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Check for equal contents
 *
 * Check that two memory allocations have equal contents.
 *
 * @param mem_descr1 One memory allocation.
 * @param mem_descr2 Other memory allocation.
 *
 * @param result is true if equal and false otherwise.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_copy(dragonMemoryDescr_t* from_mem, dragonMemoryDescr_t* to_mem, dragonMemoryPoolDescr_t* to_pool, const timespec_t* timeout)
{
    dragonError_t err;
    size_t size;
    void* from_ptr;
    void* to_ptr;

    err = dragon_memory_get_size(from_mem, &size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size of memory.");

    err = dragon_memory_alloc_blocking(to_mem, to_pool, size, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not allocate new memory.");

    err = dragon_memory_get_pointer(from_mem, &from_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get from memory pointer.");

    err = dragon_memory_get_pointer(to_mem, &to_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get to memory pointer.");

    if (size > 0 && to_ptr != NULL && from_ptr != NULL)
        memcpy(to_ptr, from_ptr, size);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_memory_clear(dragonMemoryDescr_t* mem_descr, size_t start, size_t stop)
{
    dragonError_t err;
    dragonMemory_t* mem;
    size_t num;

    err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid memory descriptor");

    if (mem->bytes == 0)
        no_err_return(DRAGON_SUCCESS);

    if (start > mem->bytes)
        err_return(DRAGON_INVALID_ARGUMENT, "Specified a start location greater than size.");

    num = stop - start;
    if (num > mem->bytes - start)
        num = mem->bytes - start;

    memset(mem->local_dptr+mem->offset+start, 0, num);

    no_err_return(DRAGON_SUCCESS);
}
