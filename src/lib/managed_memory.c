#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdatomic.h>
#include "_managed_memory.h"
#include "hostid.h"
#include "_heap_manager.h"
#include <dragon/channels.h>
#include <dragon/utils.h>

/* dragon globals */
static dragonMap_t * dg_pools   = NULL;
static dragonMap_t * dg_mallocs = NULL;

#define _obtain_manifest_lock(pool) ({\
    dragonError_t err = dragon_lock(&pool->mlock);\
    if (err != DRAGON_SUCCESS) {\
        char * err_str = _errstr_with_code("manifest lock error code", (int)err);\
        err_noreturn(err_str);\
        free(err_str);\
        return err;\
    }\
})

#define _release_manifest_lock(pool) ({\
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

static dragonError_t
_pool_from_descr(const dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPool_t ** pool)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_pools, pool_descr->_idx, (void *)pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in pools umap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
 _pool_descr_from_m_uid(const dragonM_UID_t m_uid, dragonMemoryPoolDescr_t * pool_descr)
 {
     if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonMemoryPool_t * pool;
    dragonError_t err = dragon_umap_getitem(dg_pools, m_uid, (void *)&pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in pools umap");

    /* update the descriptor with the m_uid key and note this cannot be original */
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
                     dragonM_UID_t m_uid)
{
    dragonError_t err;

    if (dg_pools == NULL) {
        dg_pools = malloc(sizeof(dragonMap_t));
        if (dg_pools == NULL) {
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for pools");
        }

        err = dragon_umap_create(dg_pools, DRAGON_MEMORY_POOL_UMAP_SEED);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to create umap for pools");
        }
    }

    err = dragon_umap_additem(dg_pools, m_uid, pool);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to insert item into pools umap");
    }

    /* store the m_uid as the key in the descriptor */
    pool_descr->_idx = m_uid;

    /* Default _original to 0 */
    pool_descr->_original = 0;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_add_alloc_umap_entry(dragonMemory_t * mem, dragonMemoryDescr_t * mem_descr)
{
    dragonError_t err;

    if (dg_mallocs == NULL) {
        dg_mallocs = malloc(sizeof(dragonMap_t));
        if (dg_mallocs == NULL) {
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

static uint64_t
_fetch_data_idx(uint64_t * local_dptr)
{
    atomic_uint_fast64_t new_idx;
    atomic_uint_fast64_t * adptr = (atomic_uint_fast64_t *)local_dptr;

    new_idx = atomic_fetch_add_explicit(adptr, 1UL, DRAGON_MEMORY_DATA_IDX_MEM_ORDER);
    return (uint64_t)new_idx;
}

static dragonError_t
_determine_heap_size(size_t requested_size, dragonMemoryPoolAttr_t * attr, size_t * required_size, uint32_t * min_block_power, uint32_t * max_block_power)
{
    uint32_t max_power, min_power;
    size_t req_size;
    dragonError_t err;

    _find_pow2_requirement(attr->data_min_block_size, &req_size, &min_power);

    _find_pow2_requirement(requested_size, &req_size, &max_power);

    err = dragon_heap_size(max_power, min_power, attr->minimum_data_alignment, attr->lock_type, &req_size);

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
    size_t hashtable_size;
    dragonError_t err = dragon_hashtable_size(pool->manifest_requested_size, sizeof(dragonULInt)*2, sizeof(dragonULInt)*2, &hashtable_size);

    if (err != DRAGON_SUCCESS) {
        char * err_str = _errstr_with_code("manifest hashtable error code", (int)err);
        err_noreturn(err_str);
        free(err_str);
        return err;
    }

    /* For the fixed header size we take the size of the structure but subtract off the size of
       three fields, the pre_allocs the filenames, and the manifest_table. All fields within the header
       are 8 byte fields so it will have the same size as pointers to each value
       in the dragonMemoryPoolHeader_t structure */
    size_t fixed_header_size = sizeof(dragonMemoryPoolHeader_t) - sizeof(void*) * 3;

    attr->manifest_allocated_size = fixed_header_size + attr->npre_allocs * sizeof(size_t) +
                    (attr->n_segments + 1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH + hashtable_size;

    /* for the requested allocation size, determine
       the actual allocation size needed to embed a heap manager into
       each while also adjusting to the POW2 it requires
    */

    uint32_t max_block_power, min_block_power;
    uint64_t total_size;

    err = _determine_heap_size(pool->data_requested_size, attr, &total_size, &min_block_power, &max_block_power);

    if (err != DRAGON_SUCCESS)
        append_err_return(err,"Could not get heap size in determining pool allocation size.");

    attr->data_min_block_size = 1UL << min_block_power;
    attr->allocatable_data_size = 1UL << max_block_power;
    attr->total_data_size = total_size;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unlink_shm_file(const char * file)
{
    if (file == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "shm filename not set");

    int ierr = shm_unlink(file);
    if (ierr == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_unlink() file");

    /* Because this is used during error recovery, don't use no_err_return.
       It would reset the backtrace string and we want the original problem. */
    return DRAGON_SUCCESS;
}

static dragonError_t
_open_map_manifest_shm(dragonMemoryPool_t * pool, const char * mfile, size_t file_size)
{
    pool->mfd = shm_open(mfile, O_RDWR, 0);
    if (pool->mfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() manifest file (file exist?)");

    pool->mptr = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->mfd, 0);
    if (pool->mptr == NULL)
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() manifest file");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_open_map_data_shm(dragonMemoryPool_t * pool, const char * dfile, size_t file_size)
{
    pool->dfd = shm_open(dfile, O_RDWR, 0);
    if (pool->dfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() data file");

    pool->local_dptr = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->dfd, 0);
    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() data file");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_create_map_manifest_shm(dragonMemoryPool_t * pool, const char * mfile, dragonMemoryPoolAttr_t * attr)
{
    pool->mfd = shm_open(mfile, O_RDWR | O_CREAT | O_EXCL , attr->mode);
    if (pool->mfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() and create manifest file (file exist?)");

    int ierr = ftruncate(pool->mfd, attr->manifest_allocated_size);
    if (ierr == -1) {
        _unlink_shm_file(mfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to ftruncate() manifest file");
    }

    pool->mptr = mmap(NULL, attr->manifest_allocated_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->mfd, 0);
    if (pool->mptr == NULL) {
        _unlink_shm_file(mfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() manifest file");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_create_map_data_shm(dragonMemoryPool_t * pool, const char * dfile, dragonMemoryPoolAttr_t * attr)
{
    /* create the data file and map it in */
    pool->dfd = shm_open(dfile, O_RDWR | O_CREAT | O_EXCL , attr->mode);
    if (pool->mfd == -1)
        err_return(DRAGON_MEMORY_ERRNO, "failed to shm_open() and create data file (file exist?)");

    int ierr = ftruncate(pool->dfd, attr->total_data_size);
    if (ierr == -1) {
        _unlink_shm_file(dfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to ftruncate() data file");
    }

    pool->local_dptr = mmap(NULL, attr->total_data_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->dfd, 0);
    if (pool->local_dptr == NULL) {
        _unlink_shm_file(dfile);
        err_return(DRAGON_MEMORY_ERRNO, "failed to mmap() data file");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unmap_manifest_shm(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    if (pool->mptr == NULL)
        err_return(DRAGON_MEMORY_ERRNO, "cannot munmp() NULL manifest pointer");

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
_unmap_data_shm(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
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
_alloc_pool_shm(dragonMemoryPool_t * pool, const char * base_name, dragonMemoryPoolAttr_t * attr)
{
    /* determine the allocation sizes we really need */
    dragonError_t err = _determine_pool_allocation_size(pool, attr);
    if (err != DRAGON_SUCCESS)
        return err;

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

    /* create the filename for the data file */
    char * dfile = malloc(sizeof(char) * (DRAGON_MEMORY_MAX_FILE_NAME_LENGTH));
    if (dfile == NULL) {
        _unmap_manifest_shm(pool, attr);
        _unlink_shm_file(mfile);
        free(mfile);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate data file name string");
    }
    nchars = snprintf(dfile, DRAGON_MEMORY_MAX_FILE_NAME_LENGTH, "/%s%s_part%i",
                      DRAGON_MEMORY_DEFAULT_FILE_PREFIX, base_name, 0);
    if (nchars == -1) {
        _unmap_manifest_shm(pool, attr);
        _unlink_shm_file(mfile);
        free(mfile);
        free(dfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "encoding error generating data filename");
    }

    if (nchars > DRAGON_MEMORY_MAX_FILE_NAME_LENGTH) {
        _unmap_manifest_shm(pool, attr);
        _unlink_shm_file(mfile);
        free(mfile);
        free(dfile);
        err_return(DRAGON_MEMORY_FILENAME_ERROR, "The filename for the pool data segment was too long.");
    }

    /* create the data file and map it in */
    err = _create_map_data_shm(pool, dfile, attr);
    if (err != DRAGON_SUCCESS) {
        _unmap_manifest_shm(pool, attr);
        _unlink_shm_file(mfile);
        free(mfile);
        free(dfile);
        append_err_return(err, "failed to create data file");
    }

    /* add in filename to the attrs */
    attr->names = malloc(sizeof(char *));
    if (attr->names == NULL) {
        _unmap_manifest_shm(pool, attr);
        _unmap_data_shm(pool, attr);
        _unlink_shm_file(mfile);
        _unlink_shm_file(dfile);
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

static bool
_pool_is_destroyed(dragonMemoryPool_t * pool)
{
    return dragon_lock_is_valid(&pool->mlock) != true;
}

static dragonError_t
_free_pool_shm(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    dragonError_t err;
    bool pool_is_destroyed = _pool_is_destroyed(pool);

    /* Free the lock since it's relying on pool resources */
    err = dragon_lock_destroy(&pool->mlock);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to release heap manager lock");

    err = _unmap_manifest_shm(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to unmap manifest");

    err = _unmap_data_shm(pool, attr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to unmap data");

    /* If another process already called destroy, then this process
       should not try to unlink the files, but should unmap
       the segments. The segments still remain in memory for
       all other processes until they are unmapped, regardless
       of whether it was destroyed. */
    if (!pool_is_destroyed) {
        err = _unlink_shm_file(attr->mname);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to unlink manifest");

        for (int i = 0; i < attr->n_segments + 1; i++) {
            err = _unlink_shm_file(attr->names[i]);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "failed to unlink data file");
        }
    }

    /* Because this is used during error recovery, don't use no_err_return.
       It would reset the backtrace string and we want the original problem. */
    return DRAGON_SUCCESS;
}

static dragonError_t
_alloc_pool(dragonMemoryPool_t * pool, const char * base_name, dragonMemoryPoolAttr_t * attr)
{
    if (attr->mem_type == DRAGON_MEMORY_TYPE_SHM) {

        dragonError_t err = _alloc_pool_shm(pool, base_name, attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot allocate memory pool with shm");

    //} else if (pool->attrs.mem_type == DRAGON_MEMORY_TYPE_FILE) {

    //} else if (pool->attrs.mem_type == DRAGON_MEMORY_TYPE_PRIVATE) {

    } else {
        no_err_return(DRAGON_MEMORY_ILLEGAL_MEMTYPE);
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_free_pool(dragonMemoryPool_t * pool, dragonMemoryPoolAttr_t * attr)
{
    if (attr->mem_type == DRAGON_MEMORY_TYPE_SHM) {

        dragonError_t err = _free_pool_shm(pool, attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "cannot free memory pool with shm");

    //} else if (pool->attrs.mem_type == DRAGON_MEMORY_TYPE_FILE) {

    //} else if (pool->attrs.mem_type == DRAGON_MEMORY_TYPE_PRIVATE) {

    } else {
        no_err_return(DRAGON_MEMORY_ILLEGAL_MEMTYPE);
    }

    /* Because this is used during error recovery, don't use no_err_return.
       It would reset the backtrace string and we want the original problem. */
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

    /* embed the one heap manager into the head of the data region */
     int nsizes = max_block_power - min_block_power;
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
    pool->heap.mgrs_dptrs[0] = pool->local_dptr;
    err = dragon_heap_init(pool->heap.mgrs_dptrs[0], &pool->heap.mgrs[0], (size_t)max_block_power,
                                               (size_t)min_block_power, attr->minimum_data_alignment, attr->lock_type,
                                               preallocated);
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

    heap->mgrs_dptrs[0] = pool->local_dptr;
    err = dragon_heap_attach(heap->mgrs_dptrs[0], &(heap->mgrs[0]));
    if (err != DRAGON_SUCCESS) {
        free(heap->mgrs);
        free(heap->mgrs_dptrs);
        append_err_return(err, "failed to attach heap manager to memory");
    }

    err = dragon_hashtable_attach(pool->header.manifest_table, &(heap->mfstmgr));
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
_generate_manifest_record(dragonMemory_t * mem, dragonMemoryPool_t * pool,
                          const dragonMemoryAllocationType_t type, const dragonULInt type_id)
{
    /* generate a record and put it in the manifest */
    mem->mfst_record.offset        = (dragonULInt)((char *)mem->local_dptr - (char *)pool->local_dptr);
    mem->mfst_record.size          = (dragonULInt)mem->bytes;
    mem->mfst_record.alloc_type    = type;
    mem->mfst_record.alloc_type_id = type_id;

    dragonError_t err = dragon_hashtable_add(&(pool->heap.mfstmgr), (char*)&(mem->mfst_record.alloc_type), (char*)&(mem->mfst_record.offset));

    if (err != DRAGON_SUCCESS) {
        char err_str[100];
        snprintf(err_str, 99, "Cannot add manifest record type=%lu and type_id=%lu to pool m_uid=%lu\n", mem->mfst_record.alloc_type, mem->mfst_record.alloc_type_id, *pool->header.m_uid);
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

    /* skip over the area for the lock */
    char * ptr = (char *)pool->mptr;
    ptr += lock_size;

    dragonULInt * hptr = (dragonULInt *)ptr;

    pool->header.m_uid                   = &hptr[0];
    pool->header.anon_data_idx           = &hptr[1];
    pool->header.hostid                  = &hptr[2];
    pool->header.allocatable_data_size   = &hptr[3];
    pool->header.total_data_size         = &hptr[4];
    pool->header.data_min_block_size     = &hptr[5];
    pool->header.manifest_allocated_size = &hptr[6];
    pool->header.segment_size            = &hptr[7];
    pool->header.max_size                = &hptr[8];
    pool->header.n_segments              = &hptr[9];
    pool->header.minimum_data_alignment  = &hptr[10];
    pool->header.mem_type                = &hptr[11];
    pool->header.lock_type               = &hptr[12];
    pool->header.growth_type             = &hptr[13];
    pool->header.mode                    = (dragonUInt *)&hptr[14];
    pool->header.npre_allocs             = &hptr[15];
    pool->header.pre_allocs              = &hptr[16];


    size_t def_npre_allocs;
    if (attr != NULL)
        def_npre_allocs = attr->npre_allocs;
    else
        def_npre_allocs = *(pool->header.npre_allocs);

    pool->header.filenames = (char *)&hptr[16+def_npre_allocs];

    if (attr != NULL) {
        pool->header.manifest_table = (void*)pool->header.filenames +
                        sizeof(char) * (attr->n_segments+1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;

    } else {
        pool->header.manifest_table = (void*)pool->header.filenames +
                        sizeof(char) * (*pool->header.n_segments+1) * DRAGON_MEMORY_MAX_FILE_NAME_LENGTH;
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
    *(pool->header.anon_data_idx)           = 1UL;
    *(pool->header.hostid)                  = dragon_host_id();
    *(pool->header.allocatable_data_size)   = (dragonULInt)attr->allocatable_data_size;
    *(pool->header.total_data_size)         = (dragonULInt)attr->total_data_size;
    *(pool->header.data_min_block_size)     = (dragonULInt)attr->data_min_block_size;
    *(pool->header.manifest_allocated_size) = (dragonULInt)attr->manifest_allocated_size;
    *(pool->header.segment_size)            = (dragonULInt)attr->segment_size;
    *(pool->header.max_size)                = (dragonULInt)attr->max_size;
    *(pool->header.n_segments)              = (dragonULInt)attr->n_segments;
    *(pool->header.minimum_data_alignment)  = (dragonULInt)attr->minimum_data_alignment;
    *(pool->header.lock_type)               = (dragonULInt)attr->lock_type;
    *(pool->header.mem_type)                = (dragonULInt)attr->mem_type;
    *(pool->header.growth_type)             = (dragonULInt)attr->growth_type;
    *(pool->header.mode)                    = (dragonUInt)attr->mode;
    *(pool->header.npre_allocs)             = (dragonULInt)attr->npre_allocs;

    for (int i = 0; i < attr->npre_allocs; i ++) {
        pool->header.pre_allocs[i] = attr->pre_allocs[i];
    }

    err = _assign_filenames(pool, attr->names, attr->n_segments+1);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot instantiate filenames into manifest header");

    /* now instantiate the hashtable for keeping track of manifest records */
    err = dragon_hashtable_init((char*)pool->header.manifest_table, &pool->heap.mfstmgr,
                                pool->manifest_requested_size, sizeof(dragonULInt)*2, sizeof(dragonULInt)*2);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to instantiate hash table into manifest memory");

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
    attr->minimum_data_alignment  = *(pool->header.minimum_data_alignment);
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
_lookup_allocation(dragonMemoryPool_t * pool, dragonMemoryAllocationType_t type, uint64_t type_id,
                        dragonMemory_t * out_mem)
{
    /* this routine is used both for positive and negative lookups, don't append an error string */
    dragonError_t err;

    out_mem->mfst_record.alloc_type = type;
    out_mem->mfst_record.alloc_type_id = type_id;

    err = dragon_hashtable_get(&(pool->heap.mfstmgr), (char*)&(out_mem->mfst_record.alloc_type), (char*)&(out_mem->mfst_record.offset));

    return err;
}

static dragonError_t
_validate_attr(const dragonMemoryPoolAttr_t * attr)
{
    if (attr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attr argument was NULL as provided to the _validate_attr function.");

    if (attr->segment_size < attr->data_min_block_size)
        err_return(DRAGON_INVALID_ARGUMENT, "The segment size must be at least as big as the minimum block size.");

    if (attr->minimum_data_alignment > attr->data_min_block_size)
        err_return(DRAGON_INVALID_ARGUMENT, "The minimum_data_alignment cannot be greater than the minimum block size");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_copy_attr_nonames(dragonMemoryPoolAttr_t * new_attr, const dragonMemoryPoolAttr_t * attr)
{
    new_attr->data_min_block_size     = attr->data_min_block_size;
    new_attr->allocatable_data_size   = attr->allocatable_data_size;
    new_attr->total_data_size         = attr->total_data_size;
    new_attr->manifest_allocated_size = attr->manifest_allocated_size;
    new_attr->segment_size            = attr->segment_size;
    new_attr->max_size                = attr->max_size;
    new_attr->n_segments              = attr->n_segments;
    new_attr->minimum_data_alignment  = attr->minimum_data_alignment;
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
    attr->max_allocatable_block_size = DRAGON_MEMORY_DEFAULT_MAX_SIZE;
    attr->manifest_allocated_size    = 0;
    attr->max_size                   = DRAGON_MEMORY_DEFAULT_MAX_SIZE;
    attr->n_segments                 = 0;
    attr->segment_size               = DRAGON_MEMORY_DEFAULT_SEG_SIZE;
    attr->minimum_data_alignment     = DRAGON_MEMORY_DEFAULT_MIN_BLK_SIZE;
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

    /* determine size of the pool based on the requested number of bytes */
    uint32_t max_block_power, min_block_power, segment_max_block_power;
    size_t required_size;

    _find_pow2_requirement(def_attr.segment_size, &required_size, &segment_max_block_power);
    _find_pow2_requirement(def_attr.data_min_block_size, &required_size, &min_block_power);
    _find_pow2_requirement(bytes, &required_size, &max_block_power);

    /* This is what the user requested but the allocatable_data_size in the header will
       contain the actually allocated size since they may not have requested a power of 2 */
    pool->data_requested_size = bytes;

    /* The user may not have chosen a power of 2 for the segment size, so we'll set it to
       the right value which is the smallest power of 2 that is bigger than their original
       request. */
    def_attr.segment_size = 1UL << segment_max_block_power;
    def_attr.allocatable_data_size = required_size;
    def_attr.max_size = required_size + def_attr.segment_size * def_attr.n_segments;

    /* This computes the max number of entries that will be needed in the hashtable for the
       manifest. This is needed to be able to establish the entire needed size for the manifest
       given that it could grow by adding additional segments.

       Adding segments is not yet implemented, but this allows for it. */
    pool->manifest_requested_size = (1UL << (max_block_power - min_block_power)) +
            def_attr.n_segments * (1UL << (segment_max_block_power - min_block_power));

    // Creating a pool should count as an explicit attach
    // Possibly unnecessary atomic operation since the pool only exists here right now
    atomic_store(&(pool->ref_cnt), 1);

    /* perform the allocation */
    err = _alloc_pool(pool, base_name, &def_attr);
    if (err != DRAGON_SUCCESS) {
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        append_err_return(err, "cannot allocate memory pool");
    }

    /* manifest header init should be done before much of anything else
       because error handling below (like calls to _free_pool) rely
       on the manifest header being initialized first */
    err = _initialize_manifest_header(pool, m_uid, &def_attr);
    if (err != DRAGON_SUCCESS) {
        _free_pool(pool, &def_attr);
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        append_err_return(err, "cannot initialize manifest");
    }

    /* instantiate heap managers into the manifest and data memory */
    err = _instantiate_heap_managers(pool, &def_attr);
    if (err != DRAGON_SUCCESS) {
        _free_pool(pool, &def_attr);
        free(pool);
        dragon_memory_attr_destroy(&def_attr);
        append_err_return(err, "cannot instantiate heap managers");
    }

    /* create the umap entry for the descriptor */
    err = _add_pool_umap_entry(pool_descr, pool, m_uid);
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

    /* get an attributes structure for use in the destruction */
    dragonMemoryPoolAttr_t attrs;
    err = _attrs_from_header(pool, &attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot construct pool attributes from pool");

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
    err = dragon_umap_delitem(dg_pools, pool_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in pools umap");
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
        ptr += 4; // skip id, host_id, mem_type, manifest_len
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

    /* check if we already have attached to this pool, if so we will just use that */
    dragonError_t err = _pool_descr_from_m_uid(m_uid, pool_descr);
    if (err == DRAGON_SUCCESS) {
        dragonMemoryPool_t * pool;
        _pool_from_descr(pool_descr, &pool);
        atomic_fetch_add_explicit(&(pool->ref_cnt), 1, memory_order_acq_rel);
        no_err_return(DRAGON_SUCCESS);
    }

    /* Validate dragon_host_id matches pool's host_id */
    dragonULInt local_host_id = dragon_host_id();
    dragonULInt host_id = *ptr;
    ptr++;

    /* Allocate a new pool, open the manifest file, and map it into the pool header */
    dragonMemoryPool_t * pool = (dragonMemoryPool_t*)malloc(sizeof(dragonMemoryPool_t));
    if (pool == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate internal pool structure.");

    /* If this is a non-local pool we are attaching to, then we set a flag. */
    if (local_host_id != host_id)
        local_pool = false;

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
        err = _open_map_data_shm(pool, mattr.names[0], mattr.total_data_size);
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
    } else {
        pool->local_dptr = NULL;
        pool->remote.hostid = host_id;
        pool->remote.m_uid = m_uid;
        pool->remote.mem_type = mem_type;
        pool->remote.manifest_len = manifest_len;
    }

    /* Add entry into pool umap updating pool descriptor's idx */
    err = _add_pool_umap_entry(pool_descr, pool, m_uid);

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

    pool_ser.data = dragon_base64_decode(encoded_pool_str, strlen(encoded_pool_str), &pool_ser.len);

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
    char* pool_str;

    pool_str = getenv(DRAGON_DEFAULT_PD_VAR);

    if (pool_str == NULL)
        err_return(DRAGON_INVALID_OPERATION, "Called dragon_get_default_pool with no default pool set in environment.");

    err = dragon_memory_pool_attach_from_env(pool, pool_str);
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

        err = _detach_heap_managers(pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach heap manager in pool detach.");

        /* @MCB TODO: Need control flow to manage different memory types */
        /* Unmap manifest and data pointers */
        err = _unmap_manifest_shm(pool, &attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to unmap manifest");

        err = _unmap_data_shm(pool, &attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to unmap data");

        /* Clear attributes */
        err = dragon_memory_attr_destroy(&attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to destroy pool attributes");
    }

    /* Remove from umap */
    err = dragon_umap_delitem(dg_pools, pool_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item in pools umap");

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

    /* Type of the allocation */
    *(dragonULInt*)ptr = mem->mfst_record.alloc_type;
    ptr += sizeof(dragonULInt);

    /* Alloc allocation ID */
    *(dragonULInt*)ptr = mem->mfst_record.alloc_type_id;
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

    dragonMemoryAllocationType_t type = *(dragonULInt*)ptr;
    ptr += sizeof(dragonULInt);
    dragonULInt type_id = *(dragonULInt*)ptr;
    ptr += sizeof(dragonULInt);
    dragonULInt offset = *(dragonULInt*)ptr;
    ptr += sizeof(dragonULInt);
    dragonULInt bytes = *(dragonULInt*)ptr;

    err = dragon_memory_get_alloc_memdescr(mem_descr, &pool_descr, type, type_id, offset, &bytes);
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
    size_t alloc_bytes = bytes;

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
    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL,
                   "cannot allocate memory directly on non-local pool");

    /* create the new memory object we'll hook the new descriptor to */
    dragonMemory_t * mem;
    mem = malloc(sizeof(dragonMemory_t));
    if (mem == NULL) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory object");
    }

    // A zero byte allocation is needed in channels when attributes are to be sent
    // and potentially other entities that require a shared memory descriptor when
    // there is no real allocation to make. So don't reject bytes == 0.
    if (bytes == 0)
        // To avoid special case code for this everywhere (cloning, freeing, etc.)
        // we will make a 1 byte allocation, but say that it is zero bytes.
        alloc_bytes = 1;

    err = dragon_heap_malloc_blocking(&pool->heap.mgrs[0], alloc_bytes, &mem->local_dptr, timeout);
    if (err != DRAGON_SUCCESS)
        /* Don't use append_err_return. In hot path */
        return err;

    /* _generate_manifest_record assumes these fields are set */
    mem->bytes = bytes;
    mem->offset = 0;

    /* generate a record in the manifest of this allocation */
    dragonULInt idx = _fetch_data_idx(pool->header.anon_data_idx);

    _obtain_manifest_lock(pool);
    err = _generate_manifest_record(mem, pool, DRAGON_MEMORY_ALLOC_DATA, idx);
    _release_manifest_lock(pool);

    if (err != DRAGON_SUCCESS) {
        dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
        free(mem);
        append_err_return(err, "cannot create manifest record");
    }

    /* insert the new item into our umap */
    err = _add_alloc_umap_entry(mem, mem_descr);
    if (err != DRAGON_SUCCESS) {
        dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
        free(mem);
        append_err_return(err, "Could not add umap entry");
    }

    /* store the id from the pool descriptor for this allocation so we can later reverse map
        back to the pool to get the heap managers for memory frees */
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
 * @param type_id is a user supplied value to identify the memory allocation of
 * this given type.
 *
 * @param timeout a pointer to the timeout that is to be used. Providing NULL
 * means to wait indefinitely. Providing a zero timeout (both seconds and nanoseconds)
 * means to try once and not block if the space is not available.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
 */
dragonError_t
dragon_memory_alloc_type_blocking(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type, const dragonULInt type_id, const timespec_t* timeout)
{
    size_t alloc_bytes = bytes;

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory descriptor");

    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "could not retrieve pool from descriptor");
    }

    /* if pool is non-addressable, it is off-node and we cannot directly allocate */
    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL,
                          "cannot allocate memory on non-local pool");

    dragonMemory_t * mem;
    mem = malloc(sizeof(dragonMemory_t));
    if (mem == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new memory object");

    /* Check if record for type+id already exists.  If so, return error */
    _obtain_manifest_lock(pool);
    err = _lookup_allocation(pool, type, type_id, mem);
    if (err == DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        free(mem);
        err_return(DRAGON_INVALID_ARGUMENT, "allocation record already exists");
    }

    // Record does not already exist, reserve it
    /*
      Allocate in the manifest for a record
      Insert that record
      Release the lock
     */

    // A zero byte allocation is needed in channels when attributes are to be sent
    // and potentially other entities that require a shared memory descriptor when
    // there is no real allocation to make. So don't reject bytes == 0.
    if (bytes == 0)
        // To avoid special case code for this everywhere (cloning, freeing, etc.)
        // we will make a 1 byte allocation, but say that it is zero bytes.
        alloc_bytes = 1;

    err = dragon_heap_malloc_blocking(&pool->heap.mgrs[0], alloc_bytes, &mem->local_dptr, timeout);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        free(mem);
        /* Don't use append_err_return. In hot path */
        return err;
    }

    /* _generate_manifest_record assumes these fields are set */
    mem->bytes = bytes;
    mem->offset = 0;

    err = _generate_manifest_record(mem, pool, type, type_id);
    _release_manifest_lock(pool);
    if (err != DRAGON_SUCCESS) {
        dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
        free(mem);
        append_err_return(err, "cannot create manifest record");
    }

    err = _add_alloc_umap_entry(mem, mem_descr);
    if (err != DRAGON_SUCCESS) {
        //TODO: dragon_umap_delitem
        dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
        free(mem);
        append_err_return(err, "failed to insert item into dg_mallocs umap");
    }

    err = dragon_memory_pool_descr_clone(&(mem->pool_descr), pool_descr);
    if (err != DRAGON_SUCCESS) {
        //TODO: dragon_umap_delitem
        dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
        free(mem);
        append_err_return(err, "could not clone pool descriptor");
    }

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
 * @param type_id is a user supplied value to identify the memory allocation of
 * this given type.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code. If the requested
 * size is not then DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE is returned.
 */
dragonError_t
dragon_memory_alloc_type(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type, const dragonULInt type_id)
{
    timespec_t timeout = {0,0}; /* a zero timeout tells dragon_heap_malloc_blocking not to block */
    return dragon_memory_alloc_type_blocking(mem_descr, pool_descr, bytes, type, type_id, &timeout);
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
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    if (mem->local_dptr == NULL) // it is non-local
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot free memory remotely. You must free where the pool is located.");

    dragonMemoryPool_t * pool;
    err = _pool_from_descr(&mem->pool_descr, &pool);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain pool from memory descriptor");

    /* Before freeing everything, delete the manifest record */

    _obtain_manifest_lock(pool);
    err = dragon_hashtable_remove(&(pool->heap.mfstmgr), (char*)&(mem->mfst_record.alloc_type));

    _release_manifest_lock(pool);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot remove from manifest");

    /* free the data */
    err = dragon_heap_free(&pool->heap.mgrs[0], mem->local_dptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot release memory back to data pool");

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
 * Given the manifest information (type, type id) for a particular memory allocation in a pool, return
 * true or false that the allocation exists via the flag argument. This method only works locally
 * on the node where the memory pool exists. The process calling this function and the memory pool must
 * be co-located on the same node.
 *
 * @param pool_descr is the Memory Pool being queried.
 *
 * @param type is an allocation type for the memory descriptor being queried.
 *
 * @param type_id is the user supplied identifier for the memory descriptor being queried.
 *
 * @param flag is a pointer to an integer where 1 will be stored if the memory exists and 0 otherwise.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_pool_allocation_exists(const dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryAllocationType_t type,
                                     const dragonULInt type_id, int * flag)
{
    if (pool_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid pool descriptor");

    if (flag == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid flag pointer");

    // Default result to 0
    *flag = 0;

    // Get Pool
    dragonMemoryPool_t * pool;
    dragonError_t err = _pool_from_descr(pool_descr, &pool);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "could not retrieve pool from descriptor");
    }

    if (pool->local_dptr == NULL)
        err_return(DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL, "You cannot check an allocation exists on a non-local pool.");

    // run scan function looking for a match
    dragonMemory_t mem;
    _obtain_manifest_lock(pool);
    err = _lookup_allocation(pool, type, type_id, &mem);
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

    dragonHashtableIterator_t iter;
    bool iterating = true;
    dragonMemoryManifestRec_t rec;

    /* obtain the manifest lock */
    _obtain_manifest_lock(pool);

    dragonHashtableStats_t manifest_stats;
    err = dragon_hashtable_stats(&pool->heap.mfstmgr, &manifest_stats);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        append_err_return(err, "could not retrieve manifest stats");
    }

    allocs->nallocs = manifest_stats.num_items;

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

    err = dragon_hashtable_iterator_init(&pool->heap.mfstmgr, &iter);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        free(allocs->types);
        free(allocs->ids);
        append_err_return(err, "could not initialize manifest iterator");
    }

    while (iterating) {
        err = dragon_hashtable_iterator_next(&pool->heap.mfstmgr, &iter, (char*)&rec.alloc_type, (char*)&rec.offset);

        if (err == DRAGON_HASHTABLE_ITERATION_COMPLETE) {
            iterating = false;
        } else if (err != DRAGON_SUCCESS) {
            _release_manifest_lock(pool);
            free(allocs->types);
            free(allocs->ids);
            append_err_return(err, "could not get manifest record");
        } else {
            *(type_ptr) = rec.alloc_type;
            *(id_ptr) = rec.alloc_type_id;
            type_ptr++;
            id_ptr++;
        }
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

    dragonHashtableIterator_t iter;
    bool iterating = true;
    dragonMemoryManifestRec_t rec;
    dragonULInt count = 0;

    /* obtain the manifest lock */
    _obtain_manifest_lock(pool);

    dragonHashtableStats_t manifest_stats;
    err = dragon_hashtable_stats(&pool->heap.mfstmgr, &manifest_stats);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        append_err_return(err, "could not retrieve manifest stats");
    }

    allocs->nallocs = manifest_stats.num_items;

    allocs->types = malloc(sizeof(dragonULInt) * manifest_stats.num_items);
    if (allocs->types == NULL) {
        _release_manifest_lock(pool);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for types");
    }

    allocs->ids = malloc(sizeof(dragonULInt) * manifest_stats.num_items);
    if (allocs->ids == NULL) {
        _release_manifest_lock(pool);
        free(allocs->types);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "could not allocate memory for ids");
    }

    dragonULInt * type_ptr = allocs->types;
    dragonULInt * id_ptr = allocs->ids;

    err = dragon_hashtable_iterator_init(&pool->heap.mfstmgr, &iter);
    if (err != DRAGON_SUCCESS) {
        _release_manifest_lock(pool);
        free(allocs->types);
        free(allocs->ids);
        append_err_return(err, "could not initialize manifest iterator");
    }

    while (iterating) {
        err = dragon_hashtable_iterator_next(&pool->heap.mfstmgr, &iter, (char*)&rec.alloc_type, (char*)&rec.offset);

        if (err == DRAGON_HASHTABLE_ITERATION_COMPLETE) {
            iterating = false;
        } else if (err != DRAGON_SUCCESS) {
            _release_manifest_lock(pool);
            free(allocs->types);
            free(allocs->ids);
            append_err_return(err, "could not get manifest record");
        } else {
            if (type == rec.alloc_type) {
                *(type_ptr) = rec.alloc_type;
                *(id_ptr) = rec.alloc_type_id;
                type_ptr++;
                id_ptr++;
                count++;
            }
        }
    }

    /* release the manifest lock */
    _release_manifest_lock(pool);

    allocs->nallocs = count;

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
 * @param type is the type part of the memory descriptor identifier
 *
 * @param type_id is the id part of the memory descriptor identifier
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
                                 const dragonMemoryAllocationType_t type, const dragonULInt type_id,
                                 const dragonULInt offset, const dragonULInt* bytes_size)
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

    if (pool->local_dptr != NULL) { /* It is a local pool */
        /* Acquire manifest info for the allocation */
        _obtain_manifest_lock(pool);
        err = _lookup_allocation(pool, type, type_id, mem);
        _release_manifest_lock(pool);
        if (err != DRAGON_SUCCESS) {
            char err_str[100];
            free(mem);
            snprintf(err_str, 99, "could not find matching type=%u and id=%lu allocation", type, type_id);
            err_return(err, err_str);
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
        /* non-local so use the provided bytes_size, but don't lookup allocation.
           When the allocation was serialized originally, the bytes was included
           in the serialization. So even remotely this will be valid when used
           via a clone. If a user mistakenly were to call this remotely with
           their own type, type_id, offset, and bytes_size, it will be
           verified before any pointer is handed out on a local node */
        if (bytes_size == NULL) {
            free(mem);
            err_return(DRAGON_INVALID_ARGUMENT, "A non-local memory descriptor cannot be looked up. The bytes_size argument must point to a valid size");
        }
        mem->local_dptr = NULL;
        mem->bytes = *bytes_size;
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
 * memory descriptor to go beyond the original allocation's size.
 *
 * @returns DRAGON_SUCCESS or another dragonError_t return code.
*/

dragonError_t
dragon_memory_modify_size(dragonMemoryDescr_t * mem_descr, const size_t new_size)
{
    dragonMemory_t * mem;
    dragonError_t err = _mem_from_descr(mem_descr, &mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "cannot obtain memory from descriptor");

    if (new_size + mem->offset > mem->mfst_record.size)
        err_return(DRAGON_INVALID_ARGUMENT, "The new size+offset is bigger than the allocated size.");

    mem->bytes = new_size;

    no_err_return(DRAGON_SUCCESS);
}
