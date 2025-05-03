/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_MEMORY_H
#define HAVE_DRAGON_MEMORY_H

#include <sys/stat.h>
#include <dragon/return_codes.h>
#include <dragon/shared_lock.h>
#include <dragon/global_types.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_MEMORY_TEMPORARY_TIMEOUT_CONST 300

static const int DRAGON_MEMORY_TEMPORARY_TIMEOUT_SECS = DRAGON_MEMORY_TEMPORARY_TIMEOUT_CONST;
static const timespec_t DRAGON_MEMORY_TEMPORARY_TIMEOUT = {DRAGON_MEMORY_TEMPORARY_TIMEOUT_CONST,0};

/**
 * @brief The type of memory pool.
 *
 * For future use. Currently, the only valid value is DRAGON_MEMORY_TYPE_SHM.
*/
typedef enum dragonMemoryPoolType_st {
    DRAGON_MEMORY_TYPE_SHM = 0,
    DRAGON_MEMORY_TYPE_HUGEPAGE,
    DRAGON_MEMORY_TYPE_FILE,
    DRAGON_MEMORY_TYPE_PRIVATE
} dragonMemoryPoolType_t;

/**
 * @brief The growth type for the pool.
 *
 * For future use. Currently, the only valid value is DRAGON_MEMORY_GROWTH_NONE.
*/
typedef enum dragonMemoryPoolGrowthType_st {
    DRAGON_MEMORY_GROWTH_NONE = 0,
    DRAGON_MEMORY_GROWTH_UNLIMITED,
    DRAGON_MEMORY_GROWTH_CAPPED
} dragonMemoryPoolGrowthType_t;

/** @brief Specifies the memory allocation type.
 *
 *  Dragon supports the allocation of memory based on a specific type
 *  or purpose. The generic allocation type is data and should be used by
 *  user-level code. Internally to Dragon, channel and channel buffer
 *  allocation types are also allowable.
 */
typedef enum dragonMemoryAllocationType_st {
    DRAGON_MEMORY_ALLOC_DATA = 0, /*!< Allocation is used for general-purpose data. */
    DRAGON_MEMORY_ALLOC_CHANNEL,  /*!< Internal Use Only. */
    DRAGON_MEMORY_ALLOC_CHANNEL_BUFFER, /*!< Internal Use Only. */
    DRAGON_MEMORY_ALLOC_BOOTSTRAP /* This may be used as a special type for user-specified data.
                                     It makes it possible to retrieve a set of special allocations. */
} dragonMemoryAllocationType_t;

/** @brief The attributes of the Dragon Memory Pool.
 *
 *  These are the attributes of a memory pool. Some are settable
 *  by the user. Some are read-only values and returned when requested
 *  by the user via the dragon_memory_pool_getattr function (not yet
 *  implemented). A few of these attributes are not yet implemented.
 **/
typedef struct dragonMemoryPoolAttr_st {

    size_t allocatable_data_size;
    /*!< The size of the original data segment of the pool.
     * Ignored if set by user. This is the allocatable size of the pool which
     * does not include meta information. */

    size_t total_data_size;
    /*!< The total size of the data segment include meta information.
     * Ignored if set by user. This is the total size of the data segment
     * including meta information. */

    size_t free_space;
    /*!< Free space in the pool in bytes. Read-only. */

    double utilization_pct;
    /*!< Free space as a percentage of total space of the pool. Read-only. */

    size_t data_min_block_size;
    /*!< The requested minimum allocation size from
     * the pool. The actual minimum block size is reported via
     * dragon_memory_pool_getattr. All allocations will be at least this
     * size. */

    size_t max_allocations;
    /*!< The maximum number of concurrent allocations among all processes.
    * This number is set to 1048576 entries (1024^2 entries) or the number of
    * segments in the pool, whichever is smaller. This can be set by the user
    * to something bigger if needed. */

    size_t waiters_for_manifest;
    /*!< Read-only. At the time of the call, this is the number of processes
         that are awaiting a manifest table entry because the manifest is
         currently full. Read-only. */

    size_t manifest_entries;
    /*!< Read-only. This is the number of entries of the total max_allocations
         that are currently in use. If there are waiters, then manifest_entries
         should equal max_allocations. Read-only. */

    size_t max_manifest_entries;
    /*!< The maximum concurrently used manifest locations of the pool since
     * pool creation. Read-only. */

    size_t manifest_allocated_size;
    /*!< The size in bytes of the manifest. Read-only. */

    size_t manifest_table_size;
    /*!< The size in bytes of the internal allocation table. Read-only. */

    size_t manifest_heap_size;
    /*!< The size in bytes of the internal heap manager. Read-only. */

    size_t segment_size;
    /*!< The requested size of segments when pool size is increased by the user.
     * The actual segment size may be larger. The actual segment size will be
     * reported by dragon_memory_pool_getattr.
     */

    size_t max_size;
    /*!< The requested maximum size of the memory pool.
     * The actual maximum size will not be less than this but could be bigger
     * based on requirements of the underlying heap representation. The
     * actual maximum size will be reported by dragon_memory_pool_getattr.
     * */


    size_t max_allocatable_block_size;
    /*!< The maximum size of any allocatable block. Ignored if set by the user.
     * This is the biggest block allocation this pool can support when empty. */

    size_t n_segments;
    /* !< The number of segments added to the memory pool. Ignored if set by the
     * user. */

    dragonLockKind_t lock_type;
    /*<! The type of lock to be used on the memory pool. */

    dragonMemoryPoolType_t mem_type;
    /*!< The type of memory this pool supports. Currently only SHM
     * memory pools are supported. */

    dragonMemoryPoolGrowthType_t growth_type;
    /*!< The type of growth allowed on this memory pool.
     * See dragonMemoryPoolGrowthType_t for an explanation. Currently ignored. */

    mode_t mode;
    /*!< The mode to be used for the memory pool. This provides the umask
     * for any file-backed permissions related to this memory pool. */

    size_t npre_allocs;
    /*!< The number of preallocation sizes to do on this pool.
     * To support fast allocation, a number of preallocations may be specified.
     * This is the number of preallocation sizes that are specified in
     * pre_allocs. */

    size_t * pre_allocs;
    /*!< The pre_allocs is an array of power of 2 preallocations
     * to be made for fast allocation of certain sizes of blocks. Position 0 in
     * this array indicates the number of preallocations to make with the
     * minimum block size. Position 1 in the array is the number of
     * allocations to make with two times the minimum block size. Each
     * position, n, in the array indicates the number of allocations to
     * make with 2**n times the block size. */

    char * mname;
    /*!< The name of the manifest file when backed by a file.
     * This value is ignored if set by the user. */

    char ** names;
    /*!< An array data segment file names when backed by a file.
     * The array of names is of length 1 + n_segments. This value is ignored if
     * set by the user. */
} dragonMemoryPoolAttr_t;

/**
 * @brief This is an opaque handle to a memory pool.
 *
 * When a memory pool is created, the opaque descriptor is initialized to
 * correspond to the pool in the current process. Memory pool descriptors
 * reside in process local storage in the process in which they are created or
 * attached. If you wish to share a pool descriptor with another process, you
 * must serialize the pool descriptor and the serialized descriptor can be
 * shared with another process. The other process must then attach to the
 * memory pool to initialize its own pool descriptor.
 */
typedef struct dragonMemoryPoolDescr_st {
    int _original;
    dragonM_UID_t _idx;
    dragonRT_UID_t _rt_idx;
} dragonMemoryPoolDescr_t;

/**
 * @brief This is an opaque handle to a memory allocation.
 *
 * When Dragon pool memory is allocated, a memory descriptor is initialized for
 * the current process. These memory descriptors may be shared with other processes
 * by first serializing them, and then passing the serialized descriptor to another
 * process. The other process must then attach to the memory allocation using the
 * serialized descriptor. Attaching and allocating are the two means of initializing
 * a memory descriptor.
*/
typedef struct dragonMemoryDescr_st {
    int _original;
    dragonULInt _idx;
} dragonMemoryDescr_t;

/**
 * @brief This is the type of a serialied memory pool descriptor.
 *
 * It should be treated as binary data with the given length.
 */
typedef struct dragonMemoryPoolSerial_st {
    size_t len; /*!< The length of the serialized descriptor in bytes. */
    uint8_t * data; /* !<  The serialized descriptor data to be shared. */
} dragonMemoryPoolSerial_t;


/**
 * @brief A serialized memory descriptor.
 *
 * This is the type of binary data that may be shared with other processes
 * when a memory descriptor should be shared between processes.
*/
typedef struct dragonMemorySerial_st {
    size_t len; /*!< The length of the serialized descriptor in bytes. */
    uint8_t * data; /*!< The serialized descriptor data to be shared. */
} dragonMemorySerial_t;

/**
 * @brief A structure for getting all the memory allocations in a pool.
 *
 * When desired, a pool can be queried to return all the allocations
 * within the pool. You do this by calling
 * ``dragon_memory_pool_get_allocations`` or the type specific
 * ``dragon_memory_pool_get_type_allocations``.
*/
typedef struct dragonMemoryPoolAllocations_st {
    dragonULInt nallocs;
    dragonULInt * types;
    dragonULInt * ids;
} dragonMemoryPoolAllocations_t;

/**
 * @brief The number of free memory blocks with the memory size.
 *
 * dyn_mem stats provide statistics for the heap. This struture
 * is returned as an array for the free blocks of each
 * block size. The array starts at 0 and runs to the number of
 * block sizes - 1. Each item of the array contains the
 * items given here.
 *
*/
typedef struct dragonHeapStatsAllocationItem_st {
    size_t block_size;
    /*!< The size of the block for which this free blocks applies. */

    size_t num_blocks;
    /*!< The number of blocks of this size that are free. */

} dragonHeapStatsAllocationItem_t;

dragonError_t
dragon_memory_attr_init(dragonMemoryPoolAttr_t * attr);

dragonError_t
dragon_memory_attr_destroy(dragonMemoryPoolAttr_t * attr);

dragonError_t
dragon_memory_get_attr(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAttr_t * attr);

dragonError_t
dragon_memory_pool_create(dragonMemoryPoolDescr_t * pool_descr, size_t bytes, const char * base_name,
                          const dragonM_UID_t m_uid, const dragonMemoryPoolAttr_t * attr);

dragonError_t
dragon_memory_pool_destroy(dragonMemoryPoolDescr_t * pool_descr);

size_t
dragon_memory_pool_max_serialized_len();

dragonError_t
dragon_memory_pool_serialize(dragonMemoryPoolSerial_t * pool_ser, const dragonMemoryPoolDescr_t * pool_descr);

dragonError_t
dragon_memory_pool_serial_free(dragonMemoryPoolSerial_t * pool_ser);

dragonError_t
dragon_memory_pool_attach(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolSerial_t * pool_ser);

dragonError_t
dragon_memory_pool_attach_from_env(dragonMemoryPoolDescr_t * pool_descr, const char * env_var);

dragonError_t
dragon_memory_pool_attach_default(dragonMemoryPoolDescr_t* pool);

dragonError_t
dragon_memory_pool_detach(dragonMemoryPoolDescr_t * pool_descr);

dragonError_t
dragon_memory_pool_get_hostid(dragonMemoryPoolDescr_t * pool_descr, dragonULInt * hostid);

dragonError_t
dragon_memory_pool_runtime_is_local(dragonMemoryPoolDescr_t *pool_descr, bool *runtime_is_local);

dragonError_t
dragon_memory_pool_get_rt_uid(dragonMemoryPoolDescr_t *pool_descr, dragonULInt *rt_uid);

dragonError_t
dragon_memory_pool_get_uid_fname(const dragonMemoryPoolSerial_t * pool_ser, dragonULInt * uid_out, char ** fname_out);

bool
dragon_memory_pool_is_local(dragonMemoryPoolDescr_t * pool_descr);

dragonError_t
dragon_memory_pool_descr_clone(dragonMemoryPoolDescr_t * newpool_descr, const dragonMemoryPoolDescr_t * oldpool_descr);

dragonError_t
dragon_memory_pool_allocations_free(dragonMemoryPoolAllocations_t * allocs);

dragonError_t
dragon_memory_pool_allocation_exists(dragonMemoryDescr_t * mem_descr, int * flag);

dragonError_t
dragon_memory_get_alloc_memdescr(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr,
                                 const dragonULInt id, const dragonULInt offset, const dragonULInt* bytes_size);

dragonError_t
dragon_memory_pool_allocations_destroy(dragonMemoryPoolAllocations_t * allocs);

dragonError_t
dragon_memory_pool_get_allocations(const dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAllocations_t * allocs);

dragonError_t
dragon_memory_pool_get_type_allocations(const dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryAllocationType_t type,
                                        dragonMemoryPoolAllocations_t * allocs);

dragonError_t
dragon_memory_pool_muid(dragonMemoryPoolDescr_t* pool_descr, dragonULInt* muid);

dragonError_t
dragon_memory_pool_get_free_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* free_size);

dragonError_t
dragon_memory_pool_get_total_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* total_size);

dragonError_t
dragon_memory_pool_get_utilization_pct(dragonMemoryPoolDescr_t* pool_descr, double* utilization_pct);

dragonError_t
dragon_memory_pool_get_num_block_sizes(dragonMemoryPoolDescr_t* pool_descr, size_t* num_block_sizes);

dragonError_t
dragon_memory_pool_get_free_blocks(dragonMemoryPoolDescr_t* pool_descr, dragonHeapStatsAllocationItem_t * free_blocks);

dragonError_t
dragon_memory_pool_get_pointer(const dragonMemoryPoolDescr_t * pool_descr, void **base_ptr);

dragonError_t
dragon_memory_pool_get_size(const dragonMemoryPoolDescr_t * pool_descr, size_t *size);

dragonError_t
dragon_memory_alloc(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes);

dragonError_t
dragon_memory_alloc_blocking(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes, const timespec_t* timer);

dragonError_t
dragon_memory_alloc_type(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type);

dragonError_t
dragon_memory_alloc_type_blocking(dragonMemoryDescr_t * mem_descr, const dragonMemoryPoolDescr_t * pool_descr, const size_t bytes,
                         const dragonMemoryAllocationType_t type, const timespec_t* timer);

dragonError_t
dragon_memory_get_size(const dragonMemoryDescr_t * mem_descr, size_t * bytes);

dragonError_t
dragon_memory_get_pool(const dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr);

size_t
dragon_memory_max_serialized_len();

dragonError_t
dragon_memory_serialize(dragonMemorySerial_t * mem_ser, const dragonMemoryDescr_t * mem_descr);

dragonError_t
dragon_memory_attach(dragonMemoryDescr_t * mem_descr, const dragonMemorySerial_t * mem_ser);

dragonError_t
dragon_memory_detach(dragonMemoryDescr_t * mem_descr);

dragonError_t
dragon_memory_id(dragonMemoryDescr_t * mem_descr, uint64_t* id);

dragonError_t
dragon_memory_from_id(const dragonMemoryPoolDescr_t * pool_descr, uint64_t id, dragonMemoryDescr_t * mem_descr);

dragonError_t
dragon_memory_serial_free(dragonMemorySerial_t * mem_ser);

dragonError_t
dragon_memory_get_pointer(const dragonMemoryDescr_t * mem_descr, void ** ptr);

dragonError_t
dragon_memory_free(dragonMemoryDescr_t * mem_descr);

dragonError_t
dragon_memory_descr_clone(dragonMemoryDescr_t * newmem_descr, const dragonMemoryDescr_t * oldmem_descr,
                          ptrdiff_t offset, size_t * custom_length);

dragonError_t
dragon_memory_modify_size(dragonMemoryDescr_t * mem_descr, const size_t new_size, const timespec_t* timeout);

dragonError_t
dragon_memory_hash(dragonMemoryDescr_t* mem_descr, dragonULInt* hash_value);

dragonError_t
dragon_memory_equal(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result);

dragonError_t
dragon_memory_is(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result);

dragonError_t
dragon_memory_copy(dragonMemoryDescr_t* from_mem, dragonMemoryDescr_t* to_mem, dragonMemoryPoolDescr_t* to_pool, const timespec_t* timeout);

dragonError_t
dragon_memory_clear(dragonMemoryDescr_t* mem_descr, size_t start, size_t stop);

dragonError_t
dragon_create_process_local_pool(dragonMemoryPoolDescr_t* pool, size_t bytes, const char* name, dragonMemoryPoolAttr_t * attr, const timespec_t* timeout);

dragonError_t
dragon_register_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout);

dragonError_t
dragon_deregister_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout);

/* These are only for debug purposes. */
dragonError_t
dragon_memory_manifest_info(dragonMemoryDescr_t * mem_descr, dragonULInt* type, dragonULInt* type_id);

void
dragon_memory_set_debug_flag(int the_flag);

#ifdef __cplusplus
}
#endif

#endif
