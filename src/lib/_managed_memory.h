#ifndef HAVE_DRAGON_MEMORY_INTERNAL_H
#define HAVE_DRAGON_MEMORY_INTERNAL_H

#include "_heap_manager.h"
#include "_blocks.h"
#include "err.h"
#include "umap.h"
#include <dragon/managed_memory.h>


#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_MEMORY_DEFAULT_MIN_BLK_SIZE 32
#define DRAGON_MEMORY_DEFAULT_SEG_SIZE 134217728
#define DRAGON_MEMORY_DEFAULT_MAX_SIZE 4294967296
#define DRAGON_CONCURRENT_MEM_ALLOCATIONS_THRESHOLD 2097152
#define DRAGON_MEMORY_DEFAULT_GROWTH_TYPE DRAGON_MEMORY_GROWTH_NONE
#define DRAGON_MEMORY_DEFAULT_EXE DRAGON_MEMORY_NOT_EXECUTABLE
#define DRAGON_MEMORY_DEFAULT_MEM_TYPE DRAGON_MEMORY_TYPE_SHM
#define DRAGON_MEMORY_DEFAULT_LOCK_TYPE DRAGON_LOCK_FIFO_LITE
#define DRAGON_MEMORY_DEFAULT_MODE 0600
#define DRAGON_MEMORY_DEFAULT_FILE_PREFIX "_dragon_"
#define DRAGON_MEMORY_MAX_FILE_NAME_LENGTH 256
#define DRAGON_MEMORY_MAX_ERRSTR_REC_LEN 4096
#define DRAGON_MEMORY_DATA_IDX_MEM_ORDER memory_order_acq_rel
#define DRAGON_MEMORY_MANIFEST_SPIN_WAITERS 132

/*
  Minimum size to create a pool (32KB).  Arbitrarily chosen, can be modified at a later date.
 */
#define DRAGON_MEMORY_POOL_DEFAULT_SIZE 32768

#define DRAGON_MEMORY_POOL_UMAP_SEED 7
#define DRAGON_MEMORY_MEM_UMAP_SEED 9

#define DRAGON_MEMORY_POOLSER_NULINTS 5
#define DRAGON_MEMORY_MEMSER_NULINTS 3
#define DRAGON_MEMORY_POOL_MAX_SERIALIZED_LEN (DRAGON_MEMORY_MAX_FILE_NAME_LENGTH + (sizeof(dragonULInt) * DRAGON_MEMORY_POOLSER_NULINTS))
#define DRAGON_MEMORY_MAX_SERIALIZED_LEN (DRAGON_MEMORY_POOL_MAX_SERIALIZED_LEN + (DRAGON_MEMORY_MEMSER_NULINTS * (sizeof(dragonULInt))))


/* these attributes are embedded into the manifest file
   for later discoverability.
*/
typedef struct dragonMemoryPoolHeader_st {
    dragonM_UID_t * m_uid;
    dragonULInt * hostid;
    dragonULInt * allocatable_data_size; //< Size of the allocatable space in the original pool (no segments).
    dragonULInt * total_data_size; //< Total size of the original data for the pool.
    dragonULInt * data_min_block_size;
    dragonULInt * manifest_allocated_size;
    dragonULInt * manifest_heap_size;
    dragonULInt * manifest_table_size;
    dragonULInt * segment_size;
    dragonULInt * max_size;
    dragonULInt * n_segments;
    dragonULInt * mem_type;
    dragonULInt * lock_type;
    dragonULInt * growth_type;
    dragonUInt  * mode;
    dragonULInt * npre_allocs;
    void * manifest_bcast_space;
    void * heap;
    dragonULInt * pre_allocs;
    char * filenames;
    void * manifest_table;
} dragonMemoryPoolHeader_t;

/* This is for remote memory pool's only. The fields
   recorded here are needed in place of the
   dragonMemoryPoolHeader_t fields above since a
   remote pool does not have a shared header like
   a local pool. These fields are needed to
   be able to serialize a remote memory pool */
typedef struct dragonRemoteMemoryPoolInfo_st {
    dragonULInt hostid;
    dragonULInt rt_uid;
    dragonULInt m_uid;
    dragonULInt mem_type;
    dragonULInt manifest_len;
} dragonRemoteMemoryPoolInfo_t;

typedef struct dragonMemoryPoolHeap_st {
    uint32_t nmgrs; // Number of heap managers
    void ** mgrs_dptrs; // Array of heap manager datapointers
    dragonDynHeap_t * mgrs; // Array of data heap manager handles
    dragonBlocks_t mfstmgr; // Manifest blocks manager handle
} dragonMemoryPoolHeap_t;

typedef struct dragonMemoryPool_st {
    int dfd; // Data file descriptor
    int mfd; // Manifest file descriptor
    size_t data_requested_size;
    size_t manifest_requested_size; // the max number of manifest records
    bool runtime_is_local;
    void * local_dptr; // Data blob pointer, if == NULL then this pool is non-local.
    void * mptr; // Manifest blob pointer
    dragonMemoryPoolHeap_t heap;
    dragonMemoryPoolHeader_t header;
    dragonLock_t mlock; // Memory lock
    dragonBCastDescr_t manifest_bcast;
    char * mname; // Manifest filename
    atomic_int_fast64_t ref_cnt;
    /* the following are only valid for non-local pools */
    dragonRemoteMemoryPoolInfo_t remote;
    size_t num_blocks; // the number of minimum sized blocks in the pool
    size_t min_block_size; // the minimum block size
} dragonMemoryPool_t;

/*
   The manifest is a file/shared mem that contains a references to a list of heap managers.
   Right now there is one heap manager in the list, but if memory pools support
   is expanded to allow memory pools to grow, they will grow by adding
   additional heap managers which are mapped onto their own shared memory. In
   this way we have a manifest that keeps track of a group of heap managers.
*/

typedef struct dragonMemoryManifestRec_st {
    dragonULInt id; // Arbitrary ID from the user to make it unique
    dragonULInt type; // How it was allocated
    dragonULInt offset; // Offset of the data memory blob from the pool dptr
    dragonULInt size; // Size of the allocation
} dragonMemoryManifestRec_t;

typedef struct dragonMemory_st {
    size_t bytes; // requested/available size
    size_t offset; // When cloned, this may be non-zero and represents a sub-allocation.
                   // It is the offset from the beginning of the original allocation.
    void * local_dptr; // Pointer to data blob, if == NULL then this memory is non-local.
    dragonMemoryPoolDescr_t pool_descr;
    dragonMemoryManifestRec_t mfst_record;
    // TODO, add a lock related member for serializing operations on the pool across threads
} dragonMemory_t;

#ifdef __cplusplus
}
#endif

#endif
