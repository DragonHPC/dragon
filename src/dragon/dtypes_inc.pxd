from libc.stdint cimport uint64_t, uint32_t, uint8_t, uintptr_t
from libc.stdlib cimport malloc, free, calloc
from libcpp cimport bool
from posix.types cimport mode_t
from libc.string cimport memcpy
from posix.time cimport timespec
from libc.stddef cimport size_t
from libc.stdio cimport *
from cpython.mem cimport PyMem_Malloc, PyMem_Free
cimport cython
from dragon.return_codes cimport *

cdef extern from "object.h":
    cdef int PyBUF_READ
    cdef int PyBUF_WRITE

cdef extern from "<dragon/return_codes.h>":
    const char * dragon_get_rc_string(dragonError_t rc) nogil
    char * dragon_getlasterrstr() nogil

cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char*, Py_ssize_t, int)

cdef extern from "<dragon/global_types.h>":

    ctypedef uint64_t dragonC_UID_t
    ctypedef uint64_t dragonP_UID_t
    ctypedef uint64_t dragonM_UID_t
    ctypedef uint64_t dragonULInt
    ctypedef uint32_t dragonUInt
    ctypedef uint8_t dragonUUID[16]

    ctypedef enum dragonWaitMode_t:
        DRAGON_IDLE_WAIT,
        DRAGON_SPIN_WAIT,
        DRAGON_ADAPTIVE_WAIT

    ctypedef struct timespec_t:
        int tv_sec
        int tv_nsec

    const dragonWaitMode_t DRAGON_DEFAULT_WAIT_MODE

    ctypedef void (*dragonReleaseFun)(void* unlock_arg)

cdef extern from "<dragon/managed_memory.h>":

    const int DRAGON_MEMORY_TEMPORARY_TIMEOUT_SECS
    const timespec_t DRAGON_MEMORY_TEMPORARY_TIMEOUT

    ctypedef enum dragonMemoryPoolType_t:
        DRAGON_MEMORY_TYPE_SHM = 0,
        DRAGON_MEMORY_TYPE_FILE,
        DRAGON_MEMORY_TYPE_PRIVATE

    ctypedef enum dragonMemoryPoolGrowthType_t:
        DRAGON_MEMORY_GROWTH_NONE = 0,
        DRAGON_MEMORY_GROWTH_UNLIMITED,
        DRAGON_MEMORY_GROWTH_CAPPED

    ctypedef enum dragonMemoryAllocationType_t:
        DRAGON_MEMORY_ALLOC_DATA = 0,
        DRAGON_MEMORY_ALLOC_CHANNEL,
        DRAGON_MEMORY_ALLOC_CHANNEL_BUFFER,
        DRAGON_MEMORY_ALLOC_BOOTSTRAP

    ctypedef struct dragonMemoryDescr_t:
        pass

    ctypedef struct dragonMemorySerial_t:
        size_t len
        uint8_t * data

    ctypedef struct dragonMemoryPoolDescr_t:
        pass

    ctypedef struct dragonMemoryPoolSerial_t:
        size_t len
        uint8_t * data

    ctypedef struct dragonMemoryPoolAttr_t:
        size_t allocatable_data_size
        size_t total_data_size
        size_t free_space
        double utilization_pct
        size_t data_min_block_size
        size_t max_allocations
        size_t waiters_for_manifest
        size_t manifest_entries
        size_t max_manifest_entries
        size_t npre_allocs
        size_t * pre_allocs

    ctypedef struct dragonMemoryPoolAllocations_t:
        dragonULInt nallocs
        dragonULInt * types
        dragonULInt * ids

    # Pool actions
    dragonError_t dragon_memory_pool_create(dragonMemoryPoolDescr_t * pool_descr, size_t bytes,
                                                  const char * base_name, dragonM_UID_t m_uid,
                                                  dragonMemoryPoolAttr_t * const attr) nogil
    dragonError_t dragon_memory_pool_destroy(dragonMemoryPoolDescr_t * pool_descr) nogil
    dragonError_t dragon_memory_pool_attach(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolSerial_t * pool_ser) nogil
    dragonError_t dragon_memory_pool_attach_default(dragonMemoryPoolDescr_t* pool) nogil
    dragonError_t dragon_memory_pool_detach(dragonMemoryPoolDescr_t * pool_descr) nogil
    dragonError_t dragon_memory_pool_serialize(dragonMemoryPoolSerial_t * pool_ser, const dragonMemoryPoolDescr_t * pool_descr) nogil
    dragonError_t dragon_memory_pool_descr_clone(dragonMemoryPoolDescr_t * newpool_descr, const dragonMemoryPoolDescr_t * oldpool_descr) nogil
    dragonError_t dragon_memory_pool_serial_free(dragonMemoryPoolSerial_t * pool_ser) nogil
    dragonError_t dragon_memory_pool_get_allocations(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAllocations_t * allocs) nogil
    dragonError_t dragon_memory_pool_get_type_allocations(const dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryAllocationType_t type,
                                        dragonMemoryPoolAllocations_t * allocs) nogil
    dragonError_t dragon_memory_pool_allocation_exists(dragonMemoryDescr_t * mem_descr, int * flag) nogil
    dragonError_t dragon_memory_pool_allocations_destroy(dragonMemoryPoolAllocations_t * allocs) nogil
    dragonError_t dragon_memory_pool_get_uid_fname(const dragonMemoryPoolSerial_t * pool_ser, dragonULInt * uid_out, char ** fname) nogil
    dragonError_t dragon_memory_get_alloc_memdescr(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr,
                                                         uint64_t type_id, uint64_t offset, const dragonULInt* bytes_size) nogil
    dragonError_t dragon_memory_attr_init(dragonMemoryPoolAttr_t * attr) nogil
    dragonError_t dragon_memory_attr_destroy(dragonMemoryPoolAttr_t * attr) nogil
    dragonError_t dragon_memory_get_attr(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAttr_t * attr) nogil
    bool dragon_memory_pool_is_local(dragonMemoryPoolDescr_t * pool_descr) nogil
    dragonError_t dragon_memory_pool_muid(dragonMemoryPoolDescr_t* pool_descr, dragonULInt* muid) nogil
    dragonError_t dragon_memory_pool_get_free_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* free_size) nogil
    dragonError_t dragon_memory_pool_get_total_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* total_size) nogil
    dragonError_t dragon_memory_pool_get_rt_uid(dragonMemoryPoolDescr_t * pool_descr, dragonULInt *rt_uid) nogil
    dragonError_t dragon_memory_pool_muid(dragonMemoryPoolDescr_t* pool_descr, dragonULInt* muid) nogil
    dragonError_t dragon_memory_pool_get_free_size(dragonMemoryPoolDescr_t* pool_descr, uint64_t* free_size) nogil
    dragonError_t dragon_memory_pool_get_utilization_pct(dragonMemoryPoolDescr_t* pool_descr, double* utilization_pct) nogil
    dragonError_t dragon_memory_pool_get_num_block_sizes(dragonMemoryPoolDescr_t* pool_descr, size_t* num_block_sizes)
    dragonError_t dragon_memory_pool_get_free_blocks(dragonMemoryPoolDescr_t* pool_descr, dragonHeapStatsAllocationItem_t * free_blocks) nogil

    # Memory allocation actions
    dragonError_t dragon_memory_alloc(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes) nogil
    dragonError_t dragon_memory_alloc_blocking(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, timespec_t* timer) nogil
    dragonError_t dragon_memory_alloc_type(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, dragonMemoryAllocationType_t type) nogil
    dragonError_t dragon_memory_alloc_type_blocking(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, dragonMemoryAllocationType_t type, timespec_t* timer) nogil
    dragonError_t dragon_memory_free(dragonMemoryDescr_t * mem_descr) nogil
    dragonError_t dragon_memory_serialize(dragonMemorySerial_t * mem_ser, const dragonMemoryDescr_t * mem_descr) nogil
    dragonError_t dragon_memory_serial_free(dragonMemorySerial_t * mem_ser) nogil
    dragonError_t dragon_memory_attach(dragonMemoryDescr_t * mem_descr, dragonMemorySerial_t * mem_ser) nogil
    dragonError_t dragon_memory_detach(dragonMemoryDescr_t * mem_descr) nogil
    dragonError_t dragon_memory_id(dragonMemoryDescr_t * mem_descr, uint64_t* id) nogil
    dragonError_t dragon_memory_from_id(const dragonMemoryPoolDescr_t * pool_descr, uint64_t id, dragonMemoryDescr_t * mem_descr) nogil
    dragonError_t dragon_memory_get_pointer(dragonMemoryDescr_t * mem_descr, void ** ptr) nogil
    dragonError_t dragon_memory_get_size(dragonMemoryDescr_t * mem_descr, size_t * bytes) nogil
    dragonError_t dragon_memory_descr_clone(dragonMemoryDescr_t * newmem_descr, const dragonMemoryDescr_t * oldmem_descr, ptrdiff_t offset, size_t * custom_length) nogil
    dragonError_t dragon_memory_hash(dragonMemoryDescr_t* mem_descr, dragonULInt* hash_value) nogil
    dragonError_t dragon_memory_equal(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result) nogil
    dragonError_t dragon_memory_is(dragonMemoryDescr_t* mem_descr1, dragonMemoryDescr_t* mem_descr2, bool* result) nogil
    dragonError_t dragon_memory_copy(dragonMemoryDescr_t* from_mem, dragonMemoryDescr_t* to_mem, dragonMemoryPoolDescr_t* to_pool, const timespec_t* timeout)
    dragonError_t dragon_memory_get_pool(const dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr) nogil
    dragonError_t dragon_memory_clear(dragonMemoryDescr_t* mem_descr, size_t start, size_t stop) nogil

    # Memory utility functions
    dragonError_t dragon_create_process_local_pool(dragonMemoryPoolDescr_t* pool, size_t bytes, const char* name, dragonMemoryPoolAttr_t * attr, const timespec_t* timeout) nogil
    dragonError_t dragon_register_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout) nogil
    dragonError_t dragon_deregister_process_local_pool(dragonMemoryPoolDescr_t* pool, const timespec_t* timeout) nogil

    # Memory debug functions
    dragonError_t dragon_memory_manifest_info(dragonMemoryDescr_t * mem_descr, dragonULInt* type, dragonULInt* type_id)
    void dragon_memory_set_debug_flag(int the_flag)

cdef extern from "<dragon/channels.h>":

    ctypedef enum dragonChannelOFlag_t:
        DRAGON_CHANNEL_EXCLUSIVE,
        DRAGON_CHANNEL_NONEXCLUSIVE

    ctypedef enum dragonChannelFC_t:
        DRAGON_CHANNEL_FC_NONE,
        DRAGON_CHANNEL_FC_RESOURCES,
        DRAGON_CHANNEL_FC_MEMORY,
        DRAGON_CHANNEL_FC_MSGS

    ctypedef enum dragonChannelEvent_t:
        DRAGON_CHANNEL_POLLNOTHING,
        DRAGON_CHANNEL_POLLIN,
        DRAGON_CHANNEL_POLLOUT,
        DRAGON_CHANNEL_POLLINOUT,
        DRAGON_CHANNEL_POLLEMPTY,
        DRAGON_CHANNEL_POLLFULL,
        DRAGON_CHANNEL_POLLSIZE,
        DRAGON_CHANNEL_POLLRESET,
        DRAGON_CHANNEL_POLLBARRIER_WAIT,
        DRAGON_CHANNEL_POLLBARRIER_ABORT,
        DRAGON_CHANNEL_POLLBARRIER_RELEASE,
        DRAGON_CHANNEL_POLLBARRIER_ISBROKEN,
        DRAGON_CHANNEL_POLLBARRIER_WAITERS,
        DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS
        DRAGON_SEMAPHORE_P
        DRAGON_SEMAPHORE_V
        DRAGON_SEMAPHORE_VZ
        DRAGON_SEMAPHORE_PEEK

    ctypedef enum dragonChannelFlags_t:
        DRAGON_CHANNEL_FLAGS_NONE,
        DRAGON_CHANNEL_FLAGS_MASQUERADE_AS_REMOTE

    ctypedef enum dragonGatewayMessageKind_t:
        DRAGON_GATEWAY_MESSAGE_SEND,
        DRAGON_GATEWAY_MESSAGE_GET,
        DRAGON_GATEWAY_MESSAGE_EVENT

    ctypedef enum dragonChannelSendReturnWhen_t:
        DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY,
        DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED,
        DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED,
        DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED

    ctypedef enum dragonChannelRecvNotif_t:
        DRAGON_RECV_SYNC_SIGNAL,
        DRAGON_RECV_SYNC_MANUAL

    ctypedef struct dragonChannelAttr_t:
        dragonC_UID_t c_uid
        size_t bytes_per_msg_block
        size_t capacity
        dragonLockKind_t lock_type
        dragonChannelOFlag_t oflag
        dragonChannelFC_t fc_type
        dragonULInt flags
        dragonMemoryPoolDescr_t * buffer_pool
        size_t max_spinners
        size_t max_event_bcasts
        int blocked_receivers
        int blocked_senders
        size_t num_msgs
        size_t num_avail_blocks
        bool broken_barrier
        int barrier_count
        bool semaphore
        bool bounded
        dragonULInt initial_sem_value

    ctypedef struct dragonChannelDescr_t:
        uint64_t _idx

    ctypedef struct dragonMessageAttr_t:
        dragonULInt hints
        dragonULInt clientid
        dragonUUID sendhid

    ctypedef struct dragonMessage_t:
        dragonMessageAttr_t _attr
        dragonMemoryDescr_t * _mem_descr

    ctypedef struct dragonChannelSerial_t:
        size_t len
        uint8_t * data

    ctypedef struct dragonChannelSendh_t:
        uint8_t _opened
        dragonChannelDescr_t _ch
        dragonChannelDescr_t _gw

    ctypedef struct dragonChannelRecvh_t:
        uint8_t _opened
        dragonChannelDescr_t _ch
        dragonChannelDescr_t _gw

    ctypedef struct dragonChannelSendAttr_t:
        dragonUUID sendhid
        dragonChannelSendReturnWhen_t return_mode
        timespec_t default_timeout
        dragonWaitMode_t wait_mode

    ctypedef struct dragonChannelRecvAttr_t:
        dragonChannelRecvNotif_t default_notif_type
        timespec_t default_timeout
        int signal
        dragonWaitMode_t wait_mode

    ctypedef struct dragonGatewayMessage_t:
        dragonGatewayMessageKind_t msg_kind
        dragonULInt target_hostid
        dragonChannelSendReturnWhen_t send_return_mode
        uint8_t send_payload_cleanup_required
        dragonChannelSerial_t target_ch_ser
        timespec deadline
        dragonMessage_t send_payload_message
        dragonMemorySerial_t * send_dest_mem_descr_ser
        dragonMemorySerial_t * get_dest_mem_descr_ser
        short event_mask

    ctypedef struct dragonGatewayMessageSerial_t:
        size_t len
        uint8_t * data


    dragonMemoryDescr_t * DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP
    timespec_t DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
    timespec_t DRAGON_CHANNEL_TRYONCE_TIMEOUT

    # General channel functions
    dragonError_t dragon_channel_create(dragonChannelDescr_t * ch, const dragonC_UID_t c_uid,
                                               dragonMemoryPoolDescr_t * pool_descr, const dragonChannelAttr_t * attr) nogil
    dragonError_t dragon_channel_destroy(dragonChannelDescr_t * ch) nogil
    dragonError_t dragon_channel_serialize(const dragonChannelDescr_t * ch, dragonChannelSerial_t * ch_ser) nogil
    dragonError_t dragon_channel_serial_free(dragonChannelSerial_t * ch_ser) nogil
    dragonError_t dragon_channel_attach(const dragonChannelSerial_t * ch_ser, dragonChannelDescr_t * ch) nogil
    dragonError_t dragon_channel_detach(dragonChannelDescr_t * ch) nogil
    dragonError_t dragon_channel_get_pool(const dragonChannelDescr_t * ch, dragonMemoryPoolDescr_t * pool_descr) nogil
    bint dragon_channel_is_local(const dragonChannelDescr_t * ch) nogil
    dragonError_t dragon_channel_poll(const dragonChannelDescr_t * ch, dragonWaitMode_t wait_mode, const uint8_t event_mask, timespec_t * timeout, dragonULInt* result) nogil
    dragonError_t dragon_channel_get_uid(const dragonChannelSerial_t * ch_ser, dragonULInt * uid_out) nogil
    dragonError_t dragon_channel_pool_get_uid_fname(const dragonChannelSerial_t * ch_ser, dragonULInt * pool_uid_out, char ** pool_fname_out) nogil

    # Attributes
    dragonError_t dragon_channel_attr_init(dragonChannelAttr_t * attr) nogil
    dragonError_t dragon_channel_attr_destroy(dragonChannelAttr_t * attr) nogil
    dragonError_t dragon_channel_get_attr(const dragonChannelDescr_t * ch, dragonChannelAttr_t * attr) nogil

    # Message Attributes
    dragonError_t dragon_channel_message_attr_init(dragonMessageAttr_t* attr) nogil
    dragonError_t dragon_channel_message_attr_destroy(dragonMessageAttr_t* attr) nogil
    dragonError_t dragon_channel_message_getattr(const dragonMessage_t* msg, dragonMessageAttr_t* attr) nogil
    dragonError_t dragon_channel_message_setattr(dragonMessage_t* msg, const dragonMessageAttr_t* attr) nogil

    # Messages
    dragonError_t dragon_channel_message_init(dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr, const dragonMessageAttr_t * mattrs) nogil
    dragonError_t dragon_channel_message_get_mem(const dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr) nogil
    dragonError_t dragon_channel_message_destroy(dragonMessage_t * msg, const bint free_mem_descr) nogil
    dragonError_t dragon_channel_message_count(const dragonChannelDescr_t * ch, uint64_t * count) nogil
    dragonError_t dragon_channel_barrier_waiters(const dragonChannelDescr_t* ch, uint64_t* count) nogil
    dragonError_t dragon_channel_blocked_receivers(const dragonChannelDescr_t* ch, uint64_t* count) nogil

    bool dragon_channel_barrier_is_broken(const dragonChannelDescr_t* ch) nogil

    # Send handle
    dragonError_t dragon_channel_send_attr_init(dragonChannelSendAttr_t * send_attr) nogil
    dragonError_t dragon_channel_send_attr_destroy(dragonChannelSendAttr_t * send_attr) nogil
    dragonError_t dragon_channel_sendh(dragonChannelDescr_t * ch, dragonChannelSendh_t * ch_sh, const dragonChannelSendAttr_t * sattr) nogil
    dragonError_t dragon_chsend_open(dragonChannelSendh_t * ch_sh) nogil
    dragonError_t dragon_chsend_close(dragonChannelSendh_t * ch_sh) nogil
    dragonError_t dragon_chsend_send_msg(const dragonChannelSendh_t * ch_sh, const dragonMessage_t * msg_send, dragonMemoryDescr_t * dest_mem_descr, timespec_t* timeout) nogil

    # Receive handle
    dragonError_t dragon_channel_recv_attr_init(dragonChannelRecvAttr_t * recv_attr) nogil
    dragonError_t dragon_channel_recv_attr_destroy(dragonChannelRecvAttr_t * recv_attr) nogil
    dragonError_t dragon_channel_recvh(dragonChannelDescr_t * ch, dragonChannelRecvh_t * ch_rh, const dragonChannelRecvAttr_t * rattr) nogil
    dragonError_t dragon_chrecv_open(dragonChannelRecvh_t * ch_rh) nogil
    dragonError_t dragon_chrecv_close(dragonChannelRecvh_t * ch_rh) nogil
    dragonError_t dragon_chrecv_get_msg(const dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv) nogil
    dragonError_t dragon_chrecv_get_msg_blocking(const dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv, timespec_t * timeout) nogil

    # Gateways
    dragonError_t dragon_channel_register_gateways_from_env() nogil
    dragonError_t dragon_channel_discard_gateways() nogil

    # Gateway messages
    dragonError_t dragon_channel_gatewaymessage_attach(const dragonGatewayMessageSerial_t * gmsg_ser, dragonGatewayMessage_t * gmsg) nogil
    dragonError_t dragon_channel_gatewaymessage_transport_send_cmplt(dragonGatewayMessage_t * gmsg, const dragonError_t op_err) nogil
    dragonError_t dragon_channel_gatewaymessage_transport_get_cmplt(dragonGatewayMessage_t * gmsg, dragonMessage_t * msg_recv, const dragonError_t op_err) nogil
    dragonError_t dragon_channel_gatewaymessage_transport_event_cmplt(dragonGatewayMessage_t * gmsg, const dragonULInt event_result, const dragonError_t op_err) nogil
    dragonError_t dragon_channel_gatewaymessage_destroy(dragonGatewayMessage_t * gmsg) nogil

    # Utility functions
    void dragon_gatewaymessage_silence_timeouts() nogil
    dragonError_t dragon_create_process_local_channel(dragonChannelDescr_t* ch, uint64_t muid, uint64_t block_size, uint64_t capacity, const timespec_t* timeout) nogil
    dragonError_t dragon_destroy_process_local_channel(dragonChannelDescr_t* ch, const timespec_t* timeout) nogil

cdef extern from "<dragon/channelsets.h>":
    ctypedef struct dragonChannelSetAttrs_t:
        int num_allowed_spin_waiters
        dragonLockKind_t lock_type
        dragonSyncType_t sync_type

    ctypedef struct dragonChannelSetDescr_t:
        uint64_t _idx

    ctypedef struct dragonChannelSetEventNotification_t:
        int channel_idx
        short revent

    dragonError_t dragon_channelset_attr_init(dragonChannelSetAttrs_t* attrs) nogil
    dragonError_t dragon_channelset_create(dragonChannelDescr_t * descr_list[], int num_channels, const short event_mask, dragonMemoryPoolDescr_t * pool_descr, dragonChannelSetAttrs_t * attrs, dragonChannelSetDescr_t * chset_descr) nogil
    dragonError_t dragon_channelset_destroy(dragonChannelSetDescr_t * chset_descr) nogil
    dragonError_t dragon_channelset_poll(dragonChannelSetDescr_t * chset_descr, dragonWaitMode_t wait_mode, timespec_t * timeout, dragonReleaseFun release_fun, void* release_arg, dragonChannelSetEventNotification_t ** event) nogil

cdef extern from "<dragon/bcast.h>":
    ctypedef enum dragonSyncType_t:
        DRAGON_NO_SYNC,
        DRAGON_SYNC

from libc.stdint cimport uint64_t, uint32_t

cdef extern from "priority_heap.h":

    ctypedef uint64_t dragonPriorityHeapLongUint_t
    ctypedef uint32_t dragonPriorityHeapUint_t

    ctypedef struct dragonPriorityHeap_t:
        pass

    dragonError_t dragon_priority_heap_init(dragonPriorityHeap_t * heap,
                                                      dragonPriorityHeapUint_t base,
                                                      dragonPriorityHeapLongUint_t capacity,
                                                      dragonPriorityHeapUint_t nvals_per_key,
                                                      void * ptr)
    dragonError_t dragon_priority_heap_attach(dragonPriorityHeap_t * heap, void * ptr)
    dragonError_t dragon_priority_heap_detach(dragonPriorityHeap_t * heap)
    dragonError_t dragon_priority_heap_destroy(dragonPriorityHeap_t * heap)
    dragonError_t dragon_priority_heap_insert_item(dragonPriorityHeap_t * heap,
                                                             dragonPriorityHeapLongUint_t * vals)
    dragonError_t dragon_priority_heap_insert_urgent_item(dragonPriorityHeap_t * heap,
                                                                    dragonPriorityHeapLongUint_t * vals)
    dragonError_t dragon_priority_heap_extract_highest_priority(dragonPriorityHeap_t * heap,
                                                                          dragonPriorityHeapLongUint_t * vals,
                                                                          dragonPriorityHeapLongUint_t * priority)
    size_t dragon_priority_heap_size(dragonPriorityHeapLongUint_t capacity,
                                     dragonPriorityHeapUint_t nvals_per_key)


cdef extern from "_bitset.h":

    # Use enums for numeric macro defines so Cython knows to treat them as compile-time constants
    cdef enum:
        TRUE
        FALSE

    ctypedef struct dragonBitSet_t:
        size_t size
        char * data

    size_t dragon_bitset_size(size_t num_bits)
    dragonError_t dragon_bitset_get_num_bits(const dragonBitSet_t* set, size_t* num_bits)
    dragonError_t dragon_bitset_init(void* ptr, dragonBitSet_t* set, const size_t num_bits)
    dragonError_t dragon_bitset_destroy(dragonBitSet_t* set)
    dragonError_t dragon_bitset_attach(void* ptr, dragonBitSet_t* set)
    dragonError_t dragon_bitset_detach(dragonBitSet_t* set)
    dragonError_t dragon_bitset_set(dragonBitSet_t* set, const size_t val_index)
    dragonError_t dragon_bitset_reset(dragonBitSet_t* set, const size_t val_index)
    dragonError_t dragon_bitset_get(const dragonBitSet_t* set, const size_t val_index, bool* val)
    dragonError_t dragon_bitset_length(const dragonBitSet_t* set, size_t* length)
    dragonError_t dragon_bitset_zeroes_to_right(const dragonBitSet_t* set, const size_t val_index, size_t* val)
    dragonError_t dragon_bitset_first(const dragonBitSet_t* set, size_t* first)
    dragonError_t dragon_bitset_next(const dragonBitSet_t* set, const size_t current, size_t* next)
    dragonError_t dragon_bitset_dump(const char* title, const dragonBitSet_t* set, const char* indent)
    dragonError_t dragon_bitset_dump_to_fd(FILE* fd, const char* title, const dragonBitSet_t* set, const char* indent)

cdef extern from "_heap_manager.h":

    # Use enums for numeric macro defines so Cython knows to treat them as compile-time constants
    cdef enum:
        BLOCK_SIZE_MAX_POWER
        BLOCK_SIZE_MIN_POWER

    ctypedef struct dragonHeapStatsAllocationItem_t:
        size_t block_size
        size_t num_blocks

    ctypedef struct dragonHeapStats_t:
        uint64_t num_segments
        uint64_t segment_size
        uint64_t total_size
        uint64_t total_free_space
        double utilization_pct
        size_t num_block_sizes
        dragonHeapStatsAllocationItem_t free_blocks[BLOCK_SIZE_MAX_POWER - BLOCK_SIZE_MIN_POWER + 1]

    ctypedef void dragonDynHeapSegment_t

    ctypedef struct dragonDynHeap_t:
        uint64_t * base_pointer
        uint64_t * num_waiting
        uint64_t segment_size
        uint64_t num_segments
        uint64_t total_size
        uint64_t num_block_sizes
        uint64_t biggest_block
        dragonBitSet_t* free
        dragonBitSet_t preallocated

    dragonError_t dragon_heap_size(const size_t max_block_size_power, const size_t min_block_size_power, const dragonLockKind_t lock_kind, size_t* size)
    dragonError_t dragon_heap_init(void* ptr, dragonDynHeap_t* heap, const size_t max_block_size_power,  const size_t min_block_size_power, const dragonLockKind_t lock_kind, const size_t* preallocated)
    dragonError_t dragon_heap_attach(void* ptr, dragonDynHeap_t* heap)
    dragonError_t dragon_heap_destroy(dragonDynHeap_t * heap)
    dragonError_t dragon_heap_detach(dragonDynHeap_t * heap)
    dragonError_t dragon_heap_malloc(dragonDynHeap_t* heap, const size_t size, void** offset)
    dragonError_t dragon_heap_malloc_blocking(dragonDynHeap_t* heap, const size_t size, void** offset, const timespec_t * timer)
    dragonError_t dragon_heap_free(dragonDynHeap_t* heap, void* offset, size_t size)
    dragonError_t dragon_heap_get_stats(dragonDynHeap_t* heap, dragonHeapStats_t* data)
    dragonError_t dragon_heap_dump(const char* title, dragonDynHeap_t* heap)
    dragonError_t dragon_heap_dump_to_fd(FILE* fd, const char* title, dragonDynHeap_t* heap)

cdef extern from "<dragon/utils.h>":
    dragonULInt dragon_get_local_rt_uid()
    dragonError_t dragon_set_procname(char * name)
    char * dragon_base64_encode(uint8_t *data, size_t input_length)
    uint8_t * dragon_base64_decode(const char *data, size_t *output_length)
    dragonError_t dragon_timespec_deadline(timespec_t * timer, timespec_t * deadline)
    dragonError_t dragon_timespec_remaining(timespec_t * end_time, timespec_t * remaining_timeout)
    dragonULInt dragon_hash(void* ptr, size_t num_bytes)
    bool dragon_bytes_equal(void* ptr1, void* ptr2, size_t ptr1_numbytes, size_t ptr2_numbytes)
    dragonError_t dragon_ls_set_kv(const unsigned char* key, const unsigned char* value, const timespec_t* timeout) nogil
    dragonError_t dragon_ls_get_kv(const unsigned char* key, char** value, const timespec_t* timeout) nogil
    dragonError_t dragon_get_hugepage_mount(char **mount_dir) nogil

cdef extern from "<string.h>":
    size_t strlen(const char *s)

cdef extern from "logging.h":

    ctypedef struct dragonLoggingDescr_t:
        pass

    ctypedef enum dragonLoggingMode_t:
        DRAGON_LOGGING_LOSSLESS
        DRAGON_LOGGING_FIRST
        DRAGON_LOGGING_LAST

    ctypedef struct dragonLoggingSerial_t:
        size_t len
        uint8_t * data

    ctypedef struct dragonLoggingAttr_t:
        dragonChannelAttr_t ch_attr
        dragonLoggingMode_t mode

    # These should match logging.h and python's logging module
    ctypedef enum dragonLogPriority_t:
        DG_NOTSET = 0,
        DG_DEBUG = 10,
        DG_INFO = 20,
        DG_WARNING = 30,
        DG_CRITICAL = 40,
        DG_ERROR = 50

    dragonError_t dragon_logging_init(const dragonMemoryPoolDescr_t * mpool, const dragonC_UID_t l_uid, dragonLoggingAttr_t * lattrs, dragonLoggingDescr_t * logger) nogil
    dragonError_t dragon_logging_destroy(dragonLoggingDescr_t * logger, bool destroy_pool) nogil
    dragonError_t dragon_logging_serialize(const dragonLoggingDescr_t * logger, dragonLoggingSerial_t * log_ser) nogil
    dragonError_t dragon_logging_serial_free(dragonLoggingSerial_t * log_ser) nogil
    dragonError_t dragon_logging_attach(const dragonLoggingSerial_t * log_ser, dragonLoggingDescr_t * logger, dragonMemoryPoolDescr_t * mpool) nogil
    dragonError_t dragon_logging_attr_init(dragonLoggingAttr_t * lattr) nogil
    dragonError_t dragon_logging_put(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, char * msg) nogil
    dragonError_t dragon_logging_get(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, void ** msg_out, timespec_t * timeout) nogil
    dragonError_t dragon_logging_get_str(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, char ** out_str, timespec_t * timeout) nogil
    dragonError_t dragon_logging_print(const dragonLoggingDescr_t * logger, dragonLogPriority_t priority, timespec_t * timeout) nogil
    dragonError_t dragon_logging_count(const dragonLoggingDescr_t * logger, uint64_t * count) nogil

cdef extern from "<stdatomic.h>":
    ctypedef int atomic_uint_fast64_t

cdef extern from "<dragon/shared_lock.h>":

    ctypedef atomic_uint_fast64_t dragonLockType_t

    ctypedef enum dragonLockKind_t:
        DRAGON_LOCK_FIFO
        DRAGON_LOCK_FIFO_LITE
        DRAGON_LOCK_GREEDY

    ctypedef struct dragonGreedyLock_t:
        pass

    ctypedef struct dragonFIFOLock_t:
        pass

    ctypedef struct dragonFIFOLiteLock_t:
        pass

    ctypedef struct dragonLock_u:
        pass

    ctypedef struct dragonLock_t:
        dragonLockKind_t kind
        dragonLock_u ptr

    size_t dragon_lock_size(dragonLockKind_t kind)
    dragonError_t dragon_lock_init(dragonLock_t * lock, void * ptr, dragonLockKind_t lock_kind)
    dragonError_t dragon_lock_attach(dragonLock_t * lock, void * ptr)
    dragonError_t dragon_lock_detach(dragonLock_t * lock)
    dragonError_t dragon_lock_destroy(dragonLock_t * lock)
    dragonError_t dragon_lock(dragonLock_t * lock) nogil
    dragonError_t dragon_try_lock(dragonLock_t * lock, int * locked) nogil
    dragonError_t dragon_unlock(dragonLock_t * lock) nogil

    dragonError_t dragon_fifo_lock_init(dragonFIFOLock_t * dlock, void * ptr)
    dragonError_t dragon_fifo_lock_attach(dragonFIFOLock_t * dlock, void * ptr)
    dragonError_t dragon_fifo_lock_detach(dragonFIFOLock_t * dlock)
    dragonError_t dragon_fifo_lock_destroy(dragonFIFOLock_t * dlock)
    dragonError_t dragon_fifo_lock(dragonFIFOLock_t * dlock) nogil
    dragonError_t dragon_fifo_try_lock(dragonFIFOLock_t * dlock, int * locked) nogil
    dragonError_t dragon_fifo_unlock(dragonFIFOLock_t * dlock) nogil

    dragonError_t dragon_fifolite_lock_init(dragonFIFOLiteLock_t * dlock, void * ptr)
    dragonError_t dragon_fifolite_lock_attach(dragonFIFOLiteLock_t * dlock, void * ptr)
    dragonError_t dragon_fifolite_lock_detach(dragonFIFOLiteLock_t * dlock)
    dragonError_t dragon_fifolite_lock_destroy(dragonFIFOLiteLock_t * dlock)
    dragonError_t dragon_fifolite_lock(dragonFIFOLiteLock_t * dlock) nogil
    dragonError_t dragon_fifolite_try_lock(dragonFIFOLiteLock_t * dlock, int * locked) nogil
    dragonError_t dragon_fifolite_unlock(dragonFIFOLiteLock_t * dlock) nogil

    dragonError_t dragon_greedy_lock_init(dragonGreedyLock_t * dlock, void * ptr)
    dragonError_t dragon_greedy_lock_attach(dragonGreedyLock_t * dlock, void * ptr)
    dragonError_t dragon_greedy_lock_detach(dragonGreedyLock_t * dlock)
    dragonError_t dragon_greedy_lock_destroy(dragonGreedyLock_t * dlock)
    dragonError_t dragon_greedy_lock(dragonGreedyLock_t * dlock) nogil
    dragonError_t dragon_greedy_try_lock(dragonGreedyLock_t * dlock, int * locked) nogil
    dragonError_t dragon_greedy_unlock(dragonGreedyLock_t * dlock) nogil

cdef extern from "hostid.h":
    dragonULInt dragon_host_id()
    dragonError_t dragon_set_host_id(dragonULInt id)
    dragonULInt dragon_host_id_from_k8s_uuid(char *pod_uid)

cdef extern from "dragon/pmod.h":

    const int PMOD_MAX_HOSTNAME_LEN

    ctypedef struct dragonHostname_t:
        char name[PMOD_MAX_HOSTNAME_LEN]

    ctypedef struct dragonSendJobParams_t:
        int lrank
        int ppn
        int nid
        int nnodes
        int nranks
        int *nidlist
        dragonHostname_t *hostnames
        uint64_t id

    dragonError_t dragon_pmod_send_mpi_data(dragonSendJobParams_t *job_params, dragonChannelDescr_t *child_ch)
    dragonError_t dragon_pmod_pals_get_num_nics(int *nnics)

cdef extern from "dragon/perf.h":

    ctypedef enum dragonChPerfOpcode_t:
        DRAGON_PERF_OPCODE_SEND_MSG = 0
        DRAGON_PERF_OPCODE_GET_MSG
        DRAGON_PERF_OPCODE_PEEK
        DRAGON_PERF_OPCODE_POP
        DRAGON_PERF_OPCODE_POLL
        DRAGON_PERF_OPCODE_LAST

    dragonError_t dragon_chperf_session_new(dragonChannelSerial_t *sdesc_array, int num_channels)
    dragonError_t dragon_chperf_session_cleanup()
    dragonError_t dragon_chperf_kernel_new(int kernel_idx, int ch_idx, int num_procs)
    dragonError_t dragon_chperf_kernel_append_op(int kernel_idx, dragonChPerfOpcode_t op_code, int dst_ch_idx, size_t size_in_bytes, double timeout_in_sec)
    dragonError_t dragon_chperf_kernel_run(int kernel_idx, double *run_time)

cdef extern from "dragon/fli.h":

    ctypedef struct dragonFLIAttr_t:
        pass

    ctypedef struct dragonFLIDescr_t:
        pass

    ctypedef struct dragonFLISerial_t:
        size_t len
        uint8_t * data

    ctypedef struct dragonFLISendHandleDescr_t:
        pass

    ctypedef struct dragonFLIRecvHandleDescr_t:
        pass

    dragonChannelDescr_t* STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION
    dragonChannelDescr_t* STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND

    dragonError_t dragon_fli_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                                    dragonChannelDescr_t* mgr_ch, dragonMemoryPoolDescr_t* pool,
                                    const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                                    const bool user_buffered_protocol, dragonFLIAttr_t* attrs) nogil
    dragonError_t dragon_fli_destroy(dragonFLIDescr_t* adapter) nogil
    dragonError_t dragon_fli_serialize(const dragonFLIDescr_t* adapter, dragonFLISerial_t* serial) nogil
    dragonError_t dragon_fli_serial_free(dragonFLISerial_t* serial) nogil
    dragonError_t dragon_fli_attach(const dragonFLISerial_t* serial, const dragonMemoryPoolDescr_t* pool,
                                    dragonFLIDescr_t* adapter) nogil
    dragonError_t dragon_fli_detach(dragonFLIDescr_t* adapter) nogil
    dragonError_t dragon_fli_get_available_streams(dragonFLIDescr_t* adapter, uint64_t* num_streams, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_is_buffered(const dragonFLIDescr_t* adapter, bool* is_buffered) nogil
    dragonError_t dragon_fli_open_send_handle(const dragonFLIDescr_t* adapter, dragonFLISendHandleDescr_t* send_handle,
                                              dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool, bool allow_strm_term, bool turbo_mode, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_close_send_handle(dragonFLISendHandleDescr_t* send_handle,
                                               const timespec_t* timeout) nogil
    dragonError_t dragon_fli_open_recv_handle(const dragonFLIDescr_t* adapter, dragonFLIRecvHandleDescr_t* recv_handle,
                                              dragonChannelDescr_t* strm_ch, dragonMemoryPoolDescr_t* dest_pool, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_close_recv_handle(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_set_free_flag(dragonFLIRecvHandleDescr_t* recv_handle)
    dragonError_t dragon_fli_reset_free_flag(dragonFLIRecvHandleDescr_t* recv_handle)
    dragonError_t dragon_fli_stream_received(dragonFLIRecvHandleDescr_t* recv_handle, bool* stream_received)
    dragonError_t dragon_fli_create_writable_fd(dragonFLISendHandleDescr_t* send_handle, int* fd_ptr, const bool buffer,
                            size_t chunk_size, const uint64_t arg, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_finalize_writable_fd(dragonFLISendHandleDescr_t* send_handle) nogil
    dragonError_t dragon_fli_create_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle, int* fd_ptr,
                            const timespec_t* timeout) nogil
    dragonError_t dragon_fli_finalize_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle) nogil
    dragonError_t dragon_fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                                        uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_send_mem(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem,
                    uint64_t arg, bool transfer_ownership, bool no_copy_read_only, const timespec_t* timeout) nogil
    dragonError_t dragon_fli_recv_bytes(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                                        size_t* received_size, uint8_t** bytes, uint64_t* arg,
                                        const timespec_t* timeout) nogil
    dragonError_t dragon_fli_recv_bytes_into(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                                            size_t* received_size, uint8_t* bytes, uint64_t* arg,
                                            const timespec_t* timeout) nogil
    dragonError_t dragon_fli_recv_mem(dragonFLIRecvHandleDescr_t* recv_handle, dragonMemoryDescr_t* mem,
                                      uint64_t* arg, const timespec_t* timeout) nogil
