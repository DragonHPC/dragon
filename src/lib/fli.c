#include <dragon/fli.h>
#include "_fli.h"
#include "_utils.h"
#include "err.h"
#include "umap.h"

#include <stdbool.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

/* dragon globals */
DRAGON_GLOBAL_MAP(fli_adapters);
DRAGON_GLOBAL_MAP(fli_send_handles);
DRAGON_GLOBAL_MAP(fli_recv_handles);

#define DEFAULT_CHUNK_SIZE 1024

/* Used in the File Descriptor Sender Thread */
typedef struct _SenderArg_st {
    dragonFLISendHandleDescr_t* sendh;
    int fd;
    uint64_t user_arg;
    size_t chunk_size;
    bool buffer;
} _SenderArg_t;

/* Used in the File Descriptor Receiver Thread */
typedef struct _ReceiverArg_st {
    dragonFLIRecvHandleDescr_t* recvh;
    int fd;
} _ReceiverArg_t;

/* obtain an fli structure from a given adapter descriptor */
static dragonError_t _fli_from_descr(const dragonFLIDescr_t* adapter, dragonFLI_t** fli) {
    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_adapters, adapter->_idx, (void*)fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli adapters map");

    no_err_return(DRAGON_SUCCESS);
}

/* obtain an fli structure from a given send handle descriptor */
static dragonError_t _fli_sendh_from_descr(const dragonFLISendHandleDescr_t* send_descr, dragonFLISendHandle_t** send_handle) {
    if (send_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_send_handles, send_descr->_idx, (void*)send_handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli send handles map");

    no_err_return(DRAGON_SUCCESS);
}

/* obtain an fli structure from a given recv handle descriptor */
static dragonError_t _fli_recvh_from_descr(const dragonFLIRecvHandleDescr_t* recv_descr, dragonFLIRecvHandle_t** recv_handle) {
    if (recv_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli recv handle descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_recv_handles, recv_descr->_idx, (void*)recv_handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli recv handles map");

    no_err_return(DRAGON_SUCCESS);
}

/* insert an fli structure into the unordered map using the adapter->_idx as the key */
static dragonError_t _add_umap_fli_entry(dragonFLIDescr_t* adapter, const dragonFLI_t* fli) {
    dragonError_t err;

    /* register this channel in our umap */
    if (*dg_fli_adapters == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        *dg_fli_adapters = malloc(sizeof(dragonMap_t));
        if (*dg_fli_adapters == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate umap for fli adapters");

        err = dragon_umap_create(dg_fli_adapters, DRAGON_FLI_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for fli adapters");
    }

    err = dragon_umap_additem_genkey(dg_fli_adapters, (void*)fli, &adapter->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into fli adapters umap");

    no_err_return(DRAGON_SUCCESS);
}

/* insert an fli send handle structure into the unordered map using the send_descr->_idx as the key */
static dragonError_t _add_umap_fli_sendh_entry(dragonFLISendHandleDescr_t* send_descr, const dragonFLISendHandle_t* send_handle) {
    dragonError_t err;

    /* register this channel in our umap */
    if (*dg_fli_send_handles == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        *dg_fli_send_handles = malloc(sizeof(dragonMap_t));
        if (*dg_fli_send_handles == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate umap for fli send handles");

        err = dragon_umap_create(dg_fli_send_handles, DRAGON_FLI_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for fli send handles");
    }

    err = dragon_umap_additem_genkey(dg_fli_send_handles, (void*)send_handle, &send_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into fli send handles umap");

    no_err_return(DRAGON_SUCCESS);
}

/* insert an fli recv handle structure into the unordered map using the recv_descr->_idx as the key */
static dragonError_t _add_umap_fli_recvh_entry(dragonFLIRecvHandleDescr_t* recv_descr, const dragonFLIRecvHandle_t* recv_handle) {
    dragonError_t err;

    /* register this channel in our umap */
    if (*dg_fli_recv_handles == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        *dg_fli_recv_handles = malloc(sizeof(dragonMap_t));
        if (*dg_fli_recv_handles == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate umap for fli recv handles");

        err = dragon_umap_create(dg_fli_recv_handles, DRAGON_FLI_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for fli recv handles");
    }

    err = dragon_umap_additem_genkey(dg_fli_recv_handles, (void*)recv_handle, &recv_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into fli recv handles umap");

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t _validate_attr(const dragonFLIAttr_t* attr) {
    return DRAGON_NOT_IMPLEMENTED;
}

static dragonError_t _send_mem(dragonChannelSendh_t* sendh, dragonMemoryDescr_t* mem, uint64_t arg,
                        bool transfer_ownership, bool no_copy_read_only, bool turbo_mode, bool flush,
                        dragonMemoryPoolDescr_t* dest_pool, timespec_t* deadline) {

    dragonError_t err;
    timespec_t remaining_time;
    timespec_t* timeout = NULL;
    dragonMessage_t msg;
    dragonMessageAttr_t msg_attrs;
    dragonMemoryDescr_t* dest = NULL;
    dragonChannelSendAttr_t send_attrs;
    // This is reset below when needed, but inited here to avoid compiler warning.
    dragonChannelSendReturnWhen_t orig_return_mode = DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY;
    size_t num_bytes;

    if (sendh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a channel send handle to send a message.");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a memory descriptor.");

    err = dragon_memory_get_size(mem, &num_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get memory size.");

    /* No bytes to send, so return. */
    if (num_bytes == 0)
        no_err_return(DRAGON_SUCCESS);

    if (deadline != NULL) {
        timeout = &remaining_time;
        err = dragon_timespec_remaining(deadline, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute remaining time");
    }

    err = dragon_channel_message_attr_init(&msg_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to init message attr structure.");

    msg_attrs.hints = arg;
    msg_attrs.send_transfer_ownership = transfer_ownership;
    msg_attrs.no_copy_read_only = no_copy_read_only;

    if (transfer_ownership && turbo_mode) {
        /* We don't need to wait for confirmation */
        err = dragon_chsend_get_attr(sendh, &send_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get send handle attributes.");

        orig_return_mode = send_attrs.return_mode;

        send_attrs.return_mode = DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY;

        err = dragon_chsend_set_attr(sendh, &send_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not set send handle attributes.");
    }

    if (flush) {
        /* We wait until it is deposited. We don't need to keep the original
           return mode here because the stream/send handle is being closed and
           flush is only set to true on the final send on the send handle. */
        err = dragon_chsend_get_attr(sendh, &send_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get send handle attributes.");

        send_attrs.return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED;

        err = dragon_chsend_set_attr(sendh, &send_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not set send handle attributes.");
    }

    if (dest_pool != NULL) {
        /* This won't block. A zero-byte allocation can be used to direct channels lib
        to put the message into the given pool. */
        dest = malloc(sizeof(dragonMemoryDescr_t));
        if (dest == NULL)
            append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate a memory descriptor");

        err = dragon_memory_alloc_blocking(dest, dest_pool, 0, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get zero-byte allocation.");
    }

    err = dragon_channel_message_init(&msg, mem, &msg_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize serialized stream channel message.");

    err = dragon_chsend_send_msg(sendh, &msg, dest, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not add serialized stream channel to manager channel.");

    if (transfer_ownership && turbo_mode) {
        /* We now restore original mode */
        send_attrs.return_mode = orig_return_mode;

        err = dragon_chsend_set_attr(sendh, &send_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not restore send handle attributes.");
    }

    if (dest != NULL) {
        err = dragon_memory_free(dest);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free zero-byte allocation.");

        free(dest);
        dest = NULL;
    }

    /* If transferring ownership, then don't destroy underlying memory. If not transferring
       ownership, then the sender owns the memory and also don't destroy it. */
    err = dragon_channel_message_destroy(&msg, false);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy the message.");

    err = dragon_channel_message_attr_destroy(&msg_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy message attributes.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_bytes(dragonChannelSendh_t* chan_sendh, dragonMemoryPoolDescr_t* pool, uint8_t* bytes,
                        size_t num_bytes, uint64_t arg, bool turbo_mode, bool flush, dragonMemoryPoolDescr_t* dest_pool, timespec_t* deadline) {
    dragonError_t err;
    dragonMemoryDescr_t mem_descr;
    void* mem_ptr;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot send bytes without a pool for allocations.");

    if (bytes == NULL && num_bytes != 0)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide bytes when sending a non-zero number of bytes.");

    timespec_t* timeout = NULL;
    timespec_t remaining_time;

    if (deadline != NULL) {
        timeout = &remaining_time;
        err = dragon_timespec_remaining(deadline, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute remaining time");
    }

    err = dragon_memory_alloc_blocking(&mem_descr, pool, num_bytes, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get shared memory for message data.");

    if (num_bytes > 0) {
        err = dragon_memory_get_pointer(&mem_descr, &mem_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pointer for shared memory.");
        memcpy(mem_ptr, bytes, num_bytes);
    }

    err = _send_mem(chan_sendh, &mem_descr, arg, true, false, turbo_mode, flush, dest_pool, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error when calling internal _send_mem.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _get_buffered_bytes(dragonFLISendHandle_t* sendh, dragonMemoryDescr_t* mem_descr, uint64_t* arg, timespec_t* deadline) {
    dragonError_t err;
    void* mem_ptr = NULL;
    void* dest_ptr = NULL;
    dragonFLISendBufAlloc_t* node = NULL;
    dragonFLISendBufAlloc_t* prev = NULL;

    dragonFLISendBufAlloc_t* buffered_allocations = sendh->buffered_allocations;
    size_t total_bytes = sendh->total_bytes;

    timespec_t* timeout = NULL;
    timespec_t remaining_time;

    if (deadline != NULL) {
        timeout = &remaining_time;
        err = dragon_timespec_remaining(deadline, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Send buffered bytes timed out before sending.");
    }

    err = dragon_memory_alloc_blocking(mem_descr, &sendh->adapter->pool, total_bytes, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get shared memory for message data.");

    /* Init these two fields so they are set on the next call. */
    sendh->buffered_allocations = NULL;
    sendh->total_bytes = 0;

    if (total_bytes == 0) {
        // There is no data, so return the zero-byte memory allocation.
        no_err_return(DRAGON_SUCCESS);
    }

    // Return the buffered arg value.
    *arg = sendh->buffered_arg;

    err = dragon_memory_get_pointer(mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pointer for shared memory.");

    dest_ptr = mem_ptr + total_bytes;
    node = buffered_allocations;

    while (node != NULL) {
        dest_ptr = dest_ptr - node->num_bytes;
        memcpy(dest_ptr, node->data, node->num_bytes);
        prev = node;
        node = node->next;
        if (prev->data != NULL) {
            if (prev->free_data)
                free(prev->data);
            prev->data = NULL;
        }

        free(prev);
        prev = NULL;
    }

    if (dest_ptr != mem_ptr)
        err_return(DRAGON_INVALID_OPERATION, "There was an error while unbuffering data in send operation.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_buffered_bytes(dragonFLISendHandle_t* sendh, bool flush, timespec_t* deadline) {
    dragonError_t err;
    dragonMemoryPoolDescr_t* dest_pool = NULL;
    dragonMemoryDescr_t mem_descr;
    uint64_t buffered_arg;


    if (sendh->has_dest_pool)
        dest_pool = &sendh->dest_pool;

    err = _get_buffered_bytes(sendh, &mem_descr, &buffered_arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send buffered bytes.");

    err = _send_mem(&sendh->chan_sendh, &mem_descr, buffered_arg, true, false, sendh->turbo_mode, flush, dest_pool, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error when calling internal _send_mem.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _buffer_bytes(dragonFLISendHandle_t* sendh, uint8_t* bytes, size_t num_bytes, uint64_t arg, const bool buffer) {
        void* data_ptr;
        dragonFLISendBufAlloc_t* node_ptr;

        if (sendh->buffered_allocations == NULL)
            /* first write, so grab the user's meta data arg */
            sendh->buffered_arg = arg;

        if (num_bytes > 0) {

            node_ptr = malloc(sizeof(dragonFLISendBufAlloc_t));
            if (node_ptr == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for buffering data - out of memory.");

            if (buffer) {
                data_ptr = malloc(num_bytes);
                if (data_ptr == NULL)
                    err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space to buffer data - out of memory.");
                memcpy(data_ptr, bytes, num_bytes);
                node_ptr->data = data_ptr;
                node_ptr->free_data = true;
            } else {
                node_ptr->data = bytes;
                node_ptr->free_data = false;
            }

            node_ptr->num_bytes = num_bytes;
            sendh->total_bytes+=num_bytes;
            node_ptr->next = sendh->buffered_allocations;
            sendh->buffered_allocations = node_ptr;
        }

        no_err_return(DRAGON_SUCCESS);
}


static dragonError_t _free_buffered_mem(dragonFLIRecvHandle_t* recvh) {
    dragonError_t err;
    dragonFLIRecvBufAlloc_t* node = NULL;
    dragonFLIRecvBufAlloc_t* prev = NULL;

    if (recvh->buffered_data == NULL)
        no_err_return(DRAGON_SUCCESS);

    prev = recvh->buffered_data;
    node = prev->next;

    while (node != NULL) {
        prev = node;
        if (recvh->free_mem) {
            err = dragon_memory_free(&node->mem);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not free buffered managed memory.");
        }
        node = node->next;
        free(prev);
    }

    /* Free the dummy node */
    free(recvh->buffered_data);

    recvh->buffered_data = NULL;

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t _recv_mem(dragonChannelRecvh_t* recvh, dragonMemoryDescr_t* mem, uint64_t* arg, dragonMemoryPoolDescr_t* dest_pool, timespec_t* deadline) {
    dragonError_t err;
    dragonMessage_t msg;
    dragonMessageAttr_t attrs;
    timespec_t* timeout = NULL;
    timespec_t remaining_time;
    dragonMemoryDescr_t* dest = NULL;

    if (deadline != NULL) {
        timeout = &remaining_time;
        err = dragon_timespec_remaining(deadline, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute remaining time");
    }

    if (recvh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide non-null receive handle.");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide non-null memory descriptor");

    if (arg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide a non-null arg variable pointer.");

    *arg = 0; // always init this.

    if (dest_pool != NULL) {
        /* This won't block. A zero-byte allocation can be used to direct channels lib
           to put the message into the given pool. */
        dest = malloc(sizeof(dragonMemoryDescr_t));
        if (dest == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not malloc memory descriptor for zero byte allocation.");

        err = dragon_memory_alloc_blocking(dest, dest_pool, 0, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get zero-byte allocation.");
    }

    err = dragon_channel_message_init(&msg, dest, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize message structure.");

    err = dragon_chrecv_get_msg_blocking(recvh, &msg, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive memory from channel.");

    err = dragon_channel_message_getattr(&msg, &attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get message attributes from received messsage.");

    *arg = attrs.hints;

    err = dragon_channel_message_get_mem(&msg, mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get memory for stream channel.");

    err = dragon_channel_message_destroy(&msg, false);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy message structure.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_bytes_into(dragonChannelRecvh_t* recvh, uint8_t** data, size_t* num_bytes,
                        uint64_t* arg, dragonMemoryPoolDescr_t* dest_pool, timespec_t* deadline, bool free_mem) {
    dragonError_t err;
    dragonMemoryDescr_t mem;
    void* mem_ptr;

    if (data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a non-null data pointer address");

    if (num_bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a non-null size_t pointer");

    *num_bytes = 0; // always init
    *arg = 0;

    err = _recv_mem(recvh, &mem, arg, dest_pool, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive message in _recv_bytes_into.");

    if (*arg == FLI_EOT) {
        err = dragon_memory_free(&mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free EOT memory.");

        no_err_return(DRAGON_EOT);
    }

    err = dragon_memory_get_size(&mem, num_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get memory size for stream channel.");

    if (*num_bytes > 0) {
        err = dragon_memory_get_pointer(&mem, &mem_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get memory pointer for stream channel.");

        if (*data == NULL) {
            /* If a NULL pointer is passed for the location of the data, then we initialize it to
               point to a freshly malloced space. In this case we can make it for the required size
               exactly. */
            *data = malloc(*num_bytes);
            if (*data == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not malloc memory for message.");
        }

        memcpy(*data, mem_ptr, *num_bytes);
    } else
        *data = NULL;

    if (free_mem) {
        err = dragon_memory_free(&mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free memory.");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _buffer_mem(dragonFLIRecvHandle_t* recvh, dragonMemoryDescr_t* mem, uint64_t arg) {
    dragonError_t err;
    size_t size = 0;
    dragonFLIRecvBufAlloc_t* node = NULL;

    err = dragon_memory_get_size(mem, &size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size while buffering data.");

    if (size > 0) {
        node = malloc(sizeof(dragonFLIRecvBufAlloc_t));
        if (node == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate node for buffered data.");
        err = dragon_memory_descr_clone(&node->mem, mem, 0, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Unable to clone mem descriptor while buffering data.");
        node->num_bytes = size;
        node->offset = 0;
        node->arg = arg;
        node->next = NULL;
        recvh->buffered_bytes += size;
        recvh->tail->next = node;
        recvh->tail = node;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _unbuffer_mem(dragonFLIRecvHandle_t* recvh, dragonMemoryDescr_t* mem, uint64_t* arg) {
    dragonError_t err;
    dragonFLIRecvBufAlloc_t* node = NULL;
    dragonFLIRecvBufAlloc_t* prev = NULL;
    size_t num_bytes;

    if (recvh->buffered_bytes == 0)
        err_return(DRAGON_NOT_FOUND, "No memory was found to unbuffer.");

    /* prev points at the dummy node at the beginning of the list. This
       makes management of the list easier. There will be data since
       buffered_bytes is not 0. */
    prev = recvh->buffered_data;
    node = prev->next;
    prev->next = node->next;
    num_bytes = node->num_bytes-node->offset;
    recvh->buffered_bytes -= num_bytes;
    if (node == recvh->tail)
        recvh->tail = recvh->buffered_data;

    err = dragon_memory_descr_clone(mem, &node->mem, node->offset, &num_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to clone mem descriptor while buffering data.");

    *arg = node->arg;
    free(node);

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_bytes_buffered(dragonFLIRecvHandle_t* recvh, size_t requested_size,
                        size_t* received_size, uint8_t** data, uint64_t* arg, timespec_t* deadline) {
    dragonError_t err = DRAGON_SUCCESS;
    dragonMemoryPoolDescr_t* dest_pool = NULL;
    void* src_ptr = NULL;
    void* dest_ptr = NULL;
    size_t cpy_bytes = 0;
    dragonFLIRecvBufAlloc_t* node = NULL;
    dragonFLIRecvBufAlloc_t* prev = NULL;
    dragonMemoryDescr_t chunk_mem;
    uint64_t chunk_arg = 0;
    size_t alloc_sz = 0;
    size_t node_bytes = 0;

    /* Init return data to default values. */
    *received_size = 0;
    *arg = 0;

    if (recvh->has_dest_pool)
        dest_pool = &recvh->dest_pool;

    if (requested_size == 0 && recvh->buffered_bytes == 0) {
        err = _recv_bytes_into(&recvh->chan_recvh, data, received_size, arg, dest_pool, deadline, recvh->free_mem);
        if (err == DRAGON_EOT) {
            recvh->EOT_received = true;
            no_err_return(DRAGON_EOT);
        }

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not receive bytes in helper routine.");

        if (recvh->adapter->use_buffered_protocol) {
            /* We have finished reading all the data in the case of a buffered
               FLI. So we want to signal we are done. */
            recvh->stream_received = true;
        }

        no_err_return(DRAGON_SUCCESS);
    }

    if (!recvh->EOT_received) {
        while (chunk_arg != FLI_EOT && recvh->buffered_bytes < requested_size) {
            err = _recv_mem(&recvh->chan_recvh, &chunk_mem, &chunk_arg, dest_pool, deadline);
            if (chunk_arg == FLI_EOT) {
                recvh->EOT_received = true;

                /* We have a zero sized memory descriptor to free. */
                err = dragon_memory_free(&chunk_mem);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not free zero sized memory chunk while buffering data.");
            } else {
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not get data and buffer it in file-like adapter.");

                err = _buffer_mem(recvh, &chunk_mem, chunk_arg);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not buffer memory in helper routine.");
            }
        }
    }

    /* We set the size to the minimum of what was asked for or what is available. */
    alloc_sz = requested_size;
    if ((recvh->buffered_bytes < requested_size) || (requested_size == 0))
        alloc_sz = recvh->buffered_bytes;

    /* Now check if there is any data left to return. If it is a buffered
       FLI, then there will be no more data. Otherwise we wait for EOT. */
    if (alloc_sz == 0 && (recvh->adapter->use_buffered_protocol || recvh->EOT_received)) {
        *arg = FLI_EOT;
        no_err_return(DRAGON_SUCCESS);
    }

    if (alloc_sz == 0)
        err_return(DRAGON_INVALID_OPERATION, "There is an internal failure");

    if (*data == NULL) {
        /* If NULL is passed in, then we allocate the space here for the received data. Otherwise,
           the space was provided. */
        *data = malloc(alloc_sz);
        if (*data == NULL) {
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for received data.");
        }
    }

    dest_ptr = *data;
    prev = recvh->buffered_data;
    node = prev->next;

    while (*received_size < alloc_sz) {
        cpy_bytes = alloc_sz - *received_size;
        node_bytes = node->num_bytes - node->offset;
        if (node_bytes < cpy_bytes)
            cpy_bytes = node_bytes;

        err = dragon_memory_get_pointer(&node->mem, &src_ptr);
        src_ptr += node->offset;

        memcpy(dest_ptr, src_ptr, cpy_bytes);

        node->offset = node->offset + cpy_bytes;
        *received_size += cpy_bytes;
        dest_ptr += cpy_bytes;
        recvh->buffered_bytes -= cpy_bytes;
        *arg = node->arg;

        /* Check if we have used all data in this node */
        if (node->num_bytes == node->offset) {
            if (node == recvh->tail)
                recvh->tail = recvh->buffered_data;
            prev->next = node->next;
            if (recvh->free_mem){
                err = dragon_memory_free(&node->mem);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not free buffered managed memory.");
            }
            free(node);
            node = prev->next;
        }
    }

    if (recvh->adapter->use_buffered_protocol && recvh->buffered_bytes == 0) {
        /* We have finished reading all the data in the case of a buffered
           FLI. So we want to signal we are done. */
        recvh->stream_received = true;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _get_term_channel(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout) {
    /* This is not a buffered FLI, so receive the serialized termination event channel first. */
    uint64_t hint;
    dragonFLIRecvHandle_t* recvh_obj;
    dragonChannelSerial_t ser_term;
    dragonMemoryDescr_t mem;
    dragonError_t err;

    err = dragon_fli_recv_mem(recv_handle, &mem, &hint, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive memory to check for termination channel.");

    if (err == DRAGON_EOT)
        append_err_return(err, "Got EOT.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (hint == FLI_TERMINATOR) {
        /* There is a termination channel, so attach to the channel. */
        err = dragon_memory_get_size(&mem, &ser_term.len);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get memory size for terminator channel.");

        err = dragon_memory_get_pointer(&mem, (void**)&ser_term.data);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get the pointer to terminator channel.");

        err = dragon_channel_attach(&ser_term, &recvh_obj->terminate_stream_channel);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach the terminator channel.");

        err = dragon_memory_free(&mem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free the memory for the terminator channel.");

        recvh_obj->has_term_channel = true;

    } else {
        /* The memory was not the terminator channel. Buffer it for reading later. */
        err = _buffer_mem(recvh_obj, &mem, hint);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not buffer the memory after checking for terminator channel.");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_term_msg(dragonFLIRecvHandle_t* recvh_obj, const timespec_t* timeout) {
    dragonError_t err;
    dragonMemoryDescr_t mem_descr;
    dragonMessage_t term_msg;
    dragonChannelSendh_t term_sendh;

    /* No need to store any data in it. We just need a message. */
    err = dragon_memory_alloc_blocking(&mem_descr, &recvh_obj->adapter->pool, 8, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not allocate memory for termination message.");

    err = dragon_channel_message_init(&term_msg, &mem_descr, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize message for termination.");

    /* It could be that termination channel was already destroyed by sender. If so, the
       code below will guarantee that we still get to the destroy of the message to
       free the associated memory allocation. */
    err = dragon_channel_sendh(&recvh_obj->terminate_stream_channel, &term_sendh, NULL);

    if (err == DRAGON_SUCCESS)
        err = dragon_chsend_open(&term_sendh);

    if (err == DRAGON_SUCCESS)
        err = dragon_chsend_send_msg(&term_sendh, &term_msg, NULL, timeout);

    if (err == DRAGON_SUCCESS)
        err = dragon_chsend_close(&term_sendh);

    err = dragon_channel_message_destroy(&term_msg, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy termination message.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_bytes_common(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                        size_t* received_size, uint8_t** bytes, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    uint64_t argVal;

    /* In this case the user doesn't care about the hint/arg. */
    if (arg == NULL)
        arg = &argVal;

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->stream_received && recvh_obj->buffered_bytes == 0)
        /* data has been read already so we return our end of stream error code. */
        err_return(DRAGON_EOT, "End of Stream. You must close and re-open receive handle.");

    if (!recvh_obj->buffered_receive && !recvh_obj->recv_called) {
        /* It could be that this fails because sender may have closed send handle
           already. If so, that's fine because sender is done sending then. This
           is done here and not when the recv handle is opened because then
           opening the receive handle will only block if there is no available
           stream channel which seems more intuitive. */
        recvh_obj->recv_called = true;
        err = _get_term_channel(recv_handle, timeout);
        if (err == DRAGON_EOT)
            append_err_return(err, "Got EOT");
    }

    err = _recv_bytes_buffered(recvh_obj, requested_size, received_size, bytes, arg, deadline);

    if (*arg == FLI_EOT) {
        recvh_obj->stream_received = true; // This was already set, but oh well.
        *arg = 0; /* FLI_EOT is internal only so don't expose it. */
        no_err_return(DRAGON_EOT);
    }

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error occurred while receiving data.");

    recvh_obj->num_bytes_received += *received_size;

    if (recvh_obj->buffered_receive)
        /* When buffered, mark stream as received after first read. */
        recvh_obj->stream_received = true;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_stream_channel(const dragonChannelDescr_t* strm_ch, uint64_t type, const dragonChannelDescr_t* to_chan,
                        dragonMemoryPoolDescr_t* pool, timespec_t* deadline) {
    dragonError_t err;
    dragonChannelSerial_t ser;
    dragonChannelSendh_t sendh;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The pool cannot be NULL.");

    if (strm_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stream channel descriptor cannot be NULL.");

    if (to_chan == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The channel to send to cannot be NULL.");

    err = dragon_channel_sendh(to_chan, &sendh, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize send handle");

    err = dragon_chsend_open(&sendh);
    if (err != DRAGON_SUCCESS)
         append_err_return(err, "Could not open send handle on channel.");

    err = dragon_channel_serialize(strm_ch, &ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize stream channel.");

    err = _send_bytes(&sendh, pool, ser.data, ser.len, type, false, false, NULL, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send stream channel.");

    err = dragon_channel_serial_free(&ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized channel structure.");

    err = dragon_chsend_close(&sendh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_stream_channel(dragonChannelDescr_t* from_chan, dragonChannelDescr_t* strm_ch, uint64_t* type, dragonFLIRecvHandle_t* fli_recvh, timespec_t* deadline) {
    dragonError_t err;
    dragonChannelSerial_t ser;
    dragonChannelRecvh_t recvh;
    dragonMemoryDescr_t mem;
    uint64_t arg;

    if (strm_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stream channel descriptor cannot be NULL.");

    if (from_chan == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The channel to receive from cannot be NULL.");

    if (type == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stream channel type arg cannot be NULL.");

    *type = 0;

    err = dragon_channel_recvh(from_chan, &recvh, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize receive handle.");

    err = dragon_chrecv_open(&recvh);
    if (err != DRAGON_SUCCESS)
         append_err_return(err, "Could not open send handle on channel.");

    /* We zero the pointer so the _recv_bytes_into will allocate space for us. */
    ser.data = NULL;

    err = _recv_mem(&recvh, &mem, &arg, NULL, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive the stream channel.");

    if (arg != FLI_INTERNAL_STRM_CHANNEL && arg != FLI_EXTERNAL_STRM_CHANNEL) {
        /* The memory was not a stream channel. Buffer it for reading later. */
        if (fli_recvh == NULL)
            err_return(DRAGON_INVALID_VALUE, "Receiving stream channel failed. Found incorrectly typed data.");

        err = _buffer_mem(fli_recvh, &mem, arg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not buffer the memory after checking for stream channel.");

        fli_recvh->buffered_receive = true;
        fli_recvh->EOT_received = true;

        no_err_return(DRAGON_SUCCESS);
    }

    *type = arg; // return the type of stream channel.

    err = dragon_memory_get_pointer(&mem, (void**)&ser.data);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pointer for shared memory.");

    err = dragon_memory_get_size(&mem, &ser.len);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size of received memory descriptor.");

    err = dragon_channel_attach(&ser, strm_ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach stream channel");

    err = dragon_memory_free(&mem);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized stream channel.");

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _empty_the_channel(dragonChannelDescr_t* channel, bool* found_EOT, int max, bool free_mem) {
    /* passing max == 0 will result in all messages being emptied from the channel. */
    dragonError_t err;
    dragonChannelRecvh_t recvh;
    timespec_t deadline = {0,0};
    int num_read = 0;

    if (found_EOT != NULL)
        *found_EOT = false;

    err = dragon_channel_recvh(channel, &recvh, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create receive handle on channel.");

    err = dragon_chrecv_open(&recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open receive handle on channel.");

    err = DRAGON_SUCCESS;

    while (err == DRAGON_SUCCESS) {
        dragonMemoryDescr_t mem;
        uint64_t arg;

        err = _recv_mem(&recvh, &mem, &arg, NULL, &deadline);

        if (err == DRAGON_SUCCESS) {
            num_read += 1;
            if (arg == FLI_EOT && found_EOT != NULL)
                *found_EOT = true;

            if ((arg == FLI_EOT) || free_mem) {
                err = dragon_memory_free(&mem);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "There was an error freeing memory while emptying the channel.");
            }

            if (num_read == max)
                err = DRAGON_CHANNEL_EMPTY;
        }
    }

    if (err != DRAGON_CHANNEL_EMPTY)
        append_err_return(err, "There was an error emptying a channel in the fli adapter.");

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle on channel being emptied.");

    no_err_return(DRAGON_SUCCESS);
}

static void* _from_fd_to_fli (void* ptr) {
    dragonError_t err;
    uint8_t* buffer;
    size_t num_bytes = 0;
    _SenderArg_t* arg = (_SenderArg_t*) ptr;
    int fd = arg->fd;
    uint64_t user_arg = arg->user_arg;

    buffer = malloc(arg->chunk_size);
    if (buffer == NULL) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        /* err might be logged eventually. */
        fprintf(stderr, "ERROR: The chunk size of %lu could not be allocated for sending (ERR=%s).", arg->chunk_size, dragon_get_rc_string(err));
        fflush(stderr);
        return NULL;
    }

    err = DRAGON_SUCCESS;
    while ((err == DRAGON_SUCCESS) && ((num_bytes = read(arg->fd, buffer, arg->chunk_size)) > 0))
        err = dragon_fli_send_bytes(arg->sendh, num_bytes, buffer, user_arg, arg->buffer, NULL);


    if (err != DRAGON_SUCCESS) {
        /* err might be logged eventually. But no way to return the error to user.
        They will see a problem with the file descriptor. */
        fprintf(stderr, "ERROR: There was an error sending bytes through the fli interface (ERR=%s).\n", dragon_get_rc_string(err));
        fflush(stderr);
    }

    close(fd);
    free(buffer);
    buffer = NULL;

    if (arg->buffer) {
        err = dragon_fli_send_bytes(arg->sendh, 0, NULL, 0, false, NULL);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "ERROR: Could not flush the buffered bytes from the file descriptor thread helper.");
            fflush(stderr);
        }
    }

    pthread_exit(NULL);
}


static void* _from_fli_to_fd (void* ptr) {
    dragonError_t err;
    uint8_t* buffer;
    uint64_t recv_arg;
    size_t num_bytes = 0;
    ssize_t written_bytes = 0;
    _ReceiverArg_t* arg = (_ReceiverArg_t*) ptr;
    int fd = arg->fd;

    while ((err = dragon_fli_recv_bytes(arg->recvh, 0, &num_bytes, &buffer, &recv_arg, NULL)) == DRAGON_SUCCESS) {
        written_bytes = 0;
        while (written_bytes < num_bytes)
            written_bytes += write(arg->fd, &buffer[written_bytes], num_bytes - written_bytes);

        free(buffer);
        buffer = NULL;
    }

    if (err != DRAGON_EOT) {
        /* err might be logged eventually. */
        fprintf(stderr, "ERROR: There was an error receiving data from the fli interface (ERR=%s).\n", dragon_get_rc_string(err));
        fflush(stderr);
    }

    close(fd);
    pthread_exit(NULL);
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void
_set_thread_local_mode_fli(bool set_thread_local)
{
    if (set_thread_local) {
        dg_fli_adapters = &_dg_thread_fli_adapters;
        dg_fli_send_handles = &_dg_thread_fli_send_handles;
        dg_fli_recv_handles = &_dg_thread_fli_recv_handles;
    } else {
        dg_fli_adapters = &_dg_proc_fli_adapters;
        dg_fli_send_handles = &_dg_proc_fli_send_handles;
        dg_fli_recv_handles = &_dg_proc_fli_recv_handles;
    }
}

static
dragonError_t _fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    uint64_t count = 0;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (bytes == NULL && num_bytes > 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot send non-zero number of bytes with NULL pointer.");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (sendh_obj->close_required)
        err_return(DRAGON_INVALID_ARGUMENT, "When using a Buffered FLI, the buffer argument must be true unless send_bytes is called exactly once before closing the send handle.");

    /* buffering bytes to send */
    if (sendh_obj->buffered_send && !buffer) {
        sendh_obj->close_required = true;
    }

    /* Check that for a streaming connection the receiver has not closed the receive handle. If
       it has, then exit with appropriate return code. */
    if (sendh_obj->has_term_channel) {
        err = dragon_channel_message_count(&sendh_obj->terminate_stream_channel, &count);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get message count of termination channel");

        /* If a message has been received in the terminate_stream_channel, the receiver has
           canceled receipt of this stream by closing the receive handle. */
        if (count > 0)
            err_return(DRAGON_EOT, "Sending of the stream has been canceled by the receiver.");
    }

    err = _buffer_bytes(sendh_obj, bytes, num_bytes, arg, buffer);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not buffer bytes.");

    if (!buffer) {
        /* Buffered bytes are sent immediately when buffering was not requested.
           When the connection is a buffered connection, this allows a fast path
           with minimal copying when calling send_bytes exactly once. */
        err = _send_buffered_bytes(sendh_obj, false, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send data.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/****************************************************************************************/
/* Beginning of user API                                                                */
/****************************************************************************************/

dragonError_t dragon_fli_attr_init(dragonFLIAttr_t* attr) {
    attr->_placeholder = 0;
    return DRAGON_SUCCESS;
}

dragonError_t dragon_fli_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                  dragonChannelDescr_t* mgr_ch, dragonMemoryPoolDescr_t* pool,
                  const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                  const bool use_buffered_protocol, dragonFLIAttr_t* attrs) {
    dragonError_t err;

    err = dragon_fli_task_create(adapter, main_ch, mgr_ch, NULL, pool, num_strm_chs, strm_channels, use_buffered_protocol, attrs);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create dragon FLI");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_task_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                  dragonChannelDescr_t* mgr_ch, dragonChannelDescr_t* task_sem, dragonMemoryPoolDescr_t* pool,
                  const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                  const bool use_buffered_protocol, dragonFLIAttr_t* attrs) {

    dragonError_t err;
    dragonFLIAttr_t def_attr;
    uint64_t msg_count;
    timespec_t deadline = {0,0}; /* try once only */

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    if (main_ch == NULL && mgr_ch == NULL && num_strm_chs == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The main channel and the manager channel cannot both be null when the number of stream channels is 0.");

    if (use_buffered_protocol) {
        if (mgr_ch != NULL)
            err_return(DRAGON_INVALID_ARGUMENT, "If using buffered protocol you may not provide a manager channel.");

        if (num_strm_chs > 0)
            err_return(DRAGON_INVALID_ARGUMENT, "If using buffered protocol you may not provide stream channels.");
    }

    /* the memory pool must be locally addressable if provided. */
    if (pool != NULL && !dragon_memory_pool_is_local(pool))
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot directly access memory pool for fli adapter");

    if (num_strm_chs > 0 && mgr_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "If providing stream channels, you must provide a manager channel as well.");

    /* if the attrs are NULL populate a default one */
    if (attrs == NULL) {
        err = dragon_fli_attr_init(&def_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize channel attributes.");

        attrs = &def_attr;
    } else {
        err = _validate_attr(attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "FLI Attribute(s) are invalid.");
    }

    /* this will be freed in the fli_destroy call */
    dragonFLI_t* obj = malloc(sizeof(dragonFLI_t));
    if (obj == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate new file-like interface adapter.");

    obj->attrs = *attrs;
    obj->was_attached = false; /* created, not attached */

    if (pool == NULL) {
        /* We will attach to the default pool in this case. */
        err = dragon_memory_pool_attach_default(&obj->pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to default pool.");
    } else {
        /* make a clone of the pool descriptor for use here */
        err = dragon_memory_pool_descr_clone(&obj->pool, pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot clone pool descriptor");
    }

    obj->use_buffered_protocol = use_buffered_protocol;

    if (main_ch != NULL) {
        err = dragon_channel_descr_clone(&obj->main_ch, main_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot clone main channel descriptor.");

        /* We create a channel send handle here because it may be used to send
           and if so, we need to guarantee order of sends (in some cases - buffered
           for instance) and the way to guarantee order is to hold a send handle
           open. Not a big resource if it is not used. */
        err = dragon_channel_sendh(&obj->main_ch, &obj->main_sendh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create send handle on main channel.");

        err = dragon_chsend_open(&obj->main_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open send handle on main channel.");

        if (!use_buffered_protocol) {
            /* If we are using buffered protocol, then it does not need to be empty during creation. */
            err = dragon_channel_message_count(&obj->main_ch, &msg_count);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get the main channel message count during creation.");

            if (msg_count > 0)
                err_return(DRAGON_INVALID_ARGUMENT, "The main channel has items in it during adapter creation.");
        }

        obj->has_main_ch = true;
    } else
        obj->has_main_ch = false;

    if (mgr_ch != NULL) {
        err = dragon_channel_descr_clone(&obj->mgr_ch, mgr_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot clone manager channel descriptor.");

        err = dragon_channel_message_count(&obj->mgr_ch, &msg_count);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get the manager channel message count during creation.");

        if (msg_count > 0)
            err_return(DRAGON_INVALID_ARGUMENT, "The manager channel has items in it during adapter creation.");

        obj->has_mgr_ch = true;
    } else
        obj->has_mgr_ch = false;

    if (task_sem != NULL) {
            /* It must be a semaphore channel. We don't check it here because it may be a remote
               channel. We will detect it is not though when we try to use it if it is not. */
            err = dragon_channel_descr_clone(&obj->task_sem, task_sem);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone the task semaphore channel descriptor.");

            obj->has_task_sem = true;
        } else
            obj->has_task_sem = false;

    obj->num_strm_chs = num_strm_chs;

    for (int idx=0; idx<num_strm_chs; idx++) {
        err = _send_stream_channel(strm_channels[idx], FLI_INTERNAL_STRM_CHANNEL, &obj->mgr_ch, &obj->pool, &deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not deposit stream channel into manager channel.");
    }

    err = _add_umap_fli_entry(adapter, obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created adapter");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_destroy(dragonFLIDescr_t* adapter) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->has_mgr_ch) {
        err = _empty_the_channel(&obj->mgr_ch, NULL, 0, true);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not empty the manager channel.");
    }

    if (obj->has_main_ch) {
        err = dragon_chsend_close(&obj->main_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close main channel send handle");

        err = _empty_the_channel(&obj->main_ch, NULL, 0, true);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not empty the main channel.");
    }

    if (obj->was_attached) {
        if (obj->has_main_ch) {
            err = dragon_channel_detach(&obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from main channel of adapter.");
        }

        if (obj->has_mgr_ch) {
            err = dragon_channel_detach(&obj->mgr_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from manager channel of adapter.");
        }

        if (obj->has_task_sem) {
            err = dragon_channel_detach(&obj->task_sem);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from the task semaphore channel of adapter.");
        }
    }

    err = dragon_umap_delitem(dg_fli_adapters, adapter->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete adapter from from adapters umap");

    free(obj);
    obj = NULL;
    adapter->_idx = 0;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_serialize(const dragonFLIDescr_t* adapter, dragonFLISerial_t* serial) {
    dragonError_t err;
    dragonFLI_t* obj;
    uint8_t adapter_type = 0;
    dragonChannelSerial_t main_ch_ser;
    dragonChannelSerial_t mgr_ch_ser;
    dragonChannelSerial_t task_sem_ser;
    void* ptr;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    if (serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli serial descriptor");

    serial->data = NULL;
    serial->len = 0;

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->has_main_ch) {
        adapter_type+=FLI_HAS_MAIN_CHANNEL;
        err = dragon_channel_serialize(&obj->main_ch, &main_ch_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize main channel of fli adapter.");

        serial->len+=main_ch_ser.len + sizeof(main_ch_ser.len);
    }

    if (obj->has_mgr_ch) {
        adapter_type+=FLI_HAS_MANAGER_CHANNEL;
        err = dragon_channel_serialize(&obj->mgr_ch, &mgr_ch_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize manager channel of fli adapter.");

        serial->len+=mgr_ch_ser.len + sizeof(mgr_ch_ser.len);
    }

    if (obj->has_task_sem) {
        adapter_type+=FLI_HAS_TASK_SEM;
        err = dragon_channel_serialize(&obj->task_sem, &task_sem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not serialize the task semaphore of fli adapter.");

        serial->len+=task_sem_ser.len + sizeof(task_sem_ser.len);
    }

    if (obj->use_buffered_protocol)
        adapter_type+=FLI_USING_BUFFERED_PROTOCOL;

    /* Add the one byte for the adapter type. */
    serial->len += sizeof(uint8_t);

    ptr = malloc(serial->len);
    if (ptr == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not malloc space for serialized descriptor");

    serial->data = ptr;
    memcpy(ptr, &adapter_type, sizeof(uint8_t));
    ptr+=sizeof(uint8_t);

    if (obj->has_main_ch) {
        memcpy(ptr, &main_ch_ser.len, sizeof(main_ch_ser.len));
        ptr+=sizeof(main_ch_ser.len);
        memcpy(ptr, main_ch_ser.data, main_ch_ser.len);
        ptr+=main_ch_ser.len;
        err = dragon_channel_serial_free(&main_ch_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for main channel");
    }

    if (obj->has_mgr_ch) {
        memcpy(ptr, &mgr_ch_ser.len, sizeof(mgr_ch_ser.len));
        ptr+=sizeof(mgr_ch_ser.len);
        memcpy(ptr, mgr_ch_ser.data, mgr_ch_ser.len);
        ptr+=mgr_ch_ser.len;
        err = dragon_channel_serial_free(&mgr_ch_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for manager channel");
    }

    if (obj->has_task_sem) {
        memcpy(ptr, &task_sem_ser.len, sizeof(task_sem_ser.len));
        ptr+=sizeof(task_sem_ser.len);
        memcpy(ptr, task_sem_ser.data, task_sem_ser.len);
        ptr+=task_sem_ser.len;
        err = dragon_channel_serial_free(&task_sem_ser);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not free serialized descriptor for the task semaphore channel");
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_serial_free(dragonFLISerial_t* serial) {
    if (serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid serialized fli adapter.");

    if (serial->data == NULL)
        no_err_return(DRAGON_SUCCESS);

    if (serial->data != NULL)
        free(serial->data);

    serial->data = NULL;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_attach(const dragonFLISerial_t* serial, const dragonMemoryPoolDescr_t* pool,
                  dragonFLIDescr_t* adapter) {
    dragonError_t err;
    dragonFLI_t* obj;
    uint8_t adapter_type = 0;
    dragonChannelSerial_t ch_ser;
    void* ptr;
    dragonFLIAttr_t* attrs = NULL; /* Perhaps this needs to be an argument. Or perhaps
                                      attrs should be included in the serialized descriptor? */

    if (serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid serialized fli adapter.");

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter.");

    /* if the attrs are NULL populate a default one */
    dragonFLIAttr_t def_attr;
    if (attrs == NULL) {

        err = dragon_fli_attr_init(&def_attr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize channel attributes.");

        attrs = &def_attr;
    } else {

        err = _validate_attr(attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "FLI Attribute(s) are invalid.");
    }

    /* this will be freed in the fli_destroy call */
    obj = malloc(sizeof(dragonFLI_t));
    if (obj == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate new file-like interface adapter for attaching.");

    obj->attrs = *attrs;
    obj->num_strm_chs = 0; /* We don't keep track of it the channels in attached objects */
    obj->was_attached = true; /* was attached, not created */

    if (pool == NULL) {
        /* We will attach to the default pool in this case. */
        err = dragon_memory_pool_attach_default(&obj->pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to default pool.");
    } else {
        /* make a clone of the pool descriptor for use here */
        err = dragon_memory_pool_descr_clone(&obj->pool, pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot clone pool descriptor");
    }

    ptr=serial->data;
    memcpy(&adapter_type, ptr, sizeof(adapter_type));
    ptr+=sizeof(adapter_type);

    obj->use_buffered_protocol = (adapter_type & FLI_USING_BUFFERED_PROTOCOL) != 0;
    obj->has_main_ch = (adapter_type & FLI_HAS_MAIN_CHANNEL) != 0;
    obj->has_mgr_ch = (adapter_type & FLI_HAS_MANAGER_CHANNEL) != 0;
    obj->has_task_sem = (adapter_type & FLI_HAS_TASK_SEM) != 0;

    if (obj->has_main_ch) {
        memcpy(&ch_ser.len, ptr, sizeof(ch_ser.len));
        ptr+=sizeof(ch_ser.len);
        ch_ser.data = ptr;
        err = dragon_channel_attach(&ch_ser, &obj->main_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot attach to main channel of adapter.");
        ptr+=ch_ser.len;

        /* We create a channel send handle here because it may be used to send
           and if so, we need to guarantee order of sends (in some cases - buffered
           for instance) and the way to guarantee order is to hold a send handle
           open. Not a big resource if it is not used. */
        err = dragon_channel_sendh(&obj->main_ch, &obj->main_sendh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create send handle on main channel.");

        err = dragon_chsend_open(&obj->main_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open send handle on main channel.");
    }

    if (obj->has_mgr_ch) {
        memcpy(&ch_ser.len, ptr, sizeof(ch_ser.len));
        ptr+=sizeof(ch_ser.len);
        ch_ser.data = ptr;
        err = dragon_channel_attach(&ch_ser, &obj->mgr_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot attach to manager channel of adapter.");
        ptr+=ch_ser.len;
    }

    if (obj->has_task_sem) {
        memcpy(&ch_ser.len, ptr, sizeof(ch_ser.len));
        ptr+=sizeof(ch_ser.len);
        ch_ser.data = ptr;
        err = dragon_channel_attach(&ch_ser, &obj->task_sem);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot attach to task semaphore channel of adapter.");
        ptr+=ch_ser.len;
    }

    err = _add_umap_fli_entry(adapter, obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for attached adapter");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_detach(dragonFLIDescr_t* adapter) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->was_attached) {
        if (obj->has_main_ch) {
            err = dragon_chsend_close(&obj->main_sendh);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not close main channel send handle");

            err = dragon_channel_detach(&obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from main channel of adapter.");
        }

        if (obj->has_mgr_ch) {
            err = dragon_channel_detach(&obj->mgr_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from manager channel of adapter.");
        }

        if (obj->has_task_sem) {
            err = dragon_channel_detach(&obj->task_sem);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot detach from task semaphore channel of adapter.");
        }
    }

    err = dragon_umap_delitem(dg_fli_adapters, adapter->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete adapter from from adapters umap");

    free(obj);
    obj = NULL;
    adapter->_idx = 0;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_get_available_streams(dragonFLIDescr_t* adapter, uint64_t* num_streams, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->has_mgr_ch) {
        /* This works with both an on-node and off-node manager channel */

        err = dragon_channel_poll(&obj->mgr_ch, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLSIZE, timeout, num_streams);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not empty the manager channel.");

        no_err_return(DRAGON_SUCCESS);
    }

    err_return(DRAGON_INVALID_ARGUMENT, "The fli adapter does not have a manager channel and therefore calling dragon_fli_get_available_streams is invalid.");
}


dragonError_t dragon_fli_is_buffered(const dragonFLIDescr_t* adapter, bool* is_buffered) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    if (is_buffered == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The is_buffered variable cannot be NULL.");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    *is_buffered = obj->use_buffered_protocol;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_new_task(const dragonFLIDescr_t* adapter, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (!obj->has_task_sem)
        err_return(DRAGON_SEMAPHORE_NOT_FOUND, "You cannot call new_task on an FLI that has no task semaphore.");

    err = dragon_channel_poll(&obj->task_sem, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_V, timeout, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not execute the semaphore V operation on the task semaphore.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_task_done(const dragonFLIDescr_t* adapter, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (!obj->has_task_sem)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot call task_done on an FLI that has no task semaphore.");

    err = dragon_channel_poll(&obj->task_sem, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_P, timeout, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not execute the semaphore P operation on the task semaphore.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_join(const dragonFLIDescr_t* adapter, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (!obj->has_task_sem)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot call join on an FLI that has no task semaphore.");

    err = dragon_channel_poll(&obj->task_sem, DRAGON_DEFAULT_WAIT_MODE, DRAGON_SEMAPHORE_Z, timeout, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not execute the semaphore Z operation on the task semaphore.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_send_attr_init(dragonFLISendAttr_t* attrs) {
    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attributes structure pointer cannot be NULL");

    attrs->dest_pool = NULL;
    attrs->allow_strm_term = false;
    attrs->turbo_mode = false;
    attrs->flush = false;
    attrs->debug = 0UL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_open_send_handle(const dragonFLIDescr_t* adapter, dragonFLISendHandleDescr_t* send_handle, dragonChannelDescr_t* strm_ch,
                                  dragonFLISendAttr_t* attrs, const timespec_t* timeout) {

    dragonError_t err;
    dragonFLISendAttr_t attrs_val;
    dragonFLI_t* obj;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    dragonChannelSerial_t ser_term;
    bool buffered_send = false;

    if (attrs == NULL) {
        attrs = &attrs_val;
        err = dragon_fli_send_attr_init(attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to init the send handle attrs.");
    }

    dragonMemoryPoolDescr_t* dest_pool = attrs->dest_pool;
    bool allow_strm_term = attrs->allow_strm_term;
    bool turbo_mode = attrs->turbo_mode;
    bool flush = attrs->flush;
    dragonULInt debug = attrs->debug;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    buffered_send = (strm_ch == STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND) || obj->use_buffered_protocol;

    if (buffered_send && strm_ch != NULL && strm_ch != STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot supply a stream channel while using the buffered protocol or a buffered send operation.");

    if (buffered_send && allow_strm_term)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot use stream termination when using the buffered protocol or a buffered send operation.");

    sendh_obj = malloc(sizeof(dragonFLISendHandle_t));

    if (sendh_obj == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate send handle.");

    if (dest_pool != NULL) {
        err = dragon_memory_pool_descr_clone(&sendh_obj->dest_pool, dest_pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not clone destination pool descriptor.");
        sendh_obj->has_dest_pool = true;
    } else
        sendh_obj->has_dest_pool = false;

    sendh_obj->adapter = obj;
    sendh_obj->buffered_allocations = NULL;
    sendh_obj->close_required = false;
    sendh_obj->has_term_channel = allow_strm_term;
    sendh_obj->buffered_send = buffered_send;
    sendh_obj->turbo_mode = turbo_mode;
    sendh_obj->total_bytes = 0;
    sendh_obj->tid = 0;
    sendh_obj->strm_type = FLI_EXTERNAL_STRM_CHANNEL;
    sendh_obj->flush = flush;
    sendh_obj->debug = debug;

    if (sendh_obj->buffered_send) {
        /* The main channel send handle was opened when the fli was created. This is necessary
           to maintain ordering of anything sent on the FLI, especially when it is a buffered FLI.
           So, we just copy in the opened send handle here so we can use it in fli methods. */
        sendh_obj->chan_sendh = obj->main_sendh;
    } else {
        if (strm_ch == STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION) {
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a main channel when specifying to use main channel as stream channel.");

            if (obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You cannot use 1:1 mode on the fli when there is a manager channel.");

            err = dragon_channel_descr_clone(&sendh_obj->strm_channel, &obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone main channel descriptor as stream channel descriptor.");
        } else if (strm_ch != NULL) {
            bool got_stream_channel = false;

            /* This is a user-supplied channel so we keep track of that. */
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a main channel when a sender provided stream channel is given.");

            /* Try once to get a stream channel from the manager channel if it exists. If one is available, use it.*/
            if (obj->has_mgr_ch) {
                timespec_t try_once = {0,0};

                err = _recv_stream_channel(&obj->mgr_ch, &sendh_obj->strm_channel, &sendh_obj->strm_type, NULL, &try_once);
                if (err == DRAGON_SUCCESS) {
                    /* We are using a stream channel from the manager channel. */
                    strm_ch = &sendh_obj->strm_channel;
                    got_stream_channel = true;
                }
            }

            if (!got_stream_channel) {
                sendh_obj->strm_type = FLI_EXTERNAL_STRM_CHANNEL;

                /* We must wait here for the stream channel to become empty because it may have been used on a prior send
                to a different FLI, so it must be empty before we can allow it to be used again. Otherwise two different
                processes may be trying to read from the same stream channel at the same time.  */
                err = dragon_channel_poll(strm_ch, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLEMPTY, timeout, NULL);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not wait for emptying of stream channel.");

                err = dragon_channel_descr_clone(&sendh_obj->strm_channel, strm_ch);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Cannot clone stream channel descriptor.");

            }
        } else {
            /* It is a manager supplied stream. */
            if (!obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You must provide a stream channel when there is no manager channel.");

            /* We are using a stream channel from the manager channel. */
            err = _recv_stream_channel(&obj->mgr_ch, &sendh_obj->strm_channel, &sendh_obj->strm_type, NULL, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get stream channel from manager channel.");

            strm_ch = &sendh_obj->strm_channel;
        }

        err = dragon_channel_sendh(&sendh_obj->strm_channel, &sendh_obj->chan_sendh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create send handle on stream channel.");

        err = dragon_chsend_open(&sendh_obj->chan_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open send handle on stream channel.");

        if (obj->has_main_ch && strm_ch != STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION) {
            /* If it has no main channel, then the stream channel was receiver supplied. In
               all other cases (when not buffered or not the special case of a 1:1 connection)
               the stream channel is written into the main
               channel so a receiver can receive it and start reading while writing is occurring. */
            err = _send_stream_channel(strm_ch, sendh_obj->strm_type, &obj->main_ch, &obj->pool, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not deposit stream channel into main channel.");
        }
    }

    /* This is wrong to do this here. Attributes should be supplied when the channel send handle
       is created. But this saves having to set this on multiple paths so we'll internally cheat
       and do it here since it does not affect user code. This is done only for internal debugging. */
    sendh_obj->chan_sendh._attrs.debug = debug;

    err = _add_umap_fli_sendh_entry(send_handle, sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created send handle");

    /* has_term_channel can only be true if we are not using buffered sending or protocol. Otherwise,
       the open request was already rejected above. */
    if (sendh_obj->has_term_channel) {
        /* Create a channel for receiver termination of the stream. If the receive handle
            is closed on this stream, the sender will be notified by sending a message into
            this channel. Do this first. Needed by dragon_fli_send_bytes below. */
        err = dragon_create_process_local_channel(&sendh_obj->terminate_stream_channel, 0, 0, 1, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create termination channel for FLI send handle.");

        /* This is not a buffered FLI, so send the serialized termination event channel first. */
        err = dragon_channel_serialize(&sendh_obj->terminate_stream_channel, &ser_term);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to serialize termination channel in send handle.");

        err = _fli_send_bytes(send_handle, ser_term.len, ser_term.data, FLI_TERMINATOR, false, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to send the termination channel.");

        err = dragon_channel_serial_free(&ser_term);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to free the serialized termination channel.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_close_send_handle(dragonFLISendHandleDescr_t* send_handle, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    uint8_t dummy = 0;
    bool buffer = false;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (sendh_obj->tid != 0)
        err_return(DRAGON_INVALID_OPERATION, "You must close the created file descriptor and call dragon_finalize_writable_fd first.");

    if (sendh_obj->buffered_allocations != NULL) {
        /* buffered bytes remain to send on close of send handle. If
           buffering was requested, then this is the last/only send to the buffered
           channel so it will be flushed all the way through to recipient.
        */
        buffer = sendh_obj->flush && sendh_obj->buffered_send;
        err = _send_buffered_bytes(sendh_obj, buffer, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send buffered data.");
    }

    if (!sendh_obj->buffered_send) {
        /* sending the EOT indicator for the stream. */
        err = _send_bytes(&sendh_obj->chan_sendh, &sendh_obj->adapter->pool, &dummy, 1, FLI_EOT, sendh_obj->turbo_mode, sendh_obj->flush, NULL, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the end of stream indicator down the stream channel.");

        /* Buffered FLIs have an open send handle that stays open for lifetime of the FLI
           so ordering will be maintained on all sends on the FLI. So we only close here if it is
           not a buffered FLI. */
        err = dragon_chsend_close(&sendh_obj->chan_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close send handle on channel");
    }

    if (sendh_obj->has_term_channel) {
        _empty_the_channel(&sendh_obj->terminate_stream_channel, NULL, 0, true);

        err = dragon_destroy_process_local_channel(&sendh_obj->terminate_stream_channel, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not destroy the termination channel.");
    }

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_fli_send_handles, send_handle->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete item from fli send handle umap.");

    send_handle->_idx = 0;

    free(sendh_obj);
    sendh_obj = NULL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_recv_attr_init(dragonFLIRecvAttr_t* attrs) {
    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attributes structure pointer cannot be NULL");

    attrs->dest_pool = NULL;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_open_recv_handle(const dragonFLIDescr_t* adapter, dragonFLIRecvHandleDescr_t* recv_handle,
                            dragonChannelDescr_t* strm_ch, dragonFLIRecvAttr_t* attrs, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    dragonFLIRecvAttr_t attrs_val;

    if (attrs == NULL) {
        attrs = &attrs_val;
        err = dragon_fli_recv_attr_init(attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not initialize receive attributes.");
    }

    dragonMemoryPoolDescr_t* dest_pool = attrs->dest_pool;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli receive handle descriptor");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->use_buffered_protocol && strm_ch != NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot supply a stream channel while using buffered protocol");

    recvh_obj = malloc(sizeof(dragonFLIRecvHandle_t));
    if (recvh_obj == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate receive handle.");

    if (dest_pool != NULL) {
        err = dragon_memory_pool_descr_clone(&recvh_obj->dest_pool, dest_pool);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not clone destination pool descriptor.");
        recvh_obj->has_dest_pool = true;
    } else
        recvh_obj->has_dest_pool = false;

    recvh_obj->recv_called = false;
    recvh_obj->has_term_channel = false;
    recvh_obj->adapter = obj;
    recvh_obj->stream_received = false;
    recvh_obj->EOT_received = false;
    recvh_obj->buffered_receive = obj->use_buffered_protocol;
    recvh_obj->num_bytes_received = 0;
    recvh_obj->buffered_bytes = 0;
    recvh_obj->strm_type = FLI_EXTERNAL_STRM_CHANNEL;
    recvh_obj->tid = 0;
    recvh_obj->strm_opened = false;
    recvh_obj->free_mem = true;

    /* Creating a dummy head node simplifies management of the linked list. */
    recvh_obj->buffered_data = malloc(sizeof(dragonFLIRecvBufAlloc_t));
    if (recvh_obj->buffered_data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not malloc dummy node in receive handle.");

    recvh_obj->buffered_data->num_bytes = 0;
    recvh_obj->buffered_data->next = NULL;
    recvh_obj->tail = recvh_obj->buffered_data;

    if (recvh_obj->buffered_receive) {
        /* With buffered protocol we receive off of the main channel. */
        err = dragon_channel_recvh(&recvh_obj->adapter->main_ch, &recvh_obj->chan_recvh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create recv handle on stream channel.");

        err = dragon_chrecv_open(&recvh_obj->chan_recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open recv handle on stream channel.");

    } else {
        if (strm_ch == STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION) {
            /* The main channel is used as the stream channel for a 1:1 connection */
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a main channel when a receiver specifies use main.");

            if (obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You cannot use 1:1 mode on the fli when there is a manager channel.");

            err = dragon_channel_descr_clone(&recvh_obj->strm_channel, &obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone main channel as stream channel descriptor.");

        } else if (strm_ch != NULL) {
            /* A user supplied stream channel will be used. */
            if (!obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a manager channel when a receiver provided stream channel is given.");

            /* This is a user-supplied channel so we keep track of that. */
            recvh_obj->strm_type = FLI_EXTERNAL_STRM_CHANNEL;

            err = dragon_channel_descr_clone(&recvh_obj->strm_channel, strm_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone stream channel descriptor.");

            /* We add it to the manager channel so a sender can pick it up. */
            err = _send_stream_channel(strm_ch, recvh_obj->strm_type, &obj->mgr_ch, &obj->pool, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not deposit stream channel into manager channel.");

        } else {
            /* A main channel supplied stream channel will be used */
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You must provide a stream channel when there is no main channel.");

            /* We are using a stream channel from the main channel. */
            err = _recv_stream_channel(&obj->main_ch, &recvh_obj->strm_channel, &recvh_obj->strm_type, recvh_obj, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get stream channel from manager channel.");
        }

        if (!recvh_obj->buffered_receive) {
            /* The value of buffered receive could change as a result of calling _recv_stream_channel
               because it may have not received a stream channel in which case this has become
               a buffered receive. If so, then don't do these steps since there is no stream
               channel. */
            err = dragon_channel_recvh(&recvh_obj->strm_channel, &recvh_obj->chan_recvh, NULL);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not create recv handle on stream channel.");

            err = dragon_chrecv_open(&recvh_obj->chan_recvh);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not open recv handle on stream channel.");

            recvh_obj->strm_opened = true;
        }
    }

    err = _add_umap_fli_recvh_entry(recv_handle, recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created receive handle");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_close_recv_handle(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli receive handle descriptor");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->tid != 0)
        err_return(DRAGON_INVALID_OPERATION, "You must close the created file descriptor and call dragon_finalize_readable_fd first.");

    /* We allow the FLI_EOT to be in the stream channel still. We read one more item to see if we find it. */
    if (!recvh_obj->stream_received)
        _empty_the_channel(&recvh_obj->strm_channel, &recvh_obj->stream_received, 1, recvh_obj->free_mem);

    /* If the stream has not been completely received, we must tell the sender to stop sending and
       clean out the stream channel during the close of the receive handle. */
    if (!recvh_obj->stream_received && !recvh_obj->adapter->use_buffered_protocol) {
        if (recvh_obj->has_term_channel) {
            /* Send a message to terminate the stream. The sender, if it is still streaming data,
            will look on every send to see if the stream has been terminated. However, depending
            on the depth of the stream channel, there may be several messages buffered. If so, emptying
            the channel below will take care of the rest of it. */
            _send_term_msg(recvh_obj, timeout);

            /* Done with term channel so detach. */
            dragon_channel_detach(&recvh_obj->terminate_stream_channel);
        }

        /* Now empty the stream of any unread messages. The EOT indicates the end of the
        stream whether terminated or not. Emptying the channel here one message at a time
        so we can be sure we only read up to the EOT and then stop. Otherwise we might read
        part of a second stream. */
        while (!recvh_obj->stream_received)
            _empty_the_channel(&recvh_obj->strm_channel, &recvh_obj->stream_received, 1, recvh_obj->free_mem);
    }

    if (recvh_obj->strm_opened) {
        err = dragon_chrecv_close(&recvh_obj->chan_recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close adapters stream channel receive handle.");
    }

    if (recvh_obj->strm_type == FLI_INTERNAL_STRM_CHANNEL) {
        /* If this channel is internally managed, it must be returned to the manager channel now. */
        err = _send_stream_channel(&recvh_obj->strm_channel, recvh_obj->strm_type, &recvh_obj->adapter->mgr_ch, &recvh_obj->adapter->pool, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not return stream channel to manager channel in receive handle close of FLI adapter.");
    }

    /* free any unused buffered data and the dummy node. */
    err = _free_buffered_mem(recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to free unused buffered memory on receive handle close.");

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_fli_recv_handles, recv_handle->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete item from fli receive handle umap.");

    recv_handle->_idx = 0;
    free(recvh_obj);
    recvh_obj = NULL;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_set_free_flag(dragonFLIRecvHandleDescr_t* recv_handle) {

    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli receive handle descriptor");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    recvh_obj->free_mem = true;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_reset_free_flag(dragonFLIRecvHandleDescr_t* recv_handle) {

    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli receive handle descriptor");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    recvh_obj->free_mem = false;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_stream_received(dragonFLIRecvHandleDescr_t* recv_handle, bool* stream_received) {

    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli receive handle descriptor");

    if (stream_received == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stream_received variable cannot be NULL.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    *stream_received = recvh_obj->stream_received;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_create_writable_fd(dragonFLISendHandleDescr_t* send_handle, int* fd_ptr,
                              const bool buffer, size_t chunk_size,
                              const uint64_t user_arg, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The FLI send handle cannot be NULL.");

    if (fd_ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The file descriptor pointer cannot be NULL.");

    if (chunk_size == 0)
        chunk_size = DEFAULT_CHUNK_SIZE;

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (sendh_obj->tid != 0)
        err_return(DRAGON_INVALID_OPERATION, "Cannot create a file descriptor when another is in use. Close and finalize first.");

    /* Create the pipe. */
    if (pipe(sendh_obj->pipe))
        err_return(DRAGON_FAILURE, "Could not create a pipe for the file descriptor open.");

    _SenderArg_t* arg = malloc(sizeof(_SenderArg_t));
    if (arg == NULL)
        err_return (DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for malloc'ed thread argument.");

    arg->sendh = send_handle;
    arg->user_arg = user_arg;
    arg->chunk_size = chunk_size;
    arg->buffer = buffer;
    arg->fd = sendh_obj->pipe[0]; /* pass along the read end to the thread. */

    int perr = pthread_create(&sendh_obj->tid, NULL, _from_fd_to_fli, arg);

    if (perr != 0) {
        char err_str[200];
        sendh_obj->tid = 0;
        snprintf(err_str, 199, "There was an error on the pthread_create call. ERR=%d", perr);
        err_return(DRAGON_FAILURE, err_str);
    }

    *fd_ptr = sendh_obj->pipe[1];
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_finalize_writable_fd(dragonFLISendHandleDescr_t* send_handle) {
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The FLI send handle cannot be NULL.");

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (sendh_obj->tid != 0) {
        int perr;
        void* retval;

        /* We must join here to prevent the receive handle from being destroyed before the thread
           managing the file descriptor exits. */
        perr = pthread_join(sendh_obj->tid, &retval);

        if (perr != 0) {
            char err_str[200];
            snprintf(err_str, 199, "There was an error on the pthread_join call while closing the send handle. ERR=%d", perr);
            err_return(DRAGON_FAILURE, err_str);
        }
        sendh_obj->tid = 0;
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_create_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle, int* fd_ptr, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The FLI receive handle cannot be NULL.");

    if (fd_ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The file descriptor pointer cannot be NULL.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->tid != 0)
        err_return(DRAGON_INVALID_OPERATION, "Cannot create a file descriptor when another is in use. Close and finalize first.");

    /* Create the pipe. */
    if (pipe(recvh_obj->pipe)) {
        err_return(DRAGON_FAILURE, "Could not create a pipe for the file descriptor open.");
    }

    _ReceiverArg_t* arg = malloc(sizeof(_ReceiverArg_t));
    if (arg == NULL)
        err_return (DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for malloc'ed thread argument.");

    arg->recvh = recv_handle;
    arg->fd = recvh_obj->pipe[1]; /* pass along the write end to the thread. */

    int perr = pthread_create(&recvh_obj->tid, NULL, _from_fli_to_fd, arg);

    if (perr != 0) {
        char err_str[200];
        recvh_obj->tid = 0;
        snprintf(err_str, 199, "There was an error on the pthread_create call. ERR=%d", perr);
        err_return(DRAGON_FAILURE, err_str);
    }

    *fd_ptr = recvh_obj->pipe[0]; /* Give the read end back to the caller. */
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_finalize_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle) {
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The FLI receive handle cannot be NULL.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->tid != 0) {
        int perr;
        void* retval;

        /* We must join here to prevent the receive handle from being destroyed before the thread managing
           the file descriptor exits. */
        perr = pthread_join(recvh_obj->tid, &retval);

        if (perr != 0) {
            char err_str[200];
            snprintf(err_str, 199, "There was an error on the pthread_kill call while closing the receive handle. ERR=%d", perr);
            err_return(DRAGON_FAILURE, err_str);
        }
        recvh_obj->tid = 0;
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout) {
    dragonError_t err;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (arg >= FLI_RESERVED_HINTS)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot specify an arg value greater than or equal to 0xFFFFFFFFFFFFFF00. These values are reserved for internal use.");

    err = _fli_send_bytes(send_handle, num_bytes, bytes, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Call of internal send bytes failed");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_send_mem(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem,
                    uint64_t arg, bool transfer_ownership, bool no_copy_read_only, const timespec_t* timeout) {
    dragonError_t err;
    dragonMemoryPoolDescr_t* dest_pool = NULL;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    uint64_t count = 0;

    if (arg >= FLI_RESERVED_HINTS)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot specify an arg value greater than or equal to 0xFFFFFFFFFFFFFF00. These values are reserved for internal use.");

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a valid memory descriptor pointer.");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (sendh_obj->buffered_send)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot use dragon_fli_send_mem on a buffered fli adapter or send handle. Use dragon_fli_send_bytes instead.");

    /* First send any buffered bytes that have not been sent. If there are none, it will just return. */
    err = _send_buffered_bytes(sendh_obj, false, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to send buffered bytes before sending memory.");

    /* Check that for a streaming connection the receiver has not closed the receive handle. If
       it has, then exit with appropriate return code. */
    if (sendh_obj->has_term_channel) {
        err = dragon_channel_message_count(&sendh_obj->terminate_stream_channel, &count);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get message count of termination channel");

        /* If a message has been received in the terminate_stream_channel, the receiver has
           canceled receipt of this stream by closing the receive handle. */
        if (count > 0)
            err_return(DRAGON_EOT, "Sending of the stream has been canceled by the receiver.");
    }

    if (sendh_obj->has_dest_pool)
        dest_pool = &sendh_obj->dest_pool;

    /* sending mem on stream channel */

    err = _send_mem(&sendh_obj->chan_sendh, mem, arg, transfer_ownership, no_copy_read_only, sendh_obj->turbo_mode, false, dest_pool, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the managed memory down the stream channel.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_get_buffered_bytes(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem_descr, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    uint64_t arg_val;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (mem_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor descriptor");

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _get_buffered_bytes(sendh_obj, mem_descr, &arg_val, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get the buffered bytes.");

    if (arg != NULL)
        *arg = arg_val;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t dragon_fli_recv_bytes_into(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t* bytes, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    uint8_t* buffer_ptr = bytes;

    if (bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to the allocated space for the received bytes.");

    err = _recv_bytes_common(recv_handle, requested_size, received_size, &buffer_ptr, arg, timeout);
    if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
        append_err_return(err, "Could not receive bytes into.");

    no_err_return(err);
}


dragonError_t dragon_fli_recv_bytes(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t** bytes, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;

    if (bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a pointer for the received bytes.");

    /* Initializing the pointer to NULL guarantees that the internal function will allocate space for the
       received bytes. */
    *bytes = NULL;

    err = _recv_bytes_common(recv_handle, requested_size, received_size, bytes, arg, timeout);
    if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
        append_err_return(err, "Could not receive bytes.");

    no_err_return(err);
}


dragonError_t dragon_fli_recv_mem(dragonFLIRecvHandleDescr_t* recv_handle, dragonMemoryDescr_t* mem,
                uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    dragonMemoryPoolDescr_t* dest_pool = NULL;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    size_t received_size;
    uint64_t arg_val;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid FLI receive handle descriptor");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a memory descriptor.");

    if (arg == NULL)
        arg = &arg_val;

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (!recvh_obj->buffered_receive && !recvh_obj->recv_called) {
        /* It could be that this fails because sender may have closed send handle
           already. If so, that's fine because sender is done sending then. This
           is done here and not when the recv handle is opened because then
           opening the receive handle will only block if there is no available
           stream channel which seems more intuitive. */
        recvh_obj->recv_called = true;
        err = _get_term_channel(recv_handle, timeout);
        if (err == DRAGON_EOT)
            append_err_return(err, "Got EOT");
    }

    /* If we find buffered memory that is available, return it. */
    err = _unbuffer_mem(recvh_obj, mem, arg);
    if (err == DRAGON_SUCCESS) {
        if (recvh_obj->buffered_receive)
            /* When buffered, mark stream as received after first read. */
            recvh_obj->stream_received = true;
        no_err_return(DRAGON_SUCCESS);
    }

    if (recvh_obj->stream_received)
        /* data has been read already so we return our end of stream error code. */
        err_return(DRAGON_EOT, "End of Stream. You must close and re-open receive handle.");

    if (recvh_obj->has_dest_pool)
        dest_pool = &recvh_obj->dest_pool;

    err = _recv_mem(&recvh_obj->chan_recvh, mem, arg, dest_pool, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error occurred while receiving data.");

    if (*arg == FLI_EOT) {
        recvh_obj->stream_received = true;
        *arg = 0; /* FLI_EOT is internal only so don't expose it. */
        dragon_memory_free(mem);
        err_return(DRAGON_EOT, "Reached the end of stream");
    }

    err = dragon_memory_get_size(mem, &received_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size of received memory descriptor.");

    recvh_obj->num_bytes_received += received_size;

    if (recvh_obj->buffered_receive)
        /* When buffered, mark stream as received after first read. */
        recvh_obj->stream_received = true;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_poll(const dragonFLIDescr_t* adapter, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;
    dragonULInt num_msgs = 0;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    err = dragon_channel_poll(&obj->main_ch, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLSIZE, timeout, &num_msgs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve the number of messages in the channel.");

    if (num_msgs == 0)
        err_return(DRAGON_EMPTY, "There were no messages in the channel.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_fli_full(const dragonFLIDescr_t* adapter) {
    dragonError_t err;
    dragonFLI_t* obj;
    timespec_t timeout = {0, 0};

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    err = dragon_channel_poll(&obj->main_ch, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLFULL, &timeout, NULL);

    if (err == DRAGON_TIMEOUT)
        no_err_return(DRAGON_NOT_FULL);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unexpected error occured.");

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t dragon_fli_num_msgs(const dragonFLIDescr_t* adapter, size_t* num_msgs, const timespec_t* timeout) {
    dragonError_t err;
    dragonFLI_t* obj;
    dragonULInt dnum_msgs = 0;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    err = dragon_channel_poll(&obj->main_ch, DRAGON_DEFAULT_WAIT_MODE, DRAGON_CHANNEL_POLLSIZE, timeout, &dnum_msgs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not retrieve the number of messages in the channel.");

    *num_msgs = dnum_msgs;

    no_err_return(DRAGON_SUCCESS);
}
