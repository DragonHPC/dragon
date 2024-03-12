#include <dragon/fli.h>
#include "_fli.h"
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

static dragonMap_t* dg_fli_adapters = NULL;
static dragonMap_t* dg_fli_send_handles = NULL;
static dragonMap_t* dg_fli_recv_handles = NULL;

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
static dragonError_t
_fli_from_descr(const dragonFLIDescr_t* adapter, dragonFLI_t** fli)
{
    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_adapters, adapter->_idx, (void*)fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli adapters map");

    no_err_return(DRAGON_SUCCESS);
}

/* obtain an fli structure from a given send handle descriptor */
static dragonError_t
_fli_sendh_from_descr(const dragonFLISendHandleDescr_t* send_descr, dragonFLISendHandle_t** send_handle)
{
    if (send_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_send_handles, send_descr->_idx, (void*)send_handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli send handles map");

    no_err_return(DRAGON_SUCCESS);
}

/* obtain an fli structure from a given recv handle descriptor */
static dragonError_t
_fli_recvh_from_descr(const dragonFLIRecvHandleDescr_t* recv_descr, dragonFLIRecvHandle_t** recv_handle)
{
    if (recv_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli recv handle descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_fli_recv_handles, recv_descr->_idx, (void*)recv_handle);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in fli recv handles map");

    no_err_return(DRAGON_SUCCESS);
}

/* insert an fli structure into the unordered map using the adapter->_idx as the key */
static dragonError_t
_add_umap_fli_entry(dragonFLIDescr_t* adapter, const dragonFLI_t* fli)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (dg_fli_adapters == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        dg_fli_adapters = malloc(sizeof(dragonMap_t));
        if (dg_fli_adapters == NULL)
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
static dragonError_t
_add_umap_fli_sendh_entry(dragonFLISendHandleDescr_t* send_descr, const dragonFLISendHandle_t* send_handle)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (dg_fli_send_handles == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        dg_fli_send_handles = malloc(sizeof(dragonMap_t));
        if (dg_fli_send_handles == NULL)
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
static dragonError_t
_add_umap_fli_recvh_entry(dragonFLIRecvHandleDescr_t* recv_descr, const dragonFLIRecvHandle_t* recv_handle)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (dg_fli_recv_handles == NULL) {
        /* this is a process-global variable and has no specific call to be
         * destroyed */
        dg_fli_recv_handles = malloc(sizeof(dragonMap_t));
        if (dg_fli_recv_handles == NULL)
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


static dragonError_t
_validate_attr(const dragonFLIAttr_t* attr)
{
    return DRAGON_NOT_IMPLEMENTED;
}

static dragonError_t
_send_mem(dragonChannelSendh_t* sendh, dragonMemoryDescr_t* mem, uint64_t arg, timespec_t* deadline)
{
    dragonError_t err;
    timespec_t remaining_time;
    timespec_t* timeout = NULL;
    dragonMessage_t msg;
    dragonMessageAttr_t msg_attrs;

    if (sendh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a channel send handle to send a message.");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a memory descriptor.");

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

    err = dragon_channel_message_init(&msg, mem, &msg_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize serialized stream channel message.");

    err = dragon_chsend_send_msg(sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not add serialized stream channel to manager channel.");

    err = dragon_channel_message_destroy(&msg, false);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy message.");

    err = dragon_channel_message_attr_destroy(&msg_attrs);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not destroy message attributes.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_send_bytes(dragonChannelSendh_t* chan_sendh, dragonMemoryPoolDescr_t* pool, uint8_t* bytes, size_t num_bytes, uint64_t arg, timespec_t* deadline)
{
    dragonError_t err;
    dragonMemoryDescr_t mem_descr;
    void* mem_ptr;

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot send bytes without a pool for allocations.");

    if (bytes == NULL && num_bytes != 0)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide bytes when sending a non-zero number of bytes.");

    err = dragon_memory_alloc(&mem_descr, pool, num_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get shared memory for message data.");

    if (num_bytes > 0) {
        err = dragon_memory_get_pointer(&mem_descr, &mem_ptr);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get pointer for shared memory.");
        memcpy(mem_ptr, bytes, num_bytes);
    }

    err = _send_mem(chan_sendh, &mem_descr, arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error when calling internal _send_mem.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_send_buffered_bytes(dragonFLISendHandle_t* sendh, timespec_t* deadline)
{
    dragonError_t err;
    dragonMemoryDescr_t mem_descr;
    void* mem_ptr;
    void* dest_ptr;
    dragonFLISendBufAlloc_t* node;
    dragonFLISendBufAlloc_t* prev;


    err = dragon_memory_alloc(&mem_descr, &sendh->adapter->pool, sendh->total_bytes);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get shared memory for message data.");

    err = dragon_memory_get_pointer(&mem_descr, &mem_ptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get pointer for shared memory.");

    dest_ptr = mem_ptr + sendh->total_bytes;
    node = sendh->buffered_allocations;

    while (node != NULL) {
        dest_ptr = dest_ptr - node->num_bytes;
        memcpy(dest_ptr, node->data, node->num_bytes);
        prev = node;
        node = node->next;
        free(prev->data);
        free(prev);
    }

    if (dest_ptr != mem_ptr)
        err_return(DRAGON_INVALID_OPERATION, "There was an error while unbuffering data in send operation.");

    err = _send_mem(&sendh->chan_sendh, &mem_descr, sendh->buffered_arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error when calling internal _send_mem.");

    sendh->buffered_allocations = NULL;
    sendh->total_bytes = 0;
    sendh->buffered_arg = 0;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_buffer_bytes(dragonFLISendHandle_t* sendh, uint8_t* bytes, size_t num_bytes, uint64_t arg) {
        void* data_ptr;
        dragonFLISendBufAlloc_t* node_ptr;

        if (sendh->buffered_allocations == NULL)
            /* first write, so grab the user's meta data arg */
            sendh->buffered_arg = arg;

        if (num_bytes > 0) {
            data_ptr = malloc(num_bytes);
            if (data_ptr == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space to buffer data - out of memory.");

            node_ptr = malloc(sizeof(dragonFLISendBufAlloc_t));
            if (node_ptr == NULL)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for buffering data - out of memory.");

            memcpy(data_ptr, bytes, num_bytes);

            node_ptr->data = data_ptr;
            node_ptr->num_bytes = num_bytes;
            sendh->total_bytes+=num_bytes;
            node_ptr->next = sendh->buffered_allocations;
            sendh->buffered_allocations = node_ptr;
        }

        no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_recv_mem(dragonChannelRecvh_t* recvh, dragonMemoryDescr_t* mem, uint64_t* arg, timespec_t* deadline)
{
    dragonError_t err;
    dragonMessage_t msg;
    dragonMessageAttr_t attrs;

    if (recvh == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide non-null receive handle.");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide non-null memory descriptor");

    if (arg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Must provide a non-null arg variable pointer.");

    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize message structure.");

    err = dragon_chrecv_get_msg_blocking(recvh, &msg, deadline);
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

static dragonError_t
_recv_bytes_into(dragonChannelRecvh_t* recvh, uint8_t** data, size_t* num_bytes, uint64_t* arg, timespec_t* deadline)
{
    dragonError_t err;
    dragonMemoryDescr_t mem;
    void* mem_ptr;

    if (data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a non-null data pointer address");

    if (num_bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a non-null size_t pointer");

    err = _recv_mem(recvh, &mem, arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive message in _recv_mem.");

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

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_recv_bytes_buffered(dragonFLIRecvHandle_t* recvh, size_t requested_size, size_t* received_size, uint8_t** data, uint64_t* arg, timespec_t* deadline)
{
    dragonError_t err = DRAGON_SUCCESS;
    void* src_ptr = NULL;
    void* dest_ptr = NULL;
    size_t cpy_bytes = 0;
    dragonFLIRecvBufAlloc_t* node = NULL;
    dragonFLIRecvBufAlloc_t* prev = NULL;
    dragonMemoryDescr_t chunk_mem;
    size_t chunk_size = 0;
    uint64_t chunk_arg = 0;
    size_t alloc_sz = 0;
    size_t node_bytes = 0;

    /* Init return data to default values. */
    *received_size = 0;
    *arg = 0;

    if (requested_size == 0 && recvh->buffered_bytes == 0) {
        err = _recv_bytes_into(&recvh->chan_recvh, data, received_size, arg, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not receive bytes in helper routine.");

        no_err_return(DRAGON_SUCCESS);
    }

    if (!recvh->EOT_received) {
        while (chunk_arg != FLI_EOT && recvh->buffered_bytes < requested_size) {
            err = _recv_mem(&recvh->chan_recvh, &chunk_mem, &chunk_arg, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get data and buffer it in file-like adapter.");

            err = dragon_memory_get_size(&chunk_mem, &chunk_size);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get chunk size while buffering data.");

            if (chunk_size > 0) {
                node = malloc(sizeof(dragonFLIRecvBufAlloc_t));
                if (node == NULL)
                    err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate node for buffered data.");
                err = dragon_memory_descr_clone(&node->mem, &chunk_mem, 0, NULL);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Unable to clone mem descriptor while buffering data.");
                node->num_bytes = chunk_size;
                node->offset = 0;
                node->arg = chunk_arg;
                node->next = NULL;
                recvh->buffered_bytes += chunk_size;
                recvh->tail->next = node;
                recvh->tail = node;
            }
            else {
                if (chunk_arg == FLI_EOT)
                    recvh->EOT_received = true;

                /* We have a zero sized memory descriptor to free. */
                err = dragon_memory_free(&chunk_mem);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not free zero sized memory chunk while buffering data.");
            }
        }
    }

    /* We set the size to the minimum of what was asked for or what is available. */
    alloc_sz = requested_size;
    if (recvh->buffered_bytes < requested_size)
        alloc_sz = recvh->buffered_bytes;

    /* Now check if there is any data left to return. */
    if (alloc_sz == 0 && recvh->EOT_received) {
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
            err = dragon_memory_free(&node->mem);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not free buffered managed memory.");
            free(node);
            node = prev->next;
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_recv_bytes_common(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                size_t* received_size, uint8_t** bytes, uint64_t* arg, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

    if (arg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a variable for the received arg metadata.");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->stream_received)
        /* data has been read already so we return our end of stream error code. */
        err_return(DRAGON_EOT, "End of Stream. You must close and re-open receive handle.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    err = _recv_bytes_buffered(recvh_obj, requested_size, received_size, bytes, arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error occurred while receiving data.");

    recvh_obj->num_bytes_received += *received_size;

    if (*received_size == 0 && *arg == FLI_EOT) {
        recvh_obj->stream_received = true;
        *arg = 0; /* FLI_EOT is internal only so don't expose it. */
        no_err_return(DRAGON_EOT);
    }

    if (recvh_obj->adapter->use_buffered_protocol)
        /* When buffered, mark stream as received after first read. */
        recvh_obj->stream_received = true;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_send_stream_channel(const dragonChannelDescr_t* strm_ch, const dragonChannelDescr_t* to_chan, dragonMemoryPoolDescr_t* pool, timespec_t* deadline)
{
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

    err = _send_bytes(&sendh, pool, ser.data, ser.len, 0, deadline);
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

static dragonError_t
_recv_stream_channel(dragonChannelDescr_t* from_chan, dragonChannelDescr_t* strm_ch, timespec_t* deadline)
{
    dragonError_t err;
    dragonChannelSerial_t ser;
    dragonChannelRecvh_t recvh;
    uint64_t arg;

    if (strm_ch == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The stream channel descriptor cannot be NULL.");

    if (from_chan == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The channel to receive from cannot be NULL.");

    err = dragon_channel_recvh(from_chan, &recvh, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not initialize receive handle.");

    err = dragon_chrecv_open(&recvh);
    if (err != DRAGON_SUCCESS)
         append_err_return(err, "Could not open send handle on channel.");

    /* We zero the pointer so the _recv_bytes_into will allocate space for us. */
    ser.data = NULL;

    err = _recv_bytes_into(&recvh, &ser.data, &ser.len, &arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive the stream channel.");

    err = dragon_channel_attach(&ser, strm_ch);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach stream channel");

    err = dragon_channel_serial_free(&ser);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free stream channel serialized descriptor.");

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_empty_the_channel(dragonChannelDescr_t* channel)
{
    dragonError_t err;
    dragonChannelRecvh_t recvh;
    timespec_t deadline = {0,0};

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

        err = _recv_mem(&recvh, &mem, &arg, &deadline);

        if (err == DRAGON_SUCCESS) {
            err = dragon_memory_free(&mem);
        }
    }

    if (err != DRAGON_CHANNEL_EMPTY)
        append_err_return(err, "There was an error emptying a channel in the fli adapter.");

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle on channel being emptied.");

    no_err_return(DRAGON_SUCCESS);
}


static void*
_from_fd_to_fli (void* ptr)
{
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
        return NULL;
    }

    err = DRAGON_SUCCESS;
    while ((err == DRAGON_SUCCESS) && ((num_bytes = read(arg->fd, buffer, arg->chunk_size)) > 0))
        err = dragon_fli_send_bytes(arg->sendh, num_bytes, buffer, user_arg, arg->buffer, NULL);


    if (err != DRAGON_SUCCESS) {
        /* err might be logged eventually. But no way to return the error to user.
        They will see a problem with the file descriptor. */
        fprintf(stderr, "ERROR: There was an error sending bytes through the fli interface (ERR=%s).\n", dragon_get_rc_string(err));
    }

    close(fd);
    free(buffer);

    if (arg->buffer) {
        err = dragon_fli_send_bytes(arg->sendh, 0, NULL, 0, false, NULL);
        if (err != DRAGON_SUCCESS)
            fprintf(stderr, "ERROR: Could not flush the buffered bytes from the file descriptor thread helper.");
    }

    pthread_exit(NULL);
}


static void*
_from_fli_to_fd (void* ptr)
{
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
    }

    if (err != DRAGON_EOT) {
        /* err might be logged eventually. */
        fprintf(stderr, "ERROR: There was an error receiving data from the fli interface (ERR=%s).\n", dragon_get_rc_string(err));
    }

    close(fd);
    pthread_exit(NULL);
}

/****************************************************************************************/
/* Beginning of user API                                                                */
/****************************************************************************************/

dragonError_t
dragon_fli_attr_init(dragonFLIAttr_t* attr)
{
    attr->_placeholder = 0;

    return DRAGON_SUCCESS;
}


dragonError_t
dragon_fli_create(dragonFLIDescr_t* adapter, dragonChannelDescr_t* main_ch,
                  dragonChannelDescr_t* mgr_ch, dragonMemoryPoolDescr_t* pool,
                  const dragonULInt num_strm_chs, dragonChannelDescr_t** strm_channels,
                  const bool use_buffered_protocol, dragonFLIAttr_t* attrs)
{
    dragonError_t err;
    dragonFLIAttr_t def_attr;
    uint64_t msg_count;
    timespec_t deadline = {0,0}; /* try once only */

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

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

        err = dragon_channel_message_count(&obj->main_ch, &msg_count);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get the main channel message count during creation.");

        if (msg_count > 0)
            err_return(DRAGON_INVALID_ARGUMENT, "The main channel has items in it during adapter creation.");

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

    obj->num_strm_chs = num_strm_chs;

    for (int idx=0; idx<num_strm_chs; idx++) {
        err = _send_stream_channel(strm_channels[idx], &obj->mgr_ch, &obj->pool, &deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not deposit stream channel into manager channel.");
    }

    err = _add_umap_fli_entry(adapter, obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created adapter");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_destroy(dragonFLIDescr_t* adapter)
{
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    if (obj->has_mgr_ch) {
        err = _empty_the_channel(&obj->mgr_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not empty the manager channel.");
    }

    if (obj->has_main_ch) {
        err = _empty_the_channel(&obj->main_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not empty the main channel.");
    }

    err = dragon_umap_delitem(dg_fli_adapters, adapter->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete adapter from from adapters umap");

    free(obj);
    adapter->_idx = 0;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_serialize(const dragonFLIDescr_t* adapter, dragonFLISerial_t* serial)
{
    dragonError_t err;
    dragonFLI_t* obj;
    uint8_t adapter_type = 0;
    dragonChannelSerial_t main_ch_ser;
    dragonChannelSerial_t mgr_ch_ser;
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

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_serial_free(dragonFLISerial_t* serial)
{
    if (serial == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid serialized fli adapter.");

    if (serial->data == NULL)
        no_err_return(DRAGON_SUCCESS);

    free(serial->data);
    serial->data = NULL;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_attach(const dragonFLISerial_t* serial, const dragonMemoryPoolDescr_t* pool,
                  dragonFLIDescr_t* adapter)
{
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

    if (obj->has_main_ch) {
        memcpy(&ch_ser.len, ptr, sizeof(ch_ser.len));
        ptr+=sizeof(ch_ser.len);
        ch_ser.data = ptr;
        err = dragon_channel_attach(&ch_ser, &obj->main_ch);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot attach to main channel of adapter.");
        ptr+=ch_ser.len;
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

    err = _add_umap_fli_entry(adapter, obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for attached adapter");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_detach(dragonFLIDescr_t* adapter)
{
    dragonError_t err;
    dragonFLI_t* obj;

    if (adapter == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli adapter descriptor");

    err = _fli_from_descr(adapter, &obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve adapter to internal fli object");

    err = dragon_umap_delitem(dg_fli_adapters, adapter->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete adapter from from adapters umap");

    free(obj);
    adapter->_idx = 0;

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_open_send_handle(const dragonFLIDescr_t* adapter, dragonFLISendHandleDescr_t* send_handle,
                            dragonChannelDescr_t* strm_ch, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLI_t* obj;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

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

    if (obj->use_buffered_protocol && strm_ch != NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot supply a stream channel while using buffered protocol");

    sendh_obj = malloc(sizeof(dragonFLISendHandle_t));

    if (sendh_obj == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate send handle.");

    sendh_obj->adapter = obj;
    sendh_obj->buffered_allocations = NULL;
    sendh_obj->total_bytes = 0;
    sendh_obj->tid = 0;

    if (obj->use_buffered_protocol) {
        /* The main channel send handle will be opened only when a buffered message is written,
           which occurs when this send handle is closed.*/
        sendh_obj->user_supplied = false;
        err = dragon_channel_sendh(&obj->main_ch, &sendh_obj->chan_sendh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create send handle on main channel.");

        err = dragon_chsend_open(&sendh_obj->chan_sendh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open send handle on main channel.");

    } else {
        if (strm_ch == STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION) {
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a main channel when specifying to use main channel as stream channel.");

            if (obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You cannot use 1:1 mode on the fli when there is a manager channel.");

            sendh_obj->user_supplied = false;

            err = dragon_channel_descr_clone(&sendh_obj->strm_channel, &obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone main channel descriptor as stream channel descriptor.");
        } else if (strm_ch != NULL) {
            /* This is a user-supplied channel so we keep track of that. */
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a main channel when a sender provided stream channel is given.");

            sendh_obj->user_supplied = true;

            err = dragon_channel_descr_clone(&sendh_obj->strm_channel, strm_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone stream channel descriptor.");
        } else {
            /* It is a manager supplied stream. */
            if (!obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You must provide a stream channel when there is no manager channel.");

            /* We are using a stream channel from the manager channel. */
            sendh_obj->user_supplied = false;

            err = _recv_stream_channel(&obj->mgr_ch, &sendh_obj->strm_channel, deadline);
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
            err = _send_stream_channel(strm_ch, &obj->main_ch, &obj->pool, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not deposit stream channel into main channel.");
        }
    }

    err = _add_umap_fli_sendh_entry(send_handle, sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created send handle");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_close_send_handle(dragonFLISendHandleDescr_t* send_handle, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

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
        /* buffered bytes remain to send on close of send handle. */
        err = _send_buffered_bytes(sendh_obj, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send buffered data.");
    }

    if (!sendh_obj->adapter->use_buffered_protocol) {
        /* sending the EOT indicator for the stream. */
        err = _send_bytes(&sendh_obj->chan_sendh, &sendh_obj->adapter->pool, NULL, 0, FLI_EOT, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the end of stream indicator down the stream channel.");
    }

    err = dragon_chsend_close(&sendh_obj->chan_sendh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle on channel");

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_fli_send_handles, send_handle->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete item from fli send handle umap.");

    send_handle->_idx = 0;

    free(sendh_obj);

    no_err_return(DRAGON_SUCCESS);

}

dragonError_t
dragon_fli_open_recv_handle(const dragonFLIDescr_t* adapter, dragonFLIRecvHandleDescr_t* recv_handle,
                            dragonChannelDescr_t* strm_ch, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLI_t* obj;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

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

    recvh_obj->adapter = obj;
    recvh_obj->user_supplied = false;
    recvh_obj->stream_received = false;
    recvh_obj->EOT_received = false;
    recvh_obj->num_bytes_received = 0;
    recvh_obj->buffered_bytes = 0;
    recvh_obj->tid = 0;

    /* Creating a dummy head node simplifies management of the linked list. */
    recvh_obj->buffered_data = malloc(sizeof(dragonFLIRecvBufAlloc_t));
    if (recvh_obj->buffered_data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not malloc dummy node in receive handle.");

    recvh_obj->buffered_data->num_bytes = 0;
    recvh_obj->buffered_data->next = NULL;
    recvh_obj->tail = recvh_obj->buffered_data;

    if (obj->use_buffered_protocol) {
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

            recvh_obj->user_supplied = false ;

            err = dragon_channel_descr_clone(&recvh_obj->strm_channel, &obj->main_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone main channel as stream channel descriptor.");

        } else if (strm_ch != NULL) {
            /* A user supplied stream channel will be used. */
            if (!obj->has_mgr_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "The adapter needs a manager channel when a receiver provided stream channel is given.");

            /* This is a user-supplied channel so we keep track of that. */
            recvh_obj->user_supplied = true;

            err = dragon_channel_descr_clone(&recvh_obj->strm_channel, strm_ch);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Cannot clone stream channel descriptor.");

            /* We add it to the manager channel so a sender can pick it up. */
            err = _send_stream_channel(strm_ch, &obj->mgr_ch, &obj->pool, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not deposit stream channel into manager channel.");

        } else {
            /* A main channel supplied stream channel will be used */
            if (!obj->has_main_ch)
                err_return(DRAGON_INVALID_ARGUMENT, "You must provide a stream channel when there is no main channel.");

            /* We are using a stream channel from the main channel. */
            err = _recv_stream_channel(&obj->main_ch, &recvh_obj->strm_channel, deadline);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get stream channel from manager channel.");
        }

        err = dragon_channel_recvh(&recvh_obj->strm_channel, &recvh_obj->chan_recvh, NULL);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create recv handle on stream channel.");

        err = dragon_chrecv_open(&recvh_obj->chan_recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open recv handle on stream channel.");
    }

    err = _add_umap_fli_recvh_entry(recv_handle, recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to add umap entry for created receive handle");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_close_recv_handle(dragonFLIRecvHandleDescr_t* recv_handle, const timespec_t* timeout)
{
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

    err = dragon_chrecv_close(&recvh_obj->chan_recvh);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close adapters stream channel receive handle.");

    /* We check here that the entire stream had been read. Otherwise we are not
       done with the stream and we will be leaving partial data in the stream
       which is not good since it would lead to unpredictable results on
       future reads. */
    if (!recvh_obj->stream_received)
        err_return(DRAGON_INVALID_OPERATION, "Cannot close receive handle with partially read stream.");

    if (!recvh_obj->user_supplied && !recvh_obj->adapter->use_buffered_protocol && recvh_obj->adapter->has_mgr_ch) {
        /* We are not using the buffered protocol and stream channel must be
           returned to the manager channel if there is one. */
        err = _send_stream_channel(&recvh_obj->strm_channel, &recvh_obj->adapter->mgr_ch, &recvh_obj->adapter->pool, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not return stream channel to manager channel in receive handle close of FLI adapter.");
    }

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_fli_recv_handles, recv_handle->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to delete item from fli receive handle umap.");

    recv_handle->_idx = 0;
    free(recvh_obj);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_fli_create_writable_fd(dragonFLISendHandleDescr_t* send_handle, int* fd_ptr,
                              const bool buffer, size_t chunk_size,
                              const uint64_t user_arg, const timespec_t* timeout)
{
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

dragonError_t
dragon_fli_finalize_writable_fd(dragonFLISendHandleDescr_t* send_handle)
{
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

dragonError_t
dragon_fli_create_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle, int* fd_ptr, const timespec_t* timeout)
{
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

dragonError_t
dragon_fli_finalize_readable_fd(dragonFLIRecvHandleDescr_t* recv_handle)
{
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

dragonError_t
dragon_fli_send_bytes(dragonFLISendHandleDescr_t* send_handle, size_t num_bytes,
                uint8_t* bytes, uint64_t arg, const bool buffer, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;

    if (send_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid fli send handle descriptor");

    if (bytes == NULL && num_bytes > 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot send non-zer number of bytes with NULL pointer.");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_sendh_from_descr(send_handle, &sendh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve send handle to internal fli send handle object");

    /* buffering bytes to send */
    err = _buffer_bytes(sendh_obj, bytes, num_bytes, arg);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not buffer bytes.");

    if (!sendh_obj->adapter->use_buffered_protocol && !buffer) {
        /* buffered bytes are sent when not buffered protocol and flushing was
           requested (default behavior) */
        err = _send_buffered_bytes(sendh_obj, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send data.");
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_send_mem(dragonFLISendHandleDescr_t* send_handle, dragonMemoryDescr_t* mem,
                    uint64_t arg, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLISendHandle_t* sendh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;


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

    if (sendh_obj->adapter->use_buffered_protocol)
        err_return(DRAGON_INVALID_ARGUMENT, "You cannot use dragon_fli_send_mem on a buffered fli adapter. Use dragon_fli_send_bytes instead.");

    /* sending mem on stream channel */
    err = _send_mem(&sendh_obj->chan_sendh, mem, arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the managed memory down the stream channel.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_fli_recv_bytes_into(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t* bytes, uint64_t* arg,
                      const timespec_t* timeout)
{
    dragonError_t err;
    uint8_t* buffer_ptr = bytes;

    if (bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to the allocated space for the received bytes.");

    err = _recv_bytes_common(recv_handle, requested_size, received_size, &buffer_ptr, arg, timeout);
    if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
        append_err_return(err, "Could not receive bytes into.");

    no_err_return(err);
}


dragonError_t
dragon_fli_recv_bytes(dragonFLIRecvHandleDescr_t* recv_handle, size_t requested_size,
                      size_t* received_size, uint8_t** bytes, uint64_t* arg,
                      const timespec_t* timeout)
{
    dragonError_t err;

    if (bytes == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a pointer for the received bytes.");

    /* Initializing the pointer to NULL guarantees that the internal function will allocate space for the
       received bytes. */
    *bytes = NULL;

    err = _recv_bytes_common(recv_handle, requested_size, received_size, bytes, arg, timeout);
    if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
        append_err_return(err, "Could not receive bytes into.");

    no_err_return(err);
}


dragonError_t
dragon_fli_recv_mem(dragonFLIRecvHandleDescr_t* recv_handle, dragonMemoryDescr_t* mem,
                uint64_t* arg, const timespec_t* timeout)

{
    dragonError_t err;
    dragonFLIRecvHandle_t* recvh_obj;
    timespec_t* deadline = NULL;
    timespec_t end_time;
    size_t received_size;

    if (recv_handle == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid FLI receive handle descriptor");

    if (mem == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a memory descriptor.");

    if (arg == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a variable for the received arg metadata.");

    if (timeout != NULL) {
        deadline = &end_time;
        err = dragon_timespec_deadline(timeout, deadline);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not compute timeout deadline.");
    }

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    if (recvh_obj->stream_received)
        /* data has been read already so we return our end of stream error code. */
        err_return(DRAGON_EOT, "End of Stream. You must close and re-open receive handle.");

    err = _fli_recvh_from_descr(recv_handle, &recvh_obj);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve receive handle to internal fli receive handle object");

    err = _recv_mem(&recvh_obj->chan_recvh, mem, arg, deadline);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Error occurred while receiving data.");

    err = dragon_memory_get_size(mem, &received_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get size of received memory descriptor.");

    recvh_obj->num_bytes_received += received_size;

    if (received_size == 0 && *arg == FLI_EOT) {
        recvh_obj->stream_received = true;
        *arg = 0; /* FLI_EOT is internal only so don't expose it. */
        append_err_return(DRAGON_EOT, "Reached the end of stream");
    }

    if (recvh_obj->adapter->use_buffered_protocol)
        /* When buffered, mark stream as received after first read. */
        recvh_obj->stream_received = true;

    no_err_return(DRAGON_SUCCESS);
}