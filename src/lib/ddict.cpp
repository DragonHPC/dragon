#include <dragon/ddict.h>
#include "_ddict.h"
#include "_utils.h"
#include <dragon/messages.hpp>
#include <dragon/utils.h>
#include "err.h"


/* dragon globals */
DRAGON_GLOBAL_MAP(ddict_adapters);
DRAGON_GLOBAL_MAP(ddict_reqs);

static uint64_t tag = 42; /* Tag is not needed in the messages, but is there for
                             compatibility should multiple messages need to be
                             handled simultaneously at some future point. */

static dragonError_t
_send_receive(dragonFLIDescr_t* sendto_fli, DragonMsg* send_msg, dragonFLIDescr_t* recvfrom_fli,
              DragonResponseMsg** recv_msg, const timespec_t* timeout)
{
    dragonError_t err;
    dragonFLISendHandleDescr_t sendh;
    dragonFLIRecvHandleDescr_t recvh;

    err = dragon_fli_open_send_handle(sendto_fli, &sendh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

    err = dragon_fli_open_recv_handle(recvfrom_fli, &recvh, NULL, NULL, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open recv handle.");

    err = send_msg->send(&sendh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send message.");

    err = dragon_fli_close_send_handle(&sendh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    err = recv_fli_msg(&recvh, (DragonMsg**)recv_msg, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive response message.");

    err = dragon_fli_close_recv_handle(&recvh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close receive handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_add_umap_ddict_entry(dragonDDictDescr_t * ddict, dragonDDict_t * new_ddict)
{
    dragonError_t err;

    if (*dg_ddict_adapters == NULL) {
        *dg_ddict_adapters = (dragonMap_t*)malloc(sizeof(dragonMap_t));
        if (*dg_ddict_adapters == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for ddict");

        err = dragon_umap_create(dg_ddict_adapters, DRAGON_DDICT_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for ddict");
    }

    err = dragon_umap_additem_genkey(dg_ddict_adapters, new_ddict, &ddict->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into ddict umap");

    new_ddict->dd_uid = ddict->_idx;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_ddict_from_descr(const dragonDDictDescr_t * dd_descr, dragonDDict_t ** ddict)
{
    if (dd_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid ddict descriptor");

    dragonError_t err = dragon_umap_getitem(dg_ddict_adapters, dd_descr->_idx, (void**)ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in ddict umap");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_add_umap_ddict_req_entry(const dragonDDictRequestDescr_t * descr, const dragonDDictReq_t * new_req)
{
    // dragonError_t err;

    // if (*dg_ddict_reqs == NULL) {
    //     *dg_ddict_reqs = malloc(sizeof(dragonMap_t));
    //     if (*dg_ddict_reqs == NULL)
    //         err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for ddict requests");

    //     err = dragon_umap_create(dg_ddict_reqs, DRAGON_DDICT_UMAP_SEED);
    //     if (err != DRAGON_SUCCESS)
    //         append_err_return(err, "failed to create umap for ddict requests");
    // }

    // err = dragon_umap_additem(dg_ddict_reqs, descr->_idx, new_req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "failed to insert item into ddict request umap");

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

static dragonError_t
_ddict_req_from_descr(const dragonDDictRequestDescr_t * req_descr, dragonDDictReq_t ** req)
{
    // if (req_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "invalid ddict request descriptor");

    // dragonError_t err = dragon_umap_getitem(dg_ddict_reqs, req_descr->_idx, (void*)req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "failed to find item in ddict request umap");

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;

}

static dragonError_t
_send_buffered_bytes(dragonDDict_t * ddict)
{
    // dragonMemoryDescr_t mem_descr;
    // void * mem_ptr;
    // void * dest_ptr;
    // dragonDDictBufAlloc_t * node;
    // dragonDDictBufAlloc_t * prev;

    // // TODO: DDict needs a way to
    // dragonError_t err = dragon_memory_alloc(&mem_descr, NULL, ddict->total_send_buffer);

    return DRAGON_NOT_IMPLEMENTED;
}

static dragonError_t
_buffer_bytes(dragonDDictReq_t * req, uint8_t * bytes, size_t num_bytes)
{
    // void * data_ptr;
    // dragonDDictBufAlloc_t * node_ptr;

    // if (num_bytes > 0) {
    //     data_ptr = malloc(num_bytes);
    //     if (data_ptr == NULL)
    //         err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate buffer space -- OOM");

    //     node_ptr = malloc(sizeof(dragonDDictBufAlloc_t));
    //     if (node_ptr == NULL)
    //         err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate node pointer -- OOM");

    //     memcpy(data_ptr, bytes, num_bytes);

    //     node_ptr->data = data_ptr;
    //     node_ptr->num_bytes = num_bytes;
    //     req->buffer_size += num_bytes;
    //     node_ptr->next = req->buffered_allocs;
    //     req->buffered_allocs = node_ptr;
    // }

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;

}

static dragonError_t
_send_key(dragonDDictReq_t * req)
{
    // Buffer key into one blob and store it, then send it
    // dragonDDictBufAlloc_t * node = req->buffered_allocs;
    // size_t key_size = req->buffer_size;
    // req->buffered_allocs = NULL;

    // // We have to order this backwards, so jump to the end of the first memcpy start
    // void * data = (void*)malloc(key_size);
    // void * dst_ptr = data + (key_size - node->num_bytes);
    // while (node != NULL) {
    //     memcpy(dst_ptr, node->data, node->num_bytes);
    //     free(node->data);
    //     dragonDDictBufAlloc_t * tmp = node->next;
    //     free(node);
    //     node = tmp;
    //     if (node != NULL) {
    //         dst_ptr = dst_ptr - node->num_bytes;
    //     }
    // }

    // req->key_data = data;
    // // Once our key is constructed, hash it
    // req->key_hash = dragon_hash(data, key_size);
    // dragonDDictDescr_t descr;
    // descr._idx = req->dd_uid;
    // dragonDDict_t * ddict;
    // dragonError_t err = _ddict_from_descr(&descr, &ddict);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to retrieve ddict");
    // dragonFLIDescr_t fli = ddict->manager_flis[req->key_hash % ddict->num_managers];

    // err = dragon_fli_open_send_handle(&fli, &req->sendh, NULL, NULL, NULL);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to open send handle");

    // uint64_t unused_arg;
    // err = dragon_fli_send_bytes(&req->sendh, key_size, req->key_data, &unused_arg, false, NULL);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to send key to manager FLI");

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void
_set_thread_local_mode_ddict(bool set_thread_local)
{
    if (set_thread_local) {
        dg_ddict_adapters = &_dg_thread_ddict_adapters;
        dg_ddict_reqs = &_dg_thread_ddict_reqs;
    } else {
        dg_ddict_adapters = &_dg_proc_ddict_adapters;
        dg_ddict_reqs = &_dg_proc_ddict_reqs;
    }
}

// BEGIN USER API

dragonError_t
dragon_ddict_serialize(const dragonDDictDescr_t * dd, dragonDDictSerial_t * dd_ser)
{
    // if (dd == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "invalid ddict descriptor");

    // if (dd_ser == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "invalid ddict serial descriptor");

    // dd_ser->len = 0;
    // dd_ser->data = NULL;

    // dragonDDict_t * ddict;
    // dragonError_t err = _ddict_from_descr(dd, &ddict);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "invalid ddict descriptor");

    // dd_ser->len = ddict->ser.len // actually FLI serial data
    // dd_ser->data = malloc(dd_ser->len);
    // if (dd_ser->data == NULL)
    //     err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for serialized descriptor.");

    // memcpy(dd_ser->data, ddict->ser.data, ddict->ser.len);

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_serial_free(dragonDDictSerial_t * dd_ser)
{
    // if (dd_ser == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict serial descriptor");

    // if (dd_ser->data != NULL)
    //     free(dd_ser->data);

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_attach_b64(char* b64_str, dragonDDictDescr_t* obj, const timespec_t* timeout)
{
    // dragonDDictSerial_t serial;
    // dragonError_t err;

    // serial.data = dragon_base64_decode(b64_str, &serial.len);
    // err = dragon_ddict_attach(&serial, obj, timeout);

    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Could not attach to distributed dictionary.");

    // err = dragon_ddict_serial_free(&serial);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Could not free the serialized descriptor.");

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_attach(const dragonDDictSerial_t * dd_ser, dragonDDictDescr_t * dd, const timespec_t * timeout)
{
    dragonError_t err;
    dragonChannelDescr_t resp_ch;
    dragonChannelDescr_t buffered_resp_ch;
    dragonFLISerial_t ser_mgr_fli;
    dragonFLISerial_t ser_resp_fli;
    dragonFLISerial_t ser_buffered_resp_fli;
    dragonFLISerial_t fli_ser;
    DragonResponseMsg* resp_msg;
    DDRegisterClientResponseMsg* registerResponse;

    if (dd == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid ddict descriptor");

    if (dd_ser == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid serialized ddict descriptor");

    if (dd_ser->data == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid serialized ddict descriptor");

    // attach
    dragonDDict_t* ddict = (dragonDDict_t*)malloc(sizeof(dragonDDict_t));
    if (ddict == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "unable to allocate new ddict structure");

    // Attach to primary FLI adapter
    fli_ser.data = dd_ser->data;
    fli_ser.len = dd_ser->len;
    err = dragon_fli_attach(&fli_ser, NULL, &(ddict->orchestrator_fli));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to attach to FLI adapter");

    // Copy in the serialized descriptor.
    ddict->ser.len = dd_ser->len;
    ddict->ser.data = (uint8_t*) malloc(dd_ser->len);
    if (ddict->ser.data == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for internal serialized descriptor");
    memcpy(ddict->ser.data, dd_ser->data, dd_ser->len);

    /* Get the return channels for the client. This includes a streaming and a buffered return channel.
       Streaming requires one extra message per conversation but allow value data to be streamed back
       to the client. */

    err = dragon_create_process_local_channel(&resp_ch, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create response channel.");

    err = dragon_create_process_local_channel(&buffered_resp_ch, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create buffered response channel.");

    err = dragon_fli_create(&ddict->respFLI, &resp_ch, NULL, NULL, 0, NULL, false, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create response fli.");

    err = dragon_fli_create(&ddict->bufferedRespFLI, &buffered_resp_ch, NULL, NULL, 0, NULL, true, NULL);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create buffered response fli.");

    err = dragon_fli_serialize(&ddict->respFLI, &ser_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize respFLI.");

    err = dragon_fli_serialize(&ddict->bufferedRespFLI, &ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize bufferedRespFLI.");

    ddict->respFLIStr = dragon_base64_encode(ser_resp_fli.data, ser_resp_fli.len);
    ddict->bufferedRespFLIStr = dragon_base64_encode(ser_buffered_resp_fli.data, ser_buffered_resp_fli.len);

    err = dragon_fli_serial_free(&ser_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized response fli.");

    err = dragon_fli_serial_free(&ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized buffered response fli.");

    DDRegisterClientMsg registerClient(tag, ddict->respFLIStr, ddict->bufferedRespFLIStr);

    err = _send_receive(&ddict->orchestrator_fli, &registerClient, &ddict->bufferedRespFLI, &resp_msg, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the register client message and receive response.");

    if (resp_msg->tc() != DDRegisterClientResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Did not get expected register client response message.");

    registerResponse = (DDRegisterClientResponseMsg*) resp_msg;

    if (registerResponse->err() != DRAGON_SUCCESS)
        err_return(registerResponse->err(), registerResponse->errInfo());

    ddict->clientID = registerResponse->clientID();
    ddict->num_managers = registerResponse->numManagers();

    // ddict->manager_flis = (dragonFLIDescr_t*) malloc(sizeof(dragonFLIDescr_t) * ddict->num_managers);
    // if (ddict->manager_flis != NULL)
    //     err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for manager flis.");

    // for (size_t k=0;k<ddict->num_managers;k++) {
    //     ser_mgr_fli.data = dragon_base64_decode(registerResponse->managerFLI(k), &ser_mgr_fli.len);

    //     err = dragon_fli_attach(&ser_mgr_fli, NULL, &ddict->manager_flis[k]);
    //     if (err != DRAGON_SUCCESS)
    //         append_err_return(err, "Could not attach to manager fli.");

    //     err = dragon_fli_serial_free(&ser_mgr_fli);
    //     if (err != DRAGON_SUCCESS)
    //         append_err_return(err, "Could not free serialized fli for the manager.");
    // }

    registerResponse = NULL;
    delete resp_msg;

    /* TODO: We could eventually send all these requests and THEN wait for all the responses.
       Without hundreds of managers this shouldn't be a big deal. Once hundreds are needed, we
       should revisit this. */
    for (uint64_t k=0;k<ddict->num_managers;k++) {
        DDRegisterClientIDMsg registerWithManager(tag, ddict->clientID, ddict->respFLIStr, ddict->bufferedRespFLIStr);

        err = _send_receive(&ddict->manager_flis[k], &registerWithManager, &ddict->bufferedRespFLI, &resp_msg, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the register client message and receive response.");

        if (resp_msg->tc() != DDRegisterClientIDResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Did not get correct response message.");

        if (resp_msg->err() != DRAGON_SUCCESS)
            err_return(resp_msg->err(), resp_msg->errInfo());

        delete resp_msg;
    }

    err = _add_umap_ddict_entry(dd, ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to add new ddict entry to umap");

    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_detach(dragonDDictDescr_t * descr, const timespec_t * timeout)
{
    /* TODO: Deregister the client and DETACH FROM ALL FLIS */
    dragonDDict_t * ddict;
    dragonError_t err = _ddict_from_descr(descr, &ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "invalid ddict descriptor");

    // Detach from FLI
    err = dragon_fli_detach(&(ddict->respFLI));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to detach from FLI adapter");

    // Free any local resources on the handle

    return DRAGON_NOT_IMPLEMENTED;
}

/*
    General procedure:
    - Create request
    - Use write_bytes to write key (always happens)
    - Call operator (put, get, contains)
        - This should send a signal to the orchestrator
        - This should also send the buffered key data
    - Call appropriate following calls (write_bytes, read_bytes, read_mem)
    - Finalize request, sending appropriate signal to orchestrator we are done
        - Wait on success/fail reply
*/

dragonError_t
dragon_ddict_create_request(dragonDDictDescr_t * descr, dragonDDictRequestDescr_t * req_descr)
{
    // // Generate a request to do read/write operations to the dictionary
    // // Once accepted, perform operations, then finalize request

    // if (descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor");

    // if (req_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict request descriptor");

    // // Validate ddict exists
    // dragonDDict_t * ddict;
    // dragonError_t err = _ddict_from_descr(descr, &ddict);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "invalid ddict descriptor");

    // dragonDDictReq_t * req;
    // // Check if req exists in umap
    // err = _ddict_req_from_descr(req_descr, &req);
    // if (err == DRAGON_SUCCESS)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Request already exists, cannot overwrite");

    // // If not, add to umap
    // req = malloc(sizeof(dragonDDictReq_t));
    // err = _add_umap_ddict_req_entry(req_descr, req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to add new request entry");

    // req->dd_uid = ddict->dd_uid;
    // req->key_data = NULL;
    // req->key_hash = 0UL;
    // req->op_type = DRAGON_DDICT_NO_OP;

    // // TODO: Ping the orchestrator with a message confirming our request

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_finalize_request(dragonDDictRequestDescr_t * req_descr, const timespec_t * timeout)
{
    // // Finalize a request to do read/write operations
    // // This lets the orchestrator know the client is done for now
    // if (req_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    // dragonDDictReq_t * req;
    // dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to find request object");

    // switch(req->op_type) {
    //     case DRAGON_DDICT_NO_OP:
    //     case DRAGON_DDICT_FINALIZED:
    //         err_return(DRAGON_INVALID_OPERATION, "Request is invalid");
    //         break;

    //     case DRAGON_DDICT_CONTAINS_REQ:
    //         {
    //             //Error check to see if key is present?
    //             err = _send_key(req);
    //             if (err != DRAGON_SUCCESS)
    //                 append_err_return(err, "Failed to send key to manager");

    //             // auto send_msg = msgDDictContains()
    //             // err = dragon_fli_send_bytes(&req->sendh, send_msg.size, send_msg.bytes, arg, false, NULL);
    //             // err = dragon_fli_close_send_handle(&req->sendh, NULL);

    //         }
    //         break;

    //     case DRAGON_DDICT_GET_REQ:
    //         {
    //             // auto send_msg = msgDDictFinishRead
    //             // err = dragon_fli_send_bytes(&req->sendh, send_msg.size, send_msg.bytes, arg, false, NULL);
    //             // err = dragon_fli_close_send_handle(&req->sendh, NULL);
    //         }
    //         break;

    //     case DRAGON_DDICT_PUT_REQ:
    //         {
    //             // auto send_msg = msgDDictFinishWrite
    //             // err = dragon_fli_send_bytes(&req->sendh, send_msg.size, send_msg.bytes, arg, false, NULL);
    //             // err = dragon_fli_close_send_handle(&req->sendh, NULL);
    //         }
    //         break;

    //     default:
    //         err_return(DRAGON_INVALID_ARGUMENT, "Unimplemented or invalid operator type");
    // }

    // // Get response message
    // size_t recv_sz;
    // char * recv_bytes;
    // uint64_t arg;
    // err = dragon_fli_recv_bytes(&req->recvh, 0, &recv_sz, &recv_bytes, &arg, NULL);
    // // if (err != DRAGON_SUCCESS)

    // // Check response (arg value?)

    // req->op_type = DRAGON_DDICT_FINALIZED;
    // free(req->key_data); // Free malloc'd key
    // dragon_umap_delitem(dg_ddict_reqs, req_descr->_idx); // Remove request from umap

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_write_bytes(dragonDDictRequestDescr_t * req_descr, size_t num_bytes, uint8_t * bytes, const timespec_t * timeout)
{
    // if (req_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "invalid request descriptor");

    // dragonDDictReq_t * req;
    // dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to find request object");

    // // Buffer key writes
    // if (req->key_data == NULL) {
    //     err = _buffer_bytes(req, bytes, num_bytes);
    //     if (err != DRAGON_SUCCESS)
    //         append_err_return(err, "Failed to buffer key message");

    // } else {
    //     if (req->op_type != DRAGON_DDICT_PUT_REQ)
    //         err_return(DRAGON_INVALID_OPERATION, "Trying to perform a write operation with a non-write request");

    //     // Write data out normally, key is done
    //     uint64_t arg;
    //     err = dragon_fli_send_bytes(&req->sendh, num_bytes, bytes, &arg, false, timeout);
    //     if (err != DRAGON_SUCCESS)
    //         append_err_return(err, "Failed to write bytes to ddict");
    // }

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_read_bytes(dragonDDictRequestDescr_t* req_descr, size_t requested_size,
                        size_t* received_size, uint8_t** bytes, const timespec_t* timeout)
{
    if (req_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    dragonDDictReq_t * req;
    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object");

    // Operation type is set in the put/get/contains/etc call after setting the key
    if (req->op_type != DRAGON_DDICT_GET_REQ)
        err_return(DRAGON_INVALID_OPERATION, "Invalid operation type");

    uint64_t arg;
    err = dragon_fli_recv_bytes(&req->recvh, requested_size, received_size, bytes, &arg, timeout);
    if (err != DRAGON_SUCCESS) {
        if (err == DRAGON_EOT)
            return err;

        append_err_return(err, "Failed to read bytes from dictionary");
    }


    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ddict_read_bytes_into(dragonDDictRequestDescr_t* req, size_t requested_size,
                              size_t* received_size, uint8_t* bytes, const timespec_t* timeout)
{
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_read_mem(dragonDDictRequestDescr_t* req_descr, dragonMemoryDescr_t* mem_descr)
{
    // if (req_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    // if (mem_descr == NULL)
    //     err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor");

    // dragonDDictReq_t * req;
    // dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    // if (err != DRAGON_SUCCESS)
    //     append_err_return(err, "Failed to find request object");

    // if (req->op_type != DRAGON_DDICT_GET_REQ)
    //     err_return(DRAGON_INVALID_OPERATION, "Invalid operation type");

    // uint64_t arg;
    // err = dragon_fli_recv_mem(&req->recvh, mem_descr, &arg, NULL);
    // if (err != DRAGON_SUCCESS) {
    //     if (err == DRAGON_EOT)
    //         return err;

    //     append_err_return(err, "Failed to receive into memory descriptor");
    // }

    // no_err_return(DRAGON_SUCCESS);
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_contains(dragonDDictRequestDescr_t * req_descr)
{
    // Check if provided key exists in the ddict
    if (req_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    dragonDDictReq_t * req;
    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object");

    // key_data means we've already written and sent it, out-of-order operation
    if (req->key_data != NULL)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order");

    req->op_type = DRAGON_DDICT_CONTAINS_REQ; // So we know what to finalize

    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_get(dragonDDictRequestDescr_t* req_descr, const timespec_t* timeout)
{
    if (req_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    dragonDDictReq_t * req;
    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object");

    if (req->key_data == NULL && req->buffered_allocs == NULL)
        err_return(DRAGON_INVALID_OPERATION, "No data present in request");

    if (req->key_data == NULL && req->buffered_allocs != NULL) {
        err = _send_key(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to send key");
    } else {
        err_return(DRAGON_INVALID_OPERATION, "What goes here?");
    }

    req->op_type = DRAGON_DDICT_GET_REQ;

    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t
dragon_ddict_put(dragonDDictRequestDescr_t* req_descr, const timespec_t* timeout)
{
    if (req_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor");

    dragonDDictReq_t * req;
    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object");

    if (req->key_data == NULL && req->buffered_allocs == NULL)
        err_return(DRAGON_INVALID_OPERATION, "No data present in request");

    if (req->key_data == NULL && req->buffered_allocs != NULL) {
        err = _send_key(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to send key");
    } else {
        err_return(DRAGON_INVALID_OPERATION, "What goes here?");
    }

    req->op_type = DRAGON_DDICT_PUT_REQ;

    return DRAGON_NOT_IMPLEMENTED;
}
