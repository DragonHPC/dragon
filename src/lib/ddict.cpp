#include <iostream>
#include <sstream>
#include <string>
#include <set>
#include <dragon/ddict.h>
#include "_ddict.hpp"
#include "_utils.h"
#include <dragon/messages.hpp>
#include <dragon/dictionary.hpp>
#include <dragon/utils.h>
#include <assert.h>
#include "err.h"
#include "hostid.h"


/* dragon globals */
DRAGON_GLOBAL_MAP(ddict_adapters);
DRAGON_GLOBAL_MAP(ddict_reqs);

dragonDDict_t::dragonDDict_t(const char * dd_ser, timespec_t * default_timeout) {
    // Initialize ddict
    ddict_ser = dd_ser;
    tag = 0;
    chkpt_id = 0;
    detached = false;
    if (default_timeout == nullptr) {
        timeout = nullptr;
    } else {
        timeout = &timeout_val;
        *timeout = *default_timeout;
    }
    has_local_manager = false;
    has_chosen_manager = false;
}

static dragonError_t _send(dragonFLIDescr_t * sendto_fli, dragonChannelDescr_t* strm_ch, DragonMsg * send_msg, const timespec_t * timeout) {
    dragonFLISendHandleDescr_t sendh;
    dragonError_t err;

    if (sendto_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid send FLI descriptor.");

    if (send_msg == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid send message.");

    err = dragon_fli_open_send_handle(sendto_fli, &sendh, strm_ch, nullptr, false, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

    err = send_msg->send(&sendh, timeout);
    if (err != DRAGON_SUCCESS) {
        // We ignore the return code because there was an error on send.
        dragon_fli_close_send_handle(&sendh, timeout);
        append_err_return(err, "Could not send message.");
    }

    err = dragon_fli_close_send_handle(&sendh, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_resp(dragonFLIDescr_t * recvfrom_fli, DragonResponseMsg ** recv_msg, std::set<uint64_t>& msg_tags, const timespec_t * timeout) {
    dragonFLIRecvHandleDescr_t recvh;
    bool done = false;
    DragonMsg * msg = nullptr;

    if (recvfrom_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive FLI descriptor.");

    if (recv_msg == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive message.");

    while (!done) {

        dragonError_t err = dragon_fli_open_recv_handle(recvfrom_fli, &recvh, nullptr, nullptr, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open recv handle.");

        err = recv_fli_msg(&recvh, &msg, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not receive response message.");

        *recv_msg = (DragonResponseMsg*)msg;
        // received unexpected message
        auto it = msg_tags.find((*recv_msg)->ref());
        if (it == msg_tags.end()) {
            fprintf(stderr, "WARNING: Message with typecode %s discarded on ddict client response fli.", dragon_msg_tc_name((*recv_msg)->tc()));
            fflush(stderr);
            delete msg;
        } else {
            // Remove the message tag from the set. Each response should have a unique ref ID (e.g. ID of the request)
            done = true;
            msg_tags.erase(it);
        }

        err = dragon_fli_close_recv_handle(&recvh, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close receive handle.");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_responses(dragonFLIDescr_t * recvfrom_fli, DragonResponseMsg ** recv_msgs, std::set<uint64_t>& msg_tags, uint64_t num_responses, const timespec_t * timeout) {

    dragonError_t err;

    if (recvfrom_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive FLI descriptor.");

    for (size_t i=0 ; i<num_responses ; i++) {
        if (msg_tags.size() == 1 && num_responses > 1) {
            // Expects multiple responses with the same ref ID. (ex: DDClear, DDLength ... etc.)
            // This happens when client sends request to the root manager who then broadcasts the
            // request to all other managers. All managers then create the responses using the exact
            // same request tag and send it back to client directly. Therefore, the client should
            // expect multiple responses with the same ref ID.
            std::set<uint64_t> msg_tags_copy = msg_tags; // copy message tag set
            err = _recv_resp(recvfrom_fli, &recv_msgs[i], msg_tags_copy, timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not receive one of the responses.");
        } else {
            err = _recv_resp(recvfrom_fli, &recv_msgs[i], msg_tags, timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not receive one of the responses.");
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _recv_dmsg_no_close_recvh(dragonFLIRecvHandleDescr_t * recvh, dragonFLIDescr_t * recvfrom_fli , DragonResponseMsg ** recv_msg, uint64_t msg_tag, bool buffered, timespec_t * timeout) {

    dragonError_t err;
    bool done = false;
    dragonChannelDescr_t * channel_descr = nullptr;
    DragonMsg * msg = nullptr;

    if (recvh == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive handle descriptor.");

    if (recvfrom_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive FLI descriptor.");

    if (recv_msg == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive message.");

    while (!done) {
        if (!buffered)
            channel_descr = STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION;

        err = dragon_fli_open_recv_handle(recvfrom_fli, recvh, channel_descr, nullptr, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open recv handle.");

        err = recv_fli_msg(recvh, &msg, timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not receive response message.");

        *recv_msg = (DragonResponseMsg*)msg;
        // received unexpected message
        if ((*recv_msg)->ref() != msg_tag) {
            fprintf(stderr, "WARNING: Message with typecode %s discarded on ddict client response fli.", dragon_msg_tc_name((*recv_msg)->tc()));
            fflush(stderr);
            delete msg;
            err = dragon_fli_close_recv_handle(recvh, timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not close receive handle.");
        } else {
            // Don't close the recv handle, we need it later to receive streamed value.
            done = true;
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_receive(dragonFLIDescr_t* sendto_fli, dragonChannelDescr_t* strm_ch, DragonMsg* send_msg, dragonFLIDescr_t* recvfrom_fli,
              DragonResponseMsg** recv_msg, const timespec_t* timeout) {

    if (sendto_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid send FLI descriptor.");

    if (recvfrom_fli == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive FLI descriptor.");

    if (recv_msg == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive message.");

    dragonError_t err = _send(sendto_fli, strm_ch, send_msg, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to send message in send_recv pattern.");

    std::set<uint64_t> msg_tags;
    msg_tags.insert(send_msg->tag());
    err = _recv_resp(recvfrom_fli, recv_msg, msg_tags, timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to recveive message in send_recv pattern.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_msg_key_no_close_sendh(DragonMsg * send_msg, dragonDDictReq_t * req) {
    dragonError_t err;
    dragonFLISendHandleDescr_t sendh;

    err = dragon_fli_open_send_handle(&req->manager_fli, &sendh, &req->ddict->strm_ch, nullptr, false, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

    req->sendh = sendh;

    err = send_msg->send(&sendh, req->ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        // We ignore the return code because there was an error on send.
        dragonError_t close_fli_err = dragon_fli_close_send_handle(&sendh, req->ddict->timeout);
        if (close_fli_err != DRAGON_SUCCESS) {
            append_err_return(close_fli_err, "Could not send message and close send handle.");
        }
        append_err_return(err, "Could not send message.");
    }

    // send key
    if (req->key_data != nullptr) {
        uint64_t arg = KEY_HINT;

        err = dragon_fli_send_bytes(&req->sendh, req->buffer_size, req->key_data, arg, false, req->ddict->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send key to manager.");
    }
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _add_umap_ddict_entry(dragonDDictDescr_t * ddict_descr, dragonDDict_t * ddict) {
    dragonError_t err;

    if (*dg_ddict_adapters == nullptr) {
        *dg_ddict_adapters = (dragonMap_t*)malloc(sizeof(dragonMap_t));
        if (*dg_ddict_adapters == nullptr)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate umap for ddict.");

        err = dragon_umap_create(dg_ddict_adapters, DRAGON_DDICT_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to create umap for ddict.");
    }

    err = dragon_umap_additem_genkey(dg_ddict_adapters, ddict, &ddict_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to insert item into ddict umap.");

    ddict->dd_uid = ddict_descr->_idx;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _ddict_from_descr(const dragonDDictDescr_t * dd_descr, dragonDDict_t ** ddict) {

    if (ddict == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    dragonError_t err = dragon_umap_getitem(dg_ddict_adapters, dd_descr->_idx, (void**)ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in ddict umap.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _add_umap_ddict_req_entry(const dragonDDictRequestDescr_t * descr, const dragonDDictReq_t * new_req) {
    dragonError_t err;

    if (*dg_ddict_reqs == nullptr) {
        *dg_ddict_reqs = (dragonMap_t*)malloc(sizeof(dragonMap_t));
        if (*dg_ddict_reqs == nullptr)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate umap for ddict requests.");

        err = dragon_umap_create(dg_ddict_reqs, DRAGON_DDICT_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to create umap for ddict requests.");
    }

    err = dragon_umap_additem(dg_ddict_reqs, descr->_idx, new_req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to insert item into ddict request umap.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _ddict_req_from_descr(const dragonDDictRequestDescr_t * req_descr, dragonDDictReq_t ** req) {
    if (req == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict request descriptor.");

    dragonError_t err = dragon_umap_getitem(dg_ddict_reqs, req_descr->_idx, (void**)req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find item in ddict request umap.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _buffer_bytes(dragonDDictReq_t * req, uint8_t * bytes, size_t num_bytes) {
    void * data_ptr = nullptr;
    dragonDDictBufAlloc_t * node_ptr = nullptr;
    dragonError_t err;

    if (num_bytes > 0) {
        data_ptr = malloc(num_bytes);
        if (data_ptr == nullptr)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate buffer space -- out of memory.");

        node_ptr = (dragonDDictBufAlloc_t*)malloc(sizeof(dragonDDictBufAlloc_t));
        if (node_ptr == nullptr) {
            err = DRAGON_INTERNAL_MALLOC_FAIL;
            err_noreturn("Could not allocate node pointer -- out of memory.");
            goto node_ptr_alloc_fail;
        }

        memcpy(data_ptr, bytes, num_bytes);

        node_ptr->data = (uint8_t*)data_ptr;
        node_ptr->num_bytes = num_bytes;
        req->buffer_size += num_bytes;
        node_ptr->next = req->buffered_allocs;
        req->buffered_allocs = node_ptr;
        req->num_writes++;
    }

    no_err_return(DRAGON_SUCCESS);

    node_ptr_alloc_fail:
        free(data_ptr);

    append_err_return(err, "Could not buffer bytes.");
}

static dragonError_t _build_key(dragonDDictReq_t * req) {
    // Buffer key into one blob and store it, then send it
    dragonDDictBufAlloc_t * node = req->buffered_allocs;
    size_t key_size = req->buffer_size;
    void * data = nullptr;
    void * dst_ptr = nullptr;

    if (req->num_writes == 1) {
        req->key_data = node->data;
        req->key_hash = dragon_hash(req->key_data, key_size);
        req->num_writes = 0;
        free(node);
        req->buffered_allocs = nullptr;
        no_err_return(DRAGON_SUCCESS);
    }

    req->buffered_allocs = nullptr;
    // We have to order this backwards, so jump to the end of the first memcpy start
    data = (void*)malloc(key_size);
    if (data == nullptr)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for data -- out of memory.");

    dst_ptr = (void*)((size_t)data + key_size);
    while (node != nullptr) {
        dst_ptr = (void*)((size_t)dst_ptr - node->num_bytes);

        memcpy(dst_ptr, node->data, node->num_bytes);

        dragonDDictBufAlloc_t * tmp = node->next;

        free(node->data);
        free(node);
        node = tmp;
    }

    req->key_data = (uint8_t*)data;

    // Once our key is constructed, hash it
    req->key_hash = dragon_hash(data, key_size);

    no_err_return(DRAGON_SUCCESS);
}

static uint64_t _tag_inc(dragonDDict_t * dd) {
    return dd->tag++;
}

static dragonError_t _connect_to_manager(dragonDDict_t * dd, uint64_t manager_id) {

    dragonError_t err;
    DDConnectToManagerMsg * connectToManagerMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDConnectToManagerResponseMsg * connectToManagerResponseMsg = nullptr;
    dragonFLISerial_t fli_ser;

    connectToManagerMsg = new DDConnectToManagerMsg(_tag_inc(dd), dd->clientID, manager_id);
    err = _send_receive(&dd->main_manager_fli, &dd->strm_ch, connectToManagerMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
    if (err != DRAGON_SUCCESS)  {
        append_err_noreturn("Could not send the connect to manager message and receive response.");
        goto send_receive_fail;
    }

    if (resp_msg->tc() != DDConnectToManagerResponseMsg::TC) {
        err = DRAGON_FAILURE;
        append_err_noreturn("Failed to get expected connect to manager response message.");
        goto send_receive_fail;
    }

    connectToManagerResponseMsg = (DDConnectToManagerResponseMsg*) resp_msg;
    if (connectToManagerResponseMsg->err() != DRAGON_SUCCESS) {
        err = connectToManagerResponseMsg->err();
        append_err_noreturn(connectToManagerResponseMsg->errInfo());
        goto send_receive_fail;
    }

    // attach to the mananger fli
    fli_ser.data = dragon_base64_decode(connectToManagerResponseMsg->managerFLI(), &fli_ser.len);
    err = dragon_fli_attach(&fli_ser, nullptr, &dd->manager_table[manager_id]);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not attach to the manager.");
        goto send_receive_fail;
    }

    delete connectToManagerMsg;
    delete connectToManagerResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete connectToManagerMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to connect to manager.");
}

static dragonError_t _register_client_ID_to_manager(dragonDDict_t * dd, uint64_t manager_id) {
    dragonError_t err;
    DDRegisterClientIDMsg * registerClientIDMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRegisterClientIDResponseMsg * registerClientIDResponseMsg = nullptr;

    registerClientIDMsg = new DDRegisterClientIDMsg(_tag_inc(dd), dd->clientID, dd->respFLIStr.c_str(), dd->bufferedRespFLIStr.c_str());
    err = _send_receive(&dd->manager_table[manager_id], &dd->strm_ch, registerClientIDMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the register client ID message and receive response.");
        goto send_receive_fail;
    }

    if (resp_msg->tc() != DDRegisterClientIDResponseMsg::TC) {
        err = DRAGON_FAILURE;
        append_err_noreturn("Failed to get expected register client ID response message.");
        goto send_receive_fail;
    }

    registerClientIDResponseMsg = (DDRegisterClientIDResponseMsg*) resp_msg;
    if (registerClientIDResponseMsg->err() != DRAGON_SUCCESS) {
        err = registerClientIDResponseMsg->err();
        append_err_noreturn(registerClientIDResponseMsg->errInfo());
        goto send_receive_fail;
    }

    delete registerClientIDMsg;
    delete registerClientIDResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete registerClientIDMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to register client ID to manager.");
}

static dragonError_t _check_manager_connection(dragonDDict_t * dd, uint64_t manager_id) {

    dragonError_t err;

    auto search = dd->manager_table.find(manager_id);
    if (search == dd->manager_table.end()) {
        // connect to manager <manager_id>
        err = _connect_to_manager(dd, manager_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not connect to the manager.");
        // register to manager <manager_id>
        err = _register_client_ID_to_manager(dd, manager_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not register client ID to the manager.");
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _check_all_manager_connection(dragonDDict_t * dd) {

    dragonError_t err;

    for (size_t i=0 ; i<dd->num_managers ; i++) {
        auto  search = dd->manager_table.find(i);
        // manager <i> is not connected
        if (search == dd->manager_table.end()) {
            // connect to manager <i>
            err = _connect_to_manager(dd, i);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not connect to one of the managers.");
            // register to manager <i>
            err = _register_client_ID_to_manager(dd, i);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not register client ID to one of the managers.");
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _choose_manager_hash_key(dragonDDictReq_t * req) {

    if (req->ddict->has_chosen_manager) {
        req->manager_id = req->ddict->chosen_manager;
    } else {
        req->manager_id = req->key_hash % req->ddict->num_managers;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _choose_manager_build_key(dragonDDictReq_t * req) {

    dragonError_t err;

    err = _build_key(req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not build key.");

    err = _choose_manager_hash_key(req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not hash the key or get the manager ID.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _cleanup_request(dragonDDictReq_t * req) {

    req->ddict = nullptr;
    dragonDDictBufAlloc_t * node = req->buffered_allocs;

    if (req == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Resquest should be non null.");

    while (node != nullptr) {
        dragonDDictBufAlloc_t * tmp = node->next;
        free(node->data);
        free(node);
        node = tmp;
    }

    if (req->key_data != nullptr) {
        free(req->key_data);
        req->key_data = nullptr;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _get_main_manager(dragonDDict_t * ddict) {

    dragonError_t err;
    char * mgr_ser;
    dragonFLISerial_t mgr_fli_ser;
    DDRandomManagerMsg * randomManagerMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRandomManagerResponseMsg * randomManagerResponseMsg = nullptr;

    err = dragon_ls_get_kv((uint8_t*)ddict->ddict_ser.c_str(), (char**)&mgr_ser, nullptr);
    if (err == DRAGON_NOT_FOUND) {
        // No manager found on node - Get random manager (check specific err code)
        // call get_random_manager (via orchestrator)
        randomManagerMsg = new DDRandomManagerMsg(_tag_inc(ddict), ddict->bufferedRespFLIStr.c_str());

        // Orchestrator FLI uses buffered protocol and we don't use stream channel with buffered protocol
        err = _send_receive(&ddict->orchestrator_fli, nullptr, randomManagerMsg, &ddict->bufferedRespFLI, &resp_msg, ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the get random manager message and receive response.");
            goto random_manager_fail;
        }

        if (resp_msg->tc() != DDRandomManagerResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected get random manager response message.");
            goto random_manager_fail;
        }

        randomManagerResponseMsg = (DDRandomManagerResponseMsg*) resp_msg;

        if (randomManagerResponseMsg->err() != DRAGON_SUCCESS) {
            err = randomManagerResponseMsg->err();
            append_err_noreturn(randomManagerResponseMsg->errInfo());
            goto random_manager_fail;
        }
        mgr_ser = (char*)randomManagerResponseMsg->managerFLI();
    } else if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Neither local nor random main manager could be acquired.");
        goto random_manager_fail;
    } else {
        ddict->has_local_manager = true;
    }

    // Attach to our main manager
    mgr_fli_ser.data = dragon_base64_decode(mgr_ser, &mgr_fli_ser.len);
    err = dragon_fli_attach(&mgr_fli_ser, nullptr, &ddict->main_manager_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not attach to main manager.");
        goto random_manager_fail;
    }

    // cleanup
    delete randomManagerMsg;
    delete randomManagerResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    random_manager_fail:
        delete randomManagerMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to get main manager.");
}

static dragonError_t _register_client_to_main_manager(dragonDDict_t * ddict) {

    dragonError_t err;
    uint64_t main_manager_id;
    DDRegisterClientMsg * registerClientMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRegisterClientResponseMsg * registerClientResponseMsg = nullptr;

    registerClientMsg = new DDRegisterClientMsg(_tag_inc(ddict), ddict->respFLIStr.c_str(), ddict->bufferedRespFLIStr.c_str());
    err = _send_receive(&ddict->main_manager_fli, &ddict->strm_ch, registerClientMsg, &ddict->bufferedRespFLI, &resp_msg, ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the register client message and receive response.");
        goto register_client_msg_send_receive_fail;
    }

    if (resp_msg->tc() != DDRegisterClientResponseMsg::TC) {
        err = DRAGON_FAILURE;
        append_err_noreturn("Failed to get expected register client response message.");
        goto register_client_msg_send_receive_fail;
    }

    registerClientResponseMsg = (DDRegisterClientResponseMsg*) resp_msg;
    if (registerClientResponseMsg->err() != DRAGON_SUCCESS) {
        err = registerClientResponseMsg->err();
        append_err_noreturn(registerClientResponseMsg->errInfo());
        goto register_client_msg_send_receive_fail;
    }
    // Store our client ID, number of managers and malloc all managers' nodes
    ddict->clientID = registerClientResponseMsg->clientID();
    ddict->num_managers = registerClientResponseMsg->numManagers();

    main_manager_id = registerClientResponseMsg->managerID();
    ddict->manager_table[main_manager_id] = ddict->main_manager_fli;
    ddict->main_manager = main_manager_id;
    if (ddict->has_local_manager)
        ddict->local_manager = main_manager_id;

    // cleanup
    delete registerClientMsg;
    delete registerClientResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    register_client_msg_send_receive_fail:
        delete registerClientMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to register client to main manager.");
}

static dragonError_t _get_local_managers(dragonDDict_t * ddict) {

    dragonError_t err;
    DDManagerNodesMsg * managerNodesMsg = nullptr;
    DDManagerNodesResponseMsg * managerNodesResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonULInt local_host_id = dragon_host_id();
    std::vector<uint64_t> huids;

    managerNodesMsg = new DDManagerNodesMsg(_tag_inc(ddict), ddict->bufferedRespFLIStr.c_str());

    // Orchestrator FLI uses buffered protocol and we don't use stream channel with buffered protocol
    err = _send_receive(&ddict->orchestrator_fli, nullptr, managerNodesMsg, &ddict->bufferedRespFLI, &resp_msg, ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the get random manager message and receive response.");
        goto send_receive_fail;
    }

    if (resp_msg->tc() != DDManagerNodesResponseMsg::TC) {
        err = DRAGON_FAILURE;
        append_err_noreturn("Failed to get expected get manager nodes response message.");
        goto send_receive_fail;
    }

    managerNodesResponseMsg = (DDManagerNodesResponseMsg*) resp_msg;
    if (managerNodesResponseMsg->err() != DRAGON_SUCCESS) {
        err = managerNodesResponseMsg->err();
        append_err_noreturn(managerNodesResponseMsg->errInfo());
        goto send_receive_fail;
    }

    ddict->hostid = local_host_id;
    huids = managerNodesResponseMsg->huids();
    for (size_t i=0 ; i<huids.size() ; i++) {
        if (huids[i] == ddict->hostid)
            ddict->local_managers.push_back(i);
    }

    delete managerNodesMsg;
    delete managerNodesResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete managerNodesMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to get local managers");
}

static dragonError_t _put(const dragonDDictRequestDescr_t * req_descr, bool persist) {
    dragonDDictReq_t * req;
    dragonError_t err;
    DDPutMsg * putMsg = nullptr;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operations.");
    req->op_type = DRAGON_DDICT_PUT_REQ;

    if (req->key_data == nullptr && req->buffered_allocs != nullptr) {

        err = _choose_manager_build_key(req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not build key or connect to manager.");
            goto choose_manager_build_key_fail;
        }

        err = _check_manager_connection(req->ddict, req->manager_id);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not connect to the manager.");
            goto choose_manager_build_key_fail;
        }
        req->manager_fli = req->ddict->manager_table[req->manager_id];

        req->msg_tag = _tag_inc(req->ddict);
        putMsg = new DDPutMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id, persist);
        err = _send_msg_key_no_close_sendh(putMsg, req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the put message and key.");
            goto choose_manager_build_key_fail;
        }

    } else {
        err_return(DRAGON_INVALID_OPERATION, "No data present in request.");
    }

    delete putMsg;

    no_err_return(DRAGON_SUCCESS);

    choose_manager_build_key_fail:
        delete putMsg;

    append_err_return(err, "Failed to perform put.");

}

static dragonError_t _recv_and_discard(dragonFLIRecvHandleDescr_t* recvh, const timespec_t * timeout) {

    dragonError_t err = DRAGON_SUCCESS;

    if (recvh == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid receive handle descriptor.");

    while (err != DRAGON_EOT) {
        size_t recv_sz = 0;
        uint8_t* bytes = nullptr;
        uint64_t arg = 0;
        err = dragon_fli_recv_bytes(recvh, 0, &recv_sz, &bytes, &arg, timeout);
        if (err == DRAGON_SUCCESS)
            free(bytes);
        else if (err == DRAGON_EOT) {
            /* do nothing */
        } else {
            err_return(err, "Could not receive remaining stuff in recive handle.");
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

/*
 * NOTE: This should only be called from dragon_set_thread_local_mode
 */
void _set_thread_local_mode_ddict(bool set_thread_local) {
    if (set_thread_local) {
        dg_ddict_adapters = &_dg_thread_ddict_adapters;
        dg_ddict_reqs = &_dg_thread_ddict_reqs;
    } else {
        dg_ddict_adapters = &_dg_proc_ddict_adapters;
        dg_ddict_reqs = &_dg_proc_ddict_reqs;
    }
}

dragonError_t _dragon_ddict_keys_vec(dragonDDict_t * dd, std::vector<uint64_t>& managers, std::vector<dragonDDictKey_t*>& keys) {

    dragonError_t err;
    dragonError_t close_recvh_err;
    DDKeysMsg * keysMsg = nullptr;
    DDKeysResponseMsg * keysResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    uint64_t msg_tag;
    dragonDDictKey_t * key;
    dragonFLIRecvHandleDescr_t recvh;

    for (size_t i=0 ; i<managers.size() ; i++) {
        // send request
        msg_tag = _tag_inc(dd);
        keysMsg = new DDKeysMsg(msg_tag, dd->clientID, dd->chkpt_id, dd->respFLIStr.c_str());
        uint64_t manager_id = managers[i];
        err = _send(&dd->manager_table[manager_id], &dd->strm_ch, keysMsg, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the keys message to manager.");
            goto send_fail;
        }
        delete keysMsg;
        // receive dmsg
        err = _recv_dmsg_no_close_recvh(&recvh, &dd->respFLI, &resp_msg, msg_tag, false, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not receive keys response message.");
            goto recv_fail;
        }
        if (resp_msg->tc() != DDKeysResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected keys response message.");
            dragonError_t recv_and_discard_err = _recv_and_discard(&recvh, dd->timeout);
            if (recv_and_discard_err != DRAGON_SUCCESS) {
                err = recv_and_discard_err;
                append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
            }
            goto recv_fail;
        }
        keysResponseMsg = (DDKeysResponseMsg*) resp_msg;
        if (keysResponseMsg->err() != DRAGON_SUCCESS) {
            err = keysResponseMsg->err();
            append_err_noreturn(keysResponseMsg->errInfo());
            dragonError_t recv_and_discard_err = _recv_and_discard(&recvh, dd->timeout);
            if (recv_and_discard_err != DRAGON_SUCCESS) {
                err = recv_and_discard_err;
                append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
            }
            goto recv_fail;
        }
        delete keysResponseMsg;
        // receive keys until EOT
        bool done = false;
        while(!done) {
            uint64_t arg = 0;
            key = (dragonDDictKey_t*)malloc(sizeof(dragonDDictKey_t));
            if (key == NULL) {
                err_noreturn("Could not allocate space for key.");
                goto recv_fail;
            }
            err = dragon_fli_recv_bytes(&recvh, 0, &key->num_bytes, &key->data, &arg, dd->timeout);
            if (err == DRAGON_SUCCESS) {
                if (arg != KEY_HINT) {
                    err = DRAGON_FAILURE;
                    done = true;
                    append_err_noreturn("Received unexpected arg value.");
                    goto recv_fail;
                }
                keys.push_back(key);
            } else if (err == DRAGON_EOT) {
                done = true;
                free(key);
            } else {
                done = true;
                append_err_noreturn("Caught an error while receiving keys.");
                goto recv_fail;
            }
            key = nullptr;
        }
        err = dragon_fli_close_recv_handle(&recvh, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not close receive handle.");
            goto close_recvh_fail;
        }
    }

    no_err_return(DRAGON_SUCCESS);
    recv_fail:
        close_recvh_err = dragon_fli_close_recv_handle(&recvh, dd->timeout);
        if (close_recvh_err != DRAGON_SUCCESS) {
            err = close_recvh_err;
            append_err_noreturn("Could not close receive handle.");
        }
    close_recvh_fail:
        if (resp_msg != nullptr)
            delete resp_msg;
    send_fail:
        if (keysMsg != nullptr) {
            delete keysMsg;
            keysMsg = nullptr;
        }
    append_err_return(err, "Failed to perform keys operation.");
}


/* This is used both in this implementation and in the C++ implementation so cannot be static. */
dragonError_t dragon_ddict_keys_vec(const dragonDDictDescr_t * dd_descr, std::vector<dragonDDictKey_t*>& keys) {
    dragonDDict_t * dd = nullptr;
    dragonError_t err;
    std::vector<uint64_t> managers;

    try {
        if (dd_descr == nullptr)
            err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

        err = _ddict_from_descr(dd_descr, &dd);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not find ddict object.");

        // create list of manager ids to request the keys from
        if (dd->has_chosen_manager) {
            err = _check_manager_connection(dd, dd->chosen_manager);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not connect to chosen manager.");

            managers.push_back(dd->chosen_manager);
        } else {
            err = _check_all_manager_connection(dd);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not connect to all managers.");

            for (size_t i=0 ; i<dd->num_managers ; i++)
                managers.push_back(i);
        }

        keys.clear();

        err = _dragon_ddict_keys_vec(dd, managers, keys);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get keys.");

        no_err_return(DRAGON_SUCCESS);

    } catch(const std::exception& ex) {
        std::stringstream err_str;
        err_str << "Exception Caught: " << ex.what() << endl;
        err_return(DRAGON_FAILURE, err_str.str().c_str());
    }
}

dragonError_t dragon_ddict_local_keys_vec(const dragonDDictDescr_t * dd_descr, std::vector<dragonDDictKey_t*>& keys) {
    dragonDDict_t * dd = nullptr;
    dragonError_t err;

    try {
        if (dd_descr == nullptr)
            err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

        err = _ddict_from_descr(dd_descr, &dd);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not find ddict object.");

        for (uint64_t manager: dd->local_managers) {
            err = _check_manager_connection(dd, manager);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not connect to local manager.");
        }

        keys.clear();

        err = _dragon_ddict_keys_vec(dd, dd->local_managers, keys);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get keys.");

        no_err_return(DRAGON_SUCCESS);

    } catch(const std::exception& ex) {
        std::stringstream err_str;
        err_str << "Exception Caught: " << ex.what() << endl;
        err_return(DRAGON_FAILURE, err_str.str().c_str());
    }
}

dragonError_t dragon_ddict_empty_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& manager_ids) {

    dragonError_t err;
    DDEmptyManagersMsg * emptyManagersMsg = nullptr;
    DDEmptyManagersResponseMsg * emptyManagersResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonDDict_t * dd = nullptr;
    std::vector<uint64_t> empty_managers;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    emptyManagersMsg = new DDEmptyManagersMsg(_tag_inc(dd), dd->bufferedRespFLIStr.c_str());
    err = _send_receive(&dd->orchestrator_fli, nullptr, emptyManagersMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);

    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the empty managers message and receive response.");
        goto send_receive_fail;
    }

    if (resp_msg->tc() != DDEmptyManagersResponseMsg::TC) {
        err = DRAGON_FAILURE;
        append_err_noreturn("Failed to get expected empty managers response message.");
        goto send_receive_fail;
    }

    emptyManagersResponseMsg = (DDEmptyManagersResponseMsg*) resp_msg;
    if (emptyManagersResponseMsg->err() != DRAGON_SUCCESS) {
        err = emptyManagersResponseMsg->err();
        append_err_noreturn(emptyManagersResponseMsg->errInfo());
        goto send_receive_fail;
    }

    empty_managers = emptyManagersResponseMsg->managers();
    for (size_t i=0 ; i<empty_managers.size() ; i++)
        manager_ids.push_back(empty_managers[i]);

    delete emptyManagersMsg;
    delete emptyManagersResponseMsg;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete emptyManagersMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    append_err_return(err, "Failed to get empty managers.");
}

// BEGIN USER API

dragonError_t dragon_ddict_serialize(const dragonDDictDescr_t * dd, const char ** dd_ser) {
    if (dd == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (dd_ser == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict serial descriptor.");

    dragonDDict_t * ddict;
    dragonError_t err = _ddict_from_descr(dd, &ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");
    size_t ser_len = strlen(ddict->ddict_ser.c_str()) + 1;
    *dd_ser = (char*)malloc(ser_len);
    if (*dd_ser == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for serialized ddict -- out of memory.");
    memcpy((void*)*dd_ser, ddict->ddict_ser.c_str(), ser_len);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_attach(const char * dd_ser, dragonDDictDescr_t * dd, timespec_t * default_timeout) {
    dragonError_t err;
    dragonChannelDescr_t resp_ch;
    dragonFLISerial_t ser_resp_fli;
    dragonChannelDescr_t buffered_resp_ch;
    dragonFLISerial_t ser_buffered_resp_fli;
    dragonFLISerial_t ddict_fli_ser;
    dragonDDict_t * ddict = nullptr;

    if (dd == nullptr)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (dd_ser == nullptr)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Invalid serialized ddict descriptor.");

    // Previously we attached to the orchestrator and got a list of managers
    // Instead, we will now request the local manager on node as the "main manager" and communicate through that
    // If the node does not have a manager, we will request a random one
    // Copy in the serialized descriptor for the orchestrator
    ddict = new dragonDDict_t(dd_ser, default_timeout);
    ddict_fli_ser.data = dragon_base64_decode(dd_ser, &ddict_fli_ser.len);
    err = dragon_fli_attach(&ddict_fli_ser, nullptr, &ddict->orchestrator_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not attach to orchestrator.");
        goto input_argument_fail;
    }

    /* Get the return channels for the client. This includes a streaming and a buffered return channel.
       Streaming requires one extra message per conversation but allow value data to be streamed back
       to the client. */

    // Create stream channel. We need it to send request to orchestrator later to ask for a random manager.
    err = dragon_create_process_local_channel(&ddict->strm_ch, 0, 0, ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create stream channel.");
        goto input_argument_fail;
    }

    // Create response channel.
    err = dragon_create_process_local_channel(&resp_ch, 0, 0, ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create response channel.");
        goto input_argument_fail;
    }

    err = dragon_fli_create(&ddict->respFLI, &resp_ch, nullptr, nullptr, 0, nullptr, false, nullptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create response FLI.");
        goto input_argument_fail;
    }

    err = dragon_fli_serialize(&ddict->respFLI, &ser_resp_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not serialize response FLI.");
        goto input_argument_fail;
    }

    ddict->respFLIStr = dragon_base64_encode(ser_resp_fli.data, ser_resp_fli.len);
    err = dragon_fli_serial_free(&ser_resp_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not free serialized response FLI.");
        goto input_argument_fail;
    }

    // Create buffered response channel
    err = dragon_create_process_local_channel(&buffered_resp_ch, 0, 0, ddict->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create buffered response channel.");
        goto input_argument_fail;
    }

    err = dragon_fli_create(&ddict->bufferedRespFLI, &buffered_resp_ch, nullptr, nullptr, 0, nullptr, true, nullptr);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not create buffered response FLI.");
        goto input_argument_fail;
    }

    err = dragon_fli_serialize(&ddict->bufferedRespFLI, &ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not serialize buffered response FLI.");
        goto input_argument_fail;
    }

    ddict->bufferedRespFLIStr = dragon_base64_encode(ser_buffered_resp_fli.data, ser_buffered_resp_fli.len);
    err = dragon_fli_serial_free(&ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not free serialized buffered response FLI.");
        goto input_argument_fail;
    }

    // Get random manage from local service.
    // If no manager on current node, client
    // request a random manager from orchestrator.
    err = _get_main_manager(ddict);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not get main manager.");
        goto input_argument_fail;
    }

    err = _register_client_to_main_manager(ddict);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not register client to main manager.");
        goto input_argument_fail;
    }

    err = _get_local_managers(ddict);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not get local managers.");
        goto input_argument_fail;
    }

    err = _add_umap_ddict_entry(dd, ddict);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not add new ddict entry to umap.");
        goto input_argument_fail;
    }

    no_err_return(DRAGON_SUCCESS);

    input_argument_fail:
        delete ddict;

    append_err_return(err, "Could not attach to the dictionary.");
}

dragonError_t dragon_ddict_detach(dragonDDictDescr_t * dd_descr) {
    dragonError_t err;
    DDDeregisterClientMsg * deregisterClientMsg = nullptr;
    DDDeregisterClientResponseMsg * deregisterClientResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonDDict_t * dd = nullptr;
    std::unordered_map<uint64_t, dragonFLIDescr_t>::iterator manager_it;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not find ddict object.");
        goto ddict_req_from_descr_fail;
    }

    if (dd->detached)
        no_err_return(DRAGON_SUCCESS);

    dd->detached = true;

    for (manager_it=dd->manager_table.begin() ; manager_it!=dd->manager_table.end() ; manager_it++) {
        deregisterClientMsg = new DDDeregisterClientMsg(_tag_inc(dd), dd->clientID, dd->bufferedRespFLIStr.c_str());
        err = _send_receive(&manager_it->second, &dd->strm_ch, deregisterClientMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
        if (err != DRAGON_SUCCESS)  {
            append_err_noreturn("Could not send the deregister client message and receive response.");
            goto send_receive_fail;
        }

        if (resp_msg->tc() != DDDeregisterClientResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Did not get expected deregister client response message.");
            goto send_receive_fail;
        }

        deregisterClientResponseMsg = (DDDeregisterClientResponseMsg*) resp_msg;
        if (deregisterClientResponseMsg->err() != DRAGON_SUCCESS) {
            err = deregisterClientResponseMsg->err();
            append_err_noreturn(deregisterClientResponseMsg->errInfo());
            goto send_receive_fail;
        }

        err = dragon_fli_detach(&manager_it->second);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not detach from the manager fli.");
            goto send_receive_fail;
        }
        delete deregisterClientMsg;
        delete resp_msg;
    }

    err = dragon_fli_detach(&dd->orchestrator_fli);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not detach from orchestrator.");
        goto send_receive_fail;
    }

    // free resources
    delete dd;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete deregisterClientMsg;
        if (resp_msg != nullptr)
            delete resp_msg;

    ddict_req_from_descr_fail:
        if (dd != nullptr)
            delete dd;

    append_err_return(err, "Could not detach from the dictionary.");
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

dragonError_t dragon_ddict_create_request(const dragonDDictDescr_t * descr, const dragonDDictRequestDescr_t * req_descr) {
    // Generate a request to do read/write operations to the dictionary
    // Once accepted, perform operations, then finalize request

    if (descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict request descriptor.");

    // // Validate ddict exists
    dragonDDict_t * ddict;
    dragonError_t err = _ddict_from_descr(descr, &ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    dragonDDictReq_t * req;
    // Check if req exists in umap
    err = _ddict_req_from_descr(req_descr, &req);
    if (err == DRAGON_SUCCESS)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Request already exists, cannot overwrite.");

    // If not, add to umap
    req = (dragonDDictReq_t*)malloc(sizeof(dragonDDictReq_t));
    if (req == nullptr)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for request -- out of memory.");

    req->ddict = ddict;
    req->dd_uid = ddict->dd_uid;
    req->buffer_size = 0;
    req->buffered_allocs = nullptr;
    req->key_data = nullptr;
    req->key_hash = 0UL;
    req->op_type = DRAGON_DDICT_NO_OP;
    req->num_writes = 0;
    req->recvh_closed = true;

    err = _add_umap_ddict_req_entry(req_descr, req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not add new request entry");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_finalize_request(const dragonDDictRequestDescr_t * req_descr) {
    // Finalize a request to do read/write operations
    // This lets the orchestrator know the client is done for now
    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    dragonDDict_t * dd = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonDDictReq_t * req = nullptr;
    DDPutResponseMsg * putResponseMsg = nullptr;
    std::set<uint64_t> msg_tags;

    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find request object.");

    switch(req->op_type) {
        case DRAGON_DDICT_NO_OP:
        case DRAGON_DDICT_FINALIZED:
            err_return(DRAGON_INVALID_OPERATION, "Invalid request.");
            break;
        case DRAGON_DDICT_CONTAINS_REQ:
            // Nothing to do currently, break and drop to cleanup
            break;
        case DRAGON_DDICT_POP_REQ:
        case DRAGON_DDICT_GET_REQ:
            // Read operations are finished, close recv handle
            if (req->recvh_closed == false) {
                err = dragon_fli_close_recv_handle(&req->recvh, req->ddict->timeout);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not close receive handle.");
                req->recvh_closed = true;
            }
            break;

        case DRAGON_DDICT_PUT_REQ:
            // Close send handle (signals "done")
            err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not close send handle.");

            // Get our response back confirming close
            dd = req->ddict;
            msg_tags.clear();
            msg_tags.insert(req->msg_tag);
            err = _recv_resp(&dd->bufferedRespFLI, &resp_msg, msg_tags, req->ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get put response.");

            if (resp_msg->tc() != DDPutResponseMsg::TC)
                err_return(DRAGON_FAILURE, "Failed to get expected put response message.");

            // TODO: Can do more handling of the message after casting
            putResponseMsg = (DDPutResponseMsg*) resp_msg;
            if (putResponseMsg->err() != DRAGON_SUCCESS) {
                err = putResponseMsg->err();
                append_err_noreturn(putResponseMsg->errInfo());
                delete putResponseMsg;
                append_err_return(err, "Failed to finalize put request.");
            }

            //Cleanup
            delete putResponseMsg;
            break;

        default:
            err_return(DRAGON_INVALID_ARGUMENT, "Unimplemented or invalid operator type.");
    }

    req->op_type = DRAGON_DDICT_FINALIZED;
    if (req != nullptr) {
        err = _cleanup_request(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not cleanup request.");
        free(req);
    }
    dragon_umap_delitem(dg_ddict_reqs, req_descr->_idx); // Remove request from umap

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_write_bytes(const dragonDDictRequestDescr_t * req_descr, size_t num_bytes, uint8_t * bytes) {
    dragonDDictReq_t * req = nullptr;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (bytes == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid bytes. Bytes should be non null.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Failed to find request object.");
    }

    switch(req->op_type) {
        case DRAGON_DDICT_PUT_REQ:
            // key is written and sent to manager, write values now
            if (req->key_data != nullptr) {
                if (req->op_type != DRAGON_DDICT_PUT_REQ)
                    err_return(DRAGON_INVALID_OPERATION, "Trying to perform a write operation with a non-write request.");

                // Write data out normally, key is done
                uint64_t arg = VALUE_HINT;
                err = dragon_fli_send_bytes(&req->sendh, num_bytes, bytes, arg, false, req->ddict->timeout);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not write bytes.");
                break;
            }
        case DRAGON_DDICT_GET_REQ:
        case DRAGON_DDICT_CONTAINS_REQ:
        case DRAGON_DDICT_NO_OP:
            // Key hasn't been written, we need to write key first
            err = _buffer_bytes(req, bytes, num_bytes);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not buffer key message.");
            break;
        default:
            err_return(DRAGON_INVALID_ARGUMENT, "Invalid writes bytes for given request.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_read_bytes(const dragonDDictRequestDescr_t* req_descr, size_t requested_size,
                        size_t* received_size, uint8_t** bytes) {

    dragonDDictReq_t * req = nullptr;
    dragonError_t err;
    uint64_t arg = 0;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (received_size == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid received_size. Received size should be non null.");

    if (bytes == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid bytes. Bytes should be non null.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    // Operation type is set in the get/pop/etc call after setting the key
    if (req->op_type != DRAGON_DDICT_GET_REQ && req->op_type != DRAGON_DDICT_POP_REQ)
        err_return(DRAGON_INVALID_OPERATION, "Invalid operation type.");

    err = dragon_fli_recv_bytes(&req->recvh, requested_size, received_size, bytes, &arg, req->ddict->timeout);
    // EOT, no more data to read back
    if (err == DRAGON_EOT) {
        no_err_return(err);
    } else if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not read bytes from dictionary.");
    }

    if (arg != VALUE_HINT)
        err_return(DRAGON_FAILURE, "Received unexpected arg value.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_read_bytes_into(const dragonDDictRequestDescr_t* req_descr, size_t requested_size,
                              size_t* received_size, uint8_t* bytes) {

    dragonDDictReq_t * req = nullptr;
    dragonError_t err;
    uint64_t arg = 0;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (received_size == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid received_size. Received size should be non null.");

    if (bytes == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid bytes. Bytes should be non null.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    // Operation type is set in the get/pop/etc call after setting the key
    if (req->op_type != DRAGON_DDICT_GET_REQ && req->op_type != DRAGON_DDICT_POP_REQ)
        err_return(DRAGON_INVALID_OPERATION, "Invalid operation type.");

    err = dragon_fli_recv_bytes_into(&req->recvh, requested_size, received_size, bytes, &arg, req->ddict->timeout);
    if (err == DRAGON_EOT) {
        no_err_return(DRAGON_EOT); //return DRAGON_EOT for user handling, handle close in ddict_finalize_request()
    } else if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not receive bytes from fli.");
    }

    if (arg != VALUE_HINT)
        err_return(DRAGON_FAILURE, "Received unexpected arg value.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_read_mem(const dragonDDictRequestDescr_t* req_descr, dragonMemoryDescr_t* mem_descr) {

    dragonDDictReq_t * req = nullptr;
    dragonError_t err;
    uint64_t arg = 0;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (mem_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid memory descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    // Operation type is set in the get/pop/etc call after setting the key
    if (req->op_type != DRAGON_DDICT_GET_REQ && req->op_type != DRAGON_DDICT_POP_REQ)
        err_return(DRAGON_INVALID_OPERATION, "Invalid operation type.");

    err = dragon_fli_recv_mem(&req->recvh, mem_descr, &arg, req->ddict->timeout);
    if (err == DRAGON_EOT) {
        no_err_return(DRAGON_EOT); //return DRAGON_EOT for user handling, handle close in ddict_finalize_request()
    } else if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Failed to read mem from dictionary.");
    }

    if (arg != VALUE_HINT)
        err_return(DRAGON_FAILURE, "Received unexpected arg value.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_contains(const dragonDDictRequestDescr_t * req_descr) {

    dragonDDictReq_t * req;
    dragonError_t err;
    DDContainsMsg * containsMsg = nullptr;
    DDContainsResponseMsg * containsResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;
    dragonError_t resp_err;
    dragonError_t close_recvh_err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    // key_data means we've already written and sent it, out-of-order operation
    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_CONTAINS_REQ;

    if (req->key_data == nullptr && req->buffered_allocs != nullptr) {

        err = _choose_manager_build_key(req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not build key or connect to manager.");
            goto choose_manager_build_key_fail;
        }

        err = _check_manager_connection(req->ddict, req->manager_id);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not connect to the manager.");
            goto choose_manager_build_key_fail;
        }
        req->manager_fli = req->ddict->manager_table[req->manager_id];

        containsMsg = new DDContainsMsg(_tag_inc(req->ddict), req->ddict->clientID, req->ddict->chkpt_id);
        err = _send_msg_key_no_close_sendh(containsMsg, req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the contains message and key.");
            goto choose_manager_build_key_fail;
        }

        err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not close send handle.");
            goto choose_manager_build_key_fail;
        }

        resp_err = _recv_dmsg_no_close_recvh(&recvh, &req->ddict->bufferedRespFLI, &resp_msg, containsMsg->tag(), true, req->ddict->timeout);
        if (resp_err != DRAGON_SUCCESS && resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED) {
            err = resp_err;
            append_err_noreturn("Could not receive contains response message.");
            goto send_receive_fail;
        }
        req->recvh_closed = false;

        err = dragon_fli_close_recv_handle(&recvh, req->ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not close receive handle");
            goto close_recvh_fail;
        }
        req->recvh_closed = true;

        if (resp_msg->tc() != DDContainsResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected contains response message.");
            goto close_recvh_fail;
        }

        containsResponseMsg = (DDContainsResponseMsg*) resp_msg;
        resp_err =containsResponseMsg->err();
        if (resp_err != DRAGON_SUCCESS && resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED) {
            err = containsResponseMsg->err();
            append_err_noreturn(containsResponseMsg->errInfo());
            goto close_recvh_fail;
        }
    } else {
        err_return(DRAGON_INVALID_OPERATION, "No data present in request");
    }

    delete containsMsg;
    delete containsResponseMsg;

    no_err_return(resp_err);

    send_receive_fail:
        close_recvh_err = dragon_fli_close_recv_handle(&recvh, req->ddict->timeout);
        if (close_recvh_err != DRAGON_SUCCESS) {
            err = close_recvh_err;
            append_err_noreturn("Could not close receive handle");
        } else {
            req->recvh_closed = true;
        }
    close_recvh_fail:
        if (resp_msg != nullptr)
            delete resp_msg;
    choose_manager_build_key_fail:
        delete containsMsg;

    append_err_return(err, "Failed to perform contains operation.");
}

dragonError_t dragon_ddict_get(const dragonDDictRequestDescr_t* req_descr) {

    dragonDDictReq_t * req;
    dragonError_t err;
    dragonError_t resp_err;
    dragonError_t recv_and_discard_err;
    DDGetMsg * getMsg = nullptr;
    DDGetResponseMsg * getResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_GET_REQ;

    if (req->key_data == nullptr && req->buffered_allocs != nullptr) {

        err = _choose_manager_build_key(req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not build key or connect to manager.");
            goto choose_manager_build_key_fail;
        }

        err = _check_manager_connection(req->ddict, req->manager_id);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not connect to the manager.");
            goto choose_manager_build_key_fail;
        }
        req->manager_fli = req->ddict->manager_table[req->manager_id];

        req->msg_tag = _tag_inc(req->ddict);
        getMsg = new DDGetMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id);
        err = _send_msg_key_no_close_sendh(getMsg, req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the get message and key.");
            goto choose_manager_build_key_fail;
        }

        err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not close send handle.");
            goto choose_manager_build_key_fail;
        }

        err = _recv_dmsg_no_close_recvh(&recvh, &req->ddict->respFLI, &resp_msg, getMsg->tag(), false, req->ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Failed to get expected get response message.");
            goto recv_fail;
        }
        req->recvh = recvh;
        req->recvh_closed = false;

        if (resp_msg->tc() != DDGetResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected get response message.");
            goto recv_fail;
        }

        getResponseMsg = (DDGetResponseMsg*) resp_msg;
        resp_err = getResponseMsg->err();
        // Close receive handle if the error code is not SUCCESS.
        if (resp_err != DRAGON_SUCCESS) {
            recv_and_discard_err = _recv_and_discard(&req->recvh, req->ddict->timeout);
            if (recv_and_discard_err != DRAGON_SUCCESS) {
                err = recv_and_discard_err;
                append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
                goto close_recvh_fail;
            }
            req->recvh_closed = true;

            if (resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED) {
                err = getResponseMsg->err();
                append_err_noreturn(getResponseMsg->errInfo());
                goto close_recvh_fail;
            }
        }
    } else {
        err_return(DRAGON_INVALID_OPERATION, "No data present in request.");
    }

    delete getMsg;
    delete getResponseMsg;

    no_err_return(resp_err);

    recv_fail:

        recv_and_discard_err = _recv_and_discard(&req->recvh, req->ddict->timeout);
        if (recv_and_discard_err != DRAGON_SUCCESS) {
            err = recv_and_discard_err;
            append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
        } else {
            req->recvh_closed = true;
        }
    close_recvh_fail:
        if (resp_msg != nullptr)
            delete resp_msg;
    choose_manager_build_key_fail:
        delete getMsg;

    append_err_return(err, "Failed to perform get operation.");
}

dragonError_t dragon_ddict_put(const dragonDDictRequestDescr_t* req_descr) {

    dragonError_t err;
    bool persist = false;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _put(req_descr, persist);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not perform put op.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_pput(const dragonDDictRequestDescr_t* req_descr) {

    dragonError_t err;
    bool persist = true;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _put(req_descr, persist);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not perform persistent put op.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_length(const dragonDDictDescr_t * dd_descr, uint64_t* length) {

    dragonError_t err;
    DDLengthMsg * lenMsg = nullptr;
    DDLengthResponseMsg * lengthResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    uint64_t msg_tag;
    bool broadcast = false;
    uint64_t selected_manager;
    uint64_t resp_num = 0;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (length == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict length. Length should be non null.");

    *length = 0;

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

    broadcast = !dd->has_chosen_manager;
    if (broadcast) {
        selected_manager = 0;
        resp_num = dd->num_managers;
    } else {
        selected_manager = dd->chosen_manager;
        resp_num = 1;
    }

    // check and connect to selected manager
    err = _check_manager_connection(dd, selected_manager);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    // prepare length request
    msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    lenMsg = new DDLengthMsg(msg_tag, dd->clientID, dd->bufferedRespFLIStr.c_str(), dd->chkpt_id, broadcast);

    // send length request to the selected manager
    err = _send(&dd->manager_table[selected_manager], &dd->strm_ch, lenMsg, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the length message to root manager.");
        goto send_receive_fail;
    }

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for responses -- out of memory.");
        goto send_receive_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to receive length response.");
        goto send_receive_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid respose.");
            goto send_receive_fail;
        }

        if (resp_msgs[i]->tc() != DDLengthResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected length response message.");
            goto send_receive_fail;
        }

        lengthResponseMsg = (DDLengthResponseMsg*) resp_msgs[i];
        if (lengthResponseMsg->err() != DRAGON_SUCCESS) {
            err = lengthResponseMsg->err();
            append_err_noreturn(lengthResponseMsg->errInfo());
            goto send_receive_fail;
        }
        *length += lengthResponseMsg->length();

        delete resp_msgs[i];
    }

    delete lenMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete lenMsg;
        for (size_t i=0 ; i<resp_num ; i++) {
            if (resp_msgs[i] != nullptr)
                delete resp_msgs[i];
        }
        delete[] resp_msgs;

    append_err_return(err, "Failed to perform length operation.");
}

dragonError_t dragon_ddict_clear(const dragonDDictDescr_t * dd_descr) {

    dragonError_t err;
    DDClearMsg * clearMsg = nullptr;
    DDClearResponseMsg * clearResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    uint64_t msg_tag;
    bool broadcast = false;
    uint64_t selected_manager;
    uint64_t resp_num = 0;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    broadcast = !dd->has_chosen_manager;
    if (broadcast) {
        selected_manager = 0;
        resp_num = dd->num_managers;
    } else {
        selected_manager = dd->chosen_manager;
        resp_num = 1;
    }

    // check and connect to selected manager
    err = _check_manager_connection(dd, selected_manager);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    // prepare clear request
    msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    clearMsg = new DDClearMsg(msg_tag, dd->clientID, dd->bufferedRespFLIStr.c_str(), dd->chkpt_id, broadcast);

    // send clear request to the selected manager
    err = _send(&dd->manager_table[selected_manager], &dd->strm_ch, clearMsg, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the clear message to root manager.");
        goto send_receive_fail;
    }

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for responses -- out of memory.");
        goto send_receive_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not receive clear responses.");
        goto send_receive_fail;
    }

    // receive response from all managers
    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid respose.");
            goto send_receive_fail;
        }

        if (resp_msgs[i]->tc() != DDClearResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected clear response message.");
            goto send_receive_fail;
        }

        clearResponseMsg = (DDClearResponseMsg*) resp_msgs[i];
        if (clearResponseMsg->err() != DRAGON_SUCCESS) {
            err = clearResponseMsg->err();
            append_err_noreturn(clearResponseMsg->errInfo());
            goto send_receive_fail;
        }

        delete resp_msgs[i];
    }

    delete clearMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);

    // cleanup with abnormal exit
    send_receive_fail:
        delete clearMsg;
        for (size_t i=0 ; i<resp_num ; i++) {
            if (resp_msgs[i] != nullptr)
                delete resp_msgs[i];
        }
        delete[] resp_msgs;

    append_err_return(err, "Failed to perform clear operation.");
}

dragonError_t dragon_ddict_pop(const dragonDDictRequestDescr_t * req_descr) {

    dragonDDictReq_t * req;
    dragonError_t err;
    dragonError_t resp_err;
    dragonError_t recv_and_discard_err;
    DDPopMsg * popMsg = nullptr;
    DDPopResponseMsg * popResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    // key_data means we've already written and sent it, out-of-order operation
    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_POP_REQ;

    if (req->key_data == nullptr && req->buffered_allocs != nullptr) {

        err = _choose_manager_build_key(req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not build key or connect to manager.");
            goto choose_manager_build_key_fail;
        }

        err = _check_manager_connection(req->ddict, req->manager_id);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not connect to the manager.");
            goto choose_manager_build_key_fail;
        }
        req->manager_fli = req->ddict->manager_table[req->manager_id];

        req->msg_tag = _tag_inc(req->ddict);
        popMsg = new DDPopMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id);
        err = _send_msg_key_no_close_sendh(popMsg, req);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the pop message and key.");
            goto choose_manager_build_key_fail;
        }

        err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not close send handle.");
            goto choose_manager_build_key_fail;
        }

        resp_err = _recv_dmsg_no_close_recvh(&recvh, &req->ddict->respFLI, &resp_msg, popMsg->tag(), false, req->ddict->timeout);
        if (resp_err != DRAGON_SUCCESS) {
            err = resp_err;
            append_err_noreturn("Could not receive pop response message.");
            goto recv_fail;
        }
        req->recvh = recvh;
        req->recvh_closed = false;

        if (resp_msg->tc() != DDPopResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected pop response message.");
            goto recv_fail;
        }

        popResponseMsg = (DDPopResponseMsg*) resp_msg;
        resp_err = popResponseMsg->err();
        // Close receive handle if the error code is not SUCCESS.
        if (resp_err != DRAGON_SUCCESS) {
            // read and discard the remaining content in the recvh
            recv_and_discard_err = _recv_and_discard(&req->recvh, req->ddict->timeout);
            if (recv_and_discard_err != DRAGON_SUCCESS) {
                err = recv_and_discard_err;
                append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
                goto close_recvh_fail;
            }
            req->recvh_closed = true;

            if (resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED) {
                err = popResponseMsg->err();
                append_err_noreturn(popResponseMsg->errInfo());
                goto close_recvh_fail;
            }
        }
    } else {
        err_return(DRAGON_INVALID_OPERATION, "No data present in request.");
    }

    delete popMsg;
    delete popResponseMsg;

    no_err_return(resp_err);

    recv_fail:
        recv_and_discard_err = _recv_and_discard(&req->recvh, req->ddict->timeout);
        if (recv_and_discard_err != DRAGON_SUCCESS) {
            err = recv_and_discard_err;
            append_err_noreturn("Could not receive and discard remaining content in the receive handle.");
        } else {
            req->recvh_closed = true;
        }
    close_recvh_fail:
        if (resp_msg != nullptr)
            delete resp_msg;

    choose_manager_build_key_fail:
        delete popMsg;

    append_err_return(err, "Failed to perform pop operation.");
}

dragonError_t dragon_ddict_keys(const dragonDDictDescr_t * dd_descr, dragonDDictKey_t*** keys, size_t * num_keys) {
    dragonError_t err;
    std::vector<dragonDDictKey_t*> key_vec;

    err = dragon_ddict_keys_vec(dd_descr, key_vec);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get keys from ddict.");

    *keys = (dragonDDictKey_t**) malloc(sizeof(dragonDDictKey_t*)*key_vec.size());

    if (*keys == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for keys array.");

    for (size_t idx=0; idx < key_vec.size(); idx++) {
        (*keys)[idx] = key_vec[idx];
        key_vec[idx] = nullptr;
    }

    *num_keys = key_vec.size();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_local_keys(const dragonDDictDescr_t * dd_descr, dragonDDictKey_t*** local_keys, size_t * num_local_keys) {
    dragonError_t err;
    std::vector<dragonDDictKey_t*> key_vec;

    err = dragon_ddict_local_keys_vec(dd_descr, key_vec);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get local keys from ddict.");

    *local_keys = (dragonDDictKey_t**) malloc(sizeof(dragonDDictKey_t*)*key_vec.size());

    if (*local_keys == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for local keys array.");

    for (size_t idx=0; idx < key_vec.size(); idx++) {
        (*local_keys)[idx] = key_vec[idx];
        key_vec[idx] = nullptr;
    }

    *num_local_keys = key_vec.size();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_stats(const dragonDDictDescr_t * dd_descr) {
    // TBD
    return DRAGON_NOT_IMPLEMENTED;
}

dragonError_t dragon_ddict_checkpoint(const dragonDDictDescr_t* dd_descr) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        append_err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    // increase checkpoint ID
    dd->chkpt_id += 1;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_rollback(const dragonDDictDescr_t * dd_descr) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    if (dd->chkpt_id > 0)
        dd->chkpt_id -= 1;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_sync_to_newest_checkpoint(const dragonDDictDescr_t * dd_descr) {

    dragonError_t err;
    DDManagerNewestChkptIDMsg * managerNewestChkptIDMsg = nullptr;
    DDManagerNewestChkptIDResponseMsg * managerNewestChkptIDResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    uint64_t msg_tag;
    uint64_t newestChkptID = 0;
    bool broadcast = false;
    uint64_t selected_manager;
    uint64_t resp_num = 0;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    broadcast = !dd->has_chosen_manager;
    if (broadcast) {
        selected_manager = 0;
        resp_num = dd->num_managers;
    } else {
        selected_manager = dd->chosen_manager;
        resp_num = 1;
    }

    // check and connect to selected manager
    err = _check_manager_connection(dd, selected_manager);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not connect to root manager.");

    // prepare request
    msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    managerNewestChkptIDMsg = new DDManagerNewestChkptIDMsg(msg_tag, dd->bufferedRespFLIStr.c_str(), broadcast);

    // send request to the selected manager
    err = _send(&dd->manager_table[selected_manager], &dd->strm_ch, managerNewestChkptIDMsg, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not send the sync to newest checkpoint message to root manager.");
        goto send_receive_fail;
    }

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for responses -- out of memory.");
        goto send_receive_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not receive sync to newest checkpoint response.");
        goto send_receive_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid respose.");
            goto send_receive_fail;
        }

        if (resp_msgs[i]->tc() != DDManagerNewestChkptIDResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected newest checkpoint response message.");
            goto send_receive_fail;
        }

        managerNewestChkptIDResponseMsg = (DDManagerNewestChkptIDResponseMsg*) resp_msgs[i];
        if (managerNewestChkptIDResponseMsg->err() != DRAGON_SUCCESS) {
            err = managerNewestChkptIDResponseMsg->err();
            append_err_noreturn(managerNewestChkptIDResponseMsg->errInfo());
            goto send_receive_fail;
        }
        newestChkptID = max(newestChkptID, managerNewestChkptIDResponseMsg->chkptID());

        delete resp_msgs[i];
    }

    dd->chkpt_id = newestChkptID;

    delete managerNewestChkptIDMsg;
    delete[] resp_msgs;
    no_err_return(DRAGON_SUCCESS);

    send_receive_fail:
        delete managerNewestChkptIDMsg;
        for (size_t i=0 ; i<resp_num ; i++) {
            if (resp_msgs[i] != nullptr)
                delete resp_msgs[i];
        }
        delete[] resp_msgs;

    append_err_return(err, "Failed to perform sync to newest checkpoint.");
}

dragonError_t dragon_ddict_checkpoint_id(const dragonDDictDescr_t * dd_descr, uint64_t * chkpt_id) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (chkpt_id == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid checkpoint ID. Checkpoint ID should be non null.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    *chkpt_id = dd->chkpt_id;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ddict_set_checkpoint_id(const dragonDDictDescr_t * dd_descr, uint64_t chkpt_id) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    dd->chkpt_id = chkpt_id;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_local_manager(const dragonDDictDescr_t * dd_descr, uint64_t * local_manager_id) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (local_manager_id == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid local manager ID. Local manager ID should be non null.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    if (dd->has_local_manager)
        *local_manager_id = dd->local_manager;
    else
        err = DRAGON_KEY_NOT_FOUND;

    no_err_return(err);
}

dragonError_t dragon_ddict_main_manager(const dragonDDictDescr_t * dd_descr, uint64_t * main_manager) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (main_manager == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid main manager. Main manager should be non null.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    *main_manager = dd->main_manager;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_manager(const dragonDDictDescr_t * dd_descr, dragonDDictDescr_t * dd_new_descr, uint64_t id) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    dragonDDict_t * dd_copy = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    if (id < 0 || id >= dd->num_managers)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid manager id.");

    // create a new client
    err = dragon_ddict_attach(dd->ddict_ser.c_str(), dd_new_descr, dd->timeout);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not create a new client.");

    err = _ddict_from_descr(dd_new_descr, &dd_copy);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    dd_copy->chkpt_id = dd->chkpt_id;
    dd_copy->chosen_manager = id;
    dd_copy->has_chosen_manager = true;

    err = _add_umap_ddict_entry(dd_new_descr, dd_copy);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not add new ddict entry to umap.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_empty_managers(const dragonDDictDescr_t * dd_descr, uint64_t ** manager_ids, size_t * num_empty_managers) {

    dragonError_t err;
    std::vector<uint64_t> id_vec;

    err = dragon_ddict_empty_managers_vec(dd_descr, id_vec);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get empty managers from ddict.");

    *manager_ids = (uint64_t*) malloc(id_vec.size() * sizeof(uint64_t));
    if (*manager_ids == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for empty managers array.");

    for (size_t i=0 ; i<id_vec.size() ; i++)
        (*manager_ids)[i] = id_vec[i];

    *num_empty_managers = id_vec.size();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _dragon_ddict_client_ID(const dragonDDictDescr_t * dd_descr, uint64_t * client_id) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    *client_id = dd->clientID;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_local_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& id_vec) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    for (size_t i=0 ; i<dd->local_managers.size() ; i++)
        id_vec.push_back(dd->local_managers[i]);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_local_managers(const dragonDDictDescr_t * dd_descr, uint64_t ** local_manager_ids, size_t * num_local_managers) {

    dragonError_t err;
    std::vector<uint64_t> id_vec;

    if (local_manager_ids == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The address of local manager id array must be non-null.");

    if (num_local_managers == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The address of number of local managers must be non-null.");

    err = dragon_ddict_local_managers_vec(dd_descr, id_vec);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get empty managers from ddict.");

    *local_manager_ids = (uint64_t*) malloc(id_vec.size() * sizeof(uint64_t));
    if (*local_manager_ids == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for local managers array.");

    for (size_t i=0 ; i<id_vec.size() ; i++)
        (*local_manager_ids)[i] = id_vec[i];

    *num_local_managers = id_vec.size();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_synchronize(const char ** serialized_ddicts, const size_t num_serialized_ddicts, timespec_t * timeout) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    dragonDDictDescr_t dd_descr;
    std::set<uint64_t> msg_tags;
    std::vector<std::string> empty_managers;
    std::vector<std::string> full_managers;
    uint64_t msg_tag = 0;

    DDGetManagersMsg * getManagersMsg = nullptr;
    DDGetManagersResponseMsg * getManagersResponseMsg = nullptr;
    DDManagerSyncMsg * managerSyncMsg = nullptr;
    DDManagerSyncResponseMsg * managerSyncResponseMsg = nullptr;
    DDUnmarkDrainedManagersMsg * unmarkDrainedManagersMsg = nullptr;
    DDUnmarkDrainedManagersResponseMsg * unmarkDrainedManagersResponseMsg = nullptr;

    DragonResponseMsg ** resp_msgs = nullptr;
    DragonResponseMsg ** manager_sync_resp_msgs = nullptr;

    dragonFLISerial_t fli_ser;
    dragonFLIDescr_t connection;

    size_t num_managers = 0;
    size_t resp_num = 0;
    size_t manager_sync_resp_num = 0;
    size_t num_vec_append = 0;
    char * empty_fli = nullptr;
    char * full_fli = nullptr;

    std::vector<uint64_t> manager_ids; // a vector of manager IDs to unmark

    if (num_serialized_ddicts == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Number of serialized dictionaries must be greater that zero.");

    if (serialized_ddicts == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "List of serialized dictionary must be non null.");

    // attach to the first dictionary
    err = dragon_ddict_attach(serialized_ddicts[0], &dd_descr, timeout);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not create a new client.");

    err = _ddict_from_descr(&dd_descr, &dd);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not find ddict object.");
        goto send_get_managers_fail;
    }

    for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
        msg_tag = _tag_inc(dd);
        getManagersMsg = new DDGetManagersMsg(msg_tag, dd->bufferedRespFLIStr.c_str());
        msg_tags.insert(msg_tag);

        fli_ser.data = dragon_base64_decode(serialized_ddicts[i], &fli_ser.len);
        err = dragon_fli_attach(&fli_ser, nullptr, &connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not attach to the orchestrator to synchronize dictionaries.");
            goto send_get_managers_fail;
        }

        err = _send(&connection, nullptr, getManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the get managers message to the orchestrator to synchronize dictionaries.");
            goto send_get_managers_fail;
        }

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not detach from the orchestrator to synchronize dictionaries.");
            goto send_get_managers_fail;
        }
        delete getManagersMsg;
    }

    // receive responsese from all orchestrators
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for get managers responses -- out of memory.");
        goto send_get_managers_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to receive get managers response.");
        goto receive_responses_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid response.");
            goto receive_responses_fail;
        }

        if (resp_msgs[i]->tc() != DDGetManagersResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected get managers response message.");
            goto receive_responses_fail;
        }

        getManagersResponseMsg = (DDGetManagersResponseMsg*) resp_msgs[i];
        if (getManagersResponseMsg->err() != DRAGON_SUCCESS) {
            err = getManagersResponseMsg->err();
            append_err_noreturn(getManagersResponseMsg->errInfo());
            goto receive_responses_fail;
        }
    }

    // build empty and full manager list
    num_managers = ((DDGetManagersResponseMsg*) resp_msgs[0])->managers().size();
    // Check managers with the same manager_id from all parallel dictionaries
    for (uint64_t manager_id=0 ; manager_id < (uint64_t) num_managers ; manager_id++) {
        empty_managers.clear();
        full_managers.clear();
        for (size_t i=0 ; i<resp_num ; i++) {
            getManagersResponseMsg = (DDGetManagersResponseMsg*) resp_msgs[i];
            // If the manager is empty, we append its fli to the empty_manager list.
            // Otherwise append its fli to full_managers.
            if (getManagersResponseMsg->emptyManagers()[manager_id])
                empty_managers.push_back(getManagersResponseMsg->managers()[manager_id]);
            else
                full_managers.push_back(getManagersResponseMsg->managers()[manager_id]);
        }

        // Sync manager only when there's any empty manager
        if (empty_managers.size() > 0) {
            if (full_managers.size() == 0) {
                fprintf(stderr, "No full manager for manager %ld\n", manager_id);
                fflush(stderr);
                err = DRAGON_FAILURE;
                append_err_noreturn("Failed to synchronize dictionary.");
                goto managers_sync_fail;
            }

            // guarantees the length of full managers is equal to or longer then empty managers.
            if (full_managers.size() < empty_managers.size()) {
                size_t full_managers_size = full_managers.size();
                num_vec_append = (size_t) empty_managers.size()/full_managers_size;
                full_managers.resize(full_managers_size * (num_vec_append + 1));
                for (size_t i=0 ; i<num_vec_append ; i++) {
                    std::copy(full_managers.begin(), full_managers.begin() + full_managers_size, full_managers.begin() + full_managers_size*(i+1));
                }
            }

            // Send sync request along with empty managers fli to full managers so that full managers can send the data to reconstruct
            // empty managers directly
            msg_tags.clear();
            for (size_t i=0 ; i<empty_managers.size() ; i++) {
                empty_fli = (char*)empty_managers[i].c_str();
                full_fli = (char*)full_managers[i].c_str();
                msg_tag = _tag_inc(dd);
                msg_tags.insert(msg_tag);
                managerSyncMsg = new DDManagerSyncMsg(msg_tag, dd->bufferedRespFLIStr.c_str(), empty_fli);

                fli_ser.data = dragon_base64_decode(full_fli, &fli_ser.len);
                err = dragon_fli_attach(&fli_ser, nullptr, &connection);
                if (err != DRAGON_SUCCESS) {
                    append_err_noreturn("Could not attach to the full manager.");
                    goto send_manager_sync_fail;
                }

                err = _send(&connection, &dd->strm_ch, managerSyncMsg, dd->timeout);
                if (err != DRAGON_SUCCESS){
                    append_err_noreturn("Could not send the manager synchronization message to the full manager.");
                    goto send_manager_sync_fail;
                }

                err = dragon_fli_detach(&connection);
                if (err != DRAGON_SUCCESS) {
                    append_err_noreturn("Could not detach from the full manager.");
                    goto send_manager_sync_fail;
                }

                delete managerSyncMsg;
            }

            // Receive sync responses from all full managers
            manager_sync_resp_num = empty_managers.size();
            manager_sync_resp_msgs = new DragonResponseMsg*[manager_sync_resp_num];
            if (manager_sync_resp_msgs == nullptr) {
                err = DRAGON_INTERNAL_MALLOC_FAIL;
                append_err_noreturn("Could not allocate space for manager sync responses -- out of memory.");
                goto managers_sync_fail;
            }

            err = _recv_responses(&dd->bufferedRespFLI, manager_sync_resp_msgs, msg_tags, manager_sync_resp_num, dd->timeout);
            if (err != DRAGON_SUCCESS) {
                append_err_noreturn("Failed to receive manager sync responses.");
                goto managers_sync_fail;
            }

            // check each manager sync responses
            for (size_t i=0 ; i<manager_sync_resp_num ; i++) {
                if (manager_sync_resp_msgs[i] == nullptr) {
                    err = DRAGON_INVALID_MESSAGE;
                    append_err_noreturn("Could not receive valid responses.");
                    goto managers_sync_fail;
                }

                if (manager_sync_resp_msgs[i]->tc() != DDManagerSyncResponseMsg::TC) {
                    err = DRAGON_FAILURE;
                    append_err_noreturn("Failed to get expected manager sync response message.");
                    goto managers_sync_fail;
                }

                managerSyncResponseMsg = (DDManagerSyncResponseMsg*) manager_sync_resp_msgs[i];
                if (managerSyncResponseMsg->err() != DRAGON_SUCCESS) {
                    err = managerSyncResponseMsg->err();
                    append_err_noreturn(managerSyncResponseMsg->errInfo());
                    goto managers_sync_fail;
                }
                delete manager_sync_resp_msgs[i];
            }
            delete[] manager_sync_resp_msgs;
        }
    }

    // free responses
    for (size_t i=0 ; i<resp_num ; i++)
        delete resp_msgs[i];
    delete[] resp_msgs;

    // unmark all managers after recovered successfully
    msg_tags.clear();
    for (size_t i=0 ; i<num_managers ; i++)
        manager_ids.push_back(i);
    for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
        msg_tag = _tag_inc(dd);
        msg_tags.insert(msg_tag);
        unmarkDrainedManagersMsg = new DDUnmarkDrainedManagersMsg(msg_tag, dd->bufferedRespFLIStr.c_str(), manager_ids);
        fli_ser.data = dragon_base64_decode(serialized_ddicts[i], &fli_ser.len);
        err = dragon_fli_attach(&fli_ser, nullptr, &connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not attach to the orchestrator to synchronize dictionaries.");
            goto send_unmark_drained_managers_fail;
        }

        err = _send(&connection, nullptr, unmarkDrainedManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the unmark drained managers message to the orchestrator to synchronize dictionaries.");
            goto send_unmark_drained_managers_fail;
        }

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not detach from the orchestrator to synchronize dictionaries.");
            goto send_unmark_drained_managers_fail;
        }
        delete unmarkDrainedManagersMsg;
    }

    // receive responsese from all orchestrators
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for responses -- out of memory.");
        goto receive_responses_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to receive length response.");
        goto receive_responses_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid response.");
            goto receive_responses_fail;
        }

        if (resp_msgs[i]->tc() != DDUnmarkDrainedManagersResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected unmark drained managers response message.");
            goto receive_responses_fail;
        }

        unmarkDrainedManagersResponseMsg = (DDUnmarkDrainedManagersResponseMsg*) resp_msgs[i];
        if (unmarkDrainedManagersResponseMsg->err() != DRAGON_SUCCESS) {
            err = unmarkDrainedManagersResponseMsg->err();
            append_err_noreturn(unmarkDrainedManagersResponseMsg->errInfo());
            goto receive_responses_fail;
        }
        delete resp_msgs[i];
    }
    delete[] resp_msgs;

    // detach from the first dictionary
    err = dragon_ddict_detach(&dd_descr);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not detach from the client.");

    no_err_return(DRAGON_SUCCESS);

    send_unmark_drained_managers_fail:
        delete unmarkDrainedManagersMsg;
    send_manager_sync_fail:
        delete managerSyncMsg;
    managers_sync_fail:
        if (manager_sync_resp_msgs != nullptr) {
            for (size_t i=0 ; i<manager_sync_resp_num ; i++) {
                if (manager_sync_resp_msgs[i] != nullptr)
                    delete manager_sync_resp_msgs[i];
            }
            delete[] manager_sync_resp_msgs;
        }
    receive_responses_fail:
        if (resp_msgs != nullptr){
            for (size_t i=0 ; i<resp_num ; i++) {
                if (resp_msgs[i] != nullptr)
                    delete resp_msgs[i];
            }
            delete[] resp_msgs;
        }
    send_get_managers_fail:
        if (getManagersMsg != nullptr)
            delete getManagersMsg;
        // detach from the first dictionary
        err = dragon_ddict_detach(&dd_descr);
        if (err != DRAGON_SUCCESS)
            err_return(err, "Could not detach from the client.");

    append_err_return(err, "Failed to synchronize dictionaries.");
}

dragonError_t dragon_ddict_clone(const dragonDDictDescr_t * dd_descr, const char ** serialized_ddicts, const size_t num_serialized_ddicts) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    DDGetMetaDataMsg * getMetaDataMsg = nullptr;
    DDGetMetaDataResponseMsg * getMetaDataResponseMsg = nullptr;
    DDMarkDrainedManagersMsg * markDrainedManagersMsg = nullptr;
    DDMarkDrainedManagersResponseMsg * markDrainedManagersResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;

    dragonFLISerial_t fli_ser;
    dragonFLIDescr_t connection;

    uint64_t msg_tag = 0;
    std::set<uint64_t> msg_tags;
    size_t resp_num = 0;
    std::vector<uint64_t> manager_ids;
    size_t num_sync_serialized_ddicts = 0;
    char ** sync_serialized_ddicts = nullptr;

    if (num_serialized_ddicts == 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Number of serialized dictionaries must be greater that zero.");

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Could not find ddict object.");

    // check if ddicts in clone_list has the same number of managers
    for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
        msg_tag = _tag_inc(dd);
        getMetaDataMsg = new DDGetMetaDataMsg(msg_tag, dd->bufferedRespFLIStr.c_str());
        msg_tags.insert(msg_tag);

        fli_ser.data = dragon_base64_decode(serialized_ddicts[i], &fli_ser.len);
        err = dragon_fli_attach(&fli_ser, nullptr, &connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not attach to the orchestrator to clone dictionaries.");
            goto send_get_metadata_fail;
        }

        err = _send(&connection, nullptr, getMetaDataMsg, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the get meta data message to the orchestrator to synchronize dictionaries.");
            goto send_get_metadata_fail;
        }

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not detach from the orchestrator to clone dictionaries.");
            goto send_get_metadata_fail;
        }
        delete getMetaDataMsg;

    }

    // receive responses from all orchestrator and check that the meta data match
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for get meta data responsese -- out of memory.");
        goto send_get_metadata_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to receive get meta data response.");
        goto receive_responses_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid response.");
            goto receive_responses_fail;
        }

        if (resp_msgs[i]->tc() != DDGetMetaDataResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected get meta data response message.");
            goto receive_responses_fail;
        }

        getMetaDataResponseMsg = (DDGetMetaDataResponseMsg*) resp_msgs[i];
        if (getMetaDataResponseMsg->err() != DRAGON_SUCCESS) {
            err = getMetaDataResponseMsg->err();
            append_err_noreturn(getMetaDataResponseMsg->errInfo());
            goto receive_responses_fail;
        }

        if (getMetaDataResponseMsg->numManagers() != dd->num_managers) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Number of managers must be the same across the dictionaries.");
            goto receive_responses_fail;
        }
        delete resp_msgs[i];
    }
    delete[] resp_msgs;

    // marks all managers as empty managers for all dictionary
    msg_tags.clear();
    for (uint64_t i=0 ; i<dd->num_managers ; i++)
        manager_ids.push_back(i);

    for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
        msg_tag = _tag_inc(dd);
        markDrainedManagersMsg = new DDMarkDrainedManagersMsg(msg_tag, dd->bufferedRespFLIStr.c_str(), manager_ids);
        msg_tags.insert(msg_tag);

        fli_ser.data = dragon_base64_decode(serialized_ddicts[i], &fli_ser.len);
        err = dragon_fli_attach(&fli_ser, nullptr, &connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not attach to the orchestrator to clone dictionaries.");
            goto send_mark_drained_managers_fail;
        }

        err = _send(&connection, nullptr, markDrainedManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not send the mark drained managers message to the orchestrator to synchronize dictionaries.");
            goto send_mark_drained_managers_fail;
        }

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not detach from the orchestrator to clone dictionaries.");
            goto send_mark_drained_managers_fail;
        }
        delete markDrainedManagersMsg;
    }

    // receive responses from all orchestrator and check that the meta data match
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        append_err_noreturn("Could not allocate space for get meta data responsese -- out of memory.");
        goto send_mark_drained_managers_fail;
    }

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Failed to receive get meta data response.");
        goto receive_responses_fail;
    }

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr) {
            err = DRAGON_INVALID_MESSAGE;
            append_err_noreturn("Could not receive valid response.");
            goto receive_responses_fail;
        }

        if (resp_msgs[i]->tc() != DDMarkDrainedManagersResponseMsg::TC) {
            err = DRAGON_FAILURE;
            append_err_noreturn("Failed to get expected mark drained managers response message.");
            goto receive_responses_fail;
        }

        markDrainedManagersResponseMsg = (DDMarkDrainedManagersResponseMsg*) resp_msgs[i];
        if (markDrainedManagersResponseMsg->err() != DRAGON_SUCCESS) {
            err = markDrainedManagersResponseMsg->err();
            append_err_noreturn(markDrainedManagersResponseMsg->errInfo());
            goto receive_responses_fail;
        }
        delete resp_msgs[i];
    }
    delete[] resp_msgs;

    // Only current dictionary has full managers. It will be used to reconstruct empty managers from other dictionaries.
    num_sync_serialized_ddicts = num_serialized_ddicts + 1;
    sync_serialized_ddicts = new char*[num_sync_serialized_ddicts];
    sync_serialized_ddicts[0] = new char[strlen(dd->ddict_ser.c_str()) + 1];
    memcpy((void*)sync_serialized_ddicts[0], dd->ddict_ser.c_str(), strlen(dd->ddict_ser.c_str()) + 1);
    for (size_t i=0 ; i<num_serialized_ddicts ; i++)
        sync_serialized_ddicts[i+1] = (char*)serialized_ddicts[i];

    // synchronize dictionaries
    err = dragon_ddict_synchronize((const char**)sync_serialized_ddicts, num_sync_serialized_ddicts, dd->timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_noreturn("Could not synchronize dictionaries.");
        goto synchronize_fail;
    }

    delete sync_serialized_ddicts[0];
    delete[] sync_serialized_ddicts;

    no_err_return(DRAGON_SUCCESS);

    synchronize_fail:
        if (sync_serialized_ddicts[0] != nullptr)
            delete sync_serialized_ddicts[0];
        delete[] sync_serialized_ddicts;

    send_mark_drained_managers_fail:
        if (markDrainedManagersMsg != nullptr)
            delete markDrainedManagersMsg;

    receive_responses_fail:
        if (resp_msgs != nullptr){
            for (size_t i=0 ; i<resp_num ; i++) {
                if (resp_msgs[i] != nullptr)
                    delete resp_msgs[i];
            }
            delete[] resp_msgs;
        }
    send_get_metadata_fail:
        if (getMetaDataMsg != nullptr)
            delete getMetaDataMsg;

    append_err_return(err, "Failed to clone dictionary.");
}