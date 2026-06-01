#include <iostream>
#include <sstream>
#include <string>
#include <set>
#include <algorithm>
#include <numeric>
#include <random>
#include <limits>
#include <dragon/ddict.h>
#include "_ddict.hpp"
#include "_ddict.h"
#include "_utils.h"
#include <dragon/messages.hpp>
#include <dragon/messages_api.h>
#include <dragon/dictionary.hpp>
#include <dragon/utils.h>
#include <assert.h>
#include "err.h"
#include "hostid.h"

using namespace dragon;

/* dragon globals */
DRAGON_GLOBAL_MAP(ddict_adapters);
DRAGON_GLOBAL_MAP(ddict_reqs);

/* Internal callback function definition. */
typedef dragonError_t (*key_collector)(void* user_arg, dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

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

    err = dragon_fli_open_send_handle(sendto_fli, &sendh, strm_ch, nullptr, timeout);
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

static dragonError_t _send_msg(DragonMsg * send_msg, dragonDDictReq_t * req, bool buffered=false) {
    dragonError_t err;
    dragonFLISendHandleDescr_t sendh;
    dragonChannelDescr_t * strm;

    if (buffered) {
        strm = STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND;
    } else {
        strm = &req->ddict->strm_ch;
    }

    err = dragon_fli_open_send_handle(&req->manager_fli, &sendh, strm, nullptr, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

    req->sendh = sendh;

    err = send_msg->send(&sendh, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send message.");

    err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _send_msg_key_no_close_sendh(DragonMsg * send_msg, dragonDDictReq_t * req) {
    dragonError_t err;
    dragonFLISendHandleDescr_t sendh;

    err = dragon_fli_open_send_handle(&req->manager_fli, &sendh, &req->ddict->strm_ch, nullptr, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

    req->sendh = sendh;

    err = send_msg->send(&sendh, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send message.");

    // send key
    // This is sent no_copy_read_only because it can be since key_mem will not be deleted
    // until request is finalized.
    err = dragon_fli_send_mem(&req->sendh, &req->key_mem, KEY_HINT, false, true, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send key to manager.");

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
    err = _send_receive(&dd->main_manager_fli, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, connectToManagerMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the connect to manager message and receive response.");

    if (resp_msg->tc() != DDConnectToManagerResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected connect to manager response message.");

    connectToManagerResponseMsg = (DDConnectToManagerResponseMsg*) resp_msg;
    err = connectToManagerResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, connectToManagerResponseMsg->errInfo());

    // attach to the mananger fli
    fli_ser.data = dragon_base64_decode(connectToManagerResponseMsg->managerFLI(), &fli_ser.len);
    err = dragon_fli_attach(&fli_ser, nullptr, &dd->manager_table[manager_id]);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to the manager.");

    delete connectToManagerMsg;
    delete connectToManagerResponseMsg;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _register_client_ID_to_manager(dragonDDict_t * dd, uint64_t manager_id) {
    dragonError_t err;
    DDRegisterClientIDMsg * registerClientIDMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRegisterClientIDResponseMsg * registerClientIDResponseMsg = nullptr;

    registerClientIDMsg = new DDRegisterClientIDMsg(_tag_inc(dd), dd->clientID, dd->respFLIStr.c_str(), dd->bufferedRespFLIStr.c_str());
    err = _send_receive(&dd->manager_table[manager_id], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, registerClientIDMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the register client ID message and receive response.");

    if (resp_msg->tc() != DDRegisterClientIDResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected register client ID response message.");

    registerClientIDResponseMsg = (DDRegisterClientIDResponseMsg*) resp_msg;
    err = registerClientIDResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, registerClientIDResponseMsg->errInfo());

    delete registerClientIDMsg;
    delete registerClientIDResponseMsg;

    no_err_return(DRAGON_SUCCESS);
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

static dragonError_t _build_key(dragonDDictReq_t * req) {
    dragonError_t err;

    err = dragon_fli_get_buffered_bytes(&req->key_sendh, &req->key_mem, NULL, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get the buffered key bytes.");

    // Once our key is constructed, hash it
    err = dragon_memory_get_size(&req->key_mem, &req->key_size);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get the buffered key size.");

    if (req->key_size == 0)
        err_return(DRAGON_INVALID_OPERATION, "The key must be written first before invoking the DDict operation. Key size was 0.");

    req->free_key_mem = true;

    err = dragon_memory_get_pointer(&req->key_mem, ((void**)&req->key_data));
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get the buffered key pointer.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _choose_manager(dragonDDictReq_t * req) {

    req->key_hash = dragon_hash(req->key_data, req->key_size);

    if (req->ddict->has_chosen_manager) {
        req->manager_id = req->ddict->chosen_manager;
    } else {
        req->manager_id = req->key_hash % req->ddict->num_managers;
    }
    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _build_key_choose_manager(dragonDDictReq_t * req) {

    // Get the buffered data into a memory allocation and prepare to send it.
    dragonError_t err;

    err = _build_key(req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to build the key.");

    err = _choose_manager(req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to hash key and choose manager.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _cleanup_request(dragonDDictReq_t * req) {
    req->ddict = nullptr;
    req->key_data = nullptr; // pointer into shared memory. Do not free.
    if (req->free_key_mem)
        dragon_memory_free(&req->key_mem);

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _get_main_manager(dragonDDict_t * ddict, char *mgr_sdesc, bool check_for_local_mgr) {
    dragonError_t err = DRAGON_SUCCESS;
    char * mgr_ser = mgr_sdesc;
    dragonFLISerial_t mgr_fli_ser;
    DDRandomManagerMsg * randomManagerMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRandomManagerResponseMsg * randomManagerResponseMsg = nullptr;

    if (mgr_ser == NULL) {
        if (check_for_local_mgr) {
            err = dragon_ls_get_kv((uint8_t*)ddict->ddict_ser.c_str(), (char**)&mgr_ser, nullptr);
            if (err != DRAGON_SUCCESS && err != DRAGON_NOT_FOUND)
                append_err_return(err, "Unable to ascertain existence of a local manager.");
        }

        if (err == DRAGON_NOT_FOUND || !check_for_local_mgr ) {
            // No manager found on node - Get random manager (check specific err code)
            // call get_random_manager (via orchestrator)
            randomManagerMsg = new DDRandomManagerMsg(_tag_inc(ddict), ddict->bufferedRespFLIStr.c_str());

            // Orchestrator FLI uses buffered protocol and we don't use stream channel with buffered protocol
            err = _send_receive(&ddict->orchestrator_fli, nullptr, randomManagerMsg, &ddict->bufferedRespFLI, &resp_msg, ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not send the get random manager message and receive response.");

            if (resp_msg->tc() != DDRandomManagerResponseMsg::TC)
                err_return(DRAGON_FAILURE, "Failed to get expected get random manager response message.");

            randomManagerResponseMsg = (DDRandomManagerResponseMsg*) resp_msg;
            err = randomManagerResponseMsg->err();
            if (err != DRAGON_SUCCESS)
                err_return(err, randomManagerResponseMsg->errInfo());

            mgr_ser = (char*)randomManagerResponseMsg->managerFLI();

        } else {
            ddict->has_local_manager = true;
        }
    }

    // Attach to our main manager
    mgr_fli_ser.data = dragon_base64_decode(mgr_ser, &mgr_fli_ser.len);
    err = dragon_fli_attach(&mgr_fli_ser, nullptr, &ddict->main_manager_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to main manager.");

    // cleanup
    delete randomManagerMsg;
    delete randomManagerResponseMsg;

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t _register_client_to_main_manager(dragonDDict_t * ddict) {

    dragonError_t err;
    uint64_t main_manager_id;
    DDRegisterClientMsg * registerClientMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    DDRegisterClientResponseMsg * registerClientResponseMsg = nullptr;

    registerClientMsg = new DDRegisterClientMsg(_tag_inc(ddict), ddict->respFLIStr.c_str(), ddict->bufferedRespFLIStr.c_str());
    err = _send_receive(&ddict->main_manager_fli, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, registerClientMsg, &ddict->bufferedRespFLI, &resp_msg, ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the register client message and receive response.");

    if (resp_msg->tc() != DDRegisterClientResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected register client response message.");

    registerClientResponseMsg = (DDRegisterClientResponseMsg*) resp_msg;
    err = registerClientResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, registerClientResponseMsg->errInfo());

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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the get random manager message and receive response.");

    if (resp_msg->tc() != DDManagerNodesResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected get manager nodes response message.");

    managerNodesResponseMsg = (DDManagerNodesResponseMsg*) resp_msg;
    err = managerNodesResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, managerNodesResponseMsg->errInfo());

    ddict->hostid = local_host_id;
    huids = managerNodesResponseMsg->huids();
    for (size_t i=0 ; i<huids.size() ; i++) {
        if (huids[i] == ddict->hostid)
            ddict->local_managers.push_back(i);
    }

    delete managerNodesMsg;
    delete managerNodesResponseMsg;

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t _put(dragonDDictReq_t * req, bool persist) {

    dragonError_t err;
    DDPutMsg * putMsg = nullptr;

    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operations.");
    req->op_type = DRAGON_DDICT_PUT_REQ;

    if (req->key_data == nullptr) {

        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key or connect to manager.");

        err = _check_manager_connection(req->ddict, req->manager_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not connect to the manager.");

        req->manager_fli = req->ddict->manager_table[req->manager_id];

        req->msg_tag = _tag_inc(req->ddict);
        putMsg = new DDPutMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id, persist);
        err = _send_msg_key_no_close_sendh(putMsg, req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the put message and key.");

    } else {
        err_return(DRAGON_INVALID_OPERATION, "No data present in request.");
    }

    delete putMsg;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _batch_put(dragonDDictReq_t * req, bool persist) {
    dragonError_t err;
    dragonFLISendHandleDescr_t * sendh;
    dragonDDict_t * dd;
    DDBatchPutMsg * batchPutMsg = nullptr;

    if (req->key_data != nullptr)
        err_return(DRAGON_INVALID_OPERATION, "Key has already been sent, invalid operation order.");

    dd = req->ddict;

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operations.");
    req->op_type = DRAGON_DDICT_BATCH_PUT_REQ;

    if (req->key_data == nullptr) {
        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key or connect to manager.");

        err = _check_manager_connection(dd, req->manager_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not connect to the manager.");

        req->manager_fli = dd->manager_table[req->manager_id];
    }

    if (dd->opened_send_handles.find(req->manager_id) != dd->opened_send_handles.end()) {
        sendh = dd->opened_send_handles[req->manager_id];
        req->sendh = *sendh;
    } else {
        dragonChannelDescr_t * strm_ch = (dragonChannelDescr_t*)malloc(sizeof(dragonChannelDescr_t));

        err = dragon_create_process_local_channel(strm_ch, 0, 0, 0, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not create stream channel.");

        dd->batch_put_stream_channels[req->manager_id] = strm_ch;
        dd->opened_send_handles[req->manager_id] = new dragonFLISendHandleDescr_t();
        err = dragon_fli_open_send_handle(&req->manager_fli, dd->opened_send_handles[req->manager_id], strm_ch, nullptr, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open send handle.");

        req->sendh = *dd->opened_send_handles[req->manager_id];

        req->msg_tag = _tag_inc(dd);

        batchPutMsg = new DDBatchPutMsg(req->msg_tag, dd->clientID, dd->chkpt_id, persist);
        dd->batch_put_msg_tags.insert(req->msg_tag);

        // send batch put message
        err = batchPutMsg->send(&req->sendh, dd->timeout);
        if (err != DRAGON_SUCCESS) {
            // We ignore the return code because there was an error on send.
            dragonError_t close_fli_err = dragon_fli_close_send_handle(&req->sendh, dd->timeout);
            if (close_fli_err != DRAGON_SUCCESS)
                append_err_return(err, "Could not send message and close send handle.");

            append_err_return(err, "Could not send message.");
        }
    }

    if (req->key_data != nullptr) {
        err = dragon_fli_send_mem(&req->sendh, &req->key_mem, KEY_HINT, false, false, req->ddict->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send key to manager.");
    }

    delete batchPutMsg;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _end_bput_with_batch(dragonDDict_t * dd) {

    dragonError_t err;

    if (dd == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict.");

    err = dragon_fli_close_send_handle(&dd->bput_root_manager_sendh, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close send handle.");

    dd->has_bput_root_manager_sendh = false;

    uint64_t resp_num = dd->num_managers;
    DragonResponseMsg ** resp_msgs = nullptr;

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    std::set<uint64_t> msg_tags;
    msg_tags.insert(dd->bput_tag);

    err = _recv_responses(&dd->bput_respFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get bput response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDBPutResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected bput response message.");

        DDBPutResponseMsg * bputResponseMsg = (DDBPutResponseMsg*) resp_msgs[i];
        err = bputResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, bputResponseMsg->errInfo());

        if (bputResponseMsg->numPuts() != dd->num_bputs) {
            char msg[200];
            snprintf(msg, 199, "Failed to store all keys in manager %" PRIu64 " in the distributed dictionary. Expected number of keys to be written: %" PRIu64 ", number of successful writes: %" PRIu64 "", bputResponseMsg->managerID(), dd->num_bputs, bputResponseMsg->numPuts());
            err_return(DRAGON_FAILURE, msg);
        }

        delete resp_msgs[i];
    }

    // cleanup
    dd->num_bputs = 0;
    dragon_fli_destroy(&dd->bput_respFLI);
    dragon_destroy_process_local_channel(&dd->bput_strm, dd->timeout);
    dragon_destroy_process_local_channel(&dd->bput_resp_strm, dd->timeout);

    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _restore(dragonDDict_t * dd, uint64_t chkpt_id) {
    dragonError_t err;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict.");

    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    DDRestoreMsg * restoreMsg = new DDRestoreMsg(msg_tag, chkpt_id, dd->clientID, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, restoreMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the restore message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive restore response.");

    for (uint64_t i=0 ; i<dd->num_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDRestoreResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected restore response message.");

        DDRestoreResponseMsg * restoreResponseMsg = (DDRestoreResponseMsg*) resp_msgs[i];
        err = restoreResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, restoreResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete restoreMsg;
    delete[] resp_msgs;

    dd->chkpt_id = chkpt_id;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _chkpt_avail(dragonDDict_t * dd, uint64_t chkpt_id) {

    dragonError_t err;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict.");

    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    DDChkptAvailMsg * chkptAvailMsg = new DDChkptAvailMsg(msg_tag, chkpt_id, dd->bufferedRespFLIStr.c_str());

    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, chkptAvailMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the chkpt available message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive chkpt available response.");

    for (uint64_t i=0 ; i<dd->num_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDChkptAvailResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected chkpt available response message.");

        DDChkptAvailResponseMsg * chkptAvailResponseMsg = (DDChkptAvailResponseMsg*) resp_msgs[i];
        err = chkptAvailResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, chkptAvailResponseMsg->errInfo());

        if (!chkptAvailResponseMsg->available()) {
            char msg[200];
            snprintf(msg, 199, "Unable to access checkpoint %" PRIu64 " from manager %" PRIu64 ". The checkpoint is not available.", chkpt_id, chkptAvailResponseMsg->managerID());
            err_return(DRAGON_INVALID_OPERATION, msg);
        }

        delete resp_msgs[i];
    }

    delete chkptAvailMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t _persisted_chkpt_avail(dragonDDict_t * dd, uint64_t chkpt_id) {
    // Check the availability of the persisted checkpoint.
    dragonError_t err;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict.");

    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    DDPersistedChkptAvailMsg * persistedChkptAvailMsg = new DDPersistedChkptAvailMsg(msg_tag, chkpt_id, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, persistedChkptAvailMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the persisted chkpt available message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive persisted chkpt available response.");

    for (uint64_t i=0 ; i<dd->num_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDPersistedChkptAvailResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected persisted chkpt available response message.");

        DDPersistedChkptAvailResponseMsg * persistedChkptAvailResponseMsg = (DDPersistedChkptAvailResponseMsg*) resp_msgs[i];
        err = persistedChkptAvailResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, persistedChkptAvailResponseMsg->errInfo());

        if (!persistedChkptAvailResponseMsg->available()) {
            char msg[200];
            snprintf(msg, 199, "Unable to access persisted checkpoint %" PRIu64 " from manager %" PRIu64 ". The checkpoint is not available.", chkpt_id, persistedChkptAvailResponseMsg->managerID());
            err_return(DRAGON_INVALID_OPERATION, msg);
        }

        delete resp_msgs[i];
    }

    delete persistedChkptAvailMsg;
    delete[] resp_msgs;

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

static dragonError_t dd_key_collector (void* user_arg, dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    dragonDDictKey_t * key;
    std::vector<dragonDDictKey_t*>* keys = (std::vector<dragonDDictKey_t*>*) user_arg;

    key = (dragonDDictKey_t*)malloc(sizeof(dragonDDictKey_t));
    if (key == NULL) {
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for key.");
    }
    err = dragon_fli_recv_bytes(recvh, 0, &key->num_bytes, &key->data, arg, timeout);
    if (err == DRAGON_SUCCESS) {
        if (*arg != KEY_HINT)
            err_return(DRAGON_FAILURE, "Received unexpected arg value.");

        keys->push_back(key);
    } else if (err == DRAGON_EOT) {
        free(key);
        no_err_return(DRAGON_EOT);
    } else
        append_err_return(err, "Caught an error while receiving keys.");

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t _dragon_ddict_keys_vec(dragonDDict_t * dd, std::vector<uint64_t>& managers, key_collector key_fun, void* key_collector_arg) {

    dragonError_t err;
    DDKeysMsg * keysMsg = nullptr;
    DDKeysResponseMsg * keysResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    uint64_t msg_tag;
    dragonFLIRecvHandleDescr_t recvh;

    for (size_t i=0 ; i<managers.size() ; i++) {
        // send request
        msg_tag = _tag_inc(dd);
        keysMsg = new DDKeysMsg(msg_tag, dd->clientID, dd->chkpt_id, dd->respFLIStr.c_str());
        uint64_t manager_id = managers[i];
        err = _send(&dd->manager_table[manager_id], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, keysMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the keys message to manager.");

        delete keysMsg;
        // receive dmsg
        err = _recv_dmsg_no_close_recvh(&recvh, &dd->respFLI, &resp_msg, msg_tag, false, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not receive keys response message.");

        if (resp_msg->tc() != DDKeysResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected keys response message.");

        keysResponseMsg = (DDKeysResponseMsg*) resp_msg;
        err = keysResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, keysResponseMsg->errInfo());

        delete keysResponseMsg;
        // receive keys until EOT
        bool done = false;
        while(!done) {
            uint64_t arg = 0;
            try {
                err = (key_fun)(key_collector_arg, &recvh, &arg, dd->timeout);
            } catch(const EmptyError& ex) {
                done = true;
            } catch(const DragonError& ex) {
                append_err_return(ex.rc(), "Unexpected Error while calling key_collector.");
            } catch(...) {
                append_err_return(DRAGON_FAILURE, "There was an unexpected error while collecting keys.");
            }
            if (err == DRAGON_EOT)
                done = true;
            else if (err != DRAGON_SUCCESS)
                append_err_return(err, "Unexpected error while getting a key.");

        }
        err = dragon_fli_close_recv_handle(&recvh, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not close receive handle.");

    }

    no_err_return(DRAGON_SUCCESS);
}


/* This is used both in this implementation and in the C++ implementation so cannot be static. */
dragonError_t dragon_ddict_keys_vec(const dragonDDictDescr_t * dd_descr, key_collector key_fun, void* key_collector_arg) {
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

        err = _dragon_ddict_keys_vec(dd, managers, key_fun, key_collector_arg);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not get keys.");

        no_err_return(DRAGON_SUCCESS);

    } catch(const std::exception& ex) {
        std::stringstream err_str;
        err_str << "Exception Caught: " << ex.what() << endl;
        err_return(DRAGON_FAILURE, err_str.str().c_str());
    }
}

dragonError_t dragon_ddict_local_keys_vec(const dragonDDictDescr_t * dd_descr, key_collector key_fun, void* key_collector_arg) {
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

        err = _dragon_ddict_keys_vec(dd, dd->local_managers, key_fun, key_collector_arg);
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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the empty managers message and receive response.");

    if (resp_msg->tc() != DDEmptyManagersResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected empty managers response message.");

    emptyManagersResponseMsg = (DDEmptyManagersResponseMsg*) resp_msg;
    err = emptyManagersResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, emptyManagersResponseMsg->errInfo());

    empty_managers = emptyManagersResponseMsg->managers();
    for (size_t i=0 ; i<empty_managers.size() ; i++)
        manager_ids.push_back(empty_managers[i]);

    delete emptyManagersMsg;
    delete emptyManagersResponseMsg;

    no_err_return(DRAGON_SUCCESS);
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

/************************************************************/
/* Private function to allow external channels to be used   */
/* to attach to the dictionary                              */
/************************************************************/
dragonError_t _dragon_ddict_attach(const char * dd_ser, dragonDDictDescr_t * dd, timespec_t * default_timeout,
                                   dragonChannelSerial_t *str_chser, dragonChannelSerial_t *resp_chser,
                                   dragonChannelSerial_t *buffered_resp_chser, char *mgr_ser,
                                   bool check_for_local_mgr)
{

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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not attach to orchestrator.");

    /* Get the return channels for the client. This includes a streaming and a buffered return channel.
       Streaming requires one extra message per conversation but allow value data to be streamed back
       to the client. */

    // Create stream channel if one's not provided. We need it to send request to orchestrator later to ask for a random manager.
    if (str_chser == NULL) {
        err = dragon_create_process_local_channel(&ddict->strm_ch, 0, 0, 0, ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not create stream channel.");
        }
    }
    else {
        err = dragon_channel_attach(str_chser, &ddict->strm_ch);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not attach to externally provided ddict stream channel");
        }
    }
    // If externally managed channel isn't provided request one be made from LS
    if (resp_chser == NULL) {
        err = dragon_create_process_local_channel(&resp_ch, 0, 0, 0, ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not create response channel.");
        }
    }
    // Otherwise, attach to the provided channel
    else {
        err = dragon_channel_attach(resp_chser, &resp_ch);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not attach to externally provided ddict return channel.");
        }
    }

    err = dragon_fli_create(&ddict->respFLI, &resp_ch, nullptr, nullptr, 0, nullptr, false, nullptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create response FLI.");

    err = dragon_fli_serialize(&ddict->respFLI, &ser_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize response FLI.");


    ddict->respFLIStr = dragon_base64_encode(ser_resp_fli.data, ser_resp_fli.len);
    err = dragon_fli_serial_free(&ser_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized response FLI.");

    // Create buffered response channel
    if (str_chser == NULL) {
        err = dragon_create_process_local_channel(&buffered_resp_ch, 0, 0, 0, ddict->timeout);
        if (err != DRAGON_SUCCESS) {
            append_err_noreturn("Could not create buffered response channel.");
        }
    }
    else {
        err = dragon_channel_attach(buffered_resp_chser, &buffered_resp_ch);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "Could not attach to externally provided ddict buffered response channel");
        }
    }

    err = dragon_fli_create(&ddict->bufferedRespFLI, &buffered_resp_ch, nullptr, nullptr, 0, nullptr, true, nullptr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create buffered response FLI.");

    err = dragon_fli_serialize(&ddict->bufferedRespFLI, &ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not serialize buffered response FLI.");

    ddict->bufferedRespFLIStr = dragon_base64_encode(ser_buffered_resp_fli.data, ser_buffered_resp_fli.len);
    err = dragon_fli_serial_free(&ser_buffered_resp_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not free serialized buffered response FLI.");

    // Get random manage from local service.
    // If no manager on current node, client
    // request a random manager from orchestrator.
    err = _get_main_manager(ddict, mgr_ser, check_for_local_mgr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get main manager.");

    err = _register_client_to_main_manager(ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not register client to main manager.");

    err = _get_local_managers(ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get local managers.");

    ddict->batch_persist = false;
    ddict->batch_put_started = false;
    ddict->has_bput_root_manager_sendh = false;
    ddict->num_bputs = 0;

    err = _add_umap_ddict_entry(dd, ddict);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not add new ddict entry to umap.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_attach(const char * dd_ser, dragonDDictDescr_t * dd, timespec_t * default_timeout) {

    // Route to the internal attach with NULL arguments for channels, so LS creates some for us.
    dragonError_t err = _dragon_ddict_attach(dd_ser, dd, default_timeout, NULL, NULL, NULL, NULL, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Unable to attach to DDict");

    no_err_return(DRAGON_SUCCESS);

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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    if (dd->detached)
        no_err_return(DRAGON_SUCCESS);

    dd->detached = true;

    for (manager_it=dd->manager_table.begin() ; manager_it!=dd->manager_table.end() ; manager_it++) {
        deregisterClientMsg = new DDDeregisterClientMsg(_tag_inc(dd), dd->clientID, dd->bufferedRespFLIStr.c_str());
        err = _send_receive(&manager_it->second, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, deregisterClientMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the deregister client message and receive response.");

        if (resp_msg->tc() != DDDeregisterClientResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Did not get expected deregister client response message.");

        deregisterClientResponseMsg = (DDDeregisterClientResponseMsg*) resp_msg;
        err = deregisterClientResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, deregisterClientResponseMsg->errInfo());

        err = dragon_fli_detach(&manager_it->second);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the manager fli.");

        delete deregisterClientMsg;
        delete resp_msg;
    }

    err = dragon_fli_detach(&dd->orchestrator_fli);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from orchestrator.");

    // free resources
    delete dd;

    no_err_return(DRAGON_SUCCESS);
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

dragonError_t dragon_ddict_create_request(const dragonDDictDescr_t * descr, dragonDDictRequestDescr_t * req_descr) {
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
    req->key_hash = 0UL;
    req->op_type = DRAGON_DDICT_NO_OP;
    req->num_writes = 0;
    req->recvh_closed = true;
    req->free_mem = true;
    req->free_key_mem = false;
    req->key_data = nullptr;
    req->key_size = 0;

    /* This send handle is opened as a means for getting a key written into an FLI's buffered
       data. This allows us to re-use the FLI code for buffering while never actually sending anything over
       this FLI. */
    err = dragon_fli_open_send_handle(&req->ddict->bufferedRespFLI, &req->key_sendh, nullptr, nullptr, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not open send handle.");

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
    uint64_t resp_num = 0;
    DragonResponseMsg ** resp_msgs = nullptr;

    dragonError_t err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find request object.");

    /* Close the "fake" send handle that allowed us to buffer key data
       if the request required that. */
    err = dragon_fli_close_send_handle(&req->key_sendh, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not close the key send handle.");

    switch(req->op_type) {
        case DRAGON_DDICT_NO_OP:
            // Never did anything. Just clean it up. which_manager uses this path.
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
            err = putResponseMsg->err();
            if (err != DRAGON_SUCCESS)
                err_return(err, putResponseMsg->errInfo());

            //Cleanup
            delete putResponseMsg;
            break;

        case DRAGON_DDICT_BATCH_PUT_REQ:
            dd = req->ddict;
            if (dd->num_batch_puts.find(req->manager_id) == dd->num_batch_puts.end())
                dd->num_batch_puts[req->manager_id] = 0;
            dd->num_batch_puts[req->manager_id] += 1;
            break;

        case DRAGON_DDICT_B_PUT_BATCH_REQ:
            // add dd->num_bput but don't free the request?
            dd = req->ddict;
            dd->num_bputs++;
            break;
        case DRAGON_DDICT_B_PUT_REQ:
            // receive bput response from buffered return fli and check resp.numBput==1
            // Close send handle (signals "done")
            err = dragon_fli_close_send_handle(&req->sendh, req->ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not close send handle.");

            // Get our response back confirming close
            dd = req->ddict;
            msg_tags.clear();
            msg_tags.insert(req->msg_tag);

            resp_num = dd->num_managers;
            resp_msgs = nullptr;

            // receive response from all managers
            resp_msgs = new DragonResponseMsg*[resp_num];
            if (resp_msgs == nullptr)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

            err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not get bput response.");

            for (size_t i=0 ; i<resp_num ; i++) {
                if (resp_msgs[i] == nullptr)
                    err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

                if (resp_msgs[i]->tc() != DDBPutResponseMsg::TC)
                    err_return(DRAGON_FAILURE, "Failed to get expected bput response message.");

                DDBPutResponseMsg * bputResponseMsg = (DDBPutResponseMsg*) resp_msgs[i];
                err = bputResponseMsg->err();
                if (err != DRAGON_SUCCESS)
                    err_return(err, bputResponseMsg->errInfo());

                if (bputResponseMsg->numPuts() != 1) {
                    char msg[200];
                    snprintf(msg, 199, "Failed to store all keys in manager %" PRIu64 " in the distributed dictionary. Expected number of keys to be written: 1, number of successful writes: %" PRIu64 "", bputResponseMsg->managerID(), bputResponseMsg->numPuts());
                    err_return(DRAGON_FAILURE, msg);
                }

                delete resp_msgs[i];
            }
            delete[] resp_msgs;
            break;
        default:
            err_return(DRAGON_INVALID_ARGUMENT, "Unimplemented or invalid operator type.");
    }

    // The Request is finalized and receiver has received the data. It will be deleted next.
    if (req != nullptr) {
        // The key in the request can't be freed until manager receives it.
        err = _cleanup_request(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not cleanup request.");

        free(req);
    }
    req->op_type = DRAGON_DDICT_FINALIZED;

    dragon_umap_delitem(dg_ddict_reqs, req_descr->_idx); // Remove request from umap

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_request_value_sendh(const dragonDDictRequestDescr_t* req_descr, dragonFLISendHandleDescr_t* sendh) {
    dragonDDictReq_t * req = nullptr;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (sendh == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid sendh descriptor pointer. It must point to a send handle descripter.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    *sendh = req->sendh;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_request_key_sendh(const dragonDDictRequestDescr_t* req_descr, dragonFLISendHandleDescr_t* sendh) {
    dragonDDictReq_t * req = nullptr;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (sendh == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid sendh descriptor pointer. It must point to a send handle descripter.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    *sendh = req->key_sendh;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_request_recvh(const dragonDDictRequestDescr_t* req_descr, dragonFLIRecvHandleDescr_t* recvh) {
    dragonDDictReq_t * req = nullptr;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (recvh == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid sendh descriptor pointer. It must point to a receive handle descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to find request object.");

    *recvh = req->recvh;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_free_required(const dragonDDictRequestDescr_t * req_descr, bool * free_mem) {
    dragonDDictReq_t * req = nullptr;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Failed to find request object.");
    }

    *free_mem = req->free_mem;

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
        case DRAGON_DDICT_BATCH_PUT_REQ:
        case DRAGON_DDICT_B_PUT_REQ:
        case DRAGON_DDICT_B_PUT_BATCH_REQ:
            // key is written and sent to manager, write values now
            err = dragon_fli_send_bytes(&req->sendh, num_bytes, bytes, VALUE_HINT, false, req->ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not write value bytes.");
            break;
        case DRAGON_DDICT_GET_REQ:
        case DRAGON_DDICT_CONTAINS_REQ:
        case DRAGON_DDICT_NO_OP:
            // Key hasn't been written, we need to write key first. The data is buffered,
            // not written. We get the buffered bytes elsewhere and then send them on the
            // actual FLI send handle or include the buffered bytes in the message that goes
            // to the manager.
            err = dragon_fli_send_bytes(&req->key_sendh, num_bytes, bytes, KEY_HINT, true, req->ddict->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not buffer key.");
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
    dragonError_t resp_err;
    std::set<uint64_t> msg_tags;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_CONTAINS_REQ;

    if (req->key_data == nullptr) {
        // indicates this has not been called yet.
        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    }

    err = _check_manager_connection(req->ddict, req->manager_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to the manager.");

    req->manager_fli = req->ddict->manager_table[req->manager_id];
    req->msg_tag = _tag_inc(req->ddict);
    msg_tags.insert(req->msg_tag);
    containsMsg = new DDContainsMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id, req->key_data, req->key_size);

    err = _send_msg(containsMsg, req, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the contains message and key.");

    err = _recv_resp(&req->ddict->bufferedRespFLI, &resp_msg, msg_tags, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get contains response.");

    req->recvh_closed = true;

    if (resp_msg->tc() != DDContainsResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected contains response message.");

    containsResponseMsg = (DDContainsResponseMsg*) resp_msg;
    resp_err = containsResponseMsg->err();
    if (resp_err != DRAGON_SUCCESS && resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED)
        err_return(resp_err, containsResponseMsg->errInfo());

    delete containsMsg;
    delete containsResponseMsg;

    no_err_return(resp_err);
}

dragonError_t dragon_ddict_get(const dragonDDictRequestDescr_t* req_descr) {

    dragonDDictReq_t * req;
    dragonError_t err;
    dragonError_t resp_err;
    DDGetMsg * getMsg = nullptr;
    DDGetResponseMsg * getResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;
    bool is_local_manager = false;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_GET_REQ;

    if (req->key_data == nullptr) {
        // indicates this has not been called yet.
        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    }

    err = _check_manager_connection(req->ddict, req->manager_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to the manager.");

    req->manager_fli = req->ddict->manager_table[req->manager_id];

    req->msg_tag = _tag_inc(req->ddict);
    getMsg = new DDGetMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id, req->key_data, req->key_size);

    err = _send_msg(getMsg, req, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the get message and key.");

    err = _recv_dmsg_no_close_recvh(&recvh, &req->ddict->respFLI, &resp_msg, getMsg->tag(), false, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get expected get response message.");

    req->recvh = recvh;
    req->recvh_closed = false;

    if (resp_msg->tc() != DDGetResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected get response message.");

    getResponseMsg = (DDGetResponseMsg*) resp_msg;
    resp_err = getResponseMsg->err();
    // Close receive handle if the error code is not SUCCESS.
    if (resp_err != DRAGON_SUCCESS) {
        dragon_fli_close_recv_handle(&req->recvh, req->ddict->timeout);
        req->recvh_closed = true;

        if (resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED)
            err_return(resp_err, getResponseMsg->errInfo());
    }

    auto it = std::find(req->ddict->local_managers.begin(), req->ddict->local_managers.end(), req->manager_id);
    is_local_manager = (it != req->ddict->local_managers.end());
    req->free_mem = getResponseMsg->freeMem() || !is_local_manager;
    if (!req->free_mem) {
        err = dragon_fli_reset_free_flag(&req->recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not reset free memory flag in receive handle.");
    }

    delete getMsg;
    delete getResponseMsg;

    no_err_return(resp_err);
}

dragonError_t dragon_ddict_which_manager(const dragonDDictRequestDescr_t * req_descr, uint64_t* manager_id) {
    dragonDDictReq_t * req;
    dragonError_t err;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    if (manager_id == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "You must provide a pointer to a uint64_t variable.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->key_data == nullptr) {
        // indicates this has not been called yet.
        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    } else {
        err = _choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to hash the key and choose manager.");
    }

    *manager_id = req->manager_id;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_put(const dragonDDictRequestDescr_t* req_descr) {

    dragonError_t err;
    dragonDDictReq_t * req;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->ddict->batch_put_started) {
        if (req->ddict->batch_persist)
            append_err_return(DRAGON_INVALID_OPERATION, "Persistent value mismatch. Could not perform batch put.");
        err = _batch_put(req, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not perform batch put op.");
    } else {
        err = _put(req, false);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not perform put op.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_pput(const dragonDDictRequestDescr_t* req_descr) {
    dragonError_t err;
    dragonDDictReq_t * req;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    err = _put(req, true);
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
    err = _send(&dd->manager_table[selected_manager], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, lenMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the length message to root manager.");

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive length response.");

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDLengthResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected length response message.");

        lengthResponseMsg = (DDLengthResponseMsg*) resp_msgs[i];
        err = lengthResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, lengthResponseMsg->errInfo());

        *length += lengthResponseMsg->length();

        delete resp_msgs[i];
    }

    delete lenMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
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
    err = _send(&dd->manager_table[selected_manager], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, clearMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the clear message to root manager.");

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive clear responses.");

    // receive response from all managers
    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid respose.");

        if (resp_msgs[i]->tc() != DDClearResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected clear response message.");

        clearResponseMsg = (DDClearResponseMsg*) resp_msgs[i];
        err = clearResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, clearResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete clearMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_pop(const dragonDDictRequestDescr_t * req_descr) {

    dragonDDictReq_t * req;
    dragonError_t err;
    dragonError_t resp_err;
    DDPopMsg * popMsg = nullptr;
    DDPopResponseMsg * popResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;
    bool is_local_manager = false;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");

    req->op_type = DRAGON_DDICT_POP_REQ;

    if (req->key_data == nullptr) {
        // indicates this has not been called yet.
        err = _build_key_choose_manager(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    }

    err = _check_manager_connection(req->ddict, req->manager_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to the manager.");

    req->manager_fli = req->ddict->manager_table[req->manager_id];

    req->msg_tag = _tag_inc(req->ddict);
    popMsg = new DDPopMsg(req->msg_tag, req->ddict->clientID, req->ddict->chkpt_id, req->key_data, req->key_size);

    err = _send_msg(popMsg, req, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the pop message and key.");

    err = _recv_dmsg_no_close_recvh(&recvh, &req->ddict->respFLI, &resp_msg, popMsg->tag(), false, req->ddict->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive pop response message.");

    req->recvh = recvh;
    req->recvh_closed = false;

    if (resp_msg->tc() != DDPopResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected pop response message.");

    popResponseMsg = (DDPopResponseMsg*) resp_msg;
    resp_err = popResponseMsg->err();
    // Close receive handle if the error code is not SUCCESS.
    if (resp_err != DRAGON_SUCCESS) {
        dragon_fli_close_recv_handle(&req->recvh, req->ddict->timeout);
        req->recvh_closed = true;

        if (resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED)
            err_return(resp_err, popResponseMsg->errInfo());
    }

    auto it = std::find(req->ddict->local_managers.begin(), req->ddict->local_managers.end(), req->manager_id);
    is_local_manager = (it != req->ddict->local_managers.end());
    req->free_mem = popResponseMsg->freeMem() || !is_local_manager;
    if (!req->free_mem) {
        err = dragon_fli_reset_free_flag(&req->recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not reset free memory flag in receive handle.");
    }

    delete popMsg;
    delete popResponseMsg;

    no_err_return(resp_err);
}

dragonError_t dragon_ddict_keys(const dragonDDictDescr_t * dd_descr, dragonDDictKey_t*** keys, size_t * num_keys) {
    dragonError_t err;
    std::vector<dragonDDictKey_t*> key_vec;

    err = dragon_ddict_keys_vec(dd_descr, dd_key_collector, &key_vec);
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

    err = dragon_ddict_local_keys_vec(dd_descr, dd_key_collector, &key_vec);
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
    err = _send(&dd->manager_table[selected_manager], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, managerNewestChkptIDMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the sync to newest checkpoint message to root manager.");

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");


    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not receive sync to newest checkpoint response.");

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid respose.");

        if (resp_msgs[i]->tc() != DDManagerNewestChkptIDResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected newest checkpoint response message.");

        managerNewestChkptIDResponseMsg = (DDManagerNewestChkptIDResponseMsg*) resp_msgs[i];
        err = managerNewestChkptIDResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, managerNewestChkptIDResponseMsg->errInfo());

        newestChkptID = max(newestChkptID, managerNewestChkptIDResponseMsg->chkptID());

        delete resp_msgs[i];
    }

    dd->chkpt_id = newestChkptID;

    delete managerNewestChkptIDMsg;
    delete[] resp_msgs;
    no_err_return(DRAGON_SUCCESS);
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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
        msg_tag = _tag_inc(dd);
        getManagersMsg = new DDGetManagersMsg(msg_tag, dd->bufferedRespFLIStr.c_str());
        msg_tags.insert(msg_tag);

        fli_ser.data = dragon_base64_decode(serialized_ddicts[i], &fli_ser.len);
        err = dragon_fli_attach(&fli_ser, nullptr, &connection);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to the orchestrator to synchronize dictionaries.");

        err = _send(&connection, nullptr, getManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the get managers message to the orchestrator to synchronize dictionaries.");

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the orchestrator to synchronize dictionaries.");

        delete getManagersMsg;
    }

    // receive responsese from all orchestrators
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for get managers responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive get managers response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid response.");

        if (resp_msgs[i]->tc() != DDGetManagersResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected get managers response message.");

        getManagersResponseMsg = (DDGetManagersResponseMsg*) resp_msgs[i];
        err = getManagersResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, getManagersResponseMsg->errInfo());
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
            if (full_managers.size() == 0)
                err_return(DRAGON_FAILURE, "Failed to synchronize dictionary.");

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
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not attach to the full manager.");

                err = _send(&connection, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, managerSyncMsg, dd->timeout);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not send the manager synchronization message to the full manager.");

                err = dragon_fli_detach(&connection);
                if (err != DRAGON_SUCCESS)
                    append_err_return(err, "Could not detach from the full manager.");

                delete managerSyncMsg;
            }

            // Receive sync responses from all full managers
            manager_sync_resp_num = empty_managers.size();
            manager_sync_resp_msgs = new DragonResponseMsg*[manager_sync_resp_num];
            if (manager_sync_resp_msgs == nullptr)
                err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for manager sync responses -- out of memory.");

            err = _recv_responses(&dd->bufferedRespFLI, manager_sync_resp_msgs, msg_tags, manager_sync_resp_num, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Failed to receive manager sync responses.");

            // check each manager sync responses
            for (size_t i=0 ; i<manager_sync_resp_num ; i++) {
                if (manager_sync_resp_msgs[i] == nullptr)
                    append_err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid responses.");

                if (manager_sync_resp_msgs[i]->tc() != DDManagerSyncResponseMsg::TC)
                    err_return(DRAGON_FAILURE, "Failed to get expected manager sync response message.");

                managerSyncResponseMsg = (DDManagerSyncResponseMsg*) manager_sync_resp_msgs[i];
                err = managerSyncResponseMsg->err();
                if (err != DRAGON_SUCCESS)
                    err_return(err, managerSyncResponseMsg->errInfo());

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
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to the orchestrator to synchronize dictionaries.");

        err = _send(&connection, nullptr, unmarkDrainedManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the unmark drained managers message to the orchestrator to synchronize dictionaries.");

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the orchestrator to synchronize dictionaries.");

        delete unmarkDrainedManagersMsg;
    }

    // receive responsese from all orchestrators
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive length response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid response.");

        if (resp_msgs[i]->tc() != DDUnmarkDrainedManagersResponseMsg::TC)
            append_err_return(DRAGON_FAILURE, "Failed to get expected unmark drained managers response message.");


        unmarkDrainedManagersResponseMsg = (DDUnmarkDrainedManagersResponseMsg*) resp_msgs[i];
        err = unmarkDrainedManagersResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, unmarkDrainedManagersResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete[] resp_msgs;

    // detach from the first dictionary
    err = dragon_ddict_detach(&dd_descr);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not detach from the client.");

    no_err_return(DRAGON_SUCCESS);
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
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to the orchestrator to clone dictionaries.");

        err = _send(&connection, nullptr, getMetaDataMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the get meta data message to the orchestrator to synchronize dictionaries.");

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the orchestrator to clone dictionaries.");

        delete getMetaDataMsg;
    }

    // receive responses from all orchestrator and check that the meta data match
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for get meta data responsese -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive get meta data response.");

    for (size_t i=0 ; i<resp_num ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid response.");

        if (resp_msgs[i]->tc() != DDGetMetaDataResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected get meta data response message.");

        getMetaDataResponseMsg = (DDGetMetaDataResponseMsg*) resp_msgs[i];
        err = getMetaDataResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, getMetaDataResponseMsg->errInfo());

        if (getMetaDataResponseMsg->numManagers() != dd->num_managers)
            err_return(DRAGON_FAILURE, "Number of managers must be the same across the dictionaries.");

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
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not attach to the orchestrator to clone dictionaries.");

        err = _send(&connection, nullptr, markDrainedManagersMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the mark drained managers message to the orchestrator to synchronize dictionaries.");

        err = dragon_fli_detach(&connection);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not detach from the orchestrator to clone dictionaries.");

        delete markDrainedManagersMsg;
    }

    // receive responses from all orchestrator and check that the meta data match
    resp_num = num_serialized_ddicts;
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for get meta data responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive get meta data response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid response.");

        if (resp_msgs[i]->tc() != DDMarkDrainedManagersResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected mark drained managers response message.");

        markDrainedManagersResponseMsg = (DDMarkDrainedManagersResponseMsg*) resp_msgs[i];
        err = markDrainedManagersResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, markDrainedManagersResponseMsg->errInfo());

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
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not synchronize dictionaries.");

    delete sync_serialized_ddicts[0];
    delete[] sync_serialized_ddicts;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_is_frozen(const dragonDDictDescr_t * dd_descr, bool * frozen) {
    dragonError_t err;
    DDGetFreezeMsg * getFreezeMsg = nullptr;
    DDGetFreezeResponseMsg * getFreezeResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    if (frozen == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict frozen pointer. Frozen should be non null.");

    *frozen = false;

    getFreezeMsg = new DDGetFreezeMsg(_tag_inc(dd), dd->clientID);
    err = _send_receive(&dd->main_manager_fli, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, getFreezeMsg, &dd->bufferedRespFLI, &resp_msg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the get frozen message and receive response.");

    if (resp_msg->tc() != DDGetFreezeResponseMsg::TC)
        append_err_return(DRAGON_FAILURE, "Failed to get expected get frozen response message.");

    getFreezeResponseMsg = (DDGetFreezeResponseMsg*) resp_msg;
    if (getFreezeResponseMsg->err() != DRAGON_SUCCESS)
        append_err_return(getFreezeResponseMsg->err(), getFreezeResponseMsg->errInfo());

    *frozen = getFreezeResponseMsg->frozen();

    delete getFreezeMsg;
    delete getFreezeResponseMsg;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_freeze(const dragonDDictDescr_t * dd_descr) {
    dragonError_t err;
    DDFreezeMsg * freezeMsg = nullptr;
    DDFreezeResponseMsg * freezeResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    uint64_t msg_tag;
    uint64_t resp_num = 0;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    // check and connect to root manager
    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    resp_num = dd->num_managers;
    msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    freezeMsg = new DDFreezeMsg(msg_tag, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, freezeMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the freeze message to root manager.");

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive freeze response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i]->tc() != DDFreezeResponseMsg::TC)
            append_err_return(DRAGON_FAILURE, "Failed to get expected freeze response message.");

        freezeResponseMsg = (DDFreezeResponseMsg*) resp_msgs[i];
        if (freezeResponseMsg->err() != DRAGON_SUCCESS)
            append_err_return(freezeResponseMsg->err(), freezeResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete freezeMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_unfreeze(const dragonDDictDescr_t * dd_descr) {
    dragonError_t err;
    DDUnFreezeMsg * unFreezeMsg = nullptr;
    DDUnFreezeResponseMsg * unFreezeResponseMsg = nullptr;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    uint64_t msg_tag;
    uint64_t resp_num = 0;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    // check and connect to root manager
    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    resp_num = dd->num_managers;
    msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    unFreezeMsg = new DDUnFreezeMsg(msg_tag, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, unFreezeMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the unfreeze message to root manager.");

    // receive response from all managers
    resp_msgs = new DragonResponseMsg*[resp_num];
    if (resp_msgs == nullptr)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, resp_num, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive unfreeze response.");

    for (size_t i=0 ; i<resp_num ; i++) {
        if (resp_msgs[i]->tc() != DDUnFreezeResponseMsg::TC)
            append_err_return(DRAGON_FAILURE, "Failed to get expected unfreeze response message.");

        unFreezeResponseMsg = (DDUnFreezeResponseMsg*) resp_msgs[i];
        if (unFreezeResponseMsg->err() != DRAGON_SUCCESS)
            append_err_return(unFreezeResponseMsg->err(), unFreezeResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete unFreezeMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_start_batch_put(const dragonDDictDescr_t * dd_descr, bool persist) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    dd->batch_put_started = true;
    dd->batch_persist = persist;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_end_batch_put(const dragonDDictDescr_t * dd_descr) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    size_t expected_num_responses = 0;
    DragonResponseMsg ** resp_msgs = nullptr;
    DDBatchPutResponseMsg * batchPutResponseMsg = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find ddict object.");

    if (dd->batch_put_started) {
        dd->batch_put_started = false;

        // TODO: If broadcast put (bput) is called during batch put, cleanup resources claimed for bput.
        if (dd->has_bput_root_manager_sendh) {
            err = _end_bput_with_batch(dd);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not end bput.");
        }

        // close all send handles created for batch put
        for (auto& opened_sendh_it: dd->opened_send_handles) {
            err = dragon_fli_close_send_handle(opened_sendh_it.second, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not close send handle.");
            delete opened_sendh_it.second;
        }
        dd->opened_send_handles.clear();

        expected_num_responses = dd->num_batch_puts.size();
        resp_msgs = new DragonResponseMsg*[expected_num_responses];
        if (resp_msgs == nullptr)
            append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

        err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, dd->batch_put_msg_tags, expected_num_responses, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Failed to receive batch put response.");

        dd->batch_put_msg_tags.clear();

        for (size_t i=0 ; i<expected_num_responses ; i++) {
            if (resp_msgs[i] == nullptr)
                append_err_return(DRAGON_INVALID_MESSAGE, "Could not receive valid respose.");

            if (resp_msgs[i]->tc() != DDBatchPutResponseMsg::TC)
                append_err_return(DRAGON_FAILURE, "Failed to get expected batch put response message.");

            batchPutResponseMsg = (DDBatchPutResponseMsg*) resp_msgs[i];
            if (batchPutResponseMsg->err() != DRAGON_SUCCESS)
                append_err_return(batchPutResponseMsg->err(), batchPutResponseMsg->errInfo());

            if (batchPutResponseMsg->numPuts() != dd->num_batch_puts[batchPutResponseMsg->managerID()])
                append_err_return(DRAGON_FAILURE, "Failed to store all keys in the distributed dictionary.");

            delete resp_msgs[i];
        }
        dd->num_batch_puts.clear();

        for (auto& strm_ch: dd->batch_put_stream_channels) {
            dragonChannelDescr_t * ch = strm_ch.second;
            dragon_destroy_process_local_channel(ch, dd->timeout);
            err = dragon_destroy_process_local_channel(ch, dd->timeout);
            free(ch);
        }

        dd->batch_put_stream_channels.clear();
    } else {
        append_err_return(DRAGON_INVALID_OPERATION, "Could not end batch put without starting.");
    }

    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_bput(const dragonDDictRequestDescr_t* req_descr) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    dragonDDictReq_t * req;
    DDBPutMsg * bputMsg = nullptr;
    dragonFLISerial_t ser_bput_resp_fli;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    dd = req->ddict;

    if (req->key_data == nullptr) {
        err = _build_key(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    }

    if (dd->batch_put_started) {
        // bput with batch
        if (dd->batch_persist)
            err_return(DRAGON_INVALID_OPERATION, "Persistent value mismatch. Could not perform broadcast put during batch put as bput only writes non-persistent key.");

        if (req->op_type != DRAGON_DDICT_NO_OP)
            err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
        req->op_type = DRAGON_DDICT_B_PUT_BATCH_REQ;

        /* First call to bput in current batch, select a random root manager and create a new send
        handle for it. A response channel is created to receive bput response during cleanup of
        the batch. */
        if (!dd->has_bput_root_manager_sendh) {
            /* This is the first call to bput in current batch, need to select a random root manager.
            For bput without batch put, a random root manager is selected every time.*/

            // For bput without batch put, a random root manager is selected every time.
            std::vector<uint64_t> managers(dd->num_managers);
            std::iota(managers.begin(), managers.end(), 0);

            // random generator
            std::random_device rd;
            std::mt19937 g(rd());

            // shuffle managers
            std::shuffle(managers.begin(), managers.end(), g);

            uint64_t root_manager = managers[0];
            err = _check_manager_connection(dd, root_manager);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not connect to the manager.");

            /* There might be other response expected in the main response channel, so create a new
            response FLI that only receives the bput response for current batch. In the cleanup
            of the broadcast put with batch, we will receive bput responses from every manager
            through this response FLI. */
            err = dragon_create_process_local_channel(&dd->bput_resp_strm, 0, 0, 0, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not create stream channel for bput response FLI.");

            err = dragon_fli_create(&dd->bput_respFLI, &dd->bput_resp_strm, nullptr, nullptr, 0, nullptr, true, nullptr);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not create bput response FLI.");

            err = dragon_fli_serialize(&dd->bput_respFLI, &ser_bput_resp_fli);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not serialize buffered response FLI.");

            dd->bput_respFLIStr = dragon_base64_encode(ser_bput_resp_fli.data, ser_bput_resp_fli.len);
            err = dragon_fli_serial_free(&ser_bput_resp_fli);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not free serialized buffered response FLI.");

            // create new send handle for the root manager
            err = dragon_create_process_local_channel(&dd->bput_strm, 0, 0, 0, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not create stream channel for root manager.");

            err = dragon_fli_open_send_handle(&dd->manager_table[root_manager], &dd->bput_root_manager_sendh, &dd->bput_strm, nullptr, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not open send handle.");

            dd->has_bput_root_manager_sendh = true;

            dd->bput_tag = _tag_inc(dd);
            bputMsg = new DDBPutMsg(dd->bput_tag, dd->clientID, dd->chkpt_id, dd->bput_respFLIStr.c_str(), managers, true);
            bputMsg->send(&dd->bput_root_manager_sendh, dd->timeout);
            if (err != DRAGON_SUCCESS)
                append_err_return(err, "Could not send message.");
        }
        // send key
        req->sendh = dd->bput_root_manager_sendh;
        err = dragon_fli_send_mem(&req->sendh, &req->key_mem, KEY_HINT, false, false, req->ddict->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send key to manager.");

    } else {
        // For bput without batch put, a random root manager is selected every time.

        if (req->op_type != DRAGON_DDICT_NO_OP)
            err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
        req->op_type = DRAGON_DDICT_B_PUT_REQ;

        std::vector<uint64_t> managers(dd->num_managers);
        std::iota(managers.begin(), managers.end(), 0);

        // random generator
        std::random_device rd;
        std::mt19937 g(rd());

        // shuffle managers
        std::shuffle(managers.begin(), managers.end(), g);

        req->manager_id = managers[0];
        err = _check_manager_connection(dd, req->manager_id);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not connect to the manager.");

        req->manager_fli = dd->manager_table[req->manager_id];

        req->msg_tag = _tag_inc(dd);
        bputMsg = new DDBPutMsg(req->msg_tag, dd->clientID, dd->chkpt_id, dd->bufferedRespFLIStr.c_str(), managers, false);
        err = _send_msg_key_no_close_sendh(bputMsg, req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the bput message and key.");
    }

    delete bputMsg;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_bget(const dragonDDictRequestDescr_t* req_descr) {

    dragonError_t err;
    dragonError_t resp_err;
    dragonDDictReq_t * req;
    dragonDDict_t * dd = nullptr;
    bool is_local_manager = false;
    DDGetMsg * getMsg = nullptr;
    DDGetResponseMsg * getResponseMsg = nullptr;
    DragonResponseMsg * resp_msg = nullptr;
    dragonFLIRecvHandleDescr_t recvh;

    if (req_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid request descriptor.");

    err = _ddict_req_from_descr(req_descr, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find valid request object.");

    if (req->op_type != DRAGON_DDICT_NO_OP)
        err_return(DRAGON_INVALID_OPERATION, "Could not change ddict operation.");
    req->op_type = DRAGON_DDICT_GET_REQ;

    dd = req->ddict;

    if (req->key_data == nullptr) {
        err = _build_key(req);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not build key.");
    }

    if (dd->has_chosen_manager) {
        err = _check_manager_connection(dd, dd->chosen_manager);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not connect to the manager.");

        req->manager_id = dd->chosen_manager;
        req->manager_fli = dd->manager_table[dd->chosen_manager];
    } else {
        req->manager_id = dd->main_manager;
        req->manager_fli = dd->main_manager_fli;
    }

    auto it = std::find(dd->local_managers.begin(), dd->local_managers.end(), req->manager_id);
    is_local_manager = (it != dd->local_managers.end());

    req->msg_tag = _tag_inc(dd);

    getMsg = new DDGetMsg(req->msg_tag, dd->clientID, dd->chkpt_id, req->key_data, req->key_size);

    err = _send_msg(getMsg, req, true);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the get message and key.");

    err = _recv_dmsg_no_close_recvh(&recvh, &dd->respFLI, &resp_msg, getMsg->tag(), false, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to get expected get response message.");

    req->recvh = recvh;
    req->recvh_closed = false;

    if (resp_msg->tc() != DDGetResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected get response message.");

    getResponseMsg = (DDGetResponseMsg*) resp_msg;
    resp_err = getResponseMsg->err();
    // Close receive handle if the error code is not SUCCESS.
    if (resp_err != DRAGON_SUCCESS) {
        dragon_fli_close_recv_handle(&req->recvh, req->ddict->timeout);
        req->recvh_closed = true;

        if (resp_err != DRAGON_KEY_NOT_FOUND && resp_err != DRAGON_DDICT_CHECKPOINT_RETIRED)
            err_return(resp_err, getResponseMsg->errInfo());
    }

    req->free_mem = getResponseMsg->freeMem() || !is_local_manager;
    if (!req->free_mem) {
        err = dragon_fli_reset_free_flag(&req->recvh);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not reset free memory flag in receive handle.");
    }

    delete getMsg;
    delete getResponseMsg;

    no_err_return(resp_err);
}

dragonError_t dragon_ddict_advance(const dragonDDictDescr_t * dd_descr) {
    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

    err = _check_manager_connection(dd, 0);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);

    DDAdvanceMsg * advanceMsg = new DDAdvanceMsg(msg_tag, dd->clientID, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, advanceMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the advance message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive advance response.");

    uint64_t min_newest_chkpt_id = UINT64_MAX;
    for (uint64_t i=0 ; i<dd->num_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDAdvanceResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected advance response message.");

        DDAdvanceResponseMsg * advanceResponseMsg = (DDAdvanceResponseMsg*) resp_msgs[i];
        err = advanceResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, advanceResponseMsg->errInfo());

        min_newest_chkpt_id = min(min_newest_chkpt_id, advanceResponseMsg->chkptID());

        delete resp_msgs[i];
    }

    delete advanceMsg;
    delete[] resp_msgs;

    dd->chkpt_id = min_newest_chkpt_id;

    err = _restore(dd, dd->chkpt_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to restore checkpoint.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_persist(const dragonDDictDescr_t * dd_descr) {
    // Persist current checkpoint to disk.
    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

    err = _chkpt_avail(dd, dd->chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_return(err, "Failed to check the availability of the checkpoint or the checkpoint is unavaileble across all managers.");

   err = _check_manager_connection(dd, 0);
   if (err != DRAGON_SUCCESS)
       append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    DDPersistMsg * persistMsg = new DDPersistMsg(msg_tag, dd->chkpt_id, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, persistMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the persist message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive persist response.");

    for (uint64_t i=0 ; i<dd->num_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDPersistResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected persist response message.");

        DDPersistResponseMsg * persistResponseMsg = (DDPersistResponseMsg*) resp_msgs[i];
        err = persistResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, persistResponseMsg->errInfo());

        delete resp_msgs[i];
    }

    delete persistMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_restore(const dragonDDictDescr_t * dd_descr, uint64_t chkpt_id) {

    dragonError_t err;
    dragonDDict_t * dd = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

    if (dd->batch_put_started)
        err_return(DRAGON_INVALID_OPERATION, "Restoring checkpoint during batch put is invalid.");

    // check persisted checkpoint availability across all managers
    err = _persisted_chkpt_avail(dd, chkpt_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to check if the checkpoint is an available persisted checkpoint.");

    err = _restore(dd, chkpt_id);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to restore the checkpoint.");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_persisted_ids_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& persisted_ids) {
    // Get a list of persisted checkpoint IDs.
    dragonError_t err;
    dragonDDict_t * dd = nullptr;
    std::set<uint64_t> msg_tags;
    DragonResponseMsg ** resp_msgs = nullptr;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

   err = _check_manager_connection(dd, 0);
   if (err != DRAGON_SUCCESS)
       append_err_return(err, "Could not connect to root manager.");

    uint64_t msg_tag = _tag_inc(dd);
    msg_tags.insert(msg_tag);
    DDPersistChkptsMsg * persistChkptsMsg = new DDPersistChkptsMsg(msg_tag, dd->clientID, dd->bufferedRespFLIStr.c_str());
    err = _send(&dd->manager_table[0], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, persistChkptsMsg, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not send the persist chkpts message to root manager.");

    resp_msgs = new DragonResponseMsg*[dd->num_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err =  _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, dd->num_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive persist chkpts response.");

    // initialize available chkpts with the chkpt IDs from the first responses
    if (resp_msgs[0] == nullptr)
        err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

    if (resp_msgs[0]->tc() != DDPersistChkptsResponseMsg::TC)
        err_return(DRAGON_FAILURE, "Failed to get expected persist response message.");

    DDPersistChkptsResponseMsg * persistChkptsResponseMsg = (DDPersistChkptsResponseMsg*) resp_msgs[0];
    err = persistChkptsResponseMsg->err();
    if (err != DRAGON_SUCCESS)
        err_return(err, persistChkptsResponseMsg->errInfo());

    persisted_ids = persistChkptsResponseMsg->chkptIDs();
    delete resp_msgs[0];

    for (uint64_t i=1 ; i<dd->num_managers ; i++) {
        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDPersistChkptsResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected persist response message.");

        DDPersistChkptsResponseMsg * persistChkptsResponseMsg = (DDPersistChkptsResponseMsg*) resp_msgs[i];
        err = persistChkptsResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, persistChkptsResponseMsg->errInfo());

        // get intersection of the two vector
        std::vector<uint64_t> chkpts = persistChkptsResponseMsg->chkptIDs();
        std::vector<uint64_t> intersection;
        std::set_intersection(persisted_ids.begin(), persisted_ids.end(), chkpts.begin(), chkpts.end(), std::back_inserter(intersection));
        persisted_ids = intersection;
        delete resp_msgs[i];
    }

    std::sort(persisted_ids.begin(), persisted_ids.end());

    delete persistChkptsMsg;
    delete[] resp_msgs;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_persisted_ids(const dragonDDictDescr_t * dd_descr, uint64_t ** persisted_ids, size_t * num_persisted_ids) {
    dragonError_t err;
    std::vector<uint64_t> persisted_ids_vec;

    err = dragon_ddict_persisted_ids_vec(dd_descr, persisted_ids_vec);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not get persisted checkpoint IDs from ddict.");

    if (persisted_ids_vec.size() != 0)
        *persisted_ids = (uint64_t*) malloc(persisted_ids_vec.size() * sizeof(uint64_t));

    for (size_t i=0 ; i<persisted_ids_vec.size() ; i++)
        (*persisted_ids)[i] = persisted_ids_vec[i];

    *num_persisted_ids = persisted_ids_vec.size();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t dragon_ddict_local_length(const dragonDDictDescr_t * dd_descr, uint64_t* local_length) {
    dragonError_t err;
    DragonResponseMsg ** resp_msgs = nullptr;
    dragonDDict_t * dd = nullptr;
    std::set<uint64_t> msg_tags;

    if (dd_descr == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict descriptor.");

    if (local_length == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ddict length. Length should be non null.");

    *local_length = 0;

    err = _ddict_from_descr(dd_descr, &dd);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not find object.");

    uint64_t num_local_managers = dd->local_managers.size();
    for (uint64_t i=0 ; i<num_local_managers ; i++) {
        err = _check_manager_connection(dd, i);
        if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not connect to local manager.");
    }

    for (uint64_t i=0 ; i<num_local_managers ; i++) {
        uint64_t msg_tag = _tag_inc(dd);
        msg_tags.insert(msg_tag);
        DDLengthMsg * lenMsg = new DDLengthMsg(msg_tag, dd->clientID, dd->bufferedRespFLIStr.c_str(), dd->chkpt_id, false);
        err = _send(&dd->manager_table[i], STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, lenMsg, dd->timeout);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send the length message to local manager.");
        delete lenMsg;
    }

    resp_msgs = new DragonResponseMsg*[num_local_managers];
    if (resp_msgs == nullptr)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for responses -- out of memory.");

    err = _recv_responses(&dd->bufferedRespFLI, resp_msgs, msg_tags, num_local_managers, dd->timeout);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to receive length response.");

    for (uint64_t i=0 ; i<num_local_managers ; i++) {

        if (resp_msgs[i] == nullptr)
            err_return(DRAGON_INVALID_MESSAGE, "Did not receive valid response.");

        if (resp_msgs[i]->tc() != DDLengthResponseMsg::TC)
            err_return(DRAGON_FAILURE, "Failed to get expected length response message.");

        DDLengthResponseMsg * lengthResponseMsg = (DDLengthResponseMsg*) resp_msgs[i];
        err = lengthResponseMsg->err();
        if (err != DRAGON_SUCCESS)
            err_return(err, lengthResponseMsg->errInfo());

        *local_length += lengthResponseMsg->length();

        delete resp_msgs[i];
    }
    delete[] resp_msgs;
    no_err_return(DRAGON_SUCCESS);
}