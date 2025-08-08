#include <time.h>

#include <dragon/messages.hpp>
#include <dragon/messages_api.h>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <capnp/serialize.h>

#include "_pmix.h"

using namespace dragon;

/*****************************************************************/
/*                                                               */
/*    This file exists because the PMIx headers are not C++      */
/*    friendly. Thus, while we link in our internal _pmix.h,     */
/*    there are no headers from the actual PMIx library.         */
/*                                                               */
/*****************************************************************/

extern "C" {

// Write a given string into the PMIx ddict at a provided key
dragonError_t _dragon_pmix_write_to_ddict(dragonDDictDescr_t * ddict, bool persist, char key[], char* val, size_t byte_len)
{

    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create a new ddict request");

    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not write the ddict key.");

    if (persist) {
        err = dragon_ddict_pput(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            append_err_return(err, "Could not perform persistent put op");
    } else {
        err = dragon_ddict_put(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            append_err_return(err, "Could not perform put op");
    }

    err = dragon_ddict_write_bytes(&req, byte_len, (uint8_t*) val);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not write value into dict");
    }

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not finalize ddict write request");
    return DRAGON_SUCCESS;

}

// Read a given key's data
dragonError_t _dragon_pmix_read_from_ddict(dragonDDictDescr_t * ddict, char key[], char **val, size_t *recv_sz)

{
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not create a new ddict request");

    // write key to req
    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not write ddict key for reading.");

    err = dragon_ddict_get(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        dragonError_t finalize_err = dragon_ddict_finalize_request(&req);
        if (finalize_err != DRAGON_SUCCESS)
            return finalize_err;
        return err;
    } else if (err != DRAGON_SUCCESS) {
        append_err_return(err, "Could not perform get op from pmix ddict");
    }

    err = dragon_ddict_read_bytes(&req, 0, recv_sz, (uint8_t **) val);

    if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
        append_err_return(err, "Could not receive pmix ddict value");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Failed to finalize GET request for pmix ddict");

    return DRAGON_SUCCESS;
}


// Put a PMIx server's fence data into a given ddict key as a serialized capnproto message
dragonError_t _dragon_pmix_put_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t ndata, char *data)
{

    dragonError_t derr;
    capnp::MallocMessageBuilder message;
    PMIxFenceMsgDef::Builder fence_builder = message.initRoot<PMIxFenceMsgDef>();
    fence_builder.setNdata(ndata);

    // Encode the data from PMIx
    if (ndata > 0) {
        char* encoded_data = dragon_base64_encode((uint8_t*) data, ndata);
        fence_builder.setData(encoded_data);
    }

    try {
        auto words = capnp::messageToFlatArray(message);
        auto bytes = words.asBytes();

        // Write to ddict
        bool persist = false;
        derr = _dragon_pmix_write_to_ddict(ddict, persist, key, (char*) bytes.begin(), bytes.size());
        if (derr != DRAGON_SUCCESS) {
            append_err_return(DRAGON_FAILURE, "Failed to write fence data to dictionary");
        }

    } catch (...) {
        append_err_return(DRAGON_INVALID_OPERATION, "Unable to flatten and write PMIxFenceMsg into a ddict");
    }


    no_err_return(DRAGON_SUCCESS);
}

// Get other PMIx servers' fence data to complet a given allgather
dragonError_t _dragon_pmix_get_fence_msg(dragonDDictDescr_t *ddict, char *key, size_t *ndata, char **data)
{

    char *b64_data;
    size_t data_len;
    dragonError_t derr;
    derr = _dragon_pmix_read_from_ddict(ddict, key, &b64_data, &data_len);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(DRAGON_FAILURE, "Unable to read data from dict for PMIx fence op");
    }

    // deserialize the message and decode the PMIx fence data
    kj::ArrayPtr<const capnp::word> words(reinterpret_cast<const capnp::word*>(b64_data), data_len / sizeof(capnp::word));
    capnp::FlatArrayMessageReader message(words);
    PMIxFenceMsgDef::Reader reader = message.getRoot<PMIxFenceMsgDef>();


    *ndata = (size_t) reader.getNdata();
    if (*ndata > 0) {
        *data = (char*) malloc(reader.getNdata());
        size_t decoded_len = 0;
        uint8_t *decoded_data = dragon_base64_decode(reader.getData().cStr(), &decoded_len);
        memcpy(*data, decoded_data, reader.getNdata());
        free(decoded_data);
    }
    else {
        *data = NULL;
    }
    no_err_return(DRAGON_SUCCESS);

}

}  // extern "C"

