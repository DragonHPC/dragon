#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dragon/utils.h>
#include <dragon/ddict.h>
#include <dragon/fli.h>
#include <assert.h>
#include <time.h>
#include "../_ctest_utils.h"

#define SERFILE "a.out"

#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

static size_t VALUE_LENGTH = 10; // length of each value
static timespec_t TIMEOUT = {0,500000000}; // Timeouts will be 0.5 second by default

dragonError_t _fill_vals(char *** vals, size_t num_vals) {
    *vals = (char**)malloc(num_vals * sizeof(char*));
    if (*vals == NULL)
        err_fail(DRAGON_FAILURE, "Could not allocate space for values.");

    for (size_t i=0 ; i<num_vals ; i++) {
        // generate a random string
        char random_str[VALUE_LENGTH + 1];
        for (size_t j=0 ; j<VALUE_LENGTH ; j++) {
            random_str[j] = 'A' + rand() % 26;
        }
        random_str[VALUE_LENGTH] = '\0';
        (*vals)[i] = (char*)malloc(VALUE_LENGTH+1);
        if ((*vals)[i] == NULL)
            err_fail(DRAGON_FAILURE, "Could not allocate space for value.");
        strcpy((*vals)[i], random_str);
    }
    return DRAGON_SUCCESS;
}

void _free_vals(char *** vals, size_t num_vals) {
    for (size_t i=0 ; i<num_vals ; i++) {
        free((*vals)[i]);
        (*vals)[i] = NULL;
    }
    free(*vals);
    *vals = NULL;
}

void _concat_key(char ** arr, size_t num_writes, char * keys) {
    strcpy(keys, arr[0]);
    for (size_t i=1 ; i<num_writes ; i++)
        strcat(keys, arr[i]);
}

dragonError_t _put(dragonDDictDescr_t * ddict, bool persist, char key[], char** vals, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    if (persist) {
        err = dragon_ddict_pput(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            err_fail(err, "Could not perform persistent put op");
    } else {
        err = dragon_ddict_put(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            err_fail(err, "Could not perform put op");
    }

    for (size_t i=0 ; i<num_vals ; i++) {
        err = dragon_ddict_write_bytes(&req, strlen(vals[i]), (uint8_t*)vals[i]);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not write value");
    }

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not finalize request");

    return DRAGON_SUCCESS;
}

dragonError_t _put_multiple_key_writes(dragonDDictDescr_t * ddict, bool persist, char** keys, char** vals, size_t num_key, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    for (size_t i=0 ; i<num_key ; i++) {
        err = dragon_ddict_write_bytes(&req, strlen(keys[i]), (uint8_t*)keys[i]);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not write key");
    }

    // Single-write of a key
    // err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    // if (err != DRAGON_SUCCESS)
    //     err_fail(err, "Could not write the key.");

    if (persist) {
        err = dragon_ddict_pput(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            err_fail(err, "Could not perform persistent put op");
    } else {
        err = dragon_ddict_put(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            err_fail(err, "Could not perform put op");
    }

    for (size_t i=0 ; i<num_vals ; i++) {
        err = dragon_ddict_write_bytes(&req, strlen(vals[i]), (uint8_t*)vals[i]);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not write value");
    }

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not finalize request");

    return DRAGON_SUCCESS;
}

dragonError_t _get_read_bytes(dragonDDictDescr_t * ddict, char key[], char ** vals, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    // write key to req
    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    err = dragon_ddict_get(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        dragonError_t finalize_err = dragon_ddict_finalize_request(&req);
        if (finalize_err != DRAGON_SUCCESS)
            return finalize_err;
        return err;
    } else if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform get op");
    }

    size_t i = 0;
    while (err == DRAGON_SUCCESS) {

        size_t recv_sz = 0;
        uint8_t * bytes = NULL;

        err = dragon_ddict_read_bytes(&req, 0, &recv_sz, &bytes);
        if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
            err_fail(err, "Could not receive value");

        // compare value
        if (i > num_vals)
            misc_fail("Received more values than expected");

        if (err == DRAGON_SUCCESS) {
            assert(recv_sz == strlen(vals[i]));
            if (memcmp((uint8_t*)vals[i], bytes, recv_sz))
                misc_fail("Expected value did not match received value");
            free(bytes);
        }

        i += 1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "Exited recveive loop before EOT");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to finalize GET request");

    return DRAGON_SUCCESS;
}

dragonError_t _get_read_bytes_multiple_key_writes(dragonDDictDescr_t * ddict, char ** keys, char ** vals, size_t num_key, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    // write key to req
    for (size_t i=0 ; i<num_key ; i++) {
        err = dragon_ddict_write_bytes(&req, strlen(keys[i]), (uint8_t*)keys[i]);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not write the key.");
    }

    err = dragon_ddict_get(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        return err;
    } else if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform get op");
    }

    size_t i = 0;
    while (err == DRAGON_SUCCESS) {

        size_t recv_sz = 0;
        uint8_t * bytes = NULL;

        err = dragon_ddict_read_bytes(&req, 0, &recv_sz, &bytes);
        if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
            err_fail(err, "Could not receive value");

        // compare value
        if (i > num_vals)
            misc_fail("Received more values than expected");

        if (err == DRAGON_SUCCESS) {
            assert(recv_sz == strlen(vals[i]));
            if (memcmp((uint8_t*)vals[i], bytes, recv_sz))
                misc_fail("Expected value did not match received value");
            free(bytes);
        }

        i += 1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "Exited recveive loop before EOT");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to finalize GET request");

    return DRAGON_SUCCESS;
}

dragonError_t _get_read_mem(dragonDDictDescr_t * ddict, char key[], char ** vals, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    // write key to req
    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    err = dragon_ddict_get(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        return err;
    } else if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform get op");
    }

    size_t i = 0;
    while (err == DRAGON_SUCCESS) {

        dragonMemoryDescr_t mem;
        err = dragon_ddict_read_mem(&req, &mem);
        if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
            err_fail(err, "Could not receive value");

        // compare value
        if (i > num_vals)
            misc_fail("Received more values than expected");

        if (err == DRAGON_SUCCESS) {

            size_t num_bytes = 0;
            dragonError_t _err;
            _err = dragon_memory_get_size(&mem, &num_bytes);
            if (_err != DRAGON_SUCCESS)
                err_fail(_err, "Could not get memory size.");
            assert(num_bytes == strlen(vals[i]));

            void * mem_ptr;
            _err = dragon_memory_get_pointer(&mem, &mem_ptr);
            if (_err != DRAGON_SUCCESS)
                err_fail(_err, "Could not get memory pointer.");

            if (memcmp((uint8_t*)vals[i], mem_ptr, num_bytes))
                misc_fail("Expected value did not match received value");
        }

        dragon_memory_free(&mem);

        i += 1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "Exited recveive loop before EOT");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to finalize GET request");

    return DRAGON_SUCCESS;
}

dragonError_t _get_receive_bytes_into(dragonDDictDescr_t * ddict, char key[], char ** vals, size_t num_vals) {
    dragonChannelError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    err = dragon_ddict_get(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        return err;
    } else if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform get op");
    }

    int i = 0;
    while (err == DRAGON_SUCCESS) {

        size_t recv_sz = 0;
        uint8_t * bytes = (uint8_t*)malloc(512);

        err = dragon_ddict_read_bytes_into(&req, 0, &recv_sz, bytes);
        if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
            err_fail(err, "Could not receive value");

        // compare value
        if (i > num_vals)
            misc_fail("Received more values than expected");

        if (err == DRAGON_SUCCESS) {
            assert(recv_sz == strlen(vals[i]));
            if (memcmp((uint8_t*)vals[i], bytes, recv_sz))
                misc_fail("Expected value did not match received value");
        }

        free(bytes);

        i += 1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "Exited recveive loop before EOT");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to finalize GET request");

    return DRAGON_SUCCESS;
}

dragonError_t _contains(dragonDDictDescr_t * ddict, char key[]) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    err = dragon_ddict_contains(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED)
        return err;
    else if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    dragonError_t result = err;
    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not finalize request");

    return result;
}

dragonError_t _pop(dragonDDictDescr_t * ddict, char key[], char ** vals, size_t num_vals) {
    dragonError_t err;
    dragonDDictRequestDescr_t req;

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not create a new request");

    err = dragon_ddict_write_bytes(&req, strlen(key), (uint8_t*)key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not write the key.");

    err = dragon_ddict_pop(&req);
    if (err == DRAGON_KEY_NOT_FOUND || err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        dragonError_t finalize_err = dragon_ddict_finalize_request(&req);
        if (finalize_err != DRAGON_SUCCESS)
            err_fail(finalize_err, "Failed to finalize POP request");
        return err;
    } else if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform pop op");
    }

    size_t i = 0;
    while (err == DRAGON_SUCCESS) {

        size_t recv_sz = 0;
        uint8_t * bytes = NULL;

        err = dragon_ddict_read_bytes(&req, 0, &recv_sz, &bytes);
        if (err != DRAGON_SUCCESS && err != DRAGON_EOT)
            err_fail(err, "Could not receive value");

        // compare value
        if (i > num_vals)
            misc_fail("Received more values than expected");

        if (err == DRAGON_SUCCESS) {
            assert(recv_sz == strlen(vals[i]));
            if (memcmp((uint8_t*)vals[i], bytes, recv_sz))
            misc_fail("Expected value did not match received value");
        }

        free(bytes);

        i += 1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "Exited recveive loop before EOT");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to finalize GET request");

    return DRAGON_SUCCESS;
}

dragonError_t test_serialize(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS){
        err_fail(err, "Could not attach");
    }

    char * ser = NULL;
    err = dragon_ddict_serialize(&ddict, (const char**)&ser);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not serialize ddict.");
    assert (strcmp(ser, ddict_ser) == 0);

    free(ser);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_attach_detach(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_manager_placement(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_length(const char * ddict_ser) {

    dragonDDictDescr_t ddict;
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t length = 0;
    err = dragon_ddict_length(&ddict, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");

    assert(length == 2);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_clear(const char * ddict_ser) {

    // attach to ddict
    dragonDDictDescr_t ddict;
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // send clear request
    err = dragon_ddict_clear(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not clear ddict");

    // get length of the dictionary and assert that it equals to 0
    /* Test Length Operation */
    uint64_t length = 0;
    err = dragon_ddict_length(&ddict, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get the length of ddict");

    assert(length == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_put(const char * ddict_ser) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = false;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_put_multiple_values(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = false;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_put_multiple_key_writes(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char ** keys = NULL;
    size_t num_key = 3;
    err = _fill_vals(&keys, num_key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = false;
    err = _put_multiple_key_writes(&ddict, persist, keys, vals, num_key, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    _free_vals(&keys, num_key);
    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_pput(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_contains_existing_key(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    //contains op
    err = _contains(&ddict, key);
    if (err == DRAGON_KEY_NOT_FOUND) {
        err_fail(err, "Could not find the key");
    } else if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in pop request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_contains_non_existing_key(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    //contains op
    char key[] = "dragon";
    err = _contains(&ddict, key);
    if (err == DRAGON_KEY_NOT_FOUND) {
        ; // get expected err code, continue
    } else if (err == DRAGON_SUCCESS) {
        err_fail(err, "Key should not exist.");
    } else
        err_fail(err, "Caught err in pop request");

    dragonError_t result = err;

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    assert (result == DRAGON_KEY_NOT_FOUND);

    return DRAGON_SUCCESS;
}

dragonError_t test_get(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get op
    err = _get_read_bytes(&ddict, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_get_multiple_values(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, (char**)vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get op
    err = _get_read_bytes(&ddict, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_get_multiple_key_writes(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char ** keys = NULL;
    size_t num_key = 3;
    err = _fill_vals(&keys, num_key);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put_multiple_key_writes(&ddict, persist, keys, vals, num_key, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get op
    err = _get_read_bytes_multiple_key_writes(&ddict, keys, vals, num_key, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_get_read_mem(const char* ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get op
    err = _get_read_mem(&ddict, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_get_non_existing_key(const char* ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    // Get op
    err = _get_read_bytes(&ddict, key, vals, num_vals);
    assert (err == DRAGON_KEY_NOT_FOUND);

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_get_receive_bytes_into(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    size_t num_vals = 3;
    // Fill values
    char ** vals = NULL;
    num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get op
    err = _get_receive_bytes_into(&ddict, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");
    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_pop_existing_key(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Delete (pop) op
    err = _pop(&ddict, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in pop request");

    //contains op
    err = _contains(&ddict, key);
    if (err == DRAGON_KEY_NOT_FOUND) {
        ;
    } else if (err == DRAGON_DDICT_CHECKPOINT_RETIRED) {
        err_fail(err, "Attempt to get a key from retired checkpoint.");
    } else if (err == DRAGON_SUCCESS) {
        err_fail(err, "Key should not exist.");
    }else
        err_fail(err, "Caught err in pop request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_pop_non_existing_key(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Delete (pop) op
    char key[] = "dragon";
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 3;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call pop API
    err = _pop(&ddict, key, vals, num_vals);
    if (err == DRAGON_KEY_NOT_FOUND) {
        ; // get expected err code, continue
    } else if (err == DRAGON_SUCCESS) {
        err_fail(err, "Key should not exist.");
    } else
        err_fail(err, "Caught err in pop request");

    _free_vals(&vals, num_vals);
    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;

}

dragonError_t test_keys(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    char* keys[] = {"dragon", "hello", "Miska"};
    // Put op
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = true;
    err = _put(&ddict, persist, keys[0], vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    _free_vals(&vals, num_vals);

    // Put op
    // Fill values
    char ** vals1 = NULL;
    num_vals = 1;
    err = _fill_vals(&vals1, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    err = _put(&ddict, persist, keys[1], vals1, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    _free_vals(&vals1, num_vals);

    // Put op
    // Fill values
    char ** vals2 = NULL;
    num_vals = 1;
    err = _fill_vals(&vals2, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    err = _put(&ddict, persist, keys[2], vals2, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    _free_vals(&vals2, num_vals);

    dragonDDictKey_t** keys_bytes = NULL;
    size_t num_keys = 0;
    err = dragon_ddict_keys(&ddict, &keys_bytes, &num_keys);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform keys op");
    assert (num_keys == 3);

    if (keys_bytes == NULL) {
        err_fail(DRAGON_FAILURE, "keys bytes is still null.");
    }

    bool correct[] = {false, false, false};

    for (int i=0;i<num_keys;i++) {
        for (int j=0;j<num_keys;j++) {
            if (!memcmp((char*)keys_bytes[i]->data, keys[j], keys_bytes[i]->num_bytes)) {
                correct[i] = true;
                free(keys_bytes[i]->data);
            }
        }
    }

    assert(correct[0] && correct[1] && correct[2]);

    free(keys_bytes);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_keys_multiple_key_writes(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    size_t num_key = 3;
    char keys[3][100] = {"hello", "world", "runtime"};
    char * keys_ptrs[3];
    for (size_t i=0 ; i<3 ; i++)
        keys_ptrs[i] = keys[i];

    // // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");
    // call put API
    bool persist = false;
    err = _put_multiple_key_writes(&ddict, persist, (char**)keys_ptrs, vals, num_key, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    _free_vals(&vals, num_vals);

    dragonDDictKey_t** keys_bytes = NULL;
    size_t num_keys = 1;
    err = dragon_ddict_keys(&ddict, &keys_bytes, &num_keys);
    if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform keys op");
    }
    assert (num_keys == 1);

    if (keys_bytes == NULL) {
        err_fail(DRAGON_FAILURE, "keys bytes is still null.");
    }

    // concatentate string for comparison later
    char target_key[100] = "helloworldruntime";
    if (memcmp((char*)keys_bytes[0]->data, (char*)target_key, strlen(target_key)))
        misc_fail("Expected key did not match received key");

    free(keys_bytes[0]->data);
    free(keys_bytes);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_keys_multiple_keys_and_writes(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Put op
    // Fill key 1
    char ** key1 = NULL;
    size_t key1_num_writes = 3;
    err = _fill_vals(&key1, key1_num_writes);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space for key1");

    // Fill key 2
    char ** key2 = NULL;
    size_t key2_num_writes = 4;
    err = _fill_vals(&key2, key2_num_writes);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space for key2.");

    // Fill key 3
    char ** key3 = NULL;
    size_t key3_num_writes = 5;
    err = _fill_vals(&key3, key3_num_writes);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space for key3.");

    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    char keys[3][100]; // An array of concatenated key
    _concat_key(key1, key1_num_writes, keys[0]);
    _concat_key(key2, key2_num_writes, keys[1]);
    _concat_key(key3, key3_num_writes, keys[2]);


    // call put API for key 1
    bool persist = false;
    err = _put_multiple_key_writes(&ddict, persist, key1, vals, key1_num_writes, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not put key1");

    // call put API for key 2
    err = _put_multiple_key_writes(&ddict, persist, key2, vals, key2_num_writes, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not put key2");

    // call put API for key 3
    err = _put_multiple_key_writes(&ddict, persist, key3, vals, key3_num_writes, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not put key3");

    _free_vals(&vals, num_vals);

    dragonDDictKey_t** keys_bytes = NULL;
    size_t num_keys = 0;
    err = dragon_ddict_keys(&ddict, &keys_bytes, &num_keys);
    if (err != DRAGON_SUCCESS) {
        err_fail(err, "Could not perform keys op");
    }

    assert (num_keys == 3);

    if (keys_bytes == NULL) {
        err_fail(DRAGON_FAILURE, "keys bytes is still null.");
    }

    for (size_t i=0 ; i<num_keys ; i++) {
        bool found = false;
        // compare received key with each expected key that hasn't been received.
        for (size_t j=0 ; j<num_keys ; j++) {
            // Key already been received
            if(!memcmp((char*)keys_bytes[i]->data, keys[j], strlen(keys[j]))) { // Received an expected key.
                found = true;
                free(keys_bytes[i]->data);
                break;
            }
        }

        if (!found) {
            misc_fail("Received an unexpected key.");
        }
    }

    _free_vals(&key1, key1_num_writes);
    _free_vals(&key2, key2_num_writes);
    _free_vals(&key3, key3_num_writes);

    free(keys_bytes);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_checkpoint(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    err = dragon_ddict_checkpoint(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform checkpoint op.");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 1);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_rollback(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    for (size_t i=0 ; i<2 ; i++) {
         err = dragon_ddict_checkpoint(&ddict);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not perform checkpoint op.");
    }

    err = dragon_ddict_rollback(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform rollback op.");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 1);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_rollback_zero_chkpt_id(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    err = dragon_ddict_rollback(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform rollback op.");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_sync_to_newest_checkpoint(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    err = dragon_ddict_sync_to_newest_checkpoint(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 2);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_current_checkpoint_id(const char * ddict_ser) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_local_manager(const char * ddict_ser, const char * local_manager) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t local_manager_id = 0;
    err = dragon_ddict_local_manager(&ddict, &local_manager_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    char local_manager_id_str[256];
    // convert to string for string comparison
    sprintf(local_manager_id_str, "%ld", local_manager_id);
    assert(strcmp(local_manager_id_str, local_manager) == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_main_manager(const char * ddict_ser, const char* main_manager) {

    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t main_manager_id = 0;
    err = dragon_ddict_main_manager(&ddict, &main_manager_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    char main_manager_id_str[256];
    // convert to string for string comparison
    sprintf(main_manager_id_str, "%ld", main_manager_id);
    assert(strcmp(main_manager_id_str, main_manager) == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_attach(const char * ddict_ser) {
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // checkpoint to 3
    for (size_t i=0 ; i<3 ; i++) {
        err = dragon_ddict_checkpoint(&ddict);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Could not perform checkpoint op.");
    }

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict_m1, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 3);

    // Detach client from the dictionary and free client object.
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    // Free client object
    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not free copy of client object.");

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_put(const char * ddict_ser) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    bool persist = true;
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");
    // Put op
    char key[] = "dragon";
    err = _put(&ddict_m0, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");
    // Put op - 1
    char key2[] = "helloworld";
    err = _put(&ddict_m1, persist, key2, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");
    // Put op - 2
    char key3[] = "test_manager_selection";
    err = _put(&ddict_m1, persist, key3, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Length of ddict
    uint64_t length = 0;
    err = dragon_ddict_length(&ddict, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 3);

    err = dragon_ddict_length(&ddict_m0, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 1);

    err = dragon_ddict_length(&ddict_m1, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 2);

    // Detach client from the dictionary and free client object.
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    _free_vals(&vals, num_vals);

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_get(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    bool persist = true;
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    // Put Ops
    char key[] = "dragon";
    err = _put(&ddict_m0, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    char key1[] = "helloworld";
    err = _put(&ddict_m1, persist, key1, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    char key2[] = "test_custom_manager_get";
    err = _put(&ddict_m1, persist, key2, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Get Ops
    err = _get_read_bytes(&ddict_m0, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    err = _get_read_bytes(&ddict_m1, key1, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    err = _get_read_bytes(&ddict_m1, key2, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in get request");

    err = _get_read_bytes(&ddict_m0, key1, vals, num_vals);
    if (err != DRAGON_KEY_NOT_FOUND)
        err_fail(err, "Could not get expected result, expected DRAGON_KEY_NOT_FOUND");

    err = _get_read_bytes(&ddict_m0, key2, vals, num_vals);
    if (err != DRAGON_KEY_NOT_FOUND)
        err_fail(err, "Could not get expected result, expected DRAGON_KEY_NOT_FOUND");

    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;

}

dragonError_t test_custom_manager_sync_to_newest_checkpoint(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    // Sync to newest checkpoint of all managers
    err = dragon_ddict_sync_to_newest_checkpoint(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    uint64_t chkpt_id = 0;
    err = dragon_ddict_current_checkpoint_id(&ddict, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 3);

    // Sync to manager 0's newest checkpoint
    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    err = dragon_ddict_sync_to_newest_checkpoint(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    err = dragon_ddict_current_checkpoint_id(&ddict_m0, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 3);

    // Sync to manager 1's newest checkpoint
    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    err = dragon_ddict_sync_to_newest_checkpoint(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not sync to newest checkpoint.");

    err = dragon_ddict_current_checkpoint_id(&ddict_m1, &chkpt_id);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get current checkpoint ID.");
    assert (chkpt_id == 2);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_clear(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    // clear manager 0
    err = dragon_ddict_clear(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not clear ddict");

    uint64_t length = 0;
    err = dragon_ddict_length(&ddict_m0, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 0);

    err = dragon_ddict_length(&ddict, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 2);

    err = dragon_ddict_length(&ddict_m1, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 2);

    // clear manager 1
    err = dragon_ddict_clear(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not clear ddict");

    err = dragon_ddict_length(&ddict, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 0);

    err = dragon_ddict_length(&ddict_m1, &length);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not generate length request");
    assert (length == 0);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_keys(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    bool persist = true;
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    // Put Ops
    char* keys[] = {"dragon", "helloworld", "test_custom_manager_keys"};

    err = _put(&ddict_m0, persist, keys[0], vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    err = _put(&ddict_m1, persist, keys[1], vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    err = _put(&ddict_m1, persist, keys[2], vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    dragonDDictKey_t** keys_bytes = NULL;
    size_t num_keys = 0;
    err = dragon_ddict_keys(&ddict_m0, &keys_bytes, &num_keys);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform keys op");
    assert (num_keys == 1);

    if (keys_bytes == NULL) {
        err_fail(DRAGON_FAILURE, "keys bytes is still null.");
    }

    if (memcmp((char*)keys_bytes[0]->data, keys[0], keys_bytes[0]->num_bytes)) {
        misc_fail("Expected value did not match received value");
    }

    free(keys_bytes[0]->data);
    free(keys_bytes);
    keys_bytes = NULL;
    num_keys = 0;

    err = dragon_ddict_keys(&ddict_m1, &keys_bytes, &num_keys);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform keys op");

    assert (num_keys == 2);

    if (keys_bytes == NULL) {
        err_fail(DRAGON_FAILURE, "keys bytes is still null.");
    }

    bool correct[] = {false, false};

    for (int i=0;i<num_keys;i++) {
        for (int j=1;j<3;j++) {
            if (!memcmp((char*)keys_bytes[i]->data, keys[j], keys_bytes[i]->num_bytes)) {
                correct[i] = true;
            }
        }
    }

    assert(correct[0] && correct[1]);

    for (int i=0;i<num_keys;i++)
        free(keys_bytes[i]);

    free(keys_bytes);

    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager_pop(const char * ddict_ser) {

    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    dragonDDictDescr_t ddict_m0;
    err = dragon_ddict_manager(&ddict, &ddict_m0, 0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    dragonDDictDescr_t ddict_m1;
    err = dragon_ddict_manager(&ddict, &ddict_m1, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not choose manager");

    bool persist = true;
    // Fill values
    char ** vals = NULL;
    size_t num_vals = 1;
    err = _fill_vals(&vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not allocate space.");

    // Put Ops
    char key[] = "dragon";
    err = _put(&ddict_m0, persist, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    char key1[] = "helloworld";
    err = _put(&ddict_m1, persist, key1, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not perform put op");

    // Client 1 delete a key on manager 0 -> should return key not found.
    err = _pop(&ddict_m1, key, vals, num_vals);
    if (err != DRAGON_KEY_NOT_FOUND)
        err_fail(err, "Could not get expected result, expected DRAGON_KEY_NOT_FOUND");

    // Delete (pop) op
    err = _pop(&ddict_m0, key, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in pop request");

    // Client 0 delete a key on manager 1 -> should return key not found.
    err = _pop(&ddict_m0, key1, vals, num_vals);
    if (err != DRAGON_KEY_NOT_FOUND)
        err_fail(err, "Could not get expected result, expected DRAGON_KEY_NOT_FOUND");

    // Delete (pop) op
    err = _pop(&ddict_m1, key1, vals, num_vals);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Caught err in pop request");

    _free_vals(&vals, num_vals);

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m0);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    err = dragon_ddict_detach(&ddict_m1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_empty_managers(const char * ddict_ser) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t* ids = NULL;
    size_t num_empty_managers = 0;
    err = dragon_ddict_empty_managers(&ddict, &ids, &num_empty_managers);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get empty managers");

    for (size_t i=0 ; i<num_empty_managers ; i++) {
        fprintf(stderr, "empty manager: %ld\n", ids[i]);
        fflush(stderr);
    }

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_local_managers(const char * ddict_ser) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t * local_manager_ids = NULL;
    size_t num_local_managers = 0;
    err = dragon_ddict_local_managers(&ddict, &local_manager_ids, &num_local_managers);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get local managers");

    fprintf(stderr, "local managers: \n");
    for(size_t i=0 ; i<num_local_managers ; i++){
        fprintf(stderr, "%ld\n", local_manager_ids[i]);
        fflush(stderr);
    }

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_local_keys(const char * ddict_ser) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    size_t num_local_keys = 0;
    dragonDDictKey_t** local_keys = NULL;
    err = dragon_ddict_local_keys(&ddict, &local_keys, &num_local_keys);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get local keys");

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

dragonError_t test_synchronize(const char ** ddicts_ser, const size_t num_ddicts) {
    dragonError_t err;

    err = dragon_ddict_synchronize(ddicts_ser, num_ddicts, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not synchronize dictionaries.");

    return DRAGON_SUCCESS;
}

dragonError_t test_clone(const char * ddict_ser, const char ** clone_ddicts_ser, const size_t num_clone_ddicts) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    // Attach to the running instance of the dictionary
    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    err = dragon_ddict_clone(&ddict, clone_ddicts_ser, num_clone_ddicts);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to clone dictionaries.");

    // Detach from the dictionary
    err = dragon_ddict_detach(&ddict);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not detach");

    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char * ddict_descr = argv[1];
    char * test = argv[2];

    dragonError_t err;
    srand(time(NULL));

    if (strcmp(test, "test_serialize") == 0) {
        err = test_serialize(ddict_descr);
    } else if (strcmp(test, "test_attach_detach") == 0) {
        err = test_attach_detach(ddict_descr);
    } else if (strcmp(test, "test_manager_placement") == 0) {
        err = test_manager_placement(ddict_descr);
    } else if (strcmp(test, "test_length") == 0) {
        err = test_length(ddict_descr);
    } else if (strcmp(test, "test_clear") == 0) {
        err = test_clear(ddict_descr);
    } else if (strcmp(test, "test_put") == 0) {
        err = test_put(ddict_descr);
    } else if (strcmp(test, "test_put_multiple_values") == 0) {
        err = test_put_multiple_values(ddict_descr);
    } else if (strcmp(test, "test_put_multiple_key_writes") == 0) {
        err = test_put_multiple_key_writes(ddict_descr);
    } else if (strcmp(test, "test_pput") == 0) {
        err = test_pput(ddict_descr);
    } else if (strcmp(test, "test_contains_existing_key") == 0) {
        err = test_contains_existing_key(ddict_descr);
    } else if (strcmp(test, "test_contains_non_existing_key") == 0) {
        err = test_contains_non_existing_key(ddict_descr);
    } else if (strcmp(test, "test_get") == 0) {
        err = test_get(ddict_descr);
    } else if (strcmp(test, "test_get_multiple_values") == 0) {
        err = test_get_multiple_values(ddict_descr);
    } else if (strcmp(test, "test_get_receive_bytes_into") == 0) {
        err = test_get_receive_bytes_into(ddict_descr);
    } else if (strcmp(test, "test_get_multiple_key_writes") == 0) {
        err = test_get_multiple_key_writes(ddict_descr);
    } else if (strcmp(test, "test_get_read_mem") == 0) {
        err = test_get_read_mem(ddict_descr);
    } else if (strcmp(test, "test_get_non_existing_key") == 0) {
        err = test_get_non_existing_key(ddict_descr);
    } else if (strcmp(test, "test_pop_existing_key") == 0) {
        err = test_pop_existing_key(ddict_descr);
    } else if (strcmp(test, "test_pop_non_existing_key") == 0) {
        err = test_pop_non_existing_key(ddict_descr);
    } else if (strcmp(test, "test_keys") == 0) {
        err = test_keys(ddict_descr);
    } else if (strcmp(test, "test_keys_multiple_key_writes") == 0) {
        err = test_keys_multiple_key_writes(ddict_descr);
    } else if (strcmp(test, "test_keys_multiple_keys_and_writes") == 0) {
        err = test_keys_multiple_keys_and_writes(ddict_descr);
    } else if (strcmp(test, "test_checkpoint") == 0) {
        err = test_checkpoint(ddict_descr);
    } else if (strcmp(test, "test_rollback") == 0) {
        err = test_rollback(ddict_descr);
    } else if (strcmp(test, "test_rollback_zero_chkpt_id") == 0) {
        err = test_rollback_zero_chkpt_id(ddict_descr);
    } else if (strcmp(test, "test_sync_to_newest_checkpoint") == 0) {
        err = test_sync_to_newest_checkpoint(ddict_descr);
    } else if (strcmp(test, "test_current_checkpoint_id") == 0) {
        err = test_current_checkpoint_id(ddict_descr);
    } else if (strcmp(test, "test_local_manager") == 0) {
        char * local_manager = argv[3];
        err = test_local_manager(ddict_descr, local_manager);
    } else if (strcmp(test, "test_main_manager") == 0) {
        char * main_manager = argv[3];
        err = test_main_manager(ddict_descr, main_manager);
    } else if (strcmp(test, "test_custom_manager_attach") == 0) {
        err = test_custom_manager_attach(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_put") == 0) {
        err = test_custom_manager_put(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_get") == 0) {
        err = test_custom_manager_get(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_sync_to_newest_checkpoint") == 0) {
        err = test_custom_manager_sync_to_newest_checkpoint(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_clear") == 0) {
        err = test_custom_manager_clear(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_keys") == 0) {
        err = test_custom_manager_keys(ddict_descr);
    } else if (strcmp(test, "test_custom_manager_pop") == 0) {
        err = test_custom_manager_pop(ddict_descr);
    } else if (strcmp(test, "test_empty_managers") == 0) {
        err = test_empty_managers(ddict_descr);
    } else if (strcmp(test, "test_local_managers") == 0) {
        err = test_local_managers(ddict_descr);
    } else if (strcmp(test, "test_local_keys") == 0) {
        err = test_local_keys(ddict_descr);
    } else if (strcmp(test, "test_synchronize") == 0) {
        char *tmpptr;
        size_t num_ddicts = strtoul(argv[3], &tmpptr, 10);
        char ** ser_ddicts = (char**)malloc(num_ddicts * sizeof(char*));
        if (ser_ddicts == NULL) {
            fprintf(stderr, "Could not allocate ser ddicts\n");
            fflush(stderr);
        }
        for (size_t i=0 ; i<num_ddicts ; i++) {
            char * ser_ddict = argv[4+i];
            ser_ddicts[i] = ser_ddict;
        }
        err = test_synchronize((const char**)ser_ddicts, num_ddicts);
        free(ser_ddicts);
    } else if (strcmp(test, "test_clone") == 0) {
        char *tmpptr;
        size_t num_ddicts = strtoul(argv[3], &tmpptr, 10);
        char ** ser_dest_ddicts = (char**)malloc(num_ddicts * sizeof(char*));
        if (ser_dest_ddicts == NULL) {
            fprintf(stderr, "Could not allocate ser ddicts\n");
            fflush(stderr);
            return DRAGON_FAILURE;
        }
        for (size_t i=0 ; i<num_ddicts ; i++) {
            char * ser_ddict = argv[4+i];
            ser_dest_ddicts[i] = ser_ddict;
        }
        err = test_clone(ddict_descr, (const char**)ser_dest_ddicts, num_ddicts);
        free(ser_dest_ddicts);
    } else {
        return DRAGON_NOT_IMPLEMENTED;
    }

    return err;
}