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

static timespec_t TIMEOUT = {0,500000000}; // Timeouts will be 0.5 second by default

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

dragonError_t test_local_managers(const char * ddict_ser, const size_t num_local_managers) {
    dragonError_t err;
    dragonDDictDescr_t ddict;

    err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    uint64_t * mids = NULL;
    size_t num_m = 0;
    err = dragon_ddict_local_managers(&ddict, &mids, &num_m);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not get local managers");

    assert (num_m == num_local_managers);
    for (size_t i=0 ; i<num_m ; i++) {
        fprintf(stdout, "local_managers: %ld\n", mids[i]);
        fflush(stdout);
    }

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

    if (strcmp(test, "test_attach_detach") == 0) {
        err = test_attach_detach(ddict_descr);
    } else if (strcmp(test, "test_manager_placement") == 0) {
        err = test_manager_placement(ddict_descr);
    } else if (strcmp(test, "test_local_managers") == 0) {
        char *tmpptr;
        size_t num_local_managers = strtoul(argv[3], &tmpptr, 10);
        err = test_local_managers(ddict_descr, (const size_t) num_local_managers);
    } else {
        return DRAGON_NOT_IMPLEMENTED;
    }

    return err;

}
