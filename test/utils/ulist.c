#include "../../src/lib/ulist.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <omp.h>

#include "../_ctest_utils.h"

#define NITEMS 10000

int main(int argc, char *argv[])
{
    dragonList_t tlist;

    dragonError_t uerr = dragon_ulist_create(&tlist);
    if (uerr != DRAGON_SUCCESS) {
        printf("ERROR: could not create ulist (err = %i)\n", uerr);
        return FAILED;
    }

    uint64_t * items = malloc(sizeof(uint64_t) * NITEMS);

    #pragma omp parallel
    {
        printf("Hello from %i\n", omp_get_thread_num());
    }

    size_t sz = dragon_ulist_get_size(&tlist);
    if (sz != 0) {
        printf("ERROR: The ulist was not recorded as size 0. It was size %lu instead\n", sz);
        return FAILED;
    }

    #pragma omp parallel for
    for (int i = 0; i < NITEMS; i++) {
        items[i] = i;
        dragonError_t uerr = dragon_ulist_additem(&tlist, &items[i]);
        if (uerr != DRAGON_SUCCESS) {
            printf("ERROR: could not add to ulist (err = %i)\n", uerr);
        }
    }

    #pragma omp parallel for
    for (int i = 0; i < NITEMS; i++) {
        items[i] = i;
        bool found = dragon_ulist_contains(&tlist, &items[i]);
        if (found != true) {
            printf("ERROR: could not add to ulist (err = %i)\n", uerr);
        }
    }

    sz = dragon_ulist_get_size(&tlist);
    if (sz != NITEMS) {
        printf("ERROR: The ulist was not recorded as size %u. It was size %lu instead\n", NITEMS, sz);
        return FAILED;
    }

    int count = 0;

    #pragma omp parallel for
    for (int i = 0; i < NITEMS; i++) {
        uint64_t * item;
        dragonError_t uerr = dragon_ulist_get_current_advance(&tlist, (void**)&item);
        if (uerr != DRAGON_SUCCESS) {
            printf("ERROR: could not get current from ulist (err = %i)\n", uerr);
        }
        int item_idx = item - items;
        if (*item != item_idx) {
            printf("ERROR: corrupt ulist at (%lu != %i)\n", *item, i);
        }
        uerr = dragon_ulist_delitem(&tlist, item);
        if (uerr != DRAGON_SUCCESS) {
            atomic_fetch_add(&count, 1);
        }
    }

    sz = dragon_ulist_get_size(&tlist);
    if (sz != 0) {
        printf("ERROR: The ulist was not back to size 0. It was size %lu instead\n", sz);
        return FAILED;
    }

    uerr = dragon_ulist_destroy(&tlist);
    if (uerr != DRAGON_SUCCESS) {
        printf("ERROR: could not destroy ulist (err = %i)\n", uerr);
        return FAILED;
    }

    free(items);

    printf("SUCCESS\n");
    return SUCCESS;
}
