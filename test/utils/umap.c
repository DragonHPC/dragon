#include "../../src/lib/umap.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <omp.h>

#define NITEMS 10000

int main(int argc, char *argv[])
{
    dragonMap_t tmap;

    dragonError_t uerr = dragon_umap_create(&tmap, 857);
    if (uerr != DRAGON_SUCCESS) {
        printf("ERROR: could not create umap (err = %i)\n", uerr);
        return 0;
    }

    uint64_t * ids = malloc(sizeof(uint64_t) * NITEMS);
    uint64_t * vals = malloc(sizeof(uint64_t) * NITEMS);

    #pragma omp parallel
    {
        printf("Hello from %i\n", omp_get_thread_num());
    }

    #pragma omp parallel for
    for (int i = 0; i < NITEMS; i++) {
        vals[i] = i;
        dragonError_t uerr = dragon_umap_additem_genkey(&tmap, &vals[i], &ids[i]);
        if (uerr != DRAGON_SUCCESS) {
            printf("ERROR: could not add to umap (err = %i)\n", uerr);
        }
    }

    #pragma omp parallel for
    for (int i = 0; i < NITEMS; i++) {
        uint64_t * val;
        dragonError_t uerr = dragon_umap_getitem(&tmap, ids[i], (void**)&val);
        if (uerr != DRAGON_SUCCESS) {
            printf("ERROR: could not get from umap (err = %i)\n", uerr);
        }
        if (*val != i) {
            printf("ERROR: corrupt umap at (%lu != %i)\n", *val, i);
        }
        uerr = dragon_umap_delitem(&tmap, ids[i]);
        if (uerr != DRAGON_SUCCESS) {
            printf("ERROR: could not delete from umap (err = %i)\n", uerr);
        }
    }

    uerr = dragon_umap_destroy(&tmap);
    if (uerr != DRAGON_SUCCESS) {
        printf("ERROR: could not destroy umap (err = %i)\n", uerr);
        return 0;
    }

    free(ids);
    free(vals);

    printf("SUCCESS\n");
}
