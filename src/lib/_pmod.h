#ifndef HAVE_DRAGON_PMOD_INTERNAL_H
#define HAVE_DRAGON_PMOD_INTERNAL_H

#include "dragon/pmod.h"

/*
 * extern declarations for global variables
 */

extern bool dragon_debug;
extern char pmod_apid[PMI_PG_ID_SIZE];

/*
 * inline functions
 */

static inline int
get_comm_timeout()
{
    static int comm_timeout = 30;
    static bool has_timeout_been_set = false;

    if (!has_timeout_been_set) {
        char *tmp = getenv("DRAGON_PMOD_COMMUNICATION_TIMEOUT");
        if (tmp != NULL) {
            comm_timeout = atoi(tmp);
        }
    }

    return comm_timeout;
}

/*
 * function prototypes
 */

dragonError_t
dragon_pmod_dragon_allocate(void **vaddr,
                            dragonMemoryDescr_t *mem_descr,
                            size_t size);

dragonError_t
dragon_pmod_dragon_free(dragonMemoryDescr_t *mem_descr);

dragonError_t
dragon_pmod_allocate_scalar_params(dragonRecvJobParams_t *mparams);

dragonError_t
dragon_pmod_allocate_array_params(dragonRecvJobParams_t *mparams);

#endif // HAVE_DRAGON_PMOD_INTERNAL_H