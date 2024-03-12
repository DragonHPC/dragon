#include <assert.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <unistd.h>
#include "dragon/channels.h"
#include "_pals.h"
#include "_pmod.h"

extern dragonRecvJobParams_t pmod_mparams;

static void *lib_pals_handle = NULL;
static int ptrs_set = 0;
static int inside_vanilla_pals = 0;


pals_rc_t (*fn_pals_init)(pals_state_t *state);
pals_rc_t (*fn_pals_init2)(pals_state_t **state);
pals_rc_t (*fn_pals_fini)(pals_state_t *state);
pals_rc_t (*fn_pals_get_peidx)(pals_state_t *state, int *peidx);
pals_rc_t (*fn_pals_get_num_pes)(pals_state_t *state, int *npes);
pals_rc_t (*fn_pals_get_pes)(pals_state_t *state, pals_pe_t **pes, int *npes);
pals_rc_t (*fn_pals_get_nodeidx)(pals_state_t *state, int *nodeidx);
pals_rc_t (*fn_pals_get_num_nodes)(pals_state_t *state, int *nnodes);
pals_rc_t (*fn_pals_get_nodes)(pals_state_t *state, pals_node_t **nodes, int *nnodes);
pals_rc_t (*fn_pals_get_num_nics)(pals_state_t *state, int *nnics);
pals_rc_t (*fn_pals_get_nics)(pals_state_t *state, pals_nic_t **nics, int *nnics);
pals_rc_t (*fn_pals_get_num_cmds)(pals_state_t *state, int *ncmds);
pals_rc_t (*fn_pals_get_cmds)(pals_state_t *state, pals_cmd_t **cmds, int *ncmds);
pals_rc_t (*fn_pals_start_barrier)(pals_state_t *state);
pals_rc_t (*fn_pals_get_apid)(pals_state_t *state, char **apid);
pals_rc_t (*fn_pals_app_spawn)(
    pals_state_t *state, int count, const char *const cmds[], int argcs[],
    const char **const argvs[], const int maxprocs[],
    const char *preput_envs[], const int num_envs, pals_rc_t errors[]);
const char *(*fn_pals_errmsg)(pals_state_t *state);

void set_pals_function_pointers()
{
    lib_pals_handle = dlopen("/opt/cray/pe/pals/default/lib/libpals.so", RTLD_LAZY | RTLD_GLOBAL);

    fn_pals_init = dlsym(lib_pals_handle, "pals_init");
    fn_pals_init2 = dlsym(lib_pals_handle, "pals_init2");
    fn_pals_fini = dlsym(lib_pals_handle, "pals_fini");
    fn_pals_get_peidx = dlsym(lib_pals_handle, "pals_get_peidx");
    fn_pals_get_num_pes = dlsym(lib_pals_handle, "pals_get_num_pes");
    fn_pals_get_pes = dlsym(lib_pals_handle, "pals_get_pes");
    fn_pals_get_nodeidx = dlsym(lib_pals_handle, "pals_get_nodeidx");
    fn_pals_get_num_nodes = dlsym(lib_pals_handle, "pals_get_num_nodes");
    fn_pals_get_nodes = dlsym(lib_pals_handle, "pals_get_nodes");
    fn_pals_get_num_nics = dlsym(lib_pals_handle, "pals_get_num_nics");
    fn_pals_get_nics = dlsym(lib_pals_handle, "pals_get_nics");
    fn_pals_get_num_cmds = dlsym(lib_pals_handle, "pals_get_num_cmds");
    fn_pals_get_cmds = dlsym(lib_pals_handle, "pals_get_cmds");
    fn_pals_start_barrier = dlsym(lib_pals_handle, "pals_start_barrier");
    fn_pals_get_apid = dlsym(lib_pals_handle, "pals_get_apid");
    fn_pals_app_spawn = dlsym(lib_pals_handle, "pals_app_spawn");
    fn_pals_errmsg = dlsym(lib_pals_handle, "pals_errmsg");
}

int get_pals_context() {
    return inside_vanilla_pals;
}

void set_pals_context() {
    inside_vanilla_pals = 1;
}

void unset_pals_context() {
    inside_vanilla_pals = 0;
}

int check_calling_context()
{
    if (getenv("_DRAGON_PALS_ENABLED") && !(get_pals_context())) {
        return 1;
    } else {
        return 0;
    }
}



pals_rc_t pals_init(pals_state_t *state)
{
    // PALS init and finalize functions will always only need to return
    // the values PALS knows to be true. Thus, we need to make sure any
    // PALS functions we wrap know to send back unmodified return values,
    // ie: only use the results from direct calls to our PALS function pointers.
    set_pals_function_pointers();
    set_pals_context();

    pals_rc_t err = fn_pals_init(state);
    unset_pals_context();
    return err;
}

//// TODO: pals_init2 will always be defined, so how can PMI check if it's NULL?
pals_rc_t pals_init2(pals_state_t **state)
{
    set_pals_function_pointers();

    set_pals_context();
    // no error checking, just pass rc through to caller
    pals_rc_t err = fn_pals_init2(state);
    unset_pals_context();
    return err;
}

pals_rc_t pals_fini(pals_state_t *state)
{
    set_pals_context();
    // no error checking, just pass rc through to caller
    pals_rc_t err = fn_pals_fini(state);
    unset_pals_context();
    return err;
}

pals_rc_t pals_get_peidx(pals_state_t *state, int *peidx)
{
    if (check_calling_context()) {
        *peidx = pmod_mparams.sp->rank;
        return PALS_OK;
    } else {
        return fn_pals_get_peidx(state, peidx);
    }
}

pals_rc_t pals_get_num_pes(pals_state_t *state, int *npes)
{
    if (check_calling_context()) {
        *npes = pmod_mparams.sp->ntasks;
        return PALS_OK;
    } else {
        return fn_pals_get_num_pes(state, npes);
    }
}

pals_rc_t pals_get_pes(pals_state_t *state, pals_pe_t **pes, int *npes)
{
    if (check_calling_context()) {
        *npes = pmod_mparams.sp->ntasks;
        *pes = (pals_pe_t *) malloc(pmod_mparams.sp->ntasks * sizeof(pals_pe_t));
        if (*pes == NULL) {
            return PALS_FAILED;
        }

        int rank;

        for (rank = 0; rank < pmod_mparams.sp->ntasks; ++rank) {
            //only need cmdidx for now (assuming we're using pmod)
            //(*pes)[rank].localidx =
            (*pes)[rank].cmdidx = 0;
           //(*pes)[rank].nodeidx =
        }

        return PALS_OK;
    } else {
        return fn_pals_get_pes(state, pes, npes);
    }
}

pals_rc_t pals_get_nodeidx(pals_state_t *state, int *nodeidx)
{
    if (check_calling_context()) {
        *nodeidx = pmod_mparams.sp->nid;
        return PALS_OK;
    } else {
        return fn_pals_get_nodeidx(state, nodeidx);
    }
}

pals_rc_t pals_get_num_nodes(pals_state_t *state, int *nnodes)
{
    if (check_calling_context()) {
        *nnodes = pmod_mparams.sp->nnodes;
        return PALS_OK;
    } else {
        return fn_pals_get_num_nodes(state, nnodes);
    }
}

pals_rc_t pals_get_nodes(pals_state_t *state, pals_node_t **nodes, int *nnodes)
{
    if (check_calling_context()) {
        *nnodes = pmod_mparams.sp->nnodes;
        *nodes = (pals_node_t *) malloc(pmod_mparams.sp->nnodes * sizeof(pals_node_t));
        if (*nodes == NULL) {
            return PALS_FAILED;
        }

        int nid;

        for (nid = 0; nid < pmod_mparams.sp->nnodes; ++nid) {
            (*nodes)[nid].nid = nid;

            strncpy((*nodes)[nid].hostname,
            pmod_mparams.np.hostnames[nid].name,
            PMOD_MAX_HOSTNAME_LEN);
        }

        return PALS_OK;
    } else {
        return fn_pals_get_nodes(state, nodes, nnodes);
    }
}

pals_rc_t pals_get_num_cmds(pals_state_t *state, int *ncmds)
{
    if (check_calling_context()) {
        *ncmds = 1;
        return PALS_OK;
    } else {
        return fn_pals_get_num_cmds(state, ncmds);
    }
}

pals_rc_t pals_get_cmds(pals_state_t *state, pals_cmd_t **cmds, int *ncmds)
{
    if (check_calling_context()) {
        cmds[0]->npes = pmod_mparams.sp->ntasks;
        cmds[0]->pes_per_node = pmod_mparams.sp->ppn;
        cmds[0]->cpus_per_pe = 1;
        *ncmds = 1;
        return PALS_OK;
    } else {
        return fn_pals_get_cmds(state, cmds, ncmds);
    }
}

pals_rc_t pals_get_num_nics(pals_state_t *state, int *nnics)
{
    // no error checking, just pass rc through to caller
    return fn_pals_get_num_nics(state, nnics);
}

pals_rc_t pals_get_nics(pals_state_t *state, pals_nic_t **nics, int *nnics)
{
    // no error checking, just pass rc through to caller
    return fn_pals_get_nics(state, nics, nnics);
}

pals_rc_t pals_start_barrier(pals_state_t *state)
{
    if (check_calling_context()) {
        // no-op
        return PALS_OK;
    } else {
        return fn_pals_start_barrier(state);
    }
}

pals_rc_t pals_get_apid(pals_state_t *state, char **apid)
{
    if (check_calling_context()) {
        *apid = (char *) malloc(PMI_PG_ID_SIZE);
        if (*apid == NULL) {
            return PALS_FAILED;
        }

        strncpy(*apid, pmod_apid, PMI_PG_ID_SIZE);

        return PALS_OK;
    } else {
        return fn_pals_get_apid(state, apid);
    }
}

pals_rc_t pals_app_spawn(
    pals_state_t *state, int count, const char *const cmds[], int argcs[],
    const char **const argvs[], const int maxprocs[],
    const char *preput_envs[], const int num_envs, pals_rc_t errors[])
{
    if (check_calling_context()) {
        // TODO: add "not supported" print (along with support for Dragon logging)
        return PALS_FAILED;
    } else {
        return fn_pals_app_spawn(state, count, cmds, argcs, argvs,
                                 maxprocs, preput_envs, num_envs, errors);
    }
}

const char *pals_errmsg(pals_state_t *state)
{
    return fn_pals_errmsg(state);
}

