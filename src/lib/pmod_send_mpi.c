#include "_pals.h"
#include "_pmod.h"
#include "err.h"

/*
 * file-scope variables
 */

static bool is_lrank_to_pe_initialized = false;
static int *lrank_to_pe = NULL;
static int lrank_count = 0;
static pals_state_t *pals_state = NULL;
static bool pals_initialized = false;

/** @brief Get the number of HSN nics available on this node.
 *
 * Called from HSTA's launch script (via cython).
 *
 * @param num_nics The number of nics returned to the caller.
 *
 * @return Dragon return code. DRAGON_SUCCESS upon success.
 */
dragonError_t
dragon_pmod_pals_get_num_nics(int *num_nics)
{
    pals_rc_t rc = PALS_OK;

    if (!pals_initialized) {
        rc = pals_init2(&pals_state);
        if (rc != PALS_OK) {
            err_return(DRAGON_FAILURE, "failed to initialize PALS");
        }
        pals_initialized = true;
    }

    rc = pals_get_num_nics(pals_state, num_nics);
    if (rc != PALS_OK) {
        char tmp[128];
        sprintf(tmp, "failed to get number of nics, PALS err is %s\n", pals_errmsg(pals_state));
        err_return(DRAGON_FAILURE, tmp);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_pmod_dragon_allocate(void **vaddr,
                            dragonMemoryDescr_t *mem_descr,
                            size_t size)
{
    dragonError_t err = DRAGON_SUCCESS;

    static bool have_attached_to_mem_pool = false;
    static dragonMemoryPoolDescr_t mem_pool;

    if (!have_attached_to_mem_pool) {
        err = dragon_memory_pool_attach_from_env(&mem_pool, "DRAGON_INF_PD");
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed attach to pool specified by DRAGON_INF_PD");
        }

        have_attached_to_mem_pool = true;
    }

    const timespec_t timeout = { 5, 0 };  // 5 sec timeout

    err = dragon_memory_alloc_blocking(mem_descr,
                                       &mem_pool,
                                       size,
                                       &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed allocate managed memory");
    }

    // get the virtual address for the memory segment

    err = dragon_memory_get_pointer(mem_descr, vaddr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed get pointer to managed memory");
    }

    return DRAGON_SUCCESS;
}

dragonError_t
dragon_pmod_dragon_free(dragonMemoryDescr_t *mem_descr)
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_memory_free(mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to free managed memory");
    }

    return DRAGON_SUCCESS;
}

dragonError_t
dragon_pmod_allocate_scalar_params(dragonRecvJobParams_t *mparams)
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_pmod_dragon_allocate((void **) &mparams->sp,
                                      &mparams->sp_mem_descr,
                                      sizeof(PMOD_scalar_params_t));
    if (err != DRAGON_SUCCESS || mparams->sp == NULL) {
        append_err_return(err, "failed to allocate scalar params for child MPI process");
    }

    mparams->is_sp_allocated = true;

    return DRAGON_SUCCESS;
}

dragonError_t
dragon_pmod_allocate_array_params(dragonRecvJobParams_t *mparams)
{
    if (!mparams->is_sp_allocated) {
        err_return(DRAGON_INVALID_ARGUMENT, "scalar params not yet allocated");
    }

    int ppn    = mparams->sp->ppn;
    int nnodes = mparams->sp->nnodes;
    int nranks = mparams->sp->ntasks;

    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_pmod_dragon_allocate((void **) &mparams->np.lrank_to_pe,
                                      &mparams->np.lrank_to_pe_mem_descr,
                                      ppn * sizeof(int));
    if (err != DRAGON_SUCCESS || mparams->np.lrank_to_pe == NULL) {
        append_err_return(err, "failed allocated local-to-global PMI rank translation array for child MPI process");
    }

    dragon_pmod_dragon_allocate((void **) &mparams->np.nodelist,
                                &mparams->np.nodelist_mem_descr,
                                nranks * sizeof(int));
    if (err != DRAGON_SUCCESS || mparams->np.nodelist == NULL) {
        append_err_return(err, "failed to allocate nidlist for child MPI process");
    }

    dragon_pmod_dragon_allocate((void **) &mparams->np.hostnames,
                                &mparams->np.hostnames_mem_descr,
                                nnodes * sizeof(dragonHostname_t));
    if (err != DRAGON_SUCCESS || mparams->np.hostnames == NULL) {
        append_err_return(err, "failed to allocate hostnames for child MPI process");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
init_lrank_to_pe(dragonSendJobParams_t *job_params)
{
    int lrank = 0;
    int rank;

    lrank_to_pe = malloc(job_params->ppn * sizeof(int));
    if (lrank_to_pe == NULL) {
        err_return(DRAGON_FAILURE, "failed to allocate heap memory");
    }

    for (rank = 0; rank < job_params->nranks; ++rank) {
        if (job_params->nid == job_params->nidlist[rank]) {
            lrank_to_pe[lrank] = rank;

            if (++lrank == job_params->ppn) {
                break;
            }
        }
    }

    is_lrank_to_pe_initialized = true;

    return DRAGON_SUCCESS;
}

static dragonError_t
set_sp(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    if (!is_lrank_to_pe_initialized) {
        err_return(DRAGON_FAILURE, "lrank_to_pe array not yet initialized");
    }

    mparams->sp->nid    = job_params->nid;
    mparams->sp->rank   = lrank_to_pe[job_params->lrank];
    mparams->sp->lrank  = job_params->lrank;
    mparams->sp->ppn    = job_params->ppn;
    mparams->sp->nnodes = job_params->nnodes;
    mparams->sp->ntasks = job_params->nranks;
    mparams->sp->job_id = job_params->id;

    return DRAGON_SUCCESS;
}

static dragonError_t
set_lrank_to_pe(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    int lrank;

    if (!is_lrank_to_pe_initialized) {
        err_return(DRAGON_FAILURE, "lrank_to_pe array not yet initialized");
    }

    for (lrank = 0; lrank < job_params->ppn; ++lrank) {
        mparams->np.lrank_to_pe[lrank] = lrank_to_pe[lrank];
    }

    if (++lrank_count == job_params->ppn) {
        free(lrank_to_pe);
        is_lrank_to_pe_initialized = false;
        lrank_count=0;
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
set_nidlist(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    int rank;

    for (rank = 0; rank < job_params->nranks; ++rank) {
        mparams->np.nodelist[rank] = job_params->nidlist[rank];
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
set_hostnames(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    int nid;

    for (nid = 0; nid < job_params->nnodes; ++nid) {
        strncpy(mparams->np.hostnames[nid].name, job_params->hostnames[nid].name, PMOD_MAX_HOSTNAME_LEN);
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
set_scalar_mparams(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    dragonError_t err = DRAGON_SUCCESS;

    if (!is_lrank_to_pe_initialized) {
        err = init_lrank_to_pe(job_params);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to initialize lrank_to_pe array");
        }
    }

    err = set_sp(job_params, mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set scalar params");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
set_array_mparams(dragonSendJobParams_t *job_params, dragonRecvJobParams_t *mparams)
{
    dragonError_t err = DRAGON_SUCCESS;

    if (!is_lrank_to_pe_initialized) {
        err = init_lrank_to_pe(job_params);
        if (err != DRAGON_SUCCESS) {
            append_err_return(err, "failed to initialize lrank_to_pe array");
        }
    }

    err = set_lrank_to_pe(job_params, mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set lrank_to_pe_array");
    }

    err = set_nidlist(job_params, mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set nidlist");
    }

    err = set_hostnames(job_params, mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set nidlist");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
alloc_child_sendh(dragonChannelDescr_t *child_ch, dragonChannelSendh_t *child_ch_sh)
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_channel_sendh(child_ch, child_ch_sh, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to create send handle");
    }

    err = dragon_chsend_open(child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to open send handle");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
free_child_sendh(dragonChannelSendh_t *child_ch_sh)
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_chsend_close(child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to close send handle");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
send_sp(dragonRecvJobParams_t *mparams, const dragonChannelSendh_t *child_ch_sh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t sp_msg;

    err = dragon_channel_message_init(&sp_msg, &mparams->sp_mem_descr, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chsend_send_msg(child_ch_sh,
                                 &sp_msg,
                                 (dragonMemoryDescr_t *) DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                 &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send channel message");
    }

    err = dragon_channel_message_destroy(&sp_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
send_lrank_to_pe(dragonRecvJobParams_t *mparams, const dragonChannelSendh_t *child_ch_sh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t lrank_to_pe_msg;

    err = dragon_channel_message_init(&lrank_to_pe_msg, &mparams->np.lrank_to_pe_mem_descr, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chsend_send_msg(child_ch_sh,
                                 &lrank_to_pe_msg,
                                 (dragonMemoryDescr_t *) DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                 &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send channel message");
    }

    err = dragon_channel_message_destroy(&lrank_to_pe_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
send_nodelist(dragonRecvJobParams_t *mparams, const dragonChannelSendh_t *child_ch_sh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t nidlist_msg;

    err = dragon_channel_message_init(&nidlist_msg, &mparams->np.nodelist_mem_descr, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chsend_send_msg(child_ch_sh,
                                 &nidlist_msg,
                                 (dragonMemoryDescr_t *) DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                 &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send channel message");
    }

    err = dragon_channel_message_destroy(&nidlist_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
send_hostnames(dragonRecvJobParams_t *mparams, const dragonChannelSendh_t *child_ch_sh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t hostnames_msg;

    err = dragon_channel_message_init(&hostnames_msg, &mparams->np.hostnames_mem_descr, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chsend_send_msg(child_ch_sh,
                                 &hostnames_msg,
                                 (dragonMemoryDescr_t *) DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,
                                 &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send channel message");
    }

    err = dragon_channel_message_destroy(&hostnames_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy message");
    }

    return DRAGON_SUCCESS;
}

/** @brief Send job parameters specified by job_params to a child MPI process.
 *
 * Called from local services (via cython).
 *
 * @param job_params The parameters for the current job obtained from local
 * services. These parameters are sent to the child MPI process to enable
 * PMI initialization.
 *
 * @param child_ch The channel to use to communicate with the child MPI process.
 * The serialized descriptor for this channel should be in the child's environment.
 *
 * @return Dragon return code. DRAGON_SUCCESS upon success.
 */
dragonError_t
dragon_pmod_send_mpi_data(dragonSendJobParams_t *job_params, dragonChannelDescr_t *child_ch)
{
    dragonError_t err = DRAGON_SUCCESS;
    dragonChannelSendh_t child_ch_sh;
    dragonRecvJobParams_t mparams = { .is_sp_allocated = false };

    err = dragon_pmod_allocate_scalar_params(&mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to allocate scalar params while sending MPI job params");
    }

    err = set_scalar_mparams(job_params, &mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set scalar params while sending MPI job params");
    }

    err = dragon_pmod_allocate_array_params(&mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to allocate array params while sending MPI job params");
    }

    err = set_array_mparams(job_params, &mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to set array params while sending MPI job params");
    }

    err = alloc_child_sendh(child_ch, &child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to allocate child send handle");
    }

    err = send_sp(&mparams, &child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send scalar MPI job params to child MPI process");
    }

    err = send_lrank_to_pe(&mparams, &child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send lrank_to_pe array to child MPI process");
    }

    err = send_nodelist(&mparams, &child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send nidlist to child MPI process");
    }

    err = send_hostnames(&mparams, &child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to send nidlist to child MPI process");
    }

    err = free_child_sendh(&child_ch_sh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to free child send handle");
    }

    return DRAGON_SUCCESS;
}
