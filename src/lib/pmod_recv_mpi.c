#include "_pmod.h"
#include "err.h"

static dragonError_t
recv_sp(dragonRecvJobParams_t *mparams, dragonChannelRecvh_t *parent_ch_rh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t sp_msg;

    err = dragon_channel_message_init(&sp_msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chrecv_get_msg_blocking(parent_ch_rh, &sp_msg, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get channel message from parent");
    }

    err = dragon_channel_message_get_mem(&sp_msg, &mparams->sp_mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get memory descriptor for received scalar job params");
    }

    err = dragon_memory_get_pointer(&mparams->sp_mem_descr, (void **) &mparams->sp);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get pointer to managed memory for received scalar job params");
    }

    err = dragon_channel_message_destroy(&sp_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
recv_lrank_to_pe(dragonRecvJobParams_t *mparams, dragonChannelRecvh_t *parent_ch_rh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t lrank_to_pe_msg;

    err = dragon_channel_message_init(&lrank_to_pe_msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chrecv_get_msg_blocking(parent_ch_rh, &lrank_to_pe_msg, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get channel message from parent");
    }

    err = dragon_channel_message_get_mem(&lrank_to_pe_msg, &mparams->np.lrank_to_pe_mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get memory descriptor for received lrank_to_pe array");
    }

    err = dragon_memory_get_pointer(&mparams->np.lrank_to_pe_mem_descr, (void **) &mparams->np.lrank_to_pe);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get pointer to managed memroy for received lrank_to_pe array");
    }

    err = dragon_channel_message_destroy(&lrank_to_pe_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
recv_nodelist(dragonRecvJobParams_t *mparams, dragonChannelRecvh_t *parent_ch_rh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t nidlist_msg;

    err = dragon_channel_message_init(&nidlist_msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chrecv_get_msg_blocking(parent_ch_rh, &nidlist_msg, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get channel message from parent");
    }

    err = dragon_channel_message_get_mem(&nidlist_msg, &mparams->np.nodelist_mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get memory descriptor for received nidlist array");
    }

    err = dragon_memory_get_pointer(&mparams->np.nodelist_mem_descr, (void **) &mparams->np.nodelist);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get pointer to managed memory for received nidlist array");
    }

    err = dragon_channel_message_destroy(&nidlist_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
recv_hostnames(dragonRecvJobParams_t *mparams, dragonChannelRecvh_t *parent_ch_rh)
{
    const timespec_t timeout = { get_comm_timeout(), 0 };

    dragonError_t err = DRAGON_SUCCESS;
    dragonMessage_t hostnames_msg;

    err = dragon_channel_message_init(&hostnames_msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to initialize channel message");
    }

    err = dragon_chrecv_get_msg_blocking(parent_ch_rh, &hostnames_msg, &timeout);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get channel message from parent");
    }

    err = dragon_channel_message_get_mem(&hostnames_msg, &mparams->np.hostnames_mem_descr);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get memory descriptor for received hostnames string");
    }

    err = dragon_memory_get_pointer(&mparams->np.hostnames_mem_descr, (void **) &mparams->np.hostnames);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to get pointer to managed memory for received hostnames string");
    }

    err = dragon_channel_message_destroy(&hostnames_msg, false);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy channel message");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
attach_to_parent_channel(dragonChannelDescr_t *parent_ch,
                         dragonChannelRecvh_t *parent_ch_rh)
{
    char *tmp = getenv("DRAGON_PMOD_CHILD_CHANNEL");
    if (tmp == NULL) {
        err_return(DRAGON_FAILURE, "unable to find parent's serialized channel in environment");
    }

    dragonChannelSerial_t parent_ch_ser;

    parent_ch_ser.data = dragon_base64_decode(tmp, &parent_ch_ser.len);

    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_channel_attach(&parent_ch_ser, parent_ch);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to attach to channel");
    }

    err = dragon_channel_recvh(parent_ch, parent_ch_rh, NULL);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to create receive handle");
    }

    err = dragon_chrecv_open(parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to open receive handle");
    }

    return DRAGON_SUCCESS;
}

static dragonError_t
close_parent_channel(dragonChannelDescr_t *parent_ch,
                     dragonChannelRecvh_t *parent_ch_rh)
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_chrecv_close(parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to close receive handle");
    }

    err = dragon_channel_destroy(parent_ch);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to destroy parent channel");
    }

    return DRAGON_SUCCESS;
}

/** @brief Receive job parameters from parent into Dragon managed memory.
 *
 * Called by the child MPI process to receive job parameters and populate pmi_pg_info,
 * lrank_to_pe and nidlist. This function is already called in Cray PMI and should not
 * be required inside Dragon (beyond testing).
 *
 * @param mparams Contains structs sp and np, which point to managed memory or contain
 * pointers to such. sp specifies the scalar parameters, such as rank and nid, being
 * passed to the MPI application. np contains the arrays lrank_to_pe and nidlist.
 *
 * @return Dragon return code. DRAGON_SUCCESS upon success.
 */
dragonError_t
dragon_pmod_recv_mpi_params(dragonRecvJobParams_t *mparams)
{
    mparams->is_sp_allocated = false;

    dragonError_t err = DRAGON_SUCCESS;
    dragonChannelDescr_t parent_ch;
    dragonChannelRecvh_t parent_ch_rh;

    err = attach_to_parent_channel(&parent_ch, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to attach to channel while receiving MPI job params from parent");
    }

    err = dragon_pmod_allocate_scalar_params(mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to allocate memory for scalar params while receiving MPI job params from parent");
    }

    err = recv_sp(mparams, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to receive scalar job params from parent");
    }

    // need to receive the scalar params before we can allocate
    // the array params
    err = dragon_pmod_allocate_array_params(mparams);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to allocate memory for array params while receiving MPI job params from parent");
    }

    err = recv_lrank_to_pe(mparams, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to receive lrank_to_pe array from parent");
    }

    err = recv_nodelist(mparams, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to receive nidlist array from parent");
    }

    err = recv_hostnames(mparams, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to receive hostnames array from parent");
    }

    err = close_parent_channel(&parent_ch, &parent_ch_rh);
    if (err != DRAGON_SUCCESS) {
        append_err_return(err, "failed to close channel while receiving MPI job params from parent");
    }

    return DRAGON_SUCCESS;
}
