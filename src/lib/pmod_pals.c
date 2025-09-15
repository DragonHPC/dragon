#include "_pmod.h"

// PEDANTIC NOTE: ideally, these should be obtained from the same
// pmi.h header that the user's application was built with (but in
// practice it probably doesn't matter)
#define PMI_SUCCESS  0
#define PMI_FAIL    -1

/*
 * global variables
 */

bool dragon_debug = false;
dragonRecvJobParams_t pmod_mparams;
char pmod_apid[PMI_PG_ID_SIZE];

/*
 * file-scope variables
 */

static FILE *pmod_log = NULL;

/*
 * main functions
 */

/** @brief Initializes PMOD inside PMI.
 *
 * Receives job parameters from the parent process and initializes \p _pmi_pals_info.
 * Called from the child MPI process during PMI[2]_Init. This function is already
 * called in Cray PMI and should not be required inside Dragon.
 *
 * @param _pmi_pals_info A pointer to a struct containing a number of values used by
 * PMI during initialization. This struct is defined in pmi_internal.h for Cray PMI.
 *
 * @return The PMI return code. Should be PMI_SUCCESS upon success. PMI_FAIL is used
 * to indicate any failure.
 */
dragonError_t
_pmod_pals_init(pmi_pg_info_t *_pmi_pals_info)
{
    char *tmp = getenv("DRAGON_DEBUG");
    if (tmp != NULL) {
        dragon_debug = (bool) atoi(tmp);
    }

    if (dragon_debug) {
        if (pmod_log == NULL) {
            char filename[64];
            sprintf(filename, "pmod_pals.%d.log", getpid());
            pmod_log = fopen(filename, "w");
        }

        fprintf(pmod_log, "Getting info from the shepherd pmod\n");
        fflush(pmod_log);
    }

    dragonError_t err = dragon_pmod_recv_mpi_params(&pmod_mparams);
    if (dragon_debug && err != DRAGON_SUCCESS) {
        fprintf(pmod_log,
                "dragon_pmod_recv_mpi_params failed with err = %d, last error string = %s\n",
                err, dragon_getlasterrstr());
        fflush(pmod_log);
    }

    int lrank;
    int napps = 1;

    _pmi_pals_info->pes_in_app = calloc(napps, sizeof(int));
    assert(_pmi_pals_info->pes_in_app);

    _pmi_pals_info->pes_in_app_this_smp_list = calloc(pmod_mparams.sp->ppn, sizeof(int));
    assert(_pmi_pals_info->pes_in_app_this_smp_list);

    _pmi_pals_info->base_pe_in_app = calloc(napps, sizeof(int));
    assert(_pmi_pals_info->base_pe_in_app);

    _pmi_pals_info->size                = pmod_mparams.sp->ntasks;
    _pmi_pals_info->my_rank             = pmod_mparams.sp->rank;
    _pmi_pals_info->my_nid              = pmod_mparams.sp->nid;
    _pmi_pals_info->nnodes              = pmod_mparams.sp->nnodes;
    _pmi_pals_info->napps               = 1;
    _pmi_pals_info->appnum              = 0;
    _pmi_pals_info->my_lrank            = pmod_mparams.sp->lrank;
    // assuming a single app for these values
    _pmi_pals_info->base_pe_on_node     = pmod_mparams.np.lrank_to_pe[0];
    _pmi_pals_info->my_app_lrank        = pmod_mparams.sp->lrank;
    _pmi_pals_info->pes_in_app_this_smp = pmod_mparams.sp->ppn;
    _pmi_pals_info->pes_this_node       = pmod_mparams.sp->ppn;
    _pmi_pals_info->base_pe_in_app[0]   = 0;
    _pmi_pals_info->pes_in_app[0]       = pmod_mparams.sp->ntasks;
    // assuming no mpmd jobs for now
    _pmi_pals_info->apps_share_node     = 0;

    sprintf(pmod_apid, "__dragon_pmod_app_%lu__", pmod_mparams.sp->job_id);
    strncpy(_pmi_pals_info->pg_id, pmod_apid, PMI_PG_ID_SIZE);

    for (lrank = 0; lrank < pmod_mparams.sp->ppn; lrank++) {
	    _pmi_pals_info->pes_in_app_this_smp_list[lrank] = pmod_mparams.np.lrank_to_pe[lrank];
    }

    if (dragon_debug) {
        int app;

        fprintf(pmod_log, "PMOD summary of job params: \n");
        fprintf(pmod_log, "    pg_id               = %s\n", _pmi_pals_info->pg_id);
        fprintf(pmod_log, "    size                = %d\n", _pmi_pals_info->size);
        fprintf(pmod_log, "    rank                = %d\n", _pmi_pals_info->my_rank);
        fprintf(pmod_log, "    nid                 = %d\n", _pmi_pals_info->my_nid);
        fprintf(pmod_log, "    nnodes              = %d\n", _pmi_pals_info->nnodes);
        fprintf(pmod_log, "    naps                = %d\n", _pmi_pals_info->napps);
        fprintf(pmod_log, "    appnum              = %d\n", _pmi_pals_info->appnum);
        fprintf(pmod_log, "    pes_this_node       = %d\n", _pmi_pals_info->pes_this_node);
        fprintf(pmod_log, "    pg_pes_per_smp      = %d\n", _pmi_pals_info->pg_pes_per_smp);
        fprintf(pmod_log, "    base_pe_on_node     = %d\n", _pmi_pals_info->base_pe_on_node);
        fprintf(pmod_log, "    my_lrank            = %d\n", _pmi_pals_info->my_lrank);
        for (app = 0; app < napps; app++) {
            fprintf(pmod_log, "    base_pe_in_app[%d]  = %d\n", app, _pmi_pals_info->base_pe_in_app[app]);
        }
        for (app = 0; app < napps; app++) {
            fprintf(pmod_log, "    pes_in_app[%d]      = %d\n", app, _pmi_pals_info->pes_in_app[app]);
        }
        fprintf(pmod_log, "    pes_in_app_this_smp = %d\n", _pmi_pals_info->pes_in_app_this_smp);
        for (lrank = 0; lrank < pmod_mparams.sp->ppn; lrank++) {
            fprintf(pmod_log, "    pes_in_app_this_smp_list[%d] = %d\n", lrank, _pmi_pals_info->pes_in_app_this_smp_list[lrank]);
        }
        fprintf(pmod_log, "    my_app_lrank       = %d\n", _pmi_pals_info->my_app_lrank);
        fprintf(pmod_log, "    apps_share_node    = %d\n", _pmi_pals_info->apps_share_node);
        fflush(pmod_log);
    }

    return PMI_SUCCESS;
}

/** @brief Get the PMI rank.
 *
 * Called from the child MPI process (currently unused).
 *
 * @param rank The PMI rank to be returned to the caller.
 *
 * @return The PMI return code. Should be PMI_SUCCESS upon success. PMI_FAIL is used
 * to indicate any failure.
 */
int
_pmod_pals_get_rank(int *rank)
{
    *rank = pmod_mparams.sp->rank;
    return PMI_SUCCESS;
}

/** @brief Set the values in lrank_to_pe for the current node.
 *
 * Sets the lrank_to_pe array that maps local ranks (on a node) to global
 * ranks in a job. Called from the child MPI process during PMI[2]_Init.
 * This function is already called in Cray PMI and should not be required
 * inside Dragon.
 *
 * @param lrank_to_pe_list An array mapping a local rank on the current node
 * to the global PMI rank in the job.
 *
 * @param The size of \p lrank_to_pe_list.
 *
 * @return The PMI return code. Should be PMI_SUCCESS upon success. PMI_FAIL is used
 * to indicate any failure.
 */
int
_pmod_pals_set_lrank_to_pe(int *lrank_to_pe_list, int lrank_list_size)
{
    int lrank;

    for (lrank = 0; lrank < lrank_list_size; ++lrank) {
        if (dragon_debug) {
	        fprintf(pmod_log,"PMOD: Assigning pe %d to local rank %d\n", pmod_mparams.np.lrank_to_pe[lrank], lrank);
            fflush(pmod_log);
        }
        lrank_to_pe_list[lrank] = pmod_mparams.np.lrank_to_pe[lrank];
    }

    // TODO: revisit this
    // not freeing lrank_to_pe in case this function gets called
    // more than once (is that possible?)
    //dragon_pmod_dragon_free(&pmod_mparams.np.lrank_to_pe_mem_descr);

    return PMI_SUCCESS;
}

/** @brief Get the list of nids for the current job.
 *
 * Gets the list of node ids (nids) in this job provided by local services.
 * Called from the child MPI process during PMI[2]_Init. This function is
 * already called in Cray PMI and should not be required inside Dragon.
 *
 * @param node_list A pointer to the array of nids.
 *
 * @return The PMI return code. Should be PMI_SUCCESS upon success. PMI_FAIL is used
 * to indicate any failure.
 */
int
_pmod_pals_get_nidlist(int **node_list)
{
    int pmi_rc = PMI_SUCCESS;
    int rank;

    *node_list = malloc(pmod_mparams.sp->ntasks * sizeof(int));
    if (*node_list == NULL) {
        fprintf(stderr, "no_mem\n");
        pmi_rc = PMI_FAIL;
        goto fn_exit;
    }

    for (rank = 0; rank < pmod_mparams.sp->ntasks; ++rank) {
        (*node_list)[rank] = pmod_mparams.np.nodelist[rank];
        if (dragon_debug) {
            fprintf(pmod_log, "PMOD: nidlist[%d] = %d\n", rank, (*node_list)[rank]);
            fflush(pmod_log);
        }
    }

    // TODO: revisit this
    // not freeing nodelist in case this function gets called
    // more than once (is that possible?)
    //dragon_pmod_dragon_free(&pmod_mparams.np.nodelist_mem_descr);

  fn_exit:

    return pmi_rc;
}
