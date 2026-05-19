/*
 * Copyright 2023 Hewlett Packard Enterprise Development LP
 */

#ifndef DRAGON_PMOD_H
#define DRAGON_PMOD_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include "dragon/channels.h"
#include "dragon/global_types.h"
#include "dragon/managed_memory.h"
#include "dragon/return_codes.h"
#include "dragon/utils.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * constants
 */

// this is based on the hostname length in PMI
#define PMOD_MAX_HOSTNAME_LEN 64

/*
 * typedefs
 */

/**
 *  Wraps a static array for a single hostname.
 **/
typedef struct dragonHostname {
    char name[PMOD_MAX_HOSTNAME_LEN];  /**< A node's hostname. */
} dragonHostname_t;

/**
 *  Contains job parameters passed to child MPI processes from local services.
 **/
typedef struct dragonSendJobParams {
    int lrank;                   /**< My local PMI rank on this node. */
    int ppn;                     /**< Number of PMI ranks on this node. */
    int nid;                     /**< My node id in this job. */
    int nnodes;                  /**< Number of nodes in this job. */
    int nranks;                  /**< Number of PMI ranks in this job. */
    int *nidlist;                /**< Array of nids for this job, indexed by rank. */
    dragonHostname_t *hostnames; /**< Array of all hostnames for this job, indexed by nid. */
    uint64_t id;                 /**< A unique id for this job created by global services. */
} dragonSendJobParams_t;

/**
 *  Contains scalar value job parameters received by the parent MPI process.
 **/
typedef struct {
    int nnodes;      /**< Number of nodes in this job. */
    int ppn;         /**< Number of PMI ranks on this node. */
    int ntasks;      /**< Number of PMI ranks in this job. */
    int lrank;       /**< My local PMI rank on this node. */
    int rank;        /**< My global PMI rank. */
    int nid;         /**< My node id in this job. */
    uint64_t job_id; /**< A unique id for this job created by global services. */
} PMOD_scalar_params_t;

/**
 *  Contains arrays of job parameters received by the parent MPI process.
 **/
typedef struct {
    int *lrank_to_pe;                           /**< Mapping from local to global PMI rank. */
    dragonMemoryDescr_t lrank_to_pe_mem_descr;  /**< Memory descriptor for lrank_to_pe. */
    int *nodelist;                              /**< Dragon managed array of nids for this job, indexed by rank. */
    dragonMemoryDescr_t nodelist_mem_descr;     /**< Memory descriptor for the nidlist. */
    dragonHostname_t *hostnames;                /**< Dragon managed array of hostnames for this job, indexed by nid. */
    dragonMemoryDescr_t hostnames_mem_descr;    /**< Memory descriptor for the hostnames array. */
} PMOD_array_params_t;

/**
 *  Contains job parameters received by the parent MPI process.
 **/
typedef struct {
    bool is_sp_allocated;             /**< Indicates if the scalar parameters struct has been allocated. */
    PMOD_scalar_params_t *sp;         /**< Dragon managed memory for the scalar parameters struct. */
    dragonMemoryDescr_t sp_mem_descr; /**< Memory descriptor for the scalar parameters struct. */
    PMOD_array_params_t np;           /**< Struct containing pointers to Dragon managed memory for array job parameters. */
} dragonRecvJobParams_t;

/*
 * >>> TODO: need to keep this in sync with PMI's internal version of <<<
 * >>> pmi_pg_info_t in the pmi_internal.h header                     <<<
 */

#define PMI_PG_ID_SIZE 37

/**
 *  Contains values used by Cray PMI during initialization and at runtime.
 **/
typedef struct {
    char pg_id[PMI_PG_ID_SIZE];    /**< String uniqely identifying this job. */
    int size;                      /**< Number of ranks in this job. */
    int my_rank;                   /**< My rank in this job. */
    int my_nid;                    /**< My node id in this job. */
    int nnodes;                    /**< Number of nodes in this job. */
    int napps;                     /**< Number of applications in this job (1 unless MPMD). */
    int appnum;                    /**< My application index in this job (0 unless MPMD). */
    int pes_this_node;             /**< Number of ranks on this node. */
    int pg_pes_per_smp;            /**< Appears to only be set for singleton launch, and never used. */
    int base_pe_on_node;           /**< The rank with the lowest value on this node. */
    int my_lrank;                  /**< My local rank on this node. */
    int *base_pe_in_app;           /**< List of base ranks for each application (). */
    int *pes_in_app;               /**< Number of ranks in this application (size unless MPMD). */
    int pes_in_app_this_smp;       /**< Number of ranks from this application on the current node (pes_this_node unless MPMD). */
    int *pes_in_app_this_smp_list; /**< List of ranks from this application on the current node (all ranks on this node unless MPMD) */
    int my_app_lrank;              /**< My local rank on this node for this application (my_lrank unless MPMD). */
    int apps_share_node;           /**< Indicates if applications in an MPMD job can have ranks on the same node. */
} pmi_pg_info_t;

dragonError_t
dragon_pmod_pals_get_num_nics(int *nnics);

dragonError_t
dragon_pmod_send_mpi_data(dragonSendJobParams_t *job_params, dragonChannelDescr_t *child_ch);

dragonError_t
dragon_pmod_recv_mpi_params(dragonRecvJobParams_t *mparams);

dragonError_t
_pmod_pals_init(pmi_pg_info_t *_pmi_pals_info);

int
_pmod_pals_get_rank(int *rank);

int
_pmod_pals_set_lrank_to_pe(int *lrank_to_pe_list, int lrank_list_size);

int
_pmod_pals_get_nidlist(int **node_list);

#ifdef __cplusplus
}
#endif

#endif // DRAGON_PMOD_H