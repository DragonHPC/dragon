/* -*- c-basic-offset: 4; indent-tabs-mode: nil;-*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * pals.h - PALS library header file
 * Copyright 2018-2023 Hewlett Packard Enterprise Development LP
 */

#include <stddef.h>
#include <stdint.h>

/**
 * Environment variables available for applications
 */
#define PALS_APID_ENV "PALS_APID"
#define PALS_APINFO_ENV "PALS_APINFO"
#define PALS_RANKID_ENV "PALS_RANKID"
#define PALS_NODEID_ENV "PALS_NODEID"
#define PALS_DEPTH_ENV "PALS_DEPTH"
#define PALS_LOCAL_RANKID_ENV "PALS_LOCAL_RANKID"
#define PALS_LOCAL_SIZE_ENV "PALS_LOCAL_SIZE"
#define PALS_SPOOL_DIR_ENV "PALS_SPOOL_DIR"
#define PALS_STARTUP_BARRIER_ENV "PALS_STARTUP_BARRIER"
#define PALS_FD_ENV "PALS_FD"
#define PALS_TIMEOUT_ENV "PALS_TIMEOUT"
#define PMI_RANK_ENV "PMI_RANK"
#define PMI_SIZE_ENV "PMI_SIZE"
#define PMI_LOCAL_RANK_ENV "PMI_LOCAL_RANK"
#define PMI_LOCAL_SIZE_ENV "PMI_LOCAL_SIZE"
#define PMI_JOBID_ENV "PMI_JOBID"
#define PMI_SHARED_SECRET_ENV "PMI_SHARED_SECRET"
#define PMI_UNIVERSE_SIZE_ENV "PMI_UNIVERSE_SIZE"

/**
 * Application file format version
 */
#define PALS_APINFO_VERSION 5

/**
 * Error codes returned by this library
 */
typedef enum {
    PALS_OK = 0,
    PALS_NOT_SUPPORTED = 1,
    PALS_FAILED = 2,
} pals_rc_t;

/**
 * Library state structure, should not be modified by caller
 */
typedef struct {
    void *apinfo;     /**< Pointer to application information file */
    size_t apinfolen; /**< Size of application information file */
    int launchfd;     /**< Launcher connection file descriptor */
    int16_t version;  /**< Version of this application information file */
    uint16_t flags;   /**< Enablement flags for advanced features */
    char errbuf[128]; /**< Contains readable error message on error */
} pals_state_t;

/**
 * Whether we're initialized, aborting, or finalized
 */
typedef enum {
    PALS_PE_INVALID      = 0,
    PALS_PE_INITIALIZED  = 1,
    PALS_PE_FINALIZED    = 2,
    PALS_PE_ABORTED      = 3
} pals_pe_status_t;

/**
 * File header structure
 */
typedef struct {
    int version;                     /**< Must be first */
    size_t total_size;
    size_t comm_profile_size;
    size_t comm_profile_offset;
    int ncomm_profiles;
    size_t cmd_size;
    size_t cmd_offset;
    int ncmds;
    size_t pe_size;
    size_t pe_offset;
    int npes;
    size_t node_size;
    size_t node_offset;
    int nnodes;
    size_t nic_size;
    size_t nic_offset;
    int nnics;
    size_t status_offset;
    size_t dist_size;
    size_t dist_offset;
} pals_header_t;

/**
 * Slingshot Traffic Class quality of service groups
 */
enum {
    PALS_TC_DEDICATED_ACCESS = 0x1,
    PALS_TC_LOW_LATENCY      = 0x2,
    PALS_TC_BULK_DATA        = 0x4,
    PALS_TC_BEST_EFFORT      = 0x8
};

/**
 * Network communication profile structure
 */
typedef struct {
    uint32_t svc_id;          /**< CXI service ID */
    uint32_t traffic_classes; /**< Bitmap of allowed traffic classes */
    uint16_t vnis[4];         /**< VNIs for this service */
    uint8_t nvnis;            /**< Number of VNIs */
    char device_name[16];     /**< NIC device for this profile */
} pals_comm_profile_t;

/**
 * MPMD command information structure
 */
typedef struct {
    int npes;         /**< Number of PEs in this command */
    int pes_per_node; /**< Number of PEs per node */
    int cpus_per_pe;  /**< Number of CPUs per PE */
} pals_cmd_t;

/**
 * PE information structure
 */
typedef struct {
    int localidx; /**< Node-local PE index */
    int cmdidx;   /**< Command index for this PE */
    int nodeidx;  /**< Node index this PE is running on */
} pals_pe_t;

/**
 * Node information structure
 */
typedef struct {
    int nid;           /**< System-unique Node ID. -1 when can't be determined  */
    char hostname[64]; /**< Node hostname */
} pals_node_t;

/**
 * NIC address type
 */
typedef enum {
    PALS_ADDR_IPV4,
    PALS_ADDR_IPV6,
    PALS_ADDR_MAC
} pals_address_type_t;

/**
 * Network connection information structure
 */
typedef struct {
    int nodeidx;                      /**< Node index this NIC belongs to */
    pals_address_type_t address_type; /**< Address type for this NIC */
    char address[40];                 /**< Address of this NIC */
} pals_nic_t;

/**
 * HSN NIC information structure
 */
typedef struct {
    int nodeidx;                      /**< Node index this NIC belongs to */
    pals_address_type_t address_type; /**< Address type for this NIC */
    char address[64];                 /**< Address of this NIC */
    short numa_node;                  /**< NUMA node it is in */
    char device_name[16];             /**< Device name */
    long _unused[2];
} pals_hsn_nic_t;

/**
 * Device types for querying NUMA distances
 */
typedef enum {
    PALS_DEV_CPU,                     /**< CPU the PE is pinned to */
    PALS_DEV_ACCELERATOR              /**< GPU or other offload engine */
} pals_device_type_t;

/**
 * Distances to each NIC. In the apinfo file, each of these will be the same
 * size, even if there are nodes with different counts of NICs or if some but
 * not all nodes have accelerators. The NIC distances are first and then the
 * accelerator distances, if they're provided.
 */
typedef struct {
    uint8_t num_nic_distances;        /**< Number of CPU->NIC distances */
    uint8_t accelerator_distances;    /**< Accel distances too? (bool) */
    uint8_t distances[0]; /**< One for each NIC, two if using accelerators */
} pals_distance_t;

/**
 * Initialize this library
 *
 * @param[out] state Pointer to library state structure
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_init(pals_state_t *state);

/**
 * Initialize this library. This should be used instead of pals_init(), as
 * it allows access to newer features.
 *
 * @param[out] state Pointer to library state structure pointer
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_init2(pals_state_t **state);

/**
 * Clean up any resources used by this library
 *
 * @param[in] state Pointer to library state structure
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_fini(pals_state_t *state);

/**
 * Return the error message from the last failing libpals library call
 *
 * @param[in] state Pointer to initialized state structure
 * @return Error message string. Do not free.
 */
const char *pals_errmsg(pals_state_t *state);

/**
 * Get our application ID
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] apid Set to application ID on success, caller must free
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_apid(pals_state_t *state, char **apid);

/**
 * Get the local node index (e.g. 0 on the head node). This index can be used to
 * look up local node information in the nodes table.
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nididx Set to local node index on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_nodeidx(pals_state_t *state, int *nodeidx);

/**
 * Get this PE's index
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] peidx Set to this PE's index on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_peidx(pals_state_t *state, int *peidx);

/**
 * Get the number of nodes in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nnodes Set to number of nodes on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_nodes(pals_state_t *state, int *nnodes);

/**
 * Get the list of nodes in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nodes Pointer to list of nodes
 * @param[out] nnodes Set to list length on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_nodes(pals_state_t *state, pals_node_t **nodes, int *nnodes);

/**
 * Get the number of MPMD commands in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] ncmds Set to number of MPMD commands on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_cmds(pals_state_t *state, int *ncmds);

/**
 * Get a list of MPMD commands in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] cmds Pointer to list of command structures
 * @param[out] ncmds Set to number of MPMD commands on success
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_cmds(pals_state_t *state, pals_cmd_t **cmds, int *ncmds);

/**
 * Get the number of PEs in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] npes Number of PEs in the application
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_pes(pals_state_t *state, int *npes);

/**
 * Get a list of PEs in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] places Pointer to list of PE structures
 * @param[out] npes Number of PEs in the application
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_pes(pals_state_t *state, pals_pe_t **pes, int *npes);

/**
 * Get the number of communication profiles usable by the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] ncomm_profiles Number of application communication profiles
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_comm_profiles(pals_state_t *state, int *ncomm_profiles);

/**
 * Get a list of communications profiles
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] profiles Pointer to list of communication profiles
 * @param[out] nprofiles Length of the profile list
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_comm_profiles(
        pals_state_t *state, pals_comm_profile_t **profiles, int *nprofiles);

/**
 * Get the number of host IP addresses in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nnics Number of NICs in the application
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_nics(pals_state_t *state, int *nnics);

/**
 * Get a list of host IP addresses this application can use
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nics Pointer to list of NICs
 * @param[out] nnics Length of the NIC list
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_nics(pals_state_t *state, pals_nic_t **nics, int *nnics);

/**
 * Get the number of HSN NICs in the application
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nnics Number of NICs in the application
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_num_hsn_nics(pals_state_t *state, int *nnics);

/**
 * Get a list of HSN NICs this application can use
 *
 * @param[in] state Pointer to initialized state structure
 * @param[out] nics Pointer to list of NICs
 * @param[out] nnics Length of the NIC list
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_hsn_nics(
    pals_state_t *state, pals_hsn_nic_t **nics, int *nnics);

/**
 * Send a signal to a particular PE, or -1 for all PEs
 *
 * @param[in] state Pointer to initialized state structure
 * @param[in] pe PE index to signal, or -1 for all PEs
 * @param[in] signum Signal number to send
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_signal(pals_state_t *state, int pe, int signum);

/**
 * Block until all ranks are ready to start
 *
 * @param[in] state Pointer to initialized state structure
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_start_barrier(pals_state_t *state);

/**
 * Notify the launcher of a normal exit
 *
 * @param[in] state Pointer to initialized state structure
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_exit_notify(pals_state_t *state);

/**
 * For the passed process, return an array of NUMA distances from the
 * process's primary device of the specified type to each of its node's HSN
 * NICs.
 *
 * @param[in] state Pointer to initialized state structure
 * @param[in] pe Process index to query
 * @param[in] type Measure from the PE's primary one of these
 * @param[out] distances Pointer to a list of distances
 * @param[out] ndists Length of the distance list
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_get_nic_distances(
    pals_state_t *state, int pe, pals_device_type_t type, uint16_t **distances,
    unsigned int *ndists);

/**
 * Have the launcher spawn some number of commands with the specified process
 * counts and arguments within this process's job environment.
 *
 * @param[in] state Pointer to initialized state structure
 * @param[in] count Number of commands to spawn
 * @param[in] cmds Array of commands to execute
 * @param[in] argcs For each command, the number of arguments in argvs[]
 * @param[in] argvs For each command, the arguments to use
 * @param[in] maxprocs The number of processes to start for each command
 * @param[in] preput_envs "key=val" environment variables add to the commands
 * @param[in] num_envs The number of variables in preput_envs[]
 * @param[out] errors Per command status codes for their launches
 * @return PALS_OK, PALS_NOT_SUPPORTED, or PALS_FAILED
 */
pals_rc_t pals_app_spawn(
    pals_state_t *state, int count, const char *const cmds[], int argcs[],
    const char **const argvs[], const int maxprocs[],
    const char *preput_envs[], const int num_envs, pals_rc_t errors[]);
