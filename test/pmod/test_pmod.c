#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "dragon/channels.h"
#include "_pmod.h"
#include "../_ctest_utils.h"

#define INF_MUID   0
#define FIRST_CUID (1ul << 63)

#define TEST_PMOD_PPN    5
#define TEST_PMOD_NNODES 1

#define pmod_assert(condition) if (!(condition)) TEST_STATUS = FAILED;

static int test_pmod_nnodes      = TEST_PMOD_NNODES;
static int test_pmod_ppn         = TEST_PMOD_PPN;
static int test_pmod_ntasks      = TEST_PMOD_PPN;
static int test_pmod_nid         = 0;
static uint64_t test_pmod_job_id = 1729;

static int test_pmod_lrank_to_pe[TEST_PMOD_PPN] = { 0, 1, 2, 3, 4 };
static int test_pmod_nidlist[TEST_PMOD_PPN] = { 0, 0, 0, 0, 0 };

static dragonMemoryPoolDescr_t mem_pool;

void do_kid_stuff(int child_count)
{
    dragonRecvJobParams_t mparams;

    dragon_pmod_recv_mpi_params(&mparams);

    // verify scalar params

    pmod_assert(mparams.sp->nnodes == test_pmod_nnodes);
    pmod_assert(mparams.sp->ppn    == test_pmod_ppn);
    pmod_assert(mparams.sp->ntasks == test_pmod_ntasks);
    pmod_assert(mparams.sp->lrank  == child_count);
    pmod_assert(mparams.sp->rank   == child_count);
    pmod_assert(mparams.sp->nid    == test_pmod_nid);
    pmod_assert(mparams.sp->job_id == test_pmod_job_id);

    // verify lrank_to_pe

    int lrank;

    for (lrank = 0; lrank < test_pmod_ppn; ++lrank) {
        pmod_assert(mparams.np.lrank_to_pe[lrank] == test_pmod_lrank_to_pe[lrank]);
    }

    // verify nidlist

    int rank;

    for (rank = 0; rank < test_pmod_ntasks; ++rank) {
        pmod_assert(mparams.np.nodelist[rank] == test_pmod_nidlist[rank]);
    }

    dragonHostname_t test_hostname;

    gethostname(test_hostname.name, sizeof(dragonHostname_t));
    pmod_assert(0 == strcmp(mparams.np.hostnames->name, test_hostname.name));
}

void create_fake_infra_pool()
{
    dragonError_t err = DRAGON_SUCCESS;
    size_t pool_size = (1ul << 30);

    dragonM_UID_t m_uid = INF_MUID;

    err = dragon_memory_pool_create(&mem_pool,
                                    pool_size,
                                    "pmod_test_fake_infra_mem_pool",
                                    m_uid,
                                    NULL);
    pmod_assert(err == DRAGON_SUCCESS);

    dragonMemoryPoolSerial_t mem_pool_ser;

    err = dragon_memory_pool_serialize(&mem_pool_ser, &mem_pool);
    pmod_assert(err == DRAGON_SUCCESS);


    char *mem_pool_str = dragon_base64_encode(mem_pool_ser.data, mem_pool_ser.len);
    pmod_assert(mem_pool_str != NULL);

    setenv("DRAGON_INF_PD", mem_pool_str, 1);

    free(mem_pool_str);
}

void destroy_fake_infra_pool()
{
    dragonError_t err = DRAGON_SUCCESS;

    err = dragon_memory_pool_destroy(&mem_pool);
    pmod_assert(err == DRAGON_SUCCESS);
}

void attach_to_mem_pool()
{
    dragonError_t err = dragon_memory_pool_attach_from_env(&mem_pool, "DRAGON_INF_PD");
    pmod_assert(err == DRAGON_SUCCESS);
}

void detach_from_mem_pool()
{
    dragonError_t err = dragon_memory_pool_detach(&mem_pool);
    pmod_assert(err == DRAGON_SUCCESS);
}

void set_job_params(int child_count, dragonSendJobParams_t *job_params)
{
    job_params->lrank  = child_count;
    job_params->ppn    = test_pmod_ppn;
    job_params->nid    = test_pmod_nid;
    job_params->nnodes = test_pmod_nnodes;
    job_params->nranks = test_pmod_ntasks;
    job_params->id     = test_pmod_job_id;

    int rank;

    for (rank = 0; rank < test_pmod_ntasks; ++rank) {
        job_params->nidlist[rank] = test_pmod_nidlist[rank];
    }

    int rc = gethostname(job_params->hostnames[0].name, sizeof(dragonHostname_t));
    pmod_assert(rc == 0);
}

void get_child_ch(int child_count, dragonChannelDescr_t *child_ch)
{
    dragonError_t err = DRAGON_SUCCESS;

    dragonC_UID_t c_uid = INF_MUID + child_count;

    err = dragon_channel_create(child_ch, c_uid, &mem_pool, NULL);
    pmod_assert(err == DRAGON_SUCCESS);

    dragonChannelSerial_t child_ch_ser;

    err = dragon_channel_serialize(child_ch, &child_ch_ser);
    pmod_assert(err == DRAGON_SUCCESS);

    char *child_ch_str = dragon_base64_encode(child_ch_ser.data, child_ch_ser.len);

    setenv("DRAGON_PMOD_CHILD_CHANNEL", child_ch_str, 1);

    free(child_ch_str);
}

void do_parent_stuff(int child_count, dragonChannelDescr_t *child_ch)
{
    // set job params

    dragonSendJobParams_t job_params;

    job_params.nidlist = (int *) malloc(test_pmod_ntasks * sizeof(int));
    pmod_assert(job_params.nidlist != NULL);

    job_params.hostnames = (dragonHostname_t *) malloc(sizeof(dragonHostname_t));
    pmod_assert(job_params.hostnames != NULL);

    set_job_params(child_count, &job_params);

    // send job params to child

    dragon_pmod_send_mpi_data(&job_params, child_ch);

    free(job_params.nidlist);
    free(job_params.hostnames);
}

int main()
{
    int child_count;
    dragonChannelDescr_t child_ch;

    create_fake_infra_pool();

    for (child_count = 0; child_count < test_pmod_ppn; ++child_count) {
        // get a channel for this child, and set DRAGON_PMOD_CHILD_CHANNEL
        // for the child's environment
        get_child_ch(child_count, &child_ch);

        pid_t child_pid = fork();

        if (child_pid == 0) {
            do_kid_stuff(child_count);
            return TEST_STATUS;
        } else {
            do_parent_stuff(child_count, &child_ch);
            int child_rc;
            waitpid(child_pid, &child_rc, 0);

            if (child_rc != 0) {
                TEST_STATUS = FAILED;
            }
        }
    }

    printf("pmod test %s\n", TEST_STATUS == SUCCESS ? "passed" : "failed");

    destroy_fake_infra_pool();

    return TEST_STATUS;
}
