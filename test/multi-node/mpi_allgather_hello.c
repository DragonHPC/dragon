#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../_ctest_utils.h"


int main(int argc, char *argv[])
{
    int rank, world_size;
    char version[MPI_MAX_LIBRARY_VERSION_STRING];
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Get_processor_name(processor_name, &name_len);

    fprintf(stdout, "Hello, world, I am %d of %d on host %s\n", rank, world_size, processor_name);
    fflush(stdout);

    int send_data = 3 * rank;
    int *recv_buf = malloc(world_size * sizeof(int));
    assert(recv_buf != NULL);

    int mpi_err = MPI_Allgather(&send_data, 1, MPI_INT,
                                recv_buf, 1, MPI_INT,
                                MPI_COMM_WORLD);
    assert(mpi_err == MPI_SUCCESS);

    int i;
    int num_failed = 0;

    for (i = 0; i < world_size; ++i) {
        int expected = 3 * i;
        if (recv_buf[i] != expected) {
            ++num_failed;
        }
    }

    TEST_STATUS = (num_failed == 0) ? SUCCESS : FAILED;

    sleep(2);

    free(recv_buf);
    MPI_Finalize();

    return TEST_STATUS;
}
