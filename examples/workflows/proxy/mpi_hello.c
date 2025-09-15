#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


int main()
{
    char filename[128];
    //sprintf(filename, "mpi_hello.%d.log", getpid());
    //FILE *log = fopen(filename, "w");
    FILE *log = stdout;

    fprintf(log, "Starting MPI process with pid %d\n", getpid());
    fflush(log);

    MPI_Init(NULL, NULL);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    fprintf(log, "Hello there, my name is MPI Process %d\n", rank);
    fflush(log);

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
        fprintf(log, "Received %d from rank %d; expected %d\n", recv_buf[i], i, expected);
        if (recv_buf[i] != expected) {
            ++num_failed;
        }
    }

    //unlink(filename);

    free(recv_buf);
    fclose(log);
    MPI_Finalize();

    sleep(2);

    return EXIT_SUCCESS;
}
