#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(void)
{
    const char *abcdef = getenv("ABCDEF");
    if (abcdef == NULL) {
        fprintf(stderr, "missing ABCDEF\n");
        return 2;
    }

    MPI_Init(NULL, NULL);

    int rank = -1;
    int world_size = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    printf("RANK=%d WORLD=%d ABCDEF=%s\n", rank, world_size, abcdef);
    fflush(stdout);

    MPI_Finalize();
    return 0;
}
