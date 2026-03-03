#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"

int main(int argc, char* argv[])
{
    int rank, size, len;
    char version[MPI_MAX_LIBRARY_VERSION_STRING];
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(processor_name, &name_len);
    MPI_Get_library_version(version, &len);
    printf("Hello, world, I am %d of %d on host %s, (%s, %d)\n",
           rank, size, processor_name, version, len);
    MPI_Finalize();

    return 0;
}