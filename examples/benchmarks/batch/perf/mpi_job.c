#include <iostream>
#include <mpi.h>
#include <string>
#include <unistd.h>
#include <limits.h>
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 256
#endif

std::string get_hostname(void) {
    char hostname[HOST_NAME_MAX] = {0};
    gethostname(hostname, HOST_NAME_MAX);
    return std::string(hostname);
}

int main() {
    MPI_Init(NULL, NULL);
    int rank = 0;
    int size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Barrier(MPI_COMM_WORLD);
    //std::cout << "Hello from host `" << get_hostname() << "`"
    //          << " with rank `" << rank <<  "/" << size << "`"
    //          << std::endl;

    MPI_Finalize();
    return 0;
}

