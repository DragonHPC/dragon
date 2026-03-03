#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

char *hi = "Hi-diddly-ho, neighborino!!!";

int main()
{
    MPI_Init(NULL, NULL);
    fprintf(stdout, "%s", hi);
    MPI_Finalize();
    return 0;
}
