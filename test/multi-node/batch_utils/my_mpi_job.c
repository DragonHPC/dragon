#include <assert.h>
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv)
{
    assert(argc == 4);

    float val_arg = atof(argv[1]);
    char *file_in_str = argv[2];
    char *file_out_str = argv[3];

    char *abcdef = getenv("ABCDEF");
    assert(abcdef != NULL);

    MPI_Init(NULL, NULL);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Only rank 0 performs file I/O to avoid races between ranks. */
    if (rank == 0) {
        FILE *file_in = fopen(file_in_str, "r");
        FILE *file_out = fopen(file_out_str, "w");

        float val_in = 0.0;
        fscanf(file_in, "%f", &val_in);
        fclose(file_in);

        float val_out = val_arg + val_in;
        fprintf(file_out, "%f", val_out);
        fclose(file_out);
    }

    MPI_Finalize();

    return 0;
}
