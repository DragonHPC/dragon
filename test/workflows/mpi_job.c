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

    FILE *file_in = fopen(file_in_str, "r");
    FILE *file_out = fopen(file_out_str, "w");

    float val_in = 0.0;
    fscanf(file_in, "%f", &val_in);

    float val_out = val_arg + val_in;
    fprintf(file_out, "%f", val_out);

    MPI_Finalize();

    return 0;
}