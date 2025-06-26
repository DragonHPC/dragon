#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <mpi.h>
#include <assert.h>

int factorial(int n)
{
    if (n == 0)
        return 1;
    return n * factorial(n - 1);
}

float taylor_expansion_local(int rank, float x) {
    int coeff = (2*rank + 1);
    float te_f_local = powf(-1.0, rank)/ (float) factorial(coeff) * powf(x, coeff);
    return te_f_local; 
}

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Wrong number of input args\n");
    exit(1);
  }
  
  /* if we don't sleep this job sometimes finishes and prints faster than we can connect to the output */ 
  sleep(1);
  
  double x = atof(argv[1]);

  MPI_Init(NULL, NULL);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // compute taylor expansion of sin(x) 
  float partial_te = taylor_expansion_local(rank, (float) x); 
  float full_te;
  MPI_Reduce(&partial_te, &full_te, 1, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);

  // print out result of the taylor expansion of sin(x)
  if (rank == 0) {
      printf("%f, %f \n",  x, full_te);fflush(stdout);
    }
  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}