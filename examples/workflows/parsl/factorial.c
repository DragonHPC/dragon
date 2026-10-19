#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <mpi.h>
#include <assert.h>

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Wrong number of input args\n");
    exit(1);
  }
  
  sleep(1);
  
  double bias = atof(argv[1]);
  double scale_factor;


  MPI_Init(NULL, NULL);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (rank == 0) {
      scanf("%lf", &scale_factor);
      //printf(" got scale factor = %f \n",  scale_factor);fflush(stdout);
  }
  
  double local_rank = rank;
  if (local_rank == 0){
    local_rank = 1;
  } 
  double factorial_of_ranks;
  MPI_Reduce(&local_rank, &factorial_of_ranks, 1, MPI_DOUBLE, MPI_PROD, 0, MPI_COMM_WORLD);


  if (rank == 0) {
      double output = factorial_of_ranks*scale_factor + bias; 
      printf("%lf * %lf + %lf = %lf \n",  scale_factor, factorial_of_ranks, bias, output);fflush(stdout);
    }
  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}