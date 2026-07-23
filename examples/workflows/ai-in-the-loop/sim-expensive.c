#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <mpi.h>
#include <assert.h>

// generate uniformly distributed samples in [min_range, max_range) 
float *generate_samples(int num_samples, float min_range, float max_range) {
  float *samples = (float *)malloc(sizeof(float) * num_samples);
  for (int i = 0; i < num_samples; i++) {
    samples[i] = ((rand()/(float)RAND_MAX)*(max_range - min_range) + min_range);
  }
  return samples;
}

// Function we want to use to generate data
float func(float x) {
    return sin(x);
}

// Apply sin(x) to the elements of the input array.
float *f(float *input_array, int num_elements) {
  float *output_array = (float *)malloc(sizeof(float) * num_elements);
  for (int i = 0; i < num_elements; i++) {
    output_array[i] = func(input_array[i]);
  }
  return output_array;
}

int main(int argc, char** argv) {
  if (argc != 5) {
    fprintf(stderr, "Wrong number of input args\n");
    exit(1);
  }
  
  /* if we don't sleep this job sometimes finishes and prints faster than we can connect to the output */ 
  sleep(1);
  
  int num_samples_per_rank = atoi(argv[1]);
  float min_sample_range = atof(argv[2]);
  float max_sample_range = atof(argv[3]);
  int num_calls = atoi(argv[4]);
  
  srand(time(NULL));
  
  MPI_Init(NULL, NULL);


  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  
  // generate samples
  float *global_samples = NULL;
  if (world_rank == 0) {
    global_samples = generate_samples(num_samples_per_rank * world_size, min_sample_range, max_sample_range);
  }

  float *local_samples = (float *)malloc(sizeof(float) * num_samples_per_rank);

  // scatter out samples 
  MPI_Scatter(global_samples, num_samples_per_rank, MPI_FLOAT, local_samples,
              num_samples_per_rank, MPI_FLOAT, 0, MPI_COMM_WORLD);

  // compute sin(x) at each ranks set of the samples
  float *f_local = NULL;
  f_local = f(local_samples, num_samples_per_rank);

  // get samples from ranks 
  float *f_global = NULL;
  if (world_rank == 0) {
    f_global = (float *)malloc(sizeof(float) * num_samples_per_rank * world_size);
  }
  MPI_Gather(f_local, num_samples_per_rank, MPI_FLOAT, f_global, num_samples_per_rank, MPI_FLOAT, 0, MPI_COMM_WORLD);

  // if rank 0 print out the samples 
  if (world_rank == 0) {
    for (int i=0; i < num_samples_per_rank*world_size; i++) {
        printf("%.6f, %.6f \n",  global_samples[i], f_global[i]);fflush(stdout);
    }
  }

  if (world_rank == 0) {
    free(global_samples);
    free(f_global);
  }
  free(local_samples);
  free(f_local);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}
