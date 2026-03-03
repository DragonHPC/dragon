CC=gcc

CFLAGS  = -g  -pedantic -Wall -I ${CRAY_MPICH_DIR}/include -L ${CRAY_MPICH_DIR}/lib
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich
H_SOURCES =

MPI_EXE=mpi_hello
MPI_SRC=mpi_hello.c
MPI_OBJECT = $(MPI_SRC:.c=.c.o)

OSU_ALLTOALL_EXE=osu_alltoall

.PHONY: all

all: $(MPI_EXE) $(OSU_ALLTOALL_EXE)
default: all

$(OSU_ALLTOALL_EXE):
	$(MAKE) -C alltoall

%.c.o: %.c $(H_SOURCES)
	$(CC) $(CFLAGS)  -c $< -o $@

$(MPI_EXE): $(MPI_OBJECT)
	$(CC) $(LD_FLAGS)  $^ -o $@

clean:
	$(RM) $(MPI_EXE) $(MPI_OBJECT)
	$(MAKE) -C alltoall clean