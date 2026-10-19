CC=gcc

CFLAGS  = -g  -pedantic -Wall -I ${CRAY_MPICH_DIR}/include -L ${CRAY_MPICH_DIR}/lib
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich
H_SOURCES =

MPI_EXE=mpi_hello
MPI_SRC=mpi_hello.c
MPI_OBJECT = $(MPI_SRC:.c=.c.o)

default: $(MPI_EXE)

%.c.o: %.c $(H_SOURCES)
	$(CC) $(CFLAGS)  -c $< -o $@

$(MPI_EXE): $(MPI_OBJECT)
	$(CC) $(LD_FLAGS)  $^ -o $@

clean:
	$(RM) $(MPI_EXE) $(MPI_OBJECT)
