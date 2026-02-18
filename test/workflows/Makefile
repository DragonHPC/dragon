CC=gcc
DRAGON=dragon

CFLAGS  = -g  -pedantic -Wall -I ${CRAY_MPICH_DIR}/include -L ${CRAY_MPICH_DIR}/lib
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich
H_SOURCES =

BATCH_UNIT_TESTS=batch_unittests
BATCH_UNIT_TESTS_SRC=batch_unittests.py

BATCH_MULTINODE_UNIT_TESTS=batch_multinode_unittests
BATCH_MULTINODE_UNIT_TESTS_SRC=batch_multinode_unittests.py

MPI_JOB_EXE=mpi_job
MPI_JOB_SRC=mpi_job.c
MPI_JOB_OBJECT = $(MPI_JOB_SRC:.c=.c.o)

MPI_JOB_STDOUT_EXE=mpi_job_stdout
MPI_JOB_STDOUT_SRC=mpi_job_stdout.c
MPI_JOB_STDOUT_OBJECT = $(MPI_JOB_STDOUT_SRC:.c=.c.o)

MY_DUMB_PROC_EXE=my_dumb_proc
MY_DUMB_PROC_SRC=my_dumb_proc.c
MY_DUMB_PROC_OBJECT = $(MY_DUMB_PROC_SRC:.c=.c.o)

.PHONY: all
all: $(BATCH_UNIT_TESTS)
default: all

$(BATCH_UNIT_TESTS):
	DRAGON_DEFAULT_SEG_SZ=21474836480 \
	$(DRAGON) $(BATCH_UNIT_TESTS_SRC)

$(BATCH_MULTINODE_UNIT_TESTS): $(MPI_JOB_EXE) $(MPI_JOB_STDOUT_EXE) $(MY_DUMB_PROC_EXE)
	DRAGON_DEFAULT_SEG_SZ=21474836480 \
	$(DRAGON) $(BATCH_MULTINODE_UNIT_TESTS_SRC)

%.c.o: %.c $(H_SOURCES)
	$(CC) $(CFLAGS)  -c $< -o $@

$(MPI_JOB_EXE): $(MPI_JOB_OBJECT)
	$(CC) $(LD_FLAGS)  $^ -o $@

$(MPI_JOB_STDOUT_EXE): $(MPI_JOB_STDOUT_OBJECT)
	$(CC) $(LD_FLAGS)  $^ -o $@

clean:
	-$(RM) \
		$(MPI_JOB_EXE) \
		$(MPI_JOB_OBJECT) \
		$(MPI_JOB_STDOUT_EXE) \
		$(MPI_JOB_STDOUT_OBJECT) \
		$(MY_DUMB_PROC_EXE) \
		$(MY_DUMB_PROC_OBJECT) \
		ddict_orc* \
		process_w_deps_data*
