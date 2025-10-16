# This Makefile runs Dragon multinode tests.
# It needs an allocation ready or it will fail.
# Tests scale to allocation size, so the tests
# become "harder" with increasing allocation size.
ifneq ("$(wildcard $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk
endif


CC = gcc
CXX = g++
CFLAGS = -Ofast -fomit-frame-pointer -Wall

ifeq ($(DRAGON_INCLUDE_DIR),)
    DRAGON_INCLUDE = $(shell dragon-config -o)
else
	DRAGON_INCLUDE = -I $(DRAGON_INCLUDE_DIR)
endif

ifeq ($(DRAGON_LIB_DIR),)
    DRAGON_LINK = $(shell  dragon-config -l)
else
	DRAGON_LINK = -L $(shell echo ${DRAGON_LIB_DIR}) -ldragon
endif

RES_NODES = 0
ifdef SLURM_NNODES
	RES_NNODES = $(shell expr $(SLURM_NNODES) - 1)
	RES_NODE_INFO = "Got $(SLURM_NNODES) and running resilient test on $(RES_NNODES) "
endif

ifdef PBS_NODEFILE
	PBS_NNODES = $(shell cat ${PBS_NODEFILE} | uniq | wc -l )
	RES_NNODES = $(shell expr ${PBS_NNODES} - 1)
	RES_NODE_INFO = "Got $(PBS_NNODES) and running resilient test on $(RES_NNODES) "
endif

HAVE_NVIDIA_GPUS = false
ifdef SLURM_JOB_PARTITION
	ifneq ($(findstring griz,$(SLURM_JOB_PARTITION)),)
		HAVE_NVIDIA_GPUS = true
	endif
endif
# TODO: Sort out the right way to get the num nodes here for K8s

CPPFLAGS = $(DRAGON_INCLUDE) -DDRAGON_DEBUG
LIBS = $(DRAGON_LINK)
INCLUDE = $(DRAGON_INCLUDE)
PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')
DEF_NODES =


SOURCES := $(wildcard *.c)
OBJECTS := $(addsuffix .o,$(basename $(SOURCES)))

MPI_HELLO_SRC := mpi_allgather_hello.c
MPI_HELLO_CRAY := mpi_hello_cray
MPI_HELLO_OMPI := mpi_hello_ompi
MPI_HELLO_MPICH := mpi_hello_mpich

# list of components for tests
TESTS_DRAGON_MULTI_NODE := test_barrier.py \
						   test_connection.py \
						   test_lock.py \
						   test_machine.py \
						   test_pool.py \
						   test_process.py \
						   test_queue.py \
						   test_value.py \
						   test_array.py \
						   test_process_group.py \
						   test_distdict.py \
						   test_fli.py \
						   test_distdict_c.py \
						   test_zarr_store.py

TESTS_DRAGON_PMI_BACKENDS := test_pmi_backends.py

TESTS_DRAGON_RUNTIME_REBOOT := test_runtime_restart.py

TESTS_DRAGON_HSTA := test_hsta.py

TESTS_DRAGON_DATA_MOVERS := test_data_mover.py

.PHONY: all
all: multinode_default_wlm multinode_drun_wlm

.PHONY: multinode_default_wlm
multinode_default_wlm:
	@echo "Running multinode tests with default WLM"
	@WLM_OPTS="" $(MAKE) multinode-tests
	@$(MAKE) -s clean

.PHONY: multinode_drun_wlm
multinode_drun_wlm:
	@echo "Running multinode tests with drun WLM"
	@temp_file=$$(mktemp) && \
	dhosts --list > $$temp_file && \
	WLM_OPTS="--wlm drun --hostfile $$temp_file" $(MAKE) multinode-tests && \
	rm -f $$temp_file && \
	$(MAKE) -s clean

.PHONY: multinode-tests
multinode-tests: c_ddict cpp_ddict $(TESTS_DRAGON_MULTI_NODE) resiliency test_hsta pmi

.PHONY: resiliency
resiliency: $(TESTS_DRAGON_RUNTIME_REBOOT)

.PHONY: test_data_movers
test_data_movers: $(TESTS_DRAGON_DATA_MOVERS)

.PHONY: test_hsta
test_hsta: $(TESTS_DRAGON_HSTA)

.PHONY: $(TESTS_DRAGON_MULTI_NODE)
$(TESTS_DRAGON_MULTI_NODE):
	$(PYTHON) -m dragon $(WLM_OPTS) $(DEF_NODES) $@ -v -f

.PHONY: $(TESTS_DRAGON_RUNTIME_REBOOT)
$(TESTS_DRAGON_RUNTIME_REBOOT):
	$(PYTHON) -m dragon $(WLM_OPTS) --resilient --nodes=$(RES_NNODES) $@ -v -f

.PHONY: $(TESTS_DRAGON_HSTA)
$(TESTS_DRAGON_HSTA):
	$(PYTHON) $@ -v -f

# Try to dynamically set up MPI compiler and linker flags for testing.
$(MPI_HELLO_CRAY): $(MPI_HELLO_SRC)
ifeq ($(TEST_CRAY_MPICH_FLAGS),)
	if dragon-config test --cray-mpich=${CRAY_MPICH_PREFIX}; then \
		LOCALLY_LOADED_FLAGS=$$(cat $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk | grep "TEST_CRAY_MPICH_FLAGS" | cut -d'=' -f 2); \
		$(CC) $$LOCALLY_LOADED_FLAGS $(INCLUDE) $(LIBS) $^ -o $@; \
	fi
else
	$(CC) $(TEST_CRAY_MPICH_FLAGS) $(INCLUDE) $(LIBS) $^ -o $@
endif

$(MPI_HELLO_OMPI): $(MPI_HELLO_SRC)
ifeq ($(TEST_OPEN_MPI_FLAGS),)
	if dragon-config test --open-mpi=/lus/scratch/dragonhpc/openmpi; then \
		LOCALLY_LOADED_FLAGS=$$(cat $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk | grep "TEST_OPEN_MPI_FLAGS" | cut -d'=' -f 2);  \
		$(CC) $$LOCALLY_LOADED_FLAGS $(INCLUDE) $(LIBS) $^ -o $@; \
	fi
else
	$(CC) $(TEST_OPEN_MPI_FLAGS) $(INCLUDE) $(LIBS) $^ -o $@
endif

$(MPI_HELLO_MPICH): $(MPI_HELLO_SRC)
ifeq ("$(TEST_ANL_MPICH_FLAGS)","")
	if dragon-config test --anl-mpich=/lus/scratch/dragonhpc/mpich; then \
		LOCALLY_LOADED_FLAGS=$$(cat $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk | grep "TEST_ANL_MPICH_FLAGS" | cut -d'=' -f 2); \
		$(CC) $$LOCALLY_LOADED_FLAGS $(INCLUDE) $(LIBS) $^ -o $@; \
	fi
else
	$(CC) $(TEST_ANL_MPICH_FLAGS) $(INCLUDE) $(LIBS) $^ -o $@
endif

.PHONY: $(TESTS_DRAGON_PMI_BACKENDS)
$(TESTS_DRAGON_PMI_BACKENDS): $(MPI_HELLO_CRAY) $(MPI_HELLO_OMPI) $(MPI_HELLO_MPICH)
	$(PYTHON) -m dragon $(DEF_NODES) $@ -v -f

.PHONY: pmi
pmi: $(TESTS_DRAGON_PMI_BACKENDS)


c_ddict: c_ddict.c
	$(CC) $(INCLUDE) $(CFLAGS) -o c_ddict $< $(LIBS) -ldl

cpp_ddict: cpp_ddict.cpp
	g++ $(INCLUDE) -std=c++14 -o cpp_ddict $< $(LIBS) -ldl

.PHONY: $(TESTS_DRAGON_DATA_MOVERS)
$(TESTS_DRAGON_DATA_MOVERS):
ifeq ($(HAVE_NVIDIA_GPUS),true)
	@echo "Running $@"
	@$(PYTHON) -m dragon $(WLM_OPTS) $@ -v -f
else
	@echo "Skipping $@ since no NVIDIA GPUs detected in allocation"
endif

.PHONY: clean
clean: $(CLEAN_COMPONENTS)
	$(RM) *.log
	$(RM) -f *.o c_ddict cpp_ddict $(MPI_HELLO_OMPI) $(MPI_HELLO_CRAY) $(MPI_HELLO_MPICH)
	$(RM) -f /dev/shm/*
