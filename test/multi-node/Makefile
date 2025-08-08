# This Makefile runs Dragon multinode tests.
# It needs an allocation ready or it will fail.
# Tests scale to allocation size, so the tests
# become "harder" with increasing allocation size.

CC = cc
CXX = CC
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

# TODO: Sort out the right way to get the num nodes here for K8s

CPPFLAGS = $(DRAGON_INCLUDE) -DDRAGON_DEBUG
LIBS = $(DRAGON_LINK)
INCLUDE = $(DRAGON_INCLUDE)
PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')
DEF_NODES =


SOURCES := $(wildcard *.c)
OBJECTS := $(addsuffix .o,$(basename $(SOURCES)))

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

TESTS_DRAGON_MULTI_NODE_PMI := test_mpi_hello_world.py

TESTS_DRAGON_RUNTIME_REBOOT := test_runtime_restart.py

TESTS_DRAGON_HSTA := test_hsta.py

.PHONY: all
all: c_ddict cpp_ddict $(TESTS_DRAGON_MULTI_NODE) resiliency test_hsta

.PHONY: resiliency
resiliency: $(TESTS_DRAGON_RUNTIME_REBOOT)

.PHONY: test_hsta 
test_hsta: $(TESTS_DRAGON_HSTA)

.PHONY: $(TESTS_DRAGON_MULTI_NODE)
$(TESTS_DRAGON_MULTI_NODE):
	$(PYTHON) -m dragon $(DEF_NODES) $@ -v -f

.PHONY: $(TESTS_DRAGON_RUNTIME_REBOOT)
$(TESTS_DRAGON_RUNTIME_REBOOT):
	$(PYTHON) -m dragon --resilient --nodes=$(RES_NNODES) $@ -v -f

.PHONY: $(TESTS_DRAGON_HSTA)
$(TESTS_DRAGON_HSTA):
	$(PYTHON) $@ -v -f

c_ddict: c_ddict.c
	$(CC) $(INCLUDE) $(CFLAGS) -o c_ddict $< $(LIBS) -ldl

cpp_ddict: cpp_ddict.cpp
	g++ $(INCLUDE) -std=c++14 -o cpp_ddict $< $(LIBS) -ldl

mpi_hello: $(OBJECTS)
	$(CC) -o mpi_hello $< $(LIBS) -ldl

.PHONY: $(TESTS_DRAGON_MULTI_NODE_PMI)
$(TESTS_DRAGON_MULTI_NODE_PMI): mpi_hello
	$(PYTHON) -m dragon $(DEF_NODES) $@ -v -f

.PHONY: clean
clean: $(CLEAN_COMPONENTS)
	$(RM) *.log
	$(RM) -f *.o mpi_hello c_ddict cpp_ddict
	$(RM) -f /dev/shm/*

