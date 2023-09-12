# This Makefile runs Dragon multinode tests.
# It needs an allocation ready or it will fail.
# Tests scale to allocation size, so the tests
# become "harder" with increasing allocation size.

CC = cc
CXX = CC
CFLAGS = -Ofast -fomit-frame-pointer -Wall
CPPFLAGS = -I $(DRAGON_INCLUDE_DIR) -DDRAGON_DEBUG
LIBS = -L $(DRAGON_LIB_DIR)
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
						   test_dict.py

TESTS_DRAGON_MULTI_NODE_PMI := test_mpi_hello_world.py

.PHONY: all
all: $(TESTS_DRAGON_MULTI_NODE)

.PHONY: $(TESTS_DRAGON_MULTI_NODE)
$(TESTS_DRAGON_MULTI_NODE):
	$(PYTHON) -m dragon $(DEF_NODES) $@ -v -f

mpi_hello: $(OBJECTS)
	$(CC) -o mpi_hello $< $(LIBS) -ldragon -ldl

.PHONY: $(TESTS_DRAGON_MULTI_NODE_PMI)
$(TESTS_DRAGON_MULTI_NODE_PMI): mpi_hello
	$(PYTHON) -m dragon $(DEF_NODES) $@ -v -f

.PHONY: clean
clean: $(CLEAN_COMPONENTS)
	$(RM) *.log
	$(RM) -f *.o mpi_hello
	$(RM) -f /dev/shm/*
