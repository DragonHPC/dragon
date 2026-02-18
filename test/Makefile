export CC = gcc
export CXX = g++
export CFLAGS = -Ofast -fomit-frame-pointer -DDRAGON_DEBUG -Wall
export PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')

INSTALL ?= install
INSTALL_DIR ?= ../src/dist
INSTALL_MODE ?= 755

# list of components for tests
COMPONENTS := broadcast \
              channels_subtests \
              hashtable \
              heapmanager \
              pmod \
              utils

DIST_SOURCES := $(wildcard pkg/*) \
				_ctest_utils.h \
                test_utils.py \
                test_channels.py \
                shim_dragon_paths.py \
                utils/test_mempool.py \
                utils/test_basic_mempool.py \
                utils/test_locks.py \
                utils/test_pyheap.py \
                utils/test_logging.py \
                channels_subtests/test_basic_channels.py \
				multi-node/test_array.py \
				multi-node/test_barrier.py \
				multi-node/test_connection.py \
				multi-node/test_distdict.py \
				multi-node/test_distdict_c.py \
				multi-node/test_fli.py \
				multi-node/test_lock.py \
				multi-node/test_machine.py \
				multi-node/test_pool.py \
				multi-node/test_process.py \
				multi-node/test_queue.py \
				multi-node/test_value.py \
                multi-node/test_process_group.py \
				multi-node/mpi_allgather_hello.c \
				multi-node/c_ddict.c \
                multi-node/cpp_ddict.cpp \
				multi-node/test_pmi_backends.py \
				multi-node/Makefile

DIST_DIR := $(INSTALL_DIR)/dragon_unittests

DIST_TARGETS := $(addprefix $(DIST_DIR)/,$(notdir $(filter pkg/%,$(DIST_SOURCES)))) \
                $(addprefix $(DIST_DIR)/,$(filter-out pkg/%,$(DIST_SOURCES)))

TESTS_PYTHON := test_c_files.py \
				test_channels.py \
                test_utils.py \
                test_connection.py \
                test_infrastructure.py \
                test_globalservices.py \
                test_policy.py \
                test_shepherd.py \
                $(wildcard transport/tcp/test_*.py) \
                transport/test_lsif.py \
                test_transport.py \
                test_integration_shep_gs.py \
                test_dragon_cleanup.py
# test_launcher.py (waiting for fix from Jira AICI-1859)

TEST_WORKFLOWS = test_workflows

TESTS_AI := test_collective_group.py

TESTS_DRAGON := test_mpbridge.py \
                test_native.py \
				test_distdict.py \
				test_telemetry.py

TESTS_CCFUTURES := test_as_completed.py \
                   test_deadlock.py \
                   test_future.py \
                   test_init.py \
                   test_interpreter_pool.py \
                   test_process_pool.py \
                   test_shutdown.py \
                   test_thread_pool.py \
                   test_wait.py

.PHONY: all
all: test

.PHONY: test
test: $(TESTS_PYTHON) $(TESTS_DRAGON) $(TESTS_CCFUTURES) $(TESTS_AI) $(TEST_WORKFLOWS)

.PHONY: $(TEST_WORKFLOWS)
$(TEST_WORKFLOWS):
	$(MAKE) -C workflows

.PHONY: $(TESTS_PYTHON)
$(TESTS_PYTHON):
	$(PYTHON) $@ -v -f

.PHONY: $(TESTS_DRAGON)
$(TESTS_DRAGON):
	$(PYTHON) -m dragon -s $@ -v -f

.PHONY: tests_multi_node
tests_multi_node:
	@echo "These tests require an allocation !"
	$(MAKE) -C multi-node


.PHONY: $(TESTS_CCFUTURES)
$(TESTS_CCFUTURES):
	@echo "Running ccfutures test"
	cd ccfutures && dragon -s $@ -v -f

.PHONY: $(TESTS_AI)
$(TESTS_AI):
	@echo "Running ai test"
	cd ai && dragon -s $@ -v -f

BUILD_COMPONENTS := $(addprefix build-,$(COMPONENTS))

.PHONY: build
build: $(BUILD_COMPONENTS)

.PHONY: $(BUILD_COMPONENTS)
$(BUILD_COMPONENTS): build-%:
	$(MAKE) -C $* build

CLEAN_COMPONENTS := $(addprefix clean-,$(COMPONENTS))

.PHONY: clean
clean: $(CLEAN_COMPONENTS)
	-$(RM) *.log
	-$(RM) /dev/shm/*
	$(MAKE) -C workflows clean

.PHONY: $(CLEAN_COMPONENTS)
$(CLEAN_COMPONENTS): clean-%:
	$(MAKE) -C $* clean

.PHONY: dist
dist: $(DIST_TARGETS)

$(addprefix $(DIST_DIR)/,$(notdir $(filter pkg/%,$(DIST_SOURCES)))): $(DIST_DIR)/%:
	$(INSTALL) -m $(INSTALL_MODE) -D pkg/$* $@

$(addprefix $(DIST_DIR)/,$(filter-out pkg/%,$(DIST_SOURCES))): $(DIST_DIR)/%:
	$(INSTALL) -m $(INSTALL_MODE) -D $* $@

.PHONY: distclean
distclean:
	$(RM) $(DIST_TARGETS)
