DRAGON_BIN = python3 -m dragon -s

COMMON = common.py

TEST_FILES = $(shell find . -type f -name "test_*.py")
TEST_CASES = $(shell grep unittest.TestCase $(TEST_FILES) | cut -d " " -f 2 | cut -d '(' -f 1)
TEST_NAMES = $(shell grep "def test_" $(TEST_FILES) | cut -d " " -f 6 | cut -d '(' -f 1)

VERBOSE = -f -v # --locals

.DEFAULT_GOAL := all

# run single tests or test cases
.DEFAULT: $(COMMON)
	@$(eval CASE = $(shell echo $@ | cut -d . -f 1))
	@$(eval FILE = $(shell grep  $(CASE) test_*.py | head -1 | cut -d : -f 1))
	@echo "File     = $(FILE)"
	@echo "TestCase = $(CASE)"
	@echo " "
	@$(DRAGON_BIN) $(FILE) $@ -v  || echo "$< failed"

# run files / API blocks

array: test_array.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE) 

barrier: test_barrier.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

condition: test_condition.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

containers: test_containers.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

connection: test_connection.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

event: test_event.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

finalize: test_finalize.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

listener: test_listener.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

lock: test_lock.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

manager: test_manager.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

others: test_others.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)

poll: test_poll.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

pool: test_pool.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

process: test_process.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

queue: test_queue.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

semaphore: test_semaphore.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

shared_ctypes: test_shared_ctypes.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

shared_memory: test_shared_memory.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE)  

value: test_value.py $(COMMON)
	-rm -rf /dev/shm/*
	@echo $< && $(DRAGON_BIN) $< $(VERBOSE) 

all: condition connection event finalize listener lock others manager poll pool process queue semaphore shared_ctypes shared_memory value
# all: array barrier condition connection event finalize listener lock others manager poll pool process queue semaphore shared_ctypes shared_memory value

clean:
	rm -rf *.log

