CC = gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer

OS = $(shell uname -s)

ifeq ("$(OS)","DARWIN")
    OPENMP = 
	EXTRA_TESTS = 
else 
	OPENMP = -fopenmp
	EXTRA_TESTS = test_attach test_threaded_lock
endif

ifeq ($(DRAGON_INCLUDE_DIR),)
    DRAGON_INCLUDE = $(shell dragon-config -o)
else
	DRAGON_INCLUDE = -I $(DRAGON_INCLUDE_DIR) -I $(DRAGON_LIB_DIR)/gpu 
endif

ifeq ($(DRAGON_LIB_DIR),)
    DRAGON_LINK = $(shell  dragon-config -l)
else
	DRAGON_LINK = -L $(shell echo ${DRAGON_LIB_DIR}) -ldragon 
endif

INCLUDE = $(DRAGON_INCLUDE)
LIBS = $(DRAGON_LINK)

BIN_FILES = lock_bench test_heap test_mem umap_test ulist_test test_log test_serialized_uid test_blocks test_gpu_mem $(EXTRA_TESTS)

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_blocks
#build: lock_bench test_attach test_heap test_mem test_threaded_lock umap_test ulist_test test_log test_serialized_uid test_blocks

test_threaded_lock: test_threaded_lock.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_threaded_lock $< $(LIBS) $(OPENMP) -ldl; \

lock_bench: lock_bench.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o lock_bench $< $(LIBS) -pthread -ldl

test_blocks: test_blocks.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_blocks $< $(LIBS) -pthread -ldl

test_heap: test_heap.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_heap $< $(LIBS) -pthread -ldl

test_mem: test_mem.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_mem $< $(LIBS) -ldl

test_gpu_mem: test_gpu_mem.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gpu_mem $< $(LIBS) -ldl

umap_test: umap.c
	$(CC) $(INCLUDE) $(CFLAGS) -o umap_test $< $(LIBS)  $(OPENMP) -ldl

ulist_test: ulist.c
	$(CC) $(INCLUDE) $(CFLAGS) -o ulist_test $< $(LIBS)  $(OPENMP) -ldl

test_attach: test_attach.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_attach $< $(LIBS) $(OPENMP) -ldl \

test_log: test_log.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_log $< $(LIBS) -ldl

test_serialized_uid: test_serialized_uid.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_serialized_uid $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES) core
