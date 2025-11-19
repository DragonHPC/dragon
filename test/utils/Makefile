CC = gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer

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

BIN_FILES = lock_bench test_attach test_heap test_mem test_threaded_lock umap_test ulist_test test_log test_serialized_uid test_blocks test_gpu_mem

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@ -fopenmp

default: build

build: lock_bench test_attach test_heap test_mem test_threaded_lock umap_test ulist_test test_log test_serialized_uid test_blocks

test_threaded_lock: test_threaded_lock.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_threaded_lock $< $(LIBS) -lrt -fopenmp -ldl

lock_bench: lock_bench.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o lock_bench $< $(LIBS) -lrt -pthread -ldl

test_blocks: test_blocks.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_blocks $< $(LIBS) -lrt -pthread -ldl

test_heap: test_heap.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_heap $< $(LIBS) -lrt -pthread -ldl

test_mem: test_mem.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_mem $< $(LIBS) -lrt -ldl

test_gpu_mem: test_gpu_mem.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gpu_mem $< $(LIBS) -lrt -ldl

umap_test: umap.c
	$(CC) $(INCLUDE) $(CFLAGS) -o umap_test $< $(LIBS)  -fopenmp -ldl

ulist_test: ulist.c
	$(CC) $(INCLUDE) $(CFLAGS) -o ulist_test $< $(LIBS)  -fopenmp -ldl

test_attach: test_attach.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_attach $< $(LIBS) -lrt -fopenmp -ldl

test_log: test_log.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_log $< $(LIBS) -lrt -ldl

test_serialized_uid: test_serialized_uid.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_serialized_uid $< $(LIBS) -lrt -ldl

clean:
	rm -rf *.o $(BIN_FILES) core
