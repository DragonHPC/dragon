CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = lock_bench test_attach test_heap test_mem test_threaded_lock umap_test ulist_test test_log test_serialized_uid test_queue

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@ -fopenmp

default: build

build: lock_bench test_attach test_heap test_mem test_threaded_lock umap_test ulist_test test_log test_serialized_uid test_queue

test_threaded_lock: test_threaded_lock.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_threaded_lock $< $(LIBS) -ldragon -lrt -fopenmp -ldl

lock_bench: lock_bench.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o lock_bench $< $(LIBS) -ldragon -lrt -pthread -ldl

test_heap: test_heap.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_heap $< $(LIBS) -ldragon -lrt -pthread -ldl

test_mem: test_mem.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_mem $< $(LIBS) -ldragon -lrt -ldl

umap_test: umap.c
	$(CC) $(INCLUDE) $(CFLAGS) -o umap_test $< $(LIBS) -ldragon  -fopenmp -ldl


ulist_test: ulist.c
	$(CC) $(INCLUDE) $(CFLAGS) -o ulist_test $< $(LIBS) -ldragon  -fopenmp -ldl

test_attach: test_attach.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_attach $< $(LIBS) -ldragon -lrt -fopenmp -ldl

test_log: test_log.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_log $< $(LIBS) -ldragon -lrt -ldl

test_queue: test_queue.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_queue $< $(LIBS) -ldragon -lrt -ldl

test_serialized_uid: test_serialized_uid.c
	$(CC) $(INCLUDE) $(CFLAGS) -o test_serialized_uid $< $(LIBS) -ldragon -lrt -ldl

clean:
	rm -rf *.o $(BIN_FILES) core
