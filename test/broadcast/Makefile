CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
#CFLAGS ?= -g -fPIC -Wall -O0
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = test_bcast perf_bcast

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_bcast perf_bcast

test_bcast: test_bcast.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_bcast $< $(LIBS) -ldragon -lrt -pthread -ldl

perf_bcast: perf_bcast.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o perf_bcast $< $(LIBS) -ldragon -lrt -pthread -ldl

clean:
	rm -rf *.o $(BIN_FILES)

