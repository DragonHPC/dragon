CC = gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
#CFLAGS ?= -g -fPIC -Wall -O0

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

INCLUDE = $(DRAGON_INCLUDE)
LIBS = $(DRAGON_LINK)

BIN_FILES = test_bcast perf_bcast

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_bcast perf_bcast

test_bcast: test_bcast.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_bcast $< $(LIBS) -lrt -pthread -ldl

perf_bcast: perf_bcast.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o perf_bcast $< $(LIBS) -lrt -pthread -ldl

clean:
	rm -rf *.o $(BIN_FILES)

