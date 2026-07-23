CC ?= gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = test_heapmanager simple

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_heapmanager

test_heapmanager: test_heapmanager.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_heapmanager $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES)
