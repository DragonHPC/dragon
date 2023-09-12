CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = test_hashtable

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_hashtable

test_hashtable: test_hashtable.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_hashtable $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES)
