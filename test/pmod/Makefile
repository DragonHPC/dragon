CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
INCLUDE = -I $(DRAGON_INCLUDE_DIR) -I ../../src/lib
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = test_pmod

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_pmod

test_pmod: test_pmod.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_pmod $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES)

