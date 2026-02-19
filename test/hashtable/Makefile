CC = gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer

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

BIN_FILES = test_hashtable

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_hashtable

test_hashtable: test_hashtable.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_hashtable $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES)
