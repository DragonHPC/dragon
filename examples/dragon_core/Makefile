CC = gcc
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_BASE_DIR)/lib
CFLAGS = -O3
INSTALL = install -D

INSTALL_LIB     = lib
INSTALL_INCLUDE = include
INSTALL_PYLIB   = pylib

BIN_FILES = logging ringproc

BASE_DIR = $(PWD)

COMPONENT_DIRS = performance

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: ringproc logging
	$(foreach p,$(COMPONENT_DIRS),cd $(p) && $(MAKE) dist && cd $(BASE_DIR);)

ringproc: ringproc.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o ringproc $< $(LIBS) -ldragon

logging: logging.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o logging $< $(LIBS) -ldragon -lrt

clean:
	$(foreach p,$(COMPONENT_DIRS),cd $(p) && $(MAKE) clean && cd $(BASE_DIR);)
	rm -rf *.o $(BIN_FILES) core *.bin
	rm -rf /dev/shm/*
