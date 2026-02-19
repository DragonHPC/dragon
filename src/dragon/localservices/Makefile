CC = gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer
# CFLAGS ?= -g -fPIC -Wall -O0

INSTALL = install
INSTALL_MODE ?= 755
EXE_DRAGON_POPEN = dragon-popen
INSTALL_DIR = $(DRAGON_BASE_DIR)/bin

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

BIN_FILES = dragon-popen

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: dragon-popen

dragon-popen: dragon_popen_wrapper.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o dragon-popen $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES)

