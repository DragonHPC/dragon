CC = gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer

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

INCLUDE = $(DRAGON_INCLUDE) -I ../../src/lib
LIBS = $(DRAGON_LINK)


ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

INCLUDE = $(DRAGON_INCLUDE) -I ${VIRTUAL_ENV}/include
LIBS = $(DRAGON_LINK)

BIN_FILES = flimsgfrom flimsgto c_ddict cpp_ddict

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: flimsgfrom flimsgto c_ddict cpp_ddict

flimsgfrom: flimsgfrom.cpp
	g++ $(INCLUDE) -std=c++14 -o flimsgfrom $< $(LIBS) -ldl

flimsgto: flimsgto.cpp
	g++ $(INCLUDE) -std=c++14 -o flimsgto $< $(LIBS) -ldl

c_ddict: c_ddict.c
	$(CC) $(INCLUDE) $(CFLAGS) -o c_ddict $< $(LIBS)  -ldl

cpp_ddict: cpp_ddict.cpp
	g++ $(INCLUDE) -std=c++14 -o cpp_ddict $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
