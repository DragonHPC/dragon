CC ?= gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

ifeq ($(DRAGON_INCLUDE_DIR),)
    DRAGON_INCLUDE = $(shell dragon-config -o)
else
    DRAGON_INCLUDE = -I $(DRAGON_INCLUDE_DIR)
endif

ifeq ($(DRAGON_LIB_DIR),)
    DRAGON_LINK = $(shell dragon-config -l)
else
    DRAGON_LINK = -L $(shell echo ${DRAGON_LIB_DIR}) -l dragon
endif

INCLUDE = -I ${VIRTUAL_ENV}/include -I . $(DRAGON_INCLUDE)
LIBS = $(DRAGON_LINK)

BIN_FILES = heat_worker

default: build

build: $(BIN_FILES)

heat_worker: heat_worker.cpp
	g++ $(INCLUDE) -std=c++14 -o heat_worker $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__ *.ddict
