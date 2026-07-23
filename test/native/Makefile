CC ?= gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer
#CFLAGS ?= -g -O0 -DDRAGON_DEBUG -Wall

ifeq (${DRAGON_INCLUDE_DIR},)
    L_DRAGON_INCLUDE_DIR = $(shell dragon-config -o)
else
	L_DRAGON_INCLUDE_DIR = -I ${DRAGON_INCLUDE_DIR}
endif

ifeq (${DRAGON_LIB_DIR},)
    L_DRAGON_LIB_DIR = $(shell  dragon-config -l)
else
	L_DRAGON_LIB_DIR = -L ${DRAGON_LIB_DIR} -l dragon
endif

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

INCLUDE = $(L_DRAGON_INCLUDE_DIR) -I ${VIRTUAL_ENV}/include
LIBS = $(L_DRAGON_LIB_DIR)

DRAGON_CPP_TEST_COMMON_DEPS = $(wildcard ../../src/include/dragon/*.hpp ../../src/include/dragon/*.h) ../../src/lib/libdragon.so
CPP_TEST_BINS = flimsgfrom flimsgto cpp_ddict cpp_queue cpp_semaphore cpp_barrier

BIN_FILES = flimsgfrom flimsgto c_ddict cpp_ddict cpp_queue cpp_semaphore cpp_barrier

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: flimsgfrom flimsgto c_ddict cpp_ddict cpp_queue cpp_semaphore cpp_barrier

flimsgfrom: flimsgfrom.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o flimsgfrom $< $(LIBS) -ldl

flimsgto: flimsgto.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o flimsgto $< $(LIBS) -ldl

c_ddict: c_ddict.c
	$(CC) $(INCLUDE) $(CFLAGS) -o c_ddict $< $(LIBS)  -ldl

cpp_ddict: cpp_ddict.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o cpp_ddict $< $(LIBS) -ldl

cpp_queue: cpp_queue.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o cpp_queue $< $(LIBS) -ldl

cpp_semaphore: cpp_semaphore.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o cpp_semaphore $< $(LIBS) -ldl

cpp_barrier: cpp_barrier.cpp $(DRAGON_CPP_TEST_COMMON_DEPS)
	g++ $(INCLUDE) -std=c++14 -o cpp_barrier $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
