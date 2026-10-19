CC ?= gcc
CFLAGS ?= -fPIC -Wall -O0 -g

ifeq (${DRAGON_INCLUDE_DIR},)
    L_DRAGON_INCLUDE_DIR = $(shell dragon-config -o)
else
	L_DRAGON_INCLUDE_DIR = -I ${DRAGON_INCLUDE_DIR}
endif

ifeq (${DRAGON_LIB_DIR},)
    L_DRAGON_LIB_DIR = $(shell dragon-config -l)
else
	L_DRAGON_LIB_DIR = -L ${DRAGON_LIB_DIR} -l dragon
endif

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

INCLUDE = $(L_DRAGON_INCLUDE_DIR) -I ${VIRTUAL_ENV}/include
LIBS = $(L_DRAGON_LIB_DIR)

DRAGON_CPP_TEST_COMMON_DEPS = $(wildcard ../../src/include/dragon/*.hpp ../../src/include/dragon/*.h ../../src/lib/libdragon.so)

BIN_FILES = test_ext_attach_client

default: build

build: $(BIN_FILES)

test_ext_attach_client: test_ext_attach_client.c $(DRAGON_CPP_TEST_COMMON_DEPS)
	$(CC) $(INCLUDE) $(CFLAGS) -o test_ext_attach_client $< $(LIBS) -ldl

clean:
	rm -rf $(BIN_FILES) *.o core __pycache__
