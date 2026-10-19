CXX ?= g++
CXXFLAGS ?= -fPIC -Wall -O0 -g -std=c++17

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

BIN_FILES = ext_attach_client

default: build

build: $(BIN_FILES)

ext_attach_client: ext_attach_client.cpp
	$(CXX) $(INCLUDE) $(CXXFLAGS) -o ext_attach_client $< $(LIBS) -lstdc++ -ldl -lm

clean:
	rm -rf $(BIN_FILES) *.o core __pycache__
