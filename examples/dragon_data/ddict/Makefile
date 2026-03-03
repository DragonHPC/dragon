CC ?= gcc
CFLAGS ?= -fPIC -Wall -O3 -ffast-math -fomit-frame-pointer
#CFLAGS ?= -g -O0 -DDRAGON_DEBUG -Wall

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

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

INCLUDE = -I ${VIRTUAL_ENV}/include -I . $(DRAGON_INCLUDE)
LIBS = $(DRAGON_LINK)

BIN_FILES = ddict_pi_sim_train ddict_pi_sim_aggregate

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: ddict_pi_sim_train ddict_pi_sim_aggregate

ddict_pi_sim_train: ddict_pi_sim_train.cpp
	g++ $(INCLUDE) -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++14 -o ddict_pi_sim_train $< $(LIBS) -ldragon -ldl

ddict_pi_sim_aggregate: ddict_pi_sim_aggregate.cpp
	g++ $(INCLUDE) -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++14 -o ddict_pi_sim_aggregate $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
