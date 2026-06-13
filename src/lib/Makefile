ifneq ("$(wildcard $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk
endif


ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

# Check if the PMIx headers exist by default, just in case the user didn't define them but we can find them in
# a default location
DEFAULT_PMIX_INCLUDE=/usr/include/pmix
ifneq ("$(wildcard $(DEFAULT_PMIX_INCLUDE)/src/include/pmix_globals.h)","")
    CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_PMIX_INCLUDE
endif

CC = gcc
#CFLAGS = -O0 -g -fPIC -Wall
CFLAGS = -O3 -ffast-math -fomit-frame-pointer -fPIC -Wall
CXX = g++
CPPFLAGS = -MMD -I ../include -I ./gpu -I $(DEFAULT_PMIX_INCLUDE) -DDRAGON_DEBUG -D_GNU_SOURCE $(CONFIG_INCLUDE) $(CONFIG_DEFINES) -I$ ${VIRTUAL_ENV}/include
CXXFLAGS = $(CFLAGS) $(CPPFLAGS) -std=c++14
CAPNP = $(VIRTUAL_ENV)/bin/capnp
CAPNP-CXX = $(VIRTUAL_ENV)/bin/capnpc-c++

CAPNP_DIR = $(shell pwd)/..
CAPNP = $(CAPNP_DIR)/bin/capnp
CAPNP-CXX = $(CAPNP_DIR)/bin/capnpc-c++


LIBRARIES := libdragon.so libpmod.so

libdragon_HEADERS := $(wildcard *.h) \
                     $(wildcard gpu/*.hpp) \
                     $(wildcard gpu/*.h) \
                     $(filter-out ../include/dragon/return_codes_map.h,$(wildcard ../include/dragon/*.h)) \
                     ../include/dragon/return_codes_map.h \
                     ../include/dragon/message_tcs.hpp

libdragon_CAPNP_SOURCES = $(wildcard *.capnp)
libdragon_SOURCES := $(wildcard *.c) $(wildcard *.cpp) gpu/gpu.cpp $(CONFIG_SOURCES)
libdragon_OBJECTS := $(libdragon_CAPNP_SOURCES:.capnp=.o) $(addsuffix .o,$(basename $(libdragon_SOURCES)))

.PHONY: all
all: ../include/dragon/message_tcs.hpp ../include/dragon/return_codes_map.h ../include/dragon/message_defs.capnp.h $(LIBRARIES)

message_defs.capnp.c++ message_defs.capnp.h: message_defs.capnp $(CAPNP) $(CAPNP-CXX)
	@test -x $(CAPNP) || (echo "Error: $(CAPNP) not found"; exit 1)
	@test -x $(CAPNP-CXX) || (echo "Error: $(CAPNP-CXX) not found"; exit 1)
	$(CAPNP) compile -o$(CAPNP-CXX) message_defs.capnp

message_defs.o: message_defs.capnp.c++ message_defs.capnp.h
	$(CXX) -c $(CXXFLAGS) -o $@ message_defs.capnp.c++

../include/dragon/message_tcs.hpp: ../dragon/infrastructure/messages.py
	python3 message_tcs_to_enum.py

../include/dragon/return_codes_map.h: ../include/%:
	$(MAKE) -C ../include $*

../include/dragon/message_defs.capnp.h: message_defs.o
	cp message_defs.capnp.h $@

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

LDLIBS += -lstdc++ -ldl

OS = $(shell uname -s)

libdragon.so: $(libdragon_OBJECTS)
	@if [ "$(OS)" != "Darwin" ]; then \
		$(CC) -L . -L ${VIRTUAL_ENV}/lib -L ${VIRTUAL_ENV}/lib64 -L $(CAPNP_DIR)/lib -L $(CAPNP_DIR)/lib64 -shared -Wl,-soname,$(notdir $@) -o $@ $^ -Wl,--whole-archive -lcapnp -lkj -Wl,--no-whole-archive $(LDLIBS) -lrt; \
	else \
		$(CC) -L . -L ${VIRTUAL_ENV}/lib -L $(CAPNP_DIR)/lib -shared -o $@ $^ -lcapnp -lkj $(LDLIBS); \
	fi

libpmod.so: libdragon.so
	ln -sf libdragon.so libpmod.so

$(filter-out libpmod.so,$(filter-out libdragon.so,$(LIBRARIES))): lib%.so: ../dragon/transport/hsta/dfabric_lib/lib%.so
	$(MAKE) -C $(<D) $(<F)
	$(INSTALL) -m 755 -D $< $@

../dragon/launcher/src/lib%.so:
	$(MAKE) -C $(@D) $(@F)

.PHONY: clean
clean:
	$(RM) $(LIBRARIES) *.o *.d libpmod.so *.capnp.c++ *.capnp.h

-include $(libdragon_OBJECTS:.o=.d)
