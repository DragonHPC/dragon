ifneq ("$(wildcard $(DRAGON_BASE_DIR)/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/.dragon-config.mk
endif

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif

CC = gcc
#CFLAGS = -O0 -g -fPIC -Wall
CFLAGS = -O3 -fomit-frame-pointer -fPIC -Wall
CXX = g++
CPPFLAGS = -I ../include -DDRAGON_DEBUG $(CONFIG_INCLUDE) $(CONFIG_DEFINES) -I$ ${VIRTUAL_ENV}/include
CXXFLAGS = $(CFLAGS) $(CPPFLAGS) -std=c++14

LIBRARIES := libdragon.so libpmod.so $(wildcard libdfabric_*.so)

libdragon_HEADERS := $(wildcard *.h) \
                     $(wildcard gpu/*.hpp) \
                     $(filter-out ../include/dragon/return_codes_map.h,$(wildcard ../include/dragon/*.h)) \
                     ../include/dragon/return_codes_map.h \
					 ../include/dragon/message_tcs.h

libdragon_CAPNP_SOURCES = $(wildcard *.capnp)
libdragon_SOURCES := $(wildcard *.c) $(wildcard *.cpp) gpu/gpu.cpp $(CONFIG_SOURCES)
libdragon_OBJECTS := $(addsuffix .o,$(libdragon_CAPNP_SOURCES)) $(addsuffix .o,$(basename $(libdragon_SOURCES)))



.PHONY: all
all: message_defs.capnp.o ../include/dragon/message_tcs.h ../include/dragon/return_codes_map.h $(LIBRARIES)


$(libdragon_OBJECTS): $(libdragon_HEADERS)

message_defs.capnp.o:
	capnp compile -oc++ message_defs.capnp
	cp message_defs.capnp.h ../include/dragon
	$(CXX) -c $(CXXFLAGS) message_defs.capnp.c++

../include/dragon/message_tcs.h: ../dragon/infrastructure/messages.py
	python3 message_tcs_to_enum.py

.PHONY: ../include/dragon/return_codes_map.h
../include/dragon/return_codes_map.h: ../include/%:
	$(MAKE) -C ../include $*

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

libdragon.so: $(libdragon_OBJECTS)
libdragon.so: LDLIBS += -lstdc++ -lrt

libdragon.so: %.so:
	$(CC) -L . -L ${VIRTUAL_ENV}/lib -shared -Wl,-soname,$(notdir $@) -o $@ $^ -Wl,--whole-archive -lcapnp -lkj -Wl,--no-whole-archive $(LDLIBS)

libpmod.so: libdragon.so
	ln -sf libdragon.so libpmod.so

.PHONY: $(filter-out libpmod.so, $(filter-out libdragon.so,$(LIBRARIES)))
$(filter-out libpmod.so,$(filter-out libdragon.so,$(LIBRARIES))): lib%.so: ../dragon/transport/hsta/dfabric_lib/lib%.so
	$(MAKE) -C $(<D) $(<F)
	$(INSTALL) -m 755 -D $< $@

.PHONY: $(addprefix ../dragon/launcher/src/lib/,$(filter-out libpmod.so, $(filter-out libdragon.so,$(LIBRARIES))))
../dragon/launcher/src/lib%.so:
	$(MAKE) -C $(@D) $(@F)

.PHONY: clean
clean:
	$(RM) $(LIBRARIES) *.o libpmod.so *.capnp.c++ *.capnp.h *.capnp.o
