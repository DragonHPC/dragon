CC = gcc
#CFLAGS = -O0 -g -fPIC -Wall
CFLAGS = -O3 -fomit-frame-pointer -fPIC -Wall
CXX = g++
CXXFLAGS = $(CFLAGS)
CPPFLAGS = -I ../include -DDRAGON_DEBUG

LIBRARIES := libdragon.so libpmsgqueue.so libpmod.so

libdragon_HEADERS := $(wildcard *.h) \
                     $(filter-out ../include/dragon/return_codes_map.h,$(wildcard ../include/dragon/*.h)) \
                     ../include/dragon/return_codes_map.h
libdragon_SOURCES := $(wildcard *.c) $(wildcard *.cpp)
libdragon_OBJECTS := $(addsuffix .o,$(basename $(libdragon_SOURCES)))

.PHONY: all
all: $(LIBRARIES)


$(libdragon_OBJECTS): $(libdragon_HEADERS)

.PHONY: ../include/dragon/return_codes_map.h
../include/dragon/return_codes_map.h: ../include/%:
	$(MAKE) -C ../include $*

libdragon.so: $(libdragon_OBJECTS)
libdragon.so: LDLIBS += -lstdc++ -lrt

libdragon.so: %.so:
	$(LINK.c) -shared -Wl,-soname,$(notdir $@) -o $@ $^ $(LDLIBS)

libpmod.so: libdragon.so
	ln -sf libdragon.so libpmod.so

.PHONY: $(filter-out libpmod.so, $(filter-out libdragon.so,$(LIBRARIES)))
$(filter-out libpmod.so,$(filter-out libdragon.so,$(LIBRARIES))): lib%.so: ../dragon/launcher/src/lib%.so
	$(MAKE) -C $(<D) $(<F)
	$(INSTALL) -m 755 -D $< $@

.PHONY: $(addprefix ../dragon/launcher/src/lib/,$(filter-out libpmod.so, $(filter-out libdragon.so,$(LIBRARIES))))
../dragon/launcher/src/lib%.so:
	$(MAKE) -C $(@D) $(@F)

.PHONY: clean
clean:
	$(RM) $(LIBRARIES) *.o libpmod.so
	$(MAKE) -C ../dragon/launcher/src clean
