CC = gcc
CFLAGS = -O3 -fomit-frame-pointer -fPIC -Wall
CXX = g++
CXXFLAGS = $(CFLAGS)

LIBRARIES = libpmsgqueue.so

all: $(LIBRARIES)

pmsgqueue.o: ../include/_pmsgqueue.h ../../../lib/err.h
pmsgqueue.o: CPPFLAGS = -I ../include -I ../../../include -I ../../../lib -DDRAGON_DEBUG

libpmsgqueue.so: pmsgqueue.o
libpmsgqueue.so: LDFLAGS = -L ../../../lib
libpmsgqueue.so: LDLIBS = -ldragon

%.so:
	$(LINK.c) -shared -Wl,-soname,$(notdir $@) -o $@ $^ $(LDLIBS)

clean:
	$(RM) $(LIBRARIES) *.o
