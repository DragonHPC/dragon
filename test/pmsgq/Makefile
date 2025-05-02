CC = gcc
CFLAGS = -O3 -fomit-frame-pointer -fPIC -Wall
CPPFLAGS = -I ../../src/include
LDFLAGS = -L ../../src/lib
LDLIBS = -lpmsgqueue -lrt -ldragon -ldl

BIN_FILES = test_pmsgq

.PHONY: all
all: $(BIN_FILES)

.PHONY: clean
clean:
	$(RM) $(BIN_FILES) *.o
