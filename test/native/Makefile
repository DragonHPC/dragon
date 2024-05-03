CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
#CFLAGS ?= -g -O0 -DDRAGON_DEBUG -Wall
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = flimsgfrom flimsgto

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: flimsgfrom flimsgto

flimsgfrom: flimsgfrom.cpp
	g++ $(INCLUDE) -std=c++14 -o flimsgfrom $< $(LIBS) -ldragon -ldl

flimsgto: flimsgto.cpp
	g++ $(INCLUDE) -std=c++14 -o flimsgto $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
