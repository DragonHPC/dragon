CC = gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer

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

INCLUDE = $(DRAGON_INCLUDE)
LIBS = $(DRAGON_LINK)


BIN_FILES = test_ch test_bch perf_fch test_send test_poll test_peek_pop test_channelsets test_wrong test_gateways test_gateway_messages test_capnp
# skip for github runner testing = test_fli

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_ch test_bch perf_fch test_send test_poll test_channelsets test_wrong test_gateways test_gateway_messages test_capnp test_peek_pop
# skip for github runner testing = test_fli

test_ch: test_ch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_ch $< $(LIBS) -ldl

test_bch: test_bch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_bch $< $(LIBS) -ldl

test_poll: test_poll.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_poll $< $(LIBS) -ldl

test_send: test_send.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_send $< $(LIBS) -ldl

test_peek_pop: test_peek_pop.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_peek_pop $< $(LIBS) -ldl

perf_fch: perf_fch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o perf_fch $< $(LIBS) -ldl

test_channelsets: test_channelsets.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_channelsets $< $(LIBS) -ldl

# test_fli: test_fli.c.o
# 	$(CC) $(INCLUDE) $(CFLAGS) -o test_fli $< $(LIBS) -ldl
# skip for github runner testing = test_fli

test_gateways: test_gateways.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gateways $< $(LIBS) -ldl

test_gateway_messages: test_gateway_messages.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gateway_messages $< $(LIBS) -ldl

test_wrong: test_wrong.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_wrong $< $(LIBS) -ldl

test_capnp: test_capnp.cpp
	g++ $(INCLUDE) -std=c++14 -o test_capnp $< $(LIBS) -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
