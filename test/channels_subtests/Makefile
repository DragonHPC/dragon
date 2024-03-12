CC ?= gcc
CFLAGS ?= -fPIC -Wall -Ofast -fomit-frame-pointer
#CFLAGS ?= -g -O0 -DDRAGON_DEBUG -Wall
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_LIB_DIR)

BIN_FILES = test_ch test_bch perf_fch test_send test_poll test_peek_pop test_channelsets test_wrong test_gateways test_gateway_messages test_fli

%.c.o: %.c
	$(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

default: build

build: test_ch test_bch perf_fch test_send test_poll test_channelsets test_wrong test_gateways test_gateway_messages test_fli

test_ch: test_ch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_ch $< $(LIBS) -ldragon -ldl

test_bch: test_bch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_bch $< $(LIBS) -ldragon -ldl

test_poll: test_poll.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_poll $< $(LIBS) -ldragon -ldl

test_send: test_send.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_send $< $(LIBS) -ldragon -ldl

test_peek_pop: test_peek_pop.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_peek_pop $< $(LIBS) -ldragon -ldl

perf_fch: perf_fch.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o perf_fch $< $(LIBS) -ldragon -ldl

test_channelsets: test_channelsets.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_channelsets $< $(LIBS) -ldragon -ldl

test_fli: test_fli.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_fli $< $(LIBS) -ldragon -ldl

test_gateways: test_gateways.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gateways $< $(LIBS) -ldragon -ldl

test_gateway_messages: test_gateway_messages.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_gateway_messages $< $(LIBS) -ldragon -ldl

test_wrong: test_wrong.c.o
	$(CC) $(INCLUDE) $(CFLAGS) -o test_wrong $< $(LIBS) -ldragon -ldl

clean:
	rm -rf *.o $(BIN_FILES) core __pycache__
