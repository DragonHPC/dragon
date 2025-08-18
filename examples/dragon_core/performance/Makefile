CC = gcc
AR = ar
INCLUDE = -I $(DRAGON_INCLUDE_DIR)
LIBS = -L $(DRAGON_BASE_DIR)/lib -L.
CFLAGS = -O3
CREQFLAGS = -fPIC
INSTALL = install -D

INSTALL_LIB     = lib
INSTALL_INCLUDE = include
INSTALL_PYLIB   = pylib

BIN_FILES = ch_p2p_latency ch_p2p_bandwidth ch_p2p_msg_rate

%.c.o: %.c
	$(CC) $(CFLAGS) $(CREQFLAGS) $(INCLUDE) -c $< -o $@

%.a: %.c.o
	$(AR) cr lib$@ $<

dist: default

default: $(BIN_FILES)

debug: CFLAGS += -DDEBUG -g
debug: default

ch_p2p_latency: ch_p2p_latency.c.o ch_p2p_common.a
	$(CC) $(INCLUDE) $(CFLAGS) -o ch_p2p_latency $< $(LIBS) -ldragon -lrt -lch_p2p_common

ch_p2p_bandwidth: ch_p2p_bandwidth.c.o ch_p2p_common.a
	$(CC) $(INCLUDE) $(CFLAGS) -o ch_p2p_bandwidth  $< $(LIBS) -ldragon -lrt -lch_p2p_common

ch_p2p_msg_rate: ch_p2p_msg_rate.c.o ch_p2p_common.a
	$(CC) $(INCLUDE) $(CFLAGS) -o ch_p2p_msg_rate $< $(LIBS) -ldragon -lrt -lch_p2p_common

clean:
	rm -rf *.o *.a $(BIN_FILES)
