CC=gcc
CFLAGS  = -g  -pedantic -Wall -I ${CRAY_MPICH_DIR}/include -L ${CRAY_MPICH_DIR}/lib
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich

default: all

factorial: factorial.o 
	$(CC) $(CFLAGS)  factorial.o -o factorial $(LD_FLAGS)

all: factorial 

.PHONY: clean

clean:
	rm factorial *.o 
