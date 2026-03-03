CC=gcc
CFLAGS  = -g  -pedantic -Wall -I ${CRAY_MPICH_DIR}/include -L ${CRAY_MPICH_DIR}/lib
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich

default: all

cheap: sim-cheap.o 
	$(CC) $(CFLAGS)  sim-cheap.o -o sim-cheap $(LD_FLAGS)

expensive: sim-expensive.o 
	$(CC) $(CFLAGS)  sim-expensive.o -o sim-expensive $(LD_FLAGS)


all: cheap expensive

.PHONY: clean

clean:
	rm sim-cheap sim-expensive *.o 
