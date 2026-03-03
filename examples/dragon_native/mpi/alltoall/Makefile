CC=gcc

CFLAGS  = -g -I. -I ${CRAY_MPICH_DIR}/include
LD_FLAGS = -lm -L ${CRAY_MPICH_DIR}/lib -lmpich

OSU_SRC = osu_alltoall.c \
          osu_util.c \
          osu_util_graph.c \
          osu_util_mpi.c \
          osu_util_papi.c \
          osu_util_validation.c

OSU_OBJ = $(OSU_SRC:.c=.c.o)
OSU_EXE = ../osu_alltoall


%.c.o: %.c
	$(CC) $(CFLAGS)  -c $< -o $@

$(OSU_EXE): $(OSU_OBJ)
	$(CC) $(LD_FLAGS)  $^ -o $@

clean:
	rm *.o $(OSU_EXE)