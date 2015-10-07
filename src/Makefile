MPICC=mpicc
GCC=gcc

all: rng_client.o ds_stack.o
	$(MPICC) rng_master.c -o rng_master
	$(MPICC) -fopenmp rng_client.o ds_stack.o -o rng_client

rng_client.o: rng_client.c
	$(MPICC) -fopenmp -c rng_client.c

ds_stack.o: ds_stack.c
	$(MPICC) -fopenmp -c ds_stack.c
