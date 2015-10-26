MPICC=mpicc
GCC=gcc

all: rng_client.o ds_stack.o
	#$(MPICC) -fopenmp rng_client.o ds_stack.o -o rng_client
	$(MPICC) -lpthread -DDEBUG rng_client.o ds_stack.o -o rng_client
	$(MPICC) --DDEBUG rng_master.o -o rng_master

rng_master.o: rng_master.c
   $(MPICC) -DDEBUG -c rng_master.c

rng_client.o: rng_client.c
	#$(MPICC) -fopenmp -c rng_client.c
	$(MPICC) -lpthread -DDEBUG -c rng_client.c

ds_stack.o: ds_stack.c
	#$(MPICC) -fopenmp -c ds_stack.c
	$(MPICC) -lpthread -DDEBUG -c ds_stack.c
