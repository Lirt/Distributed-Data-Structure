MPICC=mpicc
GCC=gcc

all: rng_client.o rng_master.o ds_stack.o queue_tester.o
	#$(MPICC) -fopenmp rng_client.o ds_stack.o -o rng_client
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG rng_client.o ds_stack.o -o rng_client
	$(MPICC) -std=c11 -Wall -DDEBUG rng_master.o -o rng_master
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG queue_tester.o ds_stack.o -o queue_tester

rng_master.o: rng_master.c
	$(MPICC) -std=c11 -Wall -DDEBUG -c rng_master.c

rng_client.o: rng_client.c
	#$(MPICC) -fopenmp -c rng_client.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c rng_client.c

ds_stack.o: ds_stack.c
	#$(MPICC) -fopenmp -c ds_stack.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c ds_stack.c
	
queue_tester.o: queue_tester.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c queue_tester.c
