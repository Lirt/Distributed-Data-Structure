MPICC=mpicc
GCC=gcc

all: rng_client.o rng_master.o ds_stack.o queue_tester.o queue_tester_callback.o
	#$(MPICC) -fopenmp rng_client.o ds_stack.o -o rng_client
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG rng_client.o ds_stack.o -o bin/rng_client
	$(MPICC) -std=c11 -Wall -DDEBUG rng_master.o -o bin/rng_master
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG queue_tester.o ds_stack.o -o bin/queue_tester
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG ds_stack.o queue_tester_callback.o -o bin/queue_tester_callback

rng_master.o: src/rng_master.c
	$(MPICC) -std=c11 -Wall -DDEBUG  -c src/rng_master.c

rng_client.o: src/rng_client.c
	#$(MPICC) -fopenmp -c rng_client.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c src/rng_client.c

ds_stack.o: lib/ds_stack.c
	#$(MPICC) -fopenmp -c ds_stack.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c lib/ds_stack.c

queue_tester.o: src/queue_tester.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c src/queue_tester.c

queue_tester_callback.o: src/queue_tester_callback.c
	$(MPICC) -std=c11 -Wall -lpthread -DDEBUG -c src/queue_tester_callback.c

clean:
	-rm *.o obj/*.o rm bin/*
