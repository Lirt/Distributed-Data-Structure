
#To run with libhoard and gsl random functions use LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 <program>

MPICC=mpicc -cc=/usr/local/bin/gcc
GCC=/usr/local/bin/gcc
CC=$(MPICC)

DEB=-DDEBUG -DCOUNTERS
DEBUG=-g
WARN=-Wall
STD=-std=gnu11
PTHREAD=-pthread
CFLAGS=$(STD) $(WARN) $(DEBUG) -D_POSIX_C_SOURCE=199309L -D_GNU_SOURCE $(PTHREAD)

#link -lprofiler for gprof
LINK=-lm -lgsl -lgslcblas -ldq

INCLUDES=-I. -Iinclude

install: 
	$(CC) $(CFLAGS) $(DEB) $(INCLUDES) -fPIC -shared src/distributed_queue.c -o lib/libdq_debug.so
	$(CC) $(CFLAGS) $(INCLUDES) -shared -fPIC src/distributed_queue.c -o lib/libdq.so
	cp lib/libdq_debug.so /usr/lib/
	cp lib/libdq.so /usr/lib/
	cp lib/libhoard.so /usr/lib/
	cp lib/libdq_debug.so /usr/lib64/
	cp lib/libdq.so /usr/lib64/
	cp lib/libdq_debug.so /usr/local/lib/
	cp lib/libdq.so /usr/local/lib/
	cp lib/libdq_debug.so /usr/local/lib64/
	cp lib/libdq.so /usr/local/lib64/
	cp include/distributed_queue_api.h /usr/include/
	cp include/distributed_queue_api.h /usr/local/include/

#all: tests/queue_tester_rand_computation.c tests/queue_tester_sequential.c tests/queue_tester_insert_performance.c tests/queue_tester_remove_performance.c
all: 
	$(CC) $(CFLAGS) $(DEB)  -o bin/queue_tester_sequential_debug  tests/queue_tester_sequential.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_sequential tests/queue_tester_sequential.c  $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o bin/queue_tester_rand_computation_debug tests/queue_tester_rand_computation.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_rand_computation tests/queue_tester_rand_computation.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_insert_performance tests/queue_tester_insert_performance.c $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o bin/queue_tester_remove_performance_debug tests/queue_tester_remove_performance.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_remove_performance tests/queue_tester_remove_performance.c $(LINK)

clean:
	-rm -fv *.o obj/*.o bin/*

