
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

#INCLUDES=-Iinclude/

install: lib/distributed_queue.c
	$(CC) $(CFLAGS) $(DEB) -shared -o libdq_debug.so -fPIC lib/distributed_queue.c
	$(CC) $(CFLAGS) -shared -o libdq.so -fPIC lib/distributed_queue.c
	cp libdq_debug.so /usr/lib/
	cp libdq.so /usr/lib/
	cp libhoard.so /usr/lib/
	#cp libdq_debug.so /usr/lib64/
	#cp libdq.so /usr/lib64/
	#cp libdq_debug.so /usr/local/lib/
	#cp libdq.so /usr/local/lib/
	#cp libdq_debug.so /usr/local/lib64/
	#cp libdq.so /usr/local/lib64/
	cp include/distributed_queue_api.h /usr/include/
	#cp include/distributed_queue_api.h /usr/local/include/

all: src/queue_tester_rand_computation.c src/queue_tester_sequential.c src/queue_tester.c src/queue_tester_equal.c src/queue_tester_insert_performance.c src/queue_tester_remove_performance.c
	$(CC) $(CFLAGS) $(DEB) -o bin/queue_tester_sequential_debug src/queue_tester_sequential.c  $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_sequential src/queue_tester_sequential.c  $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o bin/queue_tester_rand_computation_debug src/queue_tester_rand_computation.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester_rand_computation src/queue_tester_rand_computation.c $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o bin/queue_tester_debug src/queue_tester.c $(LINK)
	$(CC) $(CFLAGS) -o bin/queue_tester src/queue_tester.c $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o obj/queue_tester_equal_debug src/queue_tester_equal.c $(LINK)
	$(CC) $(CFLAGS) -o obj/queue_tester_equal src/queue_tester_equal.c  $(LINK)
	$(CC) $(CFLAGS) -o obj/queue_tester_insert_performance src/queue_tester_insert_performance.c $(LINK)
	$(CC) $(CFLAGS) $(DEB) -o obj/queue_tester_remove_performance_debug src/queue_tester_remove_performance.c $(LINK)
	$(CC) $(CFLAGS) -o obj/queue_tester_remove_performance src/queue_tester_remove_performance.c $(LINK)

clean:
	-rm -fv *.o obj/*.o bin/*

