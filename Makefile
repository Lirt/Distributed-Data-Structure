
# USE export LDFLAGS="$LDFLAGS -lm" !!
# USE export LD_PRELOAD=/usr/lib/libhoard.so
# LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_sequential -d 1 -q 1

MPICC=mpicc -cc=/usr/local/bin/gcc
GCC=/usr/local/bin/gcc
#CC=$(GCC)
CC=$(MPICC)
HASHDIR=../uthash/src

DEB=-DDEBUG -DCOUNTERS
#DEB=-DCOUNTERS
#DEB=
DEBUG=-g
#DEBUG=-pg
#DEBUG=
WARN=-Wall
#STD=-std=c11
STD=-std=gnu11
PTHREAD=-pthread
#PTHREAD=-lpthread
#CFLAGS=$(STD) $(WARN) $(DEBUG) -D_POSIX_C_SOURCE=199309L -D_XOPEN_SOURCE=500 $(PTHREAD)
CFLAGS=$(STD) $(WARN) $(DEBUG) -D_POSIX_C_SOURCE=199309L -D_GNU_SOURCE $(PTHREAD)

#-L/usr/local/lib/gcc/x86_64-unknown-linux-gnu/

#LINK=
#LINK=-lpthread
#LINK=-lpthread -lm
#LINK=-lm -lprofiler
LINK=-lm -lgsl -lgslcblas

OBJDIR=obj
LIBDIR=lib

obj/distributed_queue_debug.o: lib/distributed_queue.c 
	$(CC) $(CFLAGS) $(DEB) -c lib/distributed_queue.c -o obj/distributed_queue_debug.o $(LINK)
obj/distributed_queue.o: lib/distributed_queue.c 
	$(CC) $(CFLAGS) -c lib/distributed_queue.c -o obj/distributed_queue.o $(LINK)

obj/queue_tester_sequential_debug.o: src/queue_tester_sequential.c
	$(CC) $(CFLAGS) $(DEB) -c src/queue_tester_sequential.c -o obj/queue_tester_sequential_debug.o $(LINK)
obj/queue_tester_sequential.o: src/queue_tester_sequential.c
	$(CC) $(CFLAGS) -c src/queue_tester_sequential.c -o obj/queue_tester_sequential.o $(LINK)

obj/queue_tester_debug.o: src/queue_tester.c
	$(CC) $(CFLAGS) $(DEB) -c src/queue_tester.c -o obj/queue_tester_debug.o $(LINK)
obj/queue_tester.o: src/queue_tester.c
	$(CC) $(CFLAGS) -c src/queue_tester.c -o obj/queue_tester.o $(LINK)

obj/queue_tester_equal_debug.o: src/queue_tester_equal.c
	$(CC) $(CFLAGS) $(DEB) -c src/queue_tester_equal.c -o obj/queue_tester_equal_debug.o $(LINK)
obj/queue_tester_equal.o: src/queue_tester_equal.c
	$(CC) $(CFLAGS) -c src/queue_tester_equal.c -o obj/queue_tester_equal.o $(LINK)

obj/queue_tester_rand_computation_debug.o: src/queue_tester_rand_computation.c
	$(CC) $(CFLAGS) $(DEB) -c src/queue_tester_rand_computation.c -o obj/queue_tester_rand_computation_debug.o $(LINK)
obj/queue_tester_rand_computation.o: src/queue_tester_rand_computation.c
	$(CC) $(CFLAGS) -c src/queue_tester_rand_computation.c -o obj/queue_tester_rand_computation.o $(LINK)

obj/queue_tester_insert_performance.o: src/queue_tester_insert_performance.c
	$(CC) $(CFLAGS) -c src/queue_tester_insert_performance.c -o obj/queue_tester_insert_performance.o $(LINK)

obj/queue_tester_remove_performance_debug.o: src/queue_tester_remove_performance.c
	$(CC) $(CFLAGS) $(DEB) -c src/queue_tester_remove_performance.c -o obj/queue_tester_remove_performance_debug.o $(LINK)
obj/queue_tester_remove_performance.o: src/queue_tester_remove_performance.c
	$(CC) $(CFLAGS) -c src/queue_tester_remove_performance.c -o obj/queue_tester_remove_performance.o $(LINK)

all: obj/distributed_queue.o obj/distributed_queue_debug.o obj/queue_tester_rand_computation.o obj/queue_tester_rand_computation_debug.o obj/queue_tester_insert_performance.o obj/queue_tester_remove_performance.o obj/queue_tester_remove_performance_debug.o obj/queue_tester_sequential.o obj/queue_tester_sequential_debug.o
	$(CC) $(CFLAGS) $(DEB) obj/distributed_queue_debug.o obj/queue_tester_rand_computation_debug.o -o bin/queue_tester_rand_computation_debug $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_rand_computation.o -o bin/queue_tester_rand_computation $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_insert_performance.o -o bin/queue_tester_insert_performance $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_remove_performance.o -o bin/queue_tester_remove_performance $(LINK)
	$(CC) $(CFLAGS) $(DEB) obj/distributed_queue_debug.o obj/queue_tester_remove_performance_debug.o -o bin/queue_tester_remove_performance_debug $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_sequential.o -o bin/queue_tester_sequential $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue_debug.o obj/queue_tester_sequential_debug.o -o bin/queue_tester_sequential_debug $(LINK)

tester_equal: obj/distributed_queue.o obj/distributed_queue_debug.o obj/queue_tester_equal.o obj/queue_tester_equal_debug.o
	$(CC) $(CFLAGS) $(DEB) obj/distributed_queue_debug.o obj/queue_tester_equal_debug.o -o bin/queue_tester_equal_debug $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_equal.o -o bin/queue_tester_equal $(LINK)

tester_rand_comp: obj/distributed_queue.o obj/distributed_queue_debug.o obj/queue_tester_rand_computation.o obj/queue_tester_rand_computation_debug.o
	$(CC) $(CFLAGS) $(DEB) obj/distributed_queue_debug.o obj/queue_tester_rand_computation_debug.o -o bin/queue_tester_rand_computation_debug $(LINK)
	$(CC) $(CFLAGS) obj/distributed_queue.o obj/queue_tester_rand_computation.o -o bin/queue_tester_rand_computation $(LINK)

clean:
	-rm -fv *.o obj/*.o bin/* log/*

