MPICC=mpicc -cc=/usr/local/bin/gcc
GCC=/usr/local/bin/gcc
#CC=$(GCC)
CC=$(MPICC)
HASHDIR=../uthash/src

DEB=-DDEBUG -DCOUNTERS
#DDEBUG=-DDEBUG
#DDEBUG=""
DEBUG=-g
WARN=-Wall
STD=-std=c11
CFLAGS=$(STD) $(WARN) $(DEBUG)
#CFLAGS=$(STD) $(WARN)
MATH=-lm

OBJDIR=obj
LIBDIR=lib

obj/distributed_queue_debug.o: lib/distributed_queue.c 
	$(CC) $(CFLAGS) -lpthread $(DEB) -c lib/distributed_queue.c -o obj/distributed_queue_debug.o
obj/distributed_queue.o: lib/distributed_queue.c 
	$(CC) $(CFLAGS) -lpthread -c lib/distributed_queue.c -o obj/distributed_queue.o

obj/queue_tester_debug.o: src/queue_tester.c
	$(CC) $(CFLAGS) -lpthread $(DEB) -c src/queue_tester.c -o obj/queue_tester_debug.o $(MATH)
obj/queue_tester.o: src/queue_tester.c
	$(CC) $(CFLAGS) -lpthread -c src/queue_tester.c -o obj/queue_tester.o $(MATH)

obj/queue_tester_equal_debug.o: src/queue_tester_equal.c
	$(CC) $(CFLAGS) -lpthread $(DEB) -c src/queue_tester_equal.c -o obj/queue_tester_equal_debug.o $(MATH)
obj/queue_tester_equal.o: src/queue_tester_equal.c
	$(CC) $(CFLAGS) -lpthread -c src/queue_tester_equal.c -o obj/queue_tester_equal.o $(MATH)

obj/queue_tester_rand_computation_debug.o: src/queue_tester_rand_computation.c
	$(CC) $(CFLAGS) -lpthread $(DEB) -c src/queue_tester_equal.c -o obj/queue_tester_equal_debug.o $(MATH)
obj/queue_tester_rand_computation.o: src/queue_tester_equal.c
	$(CC) $(CFLAGS) -lpthread -c src/queue_tester_equal.c -o obj/queue_tester_equal.o $(MATH)

all: obj/distributed_queue.o obj/queue_tester.o obj/queue_tester_equal.o obj/queue_tester_rand_computation obj/queue_tester_debug.o obj/queue_tester_equal_debug.o obj/queue_tester_rand_computation_debug
	$(CC) $(CFLAGS) -lpthread $DEB obj/distributed_queue_debug.o obj/queue_tester.o -o bin/queue_tester_debug
	$(CC) $(CFLAGS) -lpthread $DEB obj/distributed_queue_debug.o obj/queue_tester_equal.o -o bin/queue_tester_equal_debug
	$(CC) $(CFLAGS) -lpthread $DEB obj/distributed_queue_debug.o obj/queue_tester_rand_computation.o -o bin/queue_tester_rand_computation_debug
	$(CC) $(CFLAGS) -lpthread obj/distributed_queue.o obj/queue_tester.o -o bin/queue_tester
	$(CC) $(CFLAGS) -lpthread obj/distributed_queue.o obj/queue_tester_equal.o -o bin/queue_tester_equal
	$(CC) $(CFLAGS) -lpthread obj/distributed_queue.o obj/queue_tester_rand_computation.o -o bin/queue_tester_rand_computation

tester_equal: obj/distributed_queue.o obj/distributed_queue_debug.o obj/queue_tester_equal.o obj/queue_tester_equal_debug.o
	$(CC) $(CFLAGS) -lpthread $(DEB) obj/distributed_queue_debug.o obj/queue_tester_equal_debug.o -o bin/queue_tester_equal_debug
	$(CC) $(CFLAGS) -lpthread obj/distributed_queue.o obj/queue_tester_equal.o -o bin/queue_tester_equal

tester_rand_comp: obj/distributed_queue.o obj/distributed_queue_debug.o obj/queue_tester_rand_computation.o obj/queue_tester_rand_computation_debug
	$(CC) $(CFLAGS) -lpthread $(DEB) obj/distributed_queue_debug.o obj/queue_tester_rand_computation_debug.o -o bin/queue_tester_rand_computation_debug
	$(CC) $(CFLAGS) -lpthread obj/distributed_queue.o obj/queue_tester_rand_computation.o -o bin/queue_tester_rand_computation

clean:
	-rm -fv *.o obj/*.o bin/* log/*

