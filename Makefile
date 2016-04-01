MPICC=mpicc
GCC=gcc
#CC=$(GCC)
CC=$(MPICC)
HASHDIR=../uthash/src

DDEBUG=-DDEBUG
#DDEBUG=""
DEBUG=-g
WARN=-Wall
STD=-std=c11
CFLAGS=$(STD) $(WARN) $(DEBUG)
#CFLAGS=$(STD) $(WARN)

OBJDIR=obj
LIBDIR=lib

obj/distributed_queue.o: lib/distributed_queue.c 
	$(CC) $(CFLAGS) -lpthread -DDEBUG -c lib/distributed_queue.c -o obj/distributed_queue.o

obj/queue_tester_callback.o: src/queue_tester_callback.c
	$(CC) $(CFLAGS) -lpthread -DDEBUG -c src/queue_tester_callback.c -o obj/queue_tester_callback.o

#obj/queue_tester_callback_2.o: src/queue_tester_callback_2.c
	#$(CC) $(CFLAGS) -lpthread -DDEBUG -c src/queue_tester_callback_2.c -o obj/queue_tester_callback_2.o

#$(OBJDIR)/%.o: $(LIBDIR)/%.c
#	$(CC) -c -o $@ $<
	
all: obj/distributed_queue.o obj/queue_tester_callback.o obj/queue_tester_callback_2.o 
	$(CC) $(CFLAGS) -lpthread -DDEBUG obj/distributed_queue.o obj/queue_tester_callback.o -o bin/queue_tester_callback
	#$(CC) $(CFLAGS) -lpthread -DDEBUG obj/distributed_queue.o obj/queue_tester_callback_2.o -o bin/queue_tester_callback_2

clean:
	-rm -fv *.o obj/*.o bin/* log/*
