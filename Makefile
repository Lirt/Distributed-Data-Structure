MPICC=mpicc
GCC=gcc
CC=$(GCC)

OBJDIR=obj
LIBDIR=lib

obj/distributed_stack.o: lib/distributed_stack.c
	$(CC) -std=c11 -Wall -lpthread -DDEBUG -c lib/distributed_stack.c -o obj/distributed_stack.o

obj/distributed_queue.o: lib/distributed_queue.c
	$(CC) -std=c11 -Wall -lpthread -DDEBUG -c lib/distributed_queue.c -o obj/distributed_queue.o

#obj/queue_tester.o: src/queue_tester.c
#	$(CC) -std=c11 -Wall -lpthread -DDEBUG -c src/queue_tester.c -o obj/queue_tester.o

obj/queue_tester_callback.o: src/queue_tester_callback.c
	$(CC) -std=c11 -Wall -lpthread -DDEBUG -c src/queue_tester_callback.c -o obj/queue_tester_callback.o

#$(OBJDIR)/%.o: $(LIBDIR)/%.c
#	$(CC) -c -o $@ $<
	
#all: obj/distributed_queue.o obj/queue_tester.o obj/queue_tester_callback.o
all: obj/distributed_queue.o obj/queue_tester_callback.o
	#$(CC) -std=c11 -Wall -lpthread -DDEBUG obj/distributed_queue.o obj/queue_tester.o -o bin/queue_tester
	$(CC) -std=c11 -Wall -lpthread -DDEBUG obj/distributed_queue.o obj/queue_tester_callback.o -o bin/queue_tester_callback	

clean:
	-rm -fv *.o obj/*.o rm bin/*
