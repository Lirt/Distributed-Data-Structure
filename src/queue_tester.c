#include <math.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_STACK_H
   #define DS_STACK_H
	#include "../include/distributed_stack.h"
#endif

#ifndef DS_QUEUE_H
   #define DS_QUEUE_H
	#include "../include/distributed_queue.h"
#endif

#ifndef MPI_H
   #define MPI_H
	#include "mpi.h"
#endif

#ifndef STDLIB_H
   #define STDLIB_H
	#include <stdlib.h>
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
	#include <pthread.h>
#endif

int generateRandomNumber(int rangeMin, int rangeMax) {
	
	int r = rand() % rangeMax + rangeMin;
	return r;

}

void *producer(void *threadid) {
   
   long *tid = threadid;
   printf("hello producer T%ld\n", *tid);
   FILE *in = fopen("in.txt", "ab+");
   int add_count = 0;
   
   for (int i = 0; i < 1000000; i++) {
      int *rn = (int*) malloc (sizeof(int));
      *rn = generateRandomNumber(1,1000000);
      
      lockfree_queue_insert_item_by_tid (tid, rn);
      if ( fprintf(in, "%d\n", *rn) < 0 ) 
         printf("ERROR: fprintf failed\n");
      add_count++;
   }
   
   printf("add_count=%d\n", add_count);
   
   fclose(in);
   pthread_exit(NULL);
   
}

void *consumer(void *threadid) {
   
   long *tid = threadid;
   *tid = *tid - 1;
   
   int *retval;
   int timeout = 0;
   FILE *out = fopen("out.txt", "ab+");
   printf("hello consumer T%ld\n", *tid);
   
   /*while (!lockfree_queue_empty()) {
   
   }*/

   long ret_null_count = 0;
   int rem_count = 0;
   
   //for (int i = 0; i < 1000000; i++) {
   while (rem_count < 1000000) {
      retval = lockfree_queue_remove_item_by_tid (tid, timeout);
      if (retval != NULL) {
         if ( fprintf(out, "%d\n", *retval) < 0 )
            printf("ERROR: fprintf failed\n");
         rem_count++;
      }
      else {
         ret_null_count++;
      }
   }
      
   printf("removed=%d - ret_null_count=%ld\n", rem_count, ret_null_count);
   
   fclose(out);
   pthread_exit(NULL);
   
}

int main(int argc, char** argv) {
   
   
   //struct ds_lockfree_queue *queue
   lockfree_queue_init();
   
   pthread_t threads[2];
   long t_num[2];
   pthread_attr_t attr;
   pthread_attr_init(&attr);
   pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   
   int rc;
   t_num[0] = 0;
   rc = pthread_create(&threads[0], NULL, producer, &t_num[0]);
   if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
   }
   t_num[1] = 1;
   rc = pthread_create(&threads[1], NULL, consumer, &t_num[1]);
   if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
   } 
 
   void *status;
   pthread_join(threads[0], &status);
   pthread_join(threads[1], &status);
   pthread_attr_destroy(&attr);
   
   
   pthread_exit(NULL);
	return 0;
}
