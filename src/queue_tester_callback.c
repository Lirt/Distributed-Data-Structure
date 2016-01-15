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

#include<string.h>

int generateRandomNumber(int rangeMin, int rangeMax) {
	
	int r = rand() % rangeMax + rangeMin;
	return r;

}

void *work(void *arg_struct) {
   
   struct q_args *args = arg_struct;
   long *tid = args->tid;
   printf("hello from work - T%ld\n", *tid);
   
   
   char filename_in[20] = "work_";
   char filename_out[20] = "work_";
   char tid_str[4];
   char in[3] = "in";
   char out[4] = "out";
   
   sprintf(tid_str, "%ld", *tid);
   strcat(filename_in, tid_str);
   strcat(filename_out, tid_str);
   
   strcat(filename_in, in);
   printf("filename_in='%s'\n", filename_in);
   FILE *work_file_in = fopen(filename_in, "ab+");
   
   strcat(filename_out, out);
   printf("filename_out='%s'\n", filename_out);
   FILE *work_file_out = fopen(filename_out, "ab+");
   
   int i = 0;
   int j = 0;
   int timeout = 0;
   
   /*
    *TODO ranges for random numbers should be lowNum and 
    *highNum and unique for all queues and should be set by parameter or config
    */

   int *rn = (int*) malloc (sizeof(int));
   int *retval;
   unsigned long sum = 0;
   unsigned long n_inserted = 0;
   unsigned long n_removed = 0;

   *rn = generateRandomNumber(0,3);
   lockfree_queue_insert_item_by_tid (tid, rn);
   if ( fprintf(work_file_in, "%d\n", *rn) < 0 ) 
      printf("ERROR: fprintf failed\n");
   printf("Queue %d started with number %d\n", *tid, *rn);
   
   
   int programDuration = 10;
   int endTime;
   int startTime = (int) time(NULL);
   printf("Start time for thread %d is %d\n", *tid, startTime);
   
   while(1) {
      retval = lockfree_queue_remove_item_by_tid (tid, timeout);
      if ( fprintf(work_file_in, "%d\n", *retval) < 0 ) 
         printf("ERROR: fprintf failed\n");
      
      if (retval != NULL) {
         for(i = 0; i < *retval; i++) {
            *rn = generateRandomNumber(1,3);
            lockfree_queue_insert_item_by_tid (tid, rn);
            n_inserted++;
         }
      }
      
      //end after N seconds
      endTime = (int) time(NULL) - startTime;
      if (endTime >= 10)
         printf("Time is up, endTime = %d\n", endTime);
         printf("Inserted %lu items\n", n_inserted);
         break;

      }
   }

   printf("Summing queue %d\n", *tid);
   fprintf(work_file_out, "Summing queue %d\n", *tid);
   
   /*
   while(1) {
      retval = lockfree_queue_remove_item_by_tid (tid, timeout);

      if (retval != NULL) {
         sum += *retval;
         if ( fprintf(work_file_out, "%d\n", *retval) < 0 ) 
            printf("ERROR: fprintf failed\n");
         }
      }
      else {
         printf("Last number\n");
         break;
      }
   }
   */
   
   /*   
   while( !lockfree_queue_empty(tid) ) {
      retval = lockfree_queue_remove_item_by_tid(tid, timeout));
      if (retval != NULL) {
         sum += *retval;
      }
   }
   */

   //remove != NULL can be bad condition
   while( (retval = lockfree_queue_remove_item_by_tid(tid, timeout)) != NULL ) {
      n_removed++;
      sum += *retval;
      
      if ( fprintf(work_file_out, "%d\n", *retval) < 0 ) 
         printf("ERROR: fprintf failed\n");
   }
   
   printf("Removed %lu numbers from Q%lu\n", n_removed, *tid);
   printf("Sum of Q%lu is %lu\n", *tid, sum);
   
   fclose(work_file_in);
   fclose(work_file_out);
   pthread_exit(NULL);
   
}


int main(int argc, char** argv) {
   
   lockfree_queue_init_callback(work, NULL);
   //void *status;
   
   pthread_exit(NULL);
	return 0;
}
