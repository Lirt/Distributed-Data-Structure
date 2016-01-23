#include <math.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

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
	#include "/usr/include/openmpi-x86_64/mpi.h"
#endif

#ifndef STDLIB_H
   #define STDLIB_H
	#include <stdlib.h>
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
	#include <pthread.h>
#endif

#ifndef UNISTD_H
   #define UNISTD_H
   #include <unistd.h>
#endif



int generateRandomNumber(int rangeMin, int rangeMax) {
	
	int r = rand() % rangeMax + rangeMin;
	return r;

}

void *work(void *arg_struct) {
   
   struct q_args *args = arg_struct;
   long *tid = args->tid;
   printf("hello from work - T%ld\n", *tid);
   
   
   char filename_ins[20] = "log/work_";
   char filename_rm[20] = "log/work_";
   char tid_str[4];
   char ins[3] = "ins";
   char rm[4] = "rm";
   
   sprintf(tid_str, "%ld", *tid);
   strcat(filename_ins, tid_str);
   strcat(filename_rm, tid_str);
   
   strcat(filename_ins, ins);
   //printf("filename_in='%s'\n", filename_in);
   FILE *work_file_ins = fopen(filename_ins, "wb");
   
   strcat(filename_rm, rm);
   //printf("filename_out='%s'\n", filename_out);
   FILE *work_file_rm = fopen(filename_rm, "wb");
   
   int timeout = 0;

   fprintf(work_file_ins, "hello from work thread - T%ld\n", *tid);
   fprintf(work_file_rm, "hello from work thread - T%ld\n", *tid);

   /*
    *TODO ranges for random numbers should be lowNum and 
    *highNum and unique for all queues and should be set by parameter or config
    */
   //TODO FIX file output -- too big
   //TODO FIX empty queues
   //

   unsigned long sum = 0;
   unsigned long n_inserted = 0;
   unsigned long n_removed = 0;

   int *rn;
   int *retval;
   
   rn = (int*) malloc (sizeof(int));
   *rn = generateRandomNumber(0,3);
   lockfree_queue_insert_item_by_tid (tid, rn);
   if ( fprintf(work_file_ins, "%d\n", *rn) < 0 ) 
      printf("ERROR: fprintf failed\n");
   printf("Queue %ld started with number %d\n", *tid, *rn);
   
   int programDuration = 1;
   int endTime;
   int startTime = (int) time(NULL);
   printf("Start time for thread %ld is %d\n", *tid, startTime);

   while(1) {
      retval = (int*) malloc (sizeof(int));
      retval = lockfree_queue_remove_item_by_tid (tid, timeout);
      
      if (retval == NULL) {
         endTime = (int) time(NULL) - startTime;
         if (endTime >= programDuration) {
            printf("Time is up, endTime = %d\n", endTime);
            printf("Inserted %lu items\n", n_inserted);
            break;
         }
         else {
            continue;
         }
      }

      if ( fprintf(work_file_rm, "%d\n", *retval) < 0 ) 
         printf("ERROR: fprintf failed\n");
      
      if (retval != NULL) {
         n_removed++;

         for(int i = 0; i < *retval; i++) {
            rn = (int*) malloc (sizeof(int));
            *rn = generateRandomNumber(1,3);

            //printf("Q%ld inserting %d\n", *tid, *rn);
            lockfree_queue_insert_item_by_tid (tid, rn);
            if ( fprintf(work_file_ins, "%d\n", *rn) < 0 ) 
               printf("ERROR: fprintf failed\n");
            n_inserted++;
         }
      }
      //end after N seconds
      endTime = (int) time(NULL) - startTime;
      if (endTime >= programDuration) {
         printf("Time is up, endTime = %d\n", endTime);
         printf("Inserted %lu items\n", n_inserted);
         break;
      }
   }

   printf("Summing queue %ld\n", *tid);
   //fprintf(work_file_rm, "Summing queue %ld\n", *tid);
   
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
   
      
   while( !lockfree_queue_is_empty(tid) ) {
      retval = lockfree_queue_remove_item_by_tid(tid, timeout);
      if (retval != NULL) {
         n_removed++;
         sum += *retval;
      }
   }
   

   //TODO change test program to empty more often then get more items and again empty more 

   //remove != NULL can be bad condition
   //while( (retval = lockfree_queue_remove_item_by_tid(tid, timeout)) != NULL ) {
   //   n_removed++;
   //   sum += *retval;
      
      /*if ( fprintf(work_file_out, "%d\n", *retval) < 0 ) 
         printf("ERROR: fprintf failed\n");*/
   //}
   //TODO 
   printf("Removed %lu numbers from Q%lu\n", n_removed, *tid);
   printf("Sum of Q%lu is %lu\n", *tid, sum);
   
   fclose(work_file_ins);
   fclose(work_file_rm);
   pthread_exit(NULL);
   
}


int main(int argc, char** argv) {
   
   setbuf(stdout, NULL);
   lockfree_queue_init_callback(work, NULL);
   //void *status;
   
   pthread_exit(NULL);
	return 0;
}
