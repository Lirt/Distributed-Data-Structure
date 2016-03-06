#include <math.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

/*
#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif
*/

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

#include <sys/types.h>
#include <sys/stat.h>

atomic_ulong finished;


int *generateRandomNumber(int rangeMin, int rangeMax) {
	
   int *r;
   r = (int*) malloc (sizeof(int));
   if (r == NULL)
      printf("ERROR: Malloc failed\n");
	*r = rand() % rangeMax + rangeMin;
	return r;

}

void *work(void *arg_struct) {
   
   struct q_args *args = arg_struct;
   long *tid = args->tid;
   //int q_count = args->q_count;
   //int t_count = args->t_count;
   unsigned long pthread_tid = pthread_self();
   printf("Hello from work - T%ld with TID %ld\n", *tid, pthread_tid);
   
   struct stat st = {0};
   if (stat("/tmp/distributed_queue", &st) == -1) {
      mkdir("/tmp/distributed_queue", 0777);
   }

   char filename_ins[40] = "/tmp/distributed_queue/work_";
   char filename_rm[40] = "/tmp/distributed_queue/work_";
   char tid_str[4];
   char ins[4] = "ins";
   char rm[3] = "rm";
   
   sprintf(tid_str, "%ld", *tid);
   strcat(filename_ins, tid_str);
   strcat(filename_rm, tid_str);
   
   strcat(filename_ins, ins);
   //printf("filename_in='%s'\n", filename_in);
   FILE *work_file_ins = fopen(filename_ins, "wb");
   if ( work_file_ins == NULL ) {
      printf("ERROR: error in opening file %s\n", filename_ins);
      exit(-1);
   }
   
   strcat(filename_rm, rm);
   //printf("filename_out='%s'\n", filename_out);
   FILE *work_file_rm = fopen(filename_rm, "wb");
   if ( work_file_rm == NULL ) {
      printf("ERROR: error in opening file %s\n", filename_rm);
      exit(-1);
   }
   
   int timeout = 0;

   //Ranges for random numbers
   unsigned long lowRange = 1;
   unsigned long highRange = *tid + 2;
   
   if ( fprintf(work_file_ins, "hello from insertion work thread - T%ld, my range is %ld-%ld\n", *tid, lowRange, highRange) < 0 )
      printf("ERROR: cannot write to file %s\n", filename_ins);
   if ( fprintf(work_file_rm, "hello from removing work thread - T%ld, my range is %ld-%ld\n", *tid, lowRange, highRange) < 0 )
      printf("ERROR: cannot write to file %s\n", filename_ins);

   unsigned long sum = 0;
   unsigned long n_inserted = 0;
   unsigned long n_removed = 0;

   int *rn;
   int *retval;
   
   int programDuration = 7;
   int endTime;
   int startTime = (int) time(NULL);
   printf("Start time for thread %ld is %d\n", *tid, startTime);

   if ( *tid % 2 == 0 ) {
      //PRODUCER
      while(1) {
         rn = generateRandomNumber(lowRange, highRange);

         lockfree_queue_insert_item (rn);
         if ( fprintf(work_file_ins, "%d\n", *rn) < 0 )
            printf("ERROR: fprintf failed\n");
         n_inserted++;

         //end after N seconds
         endTime = (int) time(NULL) - startTime;
         if (endTime >= programDuration) {
            printf("Time is up, endTime = %d\n", endTime);
            printf("Thread %ld Inserted %lu items\n", *tid, n_inserted);

            atomic_fetch_add( &finished, 1);
            fclose(work_file_ins);
            fclose(work_file_rm);
            pthread_exit(NULL);
            //break;
         }
      }

   }
   else {
      //CONSUMER
      //sleep(1);

      //unsigned long* sizes = lockfree_queue_size_total_consistent_allarr();
      printf("\n");
      printf("Starting consumer (T%ld) at time %d\n", *tid, (int) time(NULL));
      /*for (int i = 0; i < q_count; i++) {
         printf("Q%d size is %ld\n", i, sizes[i]);
      }*/

      while(1) {
         //retval = (int*) malloc (sizeof(int));
         retval = lockfree_queue_remove_item(timeout);

         if (retval == NULL) {
            //unsigned long* sizes = lockfree_queue_size_total_consistent_allarr();
            /*
            printf("\n");
            for (int i = 0; i < q_count; i++) {
               printf("Q%d size is %ld\n", i, sizes[i]);
            }
            */
            unsigned long size = lockfree_queue_size_total_consistent();
            if (size == 0) {
               //printf("Total Q%ld size is %ld\n", *tid, size);
               unsigned long fin = atomic_load( &finished );
               if ( fin != 2 )
                  continue;
               else {
                  printf("All insertion threads finished. Queues are empty - quitting consumer threads\n");
                  break;
               }
            }
         }
         else {
            if ( fprintf(work_file_rm, "%d\n", *retval) < 0 ) 
               printf("ERROR: fprintf failed\n");
            n_removed++;
            sum += *retval;
         }
      }
   }

   printf("Thread %ld inserted %lu numbers\n", *tid, n_inserted);
   printf("Thread %ld removed %lu numbers\n", *tid, n_removed);
   printf("Sum of thread %ld items is %lu\n", *tid, sum);
   
   fclose(work_file_ins);
   fclose(work_file_rm);
   //lockfree_queue_destroy();
   lockfree_queue_stop_watcher();
   pthread_exit(NULL);
   
}


int main(int argc, char** argv) {
   
   setbuf(stdout, NULL);
   atomic_init( &finished, 0 );
   lockfree_queue_init_callback(work, NULL, 2, TWO_TO_ONE);
   //void *status;
   
   pthread_exit(NULL);
	return 0;
}
