
#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_QUEUE_H
   #define DS_QUEUE_H
	#include "../include/distributed_queue.h"
#endif

#ifndef MPI_H
   #define MPI_H
	#include "mpi.h"
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
	#include <pthread.h>
#endif

#ifndef STDLIB_H
   #define STDLIB_H
	#include <stdlib.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

#ifndef UNISTD_H
   #define UNISTD_H
   #include <unistd.h>
#endif

#ifndef SYSINFO_H
   #define SYSINFO_H
   #include <sys/sysinfo.h>
#endif

#ifndef STDATOMIC_H
   #define STDATOMIC_H
   #include <stdatomic.h>
#endif

#ifndef LIMITS_H
   #define LIMITS_H
   #include <limits.h>
#endif

/*****
 * Lock-free queue
 ***
 *
 *
 * GLOBAL VARIABLES
 */

struct ds_lockfree_queue **queues;
int queue_count = 0;

pthread_attr_t attr;
pthread_t *callback_threads;
long **tids;

pthread_mutex_t *add_mutexes;
pthread_mutex_t *rm_mutexes;

pthread_mutex_t load_balance_mutex;
pthread_cond_t load_balance_cond;
pthread_t load_balancing_t;
pthread_t qsize_watcher_t;

unsigned long *qsize_history = NULL;
long q_threshold = 1000;


//http://stackoverflow.com/questions/19197836/algorithm-to-evenly-distribute-values-into-containers
//http://stackoverflow.com/questions/15258908/best-strategy-to-distribute-number-into-groups-evenly
//https://en.wikipedia.org/wiki/Partition_problem
long *load_bal_src;
long *load_bal_dest;
long *load_bal_amount;

/****
 * 
 * Functions
 * 
 **/

void lockfree_queue_destroy () {
   
   //TODO test
   //TODO handle errors and return bool true/false
   
   pthread_attr_destroy(&attr);
   
   for (int i = 0; i < queue_count; i++) {
      pthread_cancel(callback_threads[i]);
   }
   
   for (int i = 0; i < queue_count; i++) {
      lockfree_queue_free(tids[i]);
   }
   
   for (int i = 0; i < queue_count; i++) {
      pthread_mutex_destroy(&add_mutexes[i]);
      pthread_mutex_destroy(&rm_mutexes[i]);
   }
   
   //pthread_condattr_destroy (attr);
   pthread_cond_destroy (&load_balance_cond);
   pthread_mutex_destroy (&load_balance_mutex);
      
   free(queues);
   queue_count = 0;
   
   free(add_mutexes);
   free(rm_mutexes);
   free(callback_threads);
   
   free(tids);
   
}


bool lockfree_queue_is_empty(void* tid) {
   
   //TODO test
   
   long *t = tid;
   
   if ( atomic_load( &(queues[*t % queue_count]->a_qsize) ) == 0 ) {
      return true;
   }
   
   return false;
   
}


bool lockfree_queue_is_empty_all() {

   //TODO test
   //Needs lock on all or
   //would be eventually consistent
   
   for (int i = 0 ; i < queue_count; i++) {
      if ( atomic_load( &(queues[i]->a_qsize) ) != 0 ) {
         return false;
      }
   }
   
   return true;
   
}


/*void lockfree_queue_init (void) {
   
   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
/*   int i = 0;
   queue_count = get_nprocs();
  
   printf("Number of cpus by get_nprocs is : %d\n", queue_count);
   
   queues = (struct ds_lockfree_queue**) malloc ( queue_count * sizeof(struct ds_lockfree_queue) );
   for (i = 0; i < queue_count; i++) {
      queues[i] = (struct ds_lockfree_queue*) malloc ( sizeof(struct ds_lockfree_queue) );
   }
   
   for (i = 0; i < queue_count; i++) {
      queues[i]->head = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      queues[i]->tail = queues[i]->head;
      queues[i]->divider = queues[i]->head;
      //queues[i]->size = 0;
      atomic_init( &(queues[i]->a_qsize), 0 );
   }
   
   printf("Queues initialized\n");
   
}*/


void lockfree_queue_init_callback (void* (*callback)(void *args), void* arguments) {
   
   //TODO documentation must contain struct used for arguments in thread callback
   
   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
   int i = 0;
   queue_count = get_nprocs();
  
   printf("Number of cpus by get_nprocs is : %d\n", queue_count);
   
   queues = (struct ds_lockfree_queue**) malloc ( queue_count * sizeof(struct ds_lockfree_queue) );   //TODO sizeof(struct *ds_lockfree_queue)?
   for (i = 0; i < queue_count; i++) {
      queues[i] = (struct ds_lockfree_queue*) malloc ( sizeof(struct ds_lockfree_queue) );
   }
   
   for (i = 0; i < queue_count; i++) {
      queues[i]->head = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      queues[i]->tail = queues[i]->head;
      queues[i]->divider = queues[i]->head;
      atomic_init( &(queues[i]->a_qsize), 0 );
   }
   
   printf("Queues initialized\n");
   
   /*
    * Setup mutexes
    */

   add_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
   rm_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
   for (int i = 0; i < queue_count; i++) {
      pthread_mutex_init(&add_mutexes[i]);
      pthread_mutex_init(&rm_mutexes[i]);
   }
   
   
   /*
    * Initialize threads to callback function
    */
   
   pthread_t *callback_threads = (pthread_t*) malloc (queue_count * sizeof(pthread_t));
   //long **tids;
   tids = (long**) malloc (queue_count * sizeof(long));
   
   //TODO NOT SURE - argument structure doesnt have to be unique for all thread callbacks(use one structure for all callbacks). tids are different, so maybe should be unique
   struct q_args **q_args_t;
   q_args_t = (struct q_args**) malloc (queue_count * sizeof(struct q_args));
   
   pthread_attr_init(&attr);
   pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   
   int rc;
   
   for (i = 0; i < queue_count; i++) {
      
      tids[i] = (long*) malloc ( sizeof(long));
      *tids[i] = i;
      q_args_t[i] = (struct q_args*) malloc (sizeof(struct q_args));
      q_args_t[i]->args = arguments;
      q_args_t[i]->tid = tids[i];
      
      rc = pthread_create(&callback_threads[i], NULL, callback, q_args_t[i]);
      if (rc) {
         printf("ERROR: return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
      
   }
   
   printf("%d Threads to callbacks initialized\n", queue_count);
   
   /*
    * Initialize load balancer
    */ 
   
   //pthread_condattr_init (attr)
   pthread_cond_init (&load_balance_cond, NULL);
   pthread_mutex_init(&load_balance_mutex);
   
   rc = pthread_create(&qsize_watcher_t, NULL, lockfree_queue_qsize_watcher, NULL);
   if (rc) {
      printf("ERROR: return code from pthread_create() is %d\n", rc);
      exit(-1);
   }
   
   printf("Load balancing thread initialized\n");
   
}

void lockfree_queue_free(void *tid) {
   
   //TODO Check if queues are init. as well in other functions.
   //Test
   
   long *t = tid;
   struct ds_lockfree_queue *q = queues[ *t % queue_count ]; //modulo ok?
   struct lockfree_queue_item *item;
   struct lockfree_queue_item *item_tmp;
   
   item = q->head;
   while (item != NULL) {
      free(item->val);
      item_tmp = item;
      item = item->next;
      free(item_tmp);
   }
   
   free(q->head);
   free(q->divider);
   free(q->tail);
   free(q);
   
}


void lockfree_queue_insert_item_by_tid (void* tid, void* val) {

   long *t = tid;
   volatile struct ds_lockfree_queue *q = queues[ *t % queue_count ]; //modulo ok?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   struct lockfree_queue_item *tmp;
   item->val = val;

   pthread_mutex_lock(&add_mutexes[*t]);
   
   //set next and swap pointer to tail
   q->tail->next = item;   
   
   //q-tail setting is critical section
   q->tail = q->tail->next;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[*t]);
   
}


void lockfree_queue_insert_Nitems_by_tid (void* tid, void* values, int item_count) {

   long *t = tid;
   volatile struct ds_lockfree_queue *q = queues[ *t % queue_count ]; //modulo ok?

   struct lockfree_queue_item *item;
   struct lockfree_queue_item *item_tmp;
   struct lockfree_queue_item *item_first_new;

   item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   item->val = values[0];
   item_first_new = item;
   for (int i = 1; i < item_count; i++) {
      item_tmp = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      item_tmp->val = values[i];
      item->next = item_tmp;
      item = item->next;
   }
   
   pthread_mutex_lock(&add_mutexes[*t]);
   
   //set next
   q->tail->next = item_first_new;   
   
   //swap pointer to tail
   //q-tail setting is critical section
   q->tail = item;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), item_count);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[*t]);
   
}


void* lockfree_queue_load_balancer(void) {
   
   //lock Qs
   for (int i = 0; i < queue_count; i++) {
      pthread_mutex_lock(&add_mutexes[*tid]);
      pthread_mutex_lock(&rm_mutexes[*tid]);
   }
   
   /*
    * relocate data
    * count estimated size
    * count who should give whom what amount of items
    * TODO strategy pattern
    */
   
   //avg_count
   unsigned int total = lockfree_queue_size_total();
   unsigned int estimated_size = total / queue_count;
   
   unsigned long *indexes = (unsigned long*) malloc (2 * sizeof(unsigned long));
   unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
   unsigned long q_diff;

   //swap
   //TODO update condition
   //while (!lockfree_queue_same_size) {
   for (int i = 0 ; i < queue_count; i++) {
      printf("Load balance round %d\n", i);
      for (int j = 0; j < queue_count; j++) {
         q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
         printf("Queue %d size is %lu\n", );
      }
      indexes = find_max_min_element_index(q_sizes, queue_count);
      q_diff = q_sizes[indexes[0]] - q_sizes[indexes[1]];
      printf"(Max: Q%d with%lu --- Min: Q%d with%lu --- Diff: %lu\n", indexes[0], q_sizes[indexes[0]], indexes[1], q_sizes[indexes[1]], q_diff);

      for (int j = 0; j < q_diff; j++ ) {
         //remove N items from queue queues[indexes[0]]
         //add N items to queue queues[indexes[1]]]

         //TODO Cannot be remove int. must be generic swap
         int *retval = lockfree_queue_remove_item_by_tid(indexes[0], 0);
         lockfree_queue_insert_item_by_tid(indexes[1], retval);
         
         //lockfree_queue_insert_Nitems_by_tid();
      }
   }
   
   //unlock Qs
   for (int i = 0; i < queue_count; i++) {
      pthread_mutex_unlock(&add_mutexes[*tid]);
      pthread_mutex_unlock(&rm_mutexes[*tid]);
   }
   
   pthread_cond_signal (&load_balance_cond);
   return;
}


void* lockfree_queue_qsize_watcher(void) {
   
   //TODO complete function, test
   
   unsigned long estimated_size;
   unsigned long threshold = 100;
   unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
   unsigned long total;
   unsigned long *qsize_history = NULL;
   unsigned long q_diff;
   
   bool balance_flag;
   unsigned long *indexes = (unsigned long*) malloc (2 * sizeof(unsigned long));
   long index = 0;
   
   while(1) {

      //null variables
      total = 0;
      balance_flag = false;
      
      //sleep for a while so it doesnt check all time
      sleep(1);

      //get qsizes
      //total = lockfree_queue_size_total();
      for (int i = 0; i < queue_count; i++) {
         q_sizes[i] = lockfree_queue_size_by_tid(tids[i]);
         total += q_sizes[i];
      }
      
      if ( qsize_history == NULL ) {
         //if we got no history of qsizes make one and go to next iteration of watching
         qsize_history = (unsigned long*) malloc ( queue_count * sizeof (qsize_history));
         for (int i = 0; i < queue_count; i++) {
            qsize_history[i] = q_sizes[i];
         }
         continue;
      }
      else {
         
         if (total == 0) {
            continue;
         }
         
         //look if at least one of queue sizes fell under threshold after last checking
         for (int i = 0; i < queue_count; i++) {
            
            if ( (q_sizes[i] < threshold) && (qsize_history[i] >= threshold) ) {
               balance_flag = true;
               break;
            }
            else {
               //
               ;
            }
         }
      }
      
      if (!balance_flag) {
         continue;
      }
   
      pthread_mutex_lock(&load_balance_mutex);
      rc = pthread_create(&load_balancing_t, NULL, lockfree_queue_load_balancer, NULL);
      if (rc) {
         printf("ERROR: return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
      pthread_cond_wait (&load_balance_cond, &load_balance_mutex);
      
   }
   
}


void* lockfree_queue_remove_all_items () {
   //TODO implementation
   
   return NULL;
   
}


void* lockfree_queue_remove_item_by_tid (void* tid, int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */
   
   void* val = NULL;
   long* t = tid;
   
   volatile struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   
   pthread_mutex_lock(&rm_mutexes[*tid]);
   
   //TODO timeout spin
   //if (timeout > 0)
   //   usleep(timeout);
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
   }
   else {
      //TODO check other queues and relocate data to queues to be same sized
      //Or set special variable which signalize which Q was empty and wanted to get item to 1
      
   }
   
   pthread_mutex_unlock(&rm_mutexes[*tid]);
   
   return val;
   
}


void** lockfree_queue_remove_Nitems_by_tid (void* tid, unsigned long N, int timeout) {
   
   /*
    * N is amount of items to be taken from Q
    */
   
   long* t = tid;
   void **val_arr = malloc(N * sizeof(void*));
   
   volatile struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   
   pthread_mutex_lock(&rm_mutexes[*tid]);
   
   unsigned long item_count = atomic_load( q->a_qsize ); 
   if ( atomic_load( q->a_qsize ) < N ) {
      printf("Not enough items in queue %ld. There are %ld but was requested %ld.\n", *t, item_count, N);
      pthread_mutex_unlock(&rm_mutexes[*tid]);
      return NULL;
   }
   
   unsigned long i = 0;
   for (i = 0; i < N; i++) {
      if ( q->divider != q->tail ) {   //atomic reads?
         val_arr[i] = q->divider->next->val;
         q->divider = q->divider->next;
      }
      else {
         break;
      }
   }

   if (i != N-1) {
      printf("Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", *t, i);
      pthread_mutex_unlock(&rm_mutexes[*tid]);
      return NULL;
   }
   
   atomic_fetch_sub( &(q->a_qsize), N);
   
   pthread_mutex_unlock(&rm_mutexes[*tid]);
   
   return val_arr;
   
}


unsigned long lockfree_queue_size_by_tid (void *tid) {
   //TODO test
   
   long *t = tid;
   return atomic_load( &(queues[*t % queue_count]->a_qsize) );
   
}


unsigned long lockfree_queue_size_total () {
   //TODO test
   
   unsigned long size = 0;
   for (int i = 0; i < queue_count; i++) {
      size += atomic_load( &(queues[i]->a_qsize) );
   }

   return size;
   
} 

bool lockfree_queue_same_size() {

   //TODO test [tids[i]]
   unsigned long size;
   unsigned long size_history;
   unsigned long size_ref = atomic_load( &(queues[*tids[0]]->a_qsize) );
   
   for (int i = 0; i < queue_count; i++) {
      size = atomic_load( &(queues[*tids[i]]->a_qsize) );
      if ( size != size_ref ) {
         return false;
      }
   }
   
   return true;
   
}


unsigned long* find_max_min_element_index(unsigned long *array, unsigned long len) {
   
   /*
    * TODO can find max min, save to array ordered from max to min with values and then after relocation
    * only update values which were relocated and update positions if they changed
    */

    /*
     * index_max_min[0] is index of element with max number
     * index_max_min[1] is index of element with min number
     */

     //apply strategy pattern to sorting
   
   
   unsigned long *arr_max_min = (unsigned long*) malloc ( 2 * sizeof(unsigned long));
   arr_max_min[0] = 0;
   arr_max_min[1] = ULONG_MAX;
   
   unsigned long *index_max_min = (unsigned long*) malloc ( 2 * sizeof(unsigned long));
   index_max_min[0] = 0;
   index_max_min[1] = 0;
   
   for (unsigned long i = 0; i < len; i++) {
      if ( array[i] > max ) {
         max = array[i];
         index_max_min[0] = i;
      }
      if ( array[i] < min ) {
         min = array[i];
         index_max_min[1] = i;
      }
   }
   
   return index_max_min;
   
}
