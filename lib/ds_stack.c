#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_STACK_H
   #define DS_STACK_H
	#include "../include/ds_stack.h"
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


/*
typedef struct {
	int num;
} ds_stack;
*/


/*
 * GLOBAL VARIABLES
 */
pthread_mutex_t stack_lock;



/*
 * FUNCTIONS
 */
int destroy_stack(struct ds_stack *stack) {

	pthread_mutex_destroy(&stack_lock);
	free(stack->number);
	free(stack);

	return 0;
} 


struct ds_stack *init_stack(void) {

	struct ds_stack *stack = (struct ds_stack*) malloc(sizeof(struct ds_stack));
	//if (stack == NULL) {
	//	return NULL;
	//}
	stack->top = 0;
	stack->number = NULL;

	pthread_attr_t attr;
	pthread_mutex_init(&stack_lock, NULL);
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	return stack;
}
 

int pop_from_stack(struct ds_stack *stack, int *num_pointer) {

	int top;
	int ret_val = 0;
	
	//pthread_mutex_lock(stack_lock);
	top = stack->top;
	if ( top > 0 ) {
		*num_pointer = stack->number[top - 1];
		stack->top -= 1;
	}
	else {
		ret_val = -1;
	}

	//pthread_mutex_unlock(stack_lock);
	return ret_val;

}


int push_to_stack(struct ds_stack *stack, int num) {

	int top;

	//pthread_mutex_lock(stack_lock);
	stack->top += 1;
	top = stack->top;
	stack->number = (int*) realloc(stack->number, (top) * sizeof(int) );
	if (stack->number == NULL) {
		//pthread_mutex_unlock(stack_lock);
		return -1;
	}
	
	stack->number[top - 1] = num;

	//pthread_mutex_unlock(stack_lock);
	return 0;	

}





/*
 * DISTRIBUTED QUEUE
 */
 
struct ds_queue_local *init_queue (void) {
   
   struct ds_queue_local *queue = (struct ds_queue_local*) malloc ( sizeof ( struct ds_queue_local ) );
   queue->head = queue->tail = NULL;
   queue->size = 0;
   
   return queue;
}

void destroy_queue (struct ds_queue_local *queue) {
   
   if (queue == NULL)
      return;
      
   if (queue->head == NULL && queue->tail == NULL) {
      free(queue);
      return;
   }
   
   struct queue_item *queue_item_current = queue->head;
   struct queue_item *queue_item_tmp = NULL;
   
   while (queue_item_current->next != NULL) {
      
      queue_item_tmp = queue_item_current->next;
      free(queue_item_current);
      queue_item_current = queue_item_tmp;
      
   }
   
   free(queue_item_current);
   free(queue);
   
}

void insert_item_queue (struct ds_queue_local *queue, void* item) {
   
}

int queue_size (struct ds_queue_local *queue) {
   
   return queue->size;
   
}

void* remove_item_queue (struct ds_queue_local *queue) {
   
   if (queue == NULL)
      //Not initialized
      return NULL;
      
   if (queue->head == NULL)
      //Empty queue
      return NULL;
      
   if (queue->head->next == NULL)
      //Only one item in queue
      //TODO
      //Problem ak je jeden item v Q. Vtedy treba synchronizaciu
      return NULL;
   

   struct queue_item *queue_item_tmp = queue->head;
   void* item = queue_item_tmp->item;
   
   queue->head = queue->head->next;  
   free(queue_item_tmp);
   
   return item;
   
}

void* remove_all_items_queue (struct ds_queue_local *queue) {
   
   //return ??;
   
   return NULL;
   
}
 

/*****
 * Lock-free queue
 ***
 *
 *
 * GLOBAL VARIABLES
 */

struct ds_lockfree_queue **queues;
int queue_count = 0;

pthread_t *threads;
pthread_attr_t attr;
int *tids;

//TODO test a_qsize increment, decrement

void lockfree_queue_destroy () {
   
   //TODO implementation
   //TODO test
   
   pthread_attr_destroy(&attr);
   /*
   for (int i = 0; i < queue_count; i++) {
      
      //TODO close threads and free queues
      //pthread_cancel()
   }*/
   
   return;
   
}


void lockfree_queue_destroy_by_threads (pthread_t *threads) {
   
   //TODO implementation
   //TODO test

   

   return;
   
}


bool lockfree_queue_is_empty(void* tid) {
   
   //TODO test
   //
   
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


void lockfree_queue_init (void) {
   
   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
   int i = 0;
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
   
}


void lockfree_queue_init_callback (void* (*callback)(void *args), void* arguments) {
   
   //TODO documentation must contain struct used for arguments in thread callback
   
   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
   int i = 0;
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
   
   /*
    * Initialize threads to callback function
    */
   
   pthread_t *threads = (pthread_t*) malloc (queue_count * sizeof(pthread_t));
   long **tids;
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
      
      rc = pthread_create(&threads[i], NULL, callback, q_args_t[i]);
      if (rc) {
         printf("ERROR: return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
      
   }
   
   printf("%d Threads to callbacks initialized\n", queue_count);
   
}


void lockfree_queue_insert_item_by_q (struct ds_lockfree_queue *q, void* val) {
   
   //init
   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   item->val = val;
   
   //swap
   q->tail->next = item;
   q->tail = q->tail->next;   //cmp_and_swp?
   
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   struct lockfree_queue_item *tmp;
   while( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp);
   }
   
}


void lockfree_queue_insert_item_by_tid (void* tid, void* val) {

   long *t = tid;
   volatile struct ds_lockfree_queue *q = queues[ *t % queue_count ]; //modulo ok?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   item->val = val;
   
   //swap
   q->tail->next = item;
   q->tail = q->tail->next;   //cmp_and_swp?
   
   atomic_fetch_add( &(q->a_qsize), 1);
   //long __sync_fetch_and_add( (long) &(q->size), (long) 1 );    older gcc builtins
   
   //cleanup
   struct lockfree_queue_item *tmp;
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
}


void* lockfree_queue_remove_all_items () {
   //TODO implementation
   
   return NULL;
   
}


void* lockfree_queue_remove_item_by_q (struct ds_lockfree_queue *q) {
   
   void *val = NULL;
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
      return val;
   }
   else {
      return val;
   }
   
}


void* lockfree_queue_remove_item_by_tid (void* tid, int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */
   
   void* val = NULL;
   long* t = tid;
   
   volatile struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   
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
      ;
   }
   
   return val;
   
}


unsigned long lockfree_queue_size_by_q (struct ds_lockfree_queue *q) {
   //TODO test
   
   //return q->a_qsize;
   return atomic_load( &(q->a_qsize) );
   
}


unsigned long lockfree_queue_size_by_tid (void *tid) {
   //TODO test
   
   long *t = tid;
   return atomic_load( &(queues[*t % queue_count]->a_qsize) );
   
}


unsigned long lockfree_queue_size_total () {
   //TODO implementation
   
   unsigned long size = 0;
   for (int i = 0; i < queue_count; i++) {
      size += atomic_load( &(queues[i]->a_qsize) );
   }

   return size;
   
}




