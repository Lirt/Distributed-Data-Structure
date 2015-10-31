#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "ds_debug.h"
#endif

#ifndef DS_STACK_H
   #define DS_STACK_H
	#include "ds_stack.h"
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
   queue->count = 0;
   
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
   
   return queue->count;
   
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
#include <sys/sysinfo.h>

//struct ds_lockfree_queue *init_queue (int thread_count) {
void init_lockfree_queue (void) {
   
   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
   int i = 0;
   queue_count = get_nprocs();
  
   printf("Number of cpus by get_nprocs is : %d\n", queue_count);

   //int numCPU2 = sysconf( _SC_NPROCESSORS_ONLN );
   //printf("Number of cpus by _SC_NPROCESSORS_ONLN is : %d\n", numCPU2);
   
   //struct ds_lockfree_queue **queue;
   queues = (struct ds_lockfree_queue**) malloc ( queue_count * sizeof(struct ds_lockfree_queue) );
   for (i = 0; i < queue_count; i++) {
      queues[i] = (struct ds_lockfree_queue*) malloc ( sizeof(struct ds_lockfree_queue) );
   }
   
   for (i = 0; i < queue_count; i++) {
      queues[i]->head = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      queues[i]->tail = queues[i]->head;
      queues[i]->divider = queues[i]->head;
      queues[i]->count = 0;
   }
   
   printf("Queue initialized\n");
    
}

void destroy_lockfree_queue () {
   return;
}

int lockfree_queue_size (struct ds_lockfree_queue *queue) {
   return 0;
}

int lockfree_queue_size_total () {
   return 0;
}

bool lockfree_queue_empty() {
   
   
   
   return true;
}

bool is_lockfree_queue_empty_all() {
   return true;
}

void insert_item_by_q_lockfree_queue (struct ds_lockfree_queue *q, void* val) {
   
   //init
   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   item->val = val;
   
   //swap
   q->tail->next = item;
   q->tail = q->tail->next;   //cmp_and_swp?
   
   //cleanup
   struct lockfree_queue_item *tmp;
   while( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp);
   }
   
}

void insert_item_by_tid_lockfree_queue (void* tid, void* val) {

   long *t = tid;
   struct ds_lockfree_queue *q = queues[ *t % queue_count ]; //modulo ok?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   item->val = val;
   
   //swap
   q->tail->next = item;
   q->tail = q->tail->next;   //cmp_and_swp?
   
   //cleanup
   struct lockfree_queue_item *tmp;
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
}

void* remove_item_by_q_lockfree_queue (struct ds_lockfree_queue *q) {
   
   void *val = NULL;
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      return val;
   }
   else {
      return val;
   }
   
}

void* remove_item_by_tid_lockfree_queue (void* tid, int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */
   
   void* val = NULL;
   long* t = tid;
   
   //printf("tid=%ld", *t);
   struct ds_lockfree_queue *q = queues[ (*t - 1) % queue_count]; //modulo ok?
   
   if (timeout > 0)
      usleep(timeout);
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
   }
   
   return val;
   
}
void* remove_all_items_lockfree_queue (struct ds_lockfree_queue *queue) {
   return NULL;
}

void* remove_all_items_lockfree_queues () {
   return NULL;
}

