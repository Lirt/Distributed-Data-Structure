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

