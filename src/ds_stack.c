#ifndef STDLIB_H
#define STDLIB_H
	#include <stdlib.h>
#endif

#ifndef DS_STACK_H
#define DS_STACK_H
	#include "ds_stack.h"
#endif

#ifndef MPI_H
#define MPI_H
	#include "mpi.h"
#endif

#ifndef OMP_H
#define OMP_H
	#include "omp.h"
#endif

#ifndef PTHREAD_H
#define PTHREAD_H
	#include <pthread.h>
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

int push_to_stack(struct ds_stack *stack, int num) {

	int top;

	stack->top += 1;
	top = stack->top;
	stack->numbers = (int*) realloc(stack->numbers, (top) * sizeof(int) );
	if (stack->numbers == NULL) {
		return -1;
	}
	stack->numbers[top - 1] = num;
	
	return 0;	

}

int pop_from_stack(struct ds_stack *stack, int *num_pointer) {

	int top;
	
	top = stack->top;
	if ( top > 0 ) {
		*num_pointer = stack->numbers[top - 1];
		stack->top -= 1;
		return 0;
	}
	else {
		return -1;
	}
}

struct ds_stack *init_stack(void) {

	struct ds_stack *stack = (struct ds_stack*) malloc(sizeof(struct ds_stack));
	//if (stack == NULL) {
	//	return NULL;
	//}
	stack->top = 0;
	stack->numbers = NULL;

	pthread_attr_t attr;
	pthread_mutex_init(&stack_lock, NULL);
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);


	return stack;
}

