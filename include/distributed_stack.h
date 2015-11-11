
#ifndef PTHREAD_H
   #define PTHREAD_H
   #include <pthread.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

/*
 * DISTRIBUTED STACK
 */
#ifndef DS_STACK
   #define DS_STACK
	struct ds_stack {
		int *number;
		int top;
		//int maxSize;
	};
#endif

extern pthread_mutex_t stack_lock;
//typedef struct ds_stack Stack;
extern struct ds_stack *init_stack (void);
extern int destroy_stack (struct ds_stack *stack);
extern int pop_from_stack (struct ds_stack *stack, int *num_pointer);
extern int push_to_stack (struct ds_stack *stack, int num);
