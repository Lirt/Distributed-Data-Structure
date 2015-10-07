#ifndef DS_STACK_STR
#define DS_STACK_STR
	struct ds_stack {
		int *numbers;
		int top;
		//int maxSize;
	};
#endif

extern pthread_mutex_t stack_lock;
typedef struct ds_stack Stack;
extern struct ds_stack *init_stack (void);
extern int pop_from_stack (struct ds_stack *stack, int *num_pointer);
extern int push_to_stack (struct ds_stack *stack, int num);
