
#ifndef PTHREAD_H
   #define PTHREAD_H
   #include <pthread.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

#ifndef STDATOMIC_H
   #define STDATOMIC_H
   #include <stdatomic.h>
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

/*
 * QUEUE ITEM
 */
#ifndef QUEUE_ITEM
   #define QUEUE_ITEM
	struct queue_item {
		void* item;
      struct queue_item *next;
	};
#endif

/*
 * LOCAL(THREAD) QUEUE
 */
#ifndef DS_QUEUE_LOCAL
   #define DS_QUEUE_LOCAL
	struct ds_queue_local {
		long size;
      //int max;
      struct queue_item *head;
      struct queue_item *tail;
	};
#endif

/*
 * local queue
 */
//typedef struct ds_queue_local queue;
extern struct ds_queue_local *init_queue (void);
extern void destroy_queue (struct ds_queue_local *queue);
extern void insert_item_queue (struct ds_queue_local *queue, void* item);
extern int queue_size (struct ds_queue_local *queue);
extern void* remove_item_queue (struct ds_queue_local *queue);
extern void* remove_all_items_queue (struct ds_queue_local *queue);

/*
 * Lock-Free Queue(http://www.drdobbs.com/parallel/writing-lock-free-code-a-corrected-queue/210604448?pgno=2)
 */
 
/*
 * QUEUE ITEM
 */
#ifndef LOCKFREE_QUEUE_ITEM
   #define LOCKFREE_QUEUE_ITEM
	struct lockfree_queue_item {
		void* val;
      struct lockfree_queue_item *next;
	};
#endif
 
/*
 * Lock-Free Queue struct
 */
#ifndef DS_LOCKFREE_QUEUE
   #define DS_LOCKFREE_QUEUE
	struct ds_lockfree_queue {
		//int count;
      atomic_ulong a_qsize;
      //int max;
      struct lockfree_queue_item *head;
      struct lockfree_queue_item *tail;
      struct lockfree_queue_item *divider;
	};
#endif 

     
struct q_args {
   void* args;
   long* tid;
};
//or use pthread_self() to get id

   
//typedef struct ds_queue_local queue;
extern struct ds_lockfree_queue **queues;
extern void lockfree_queue_destroy (void);
extern void lockfree_queue_destroy_by_threads (pthread_t *threads);
extern bool lockfree_queue_is_empty(void* tid);
extern bool lockfree_queue_is_empty_all(void);
extern void lockfree_queue_init (void);
extern void lockfree_queue_init_callback (void* (*callback)(void *args), void* arguments);
extern void lockfree_queue_insert_item_by_q (struct ds_lockfree_queue *q, void* item);
extern void lockfree_queue_insert_item_by_tid (void* tid, void* val);
extern void* lockfree_queue_remove_item_by_q (struct ds_lockfree_queue *q);
extern void* lockfree_queue_remove_item_by_tid (void* tid, int timeout);
extern void* lockfree_queue_remove_all_itemss ();
extern unsigned long lockfree_queue_size_by_q (struct ds_lockfree_queue *queue);
extern unsigned long lockfree_queue_size_by_tid (void* tid);
extern unsigned long lockfree_queue_size_total (void);
extern int queue_count; //number of created queues

 
