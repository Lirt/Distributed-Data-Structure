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
		int count;
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
 * Lock-Free Queue struct
 */
#ifndef DS_LOCKFREE_QUEUE
   #define DS_LOCKFREE_QUEUE
	struct ds_lockfree_queue {
		int count;
      //int max;
      struct queue_item *head;
      struct queue_item *tail;
      struct queue_item *divider;
	};
#endif 

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
 
//typedef struct ds_queue_local queue;

extern struct ds_lockfree_queue **queues;
extern int queue_count;

extern struct ds_lockfree_queue *init_queue (void);
extern void destroy_lockfree_queue (void);
extern int lockfree_queue_size (struct ds_lockfree_queue *queue);
extern int lockfree_queue_size_total (void);
extern bool is_lockfree_queue_empty(struct ds_lockfree_queue *queue);
extern bool is_lockfree_queue_empty_all(void);
extern void insert_item_by_q_lockfree_queue (struct ds_lockfree_queue *q, void* item);
extern void insert_item_by_tid_lockfree_queue (int tid, void* val);
extern void* remove_item_by_q_lockfree_queue (struct ds_lockfree_queue *q);
extern void* remove_item_by_tid_lockfree_queue (int tid);
extern void* remove_all_items_lockfree_queue (struct ds_lockfree_queue *q);
extern void* remove_all_items_lockfree_queues ();
 
