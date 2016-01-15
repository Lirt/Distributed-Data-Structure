
#ifndef STDATOMIC_H
   #define STDATOMIC_H
   #include <stdatomic.h>
#endif 

#ifndef PTHREAD_H
   #define PTHREAD_H
   #include <pthread.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

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
      //const int max;
      atomic_ulong a_qsize;
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
extern int queue_count; //number of created queues

extern pthread_mutex_t *add_mutexes;
extern pthread_mutex_t *rm_mutexes;
extern pthread_t threads;
extern pthread_t load_balancing_t;
extern pthread_attr_t attr;
extern pthread_mutexattr_t mutex_attr;
//extern int *tids;
extern long **tids;

extern void lockfree_queue_free(void *tid);
extern void lockfree_queue_destroy (void);
extern bool lockfree_queue_is_empty(void *tid);
extern bool lockfree_queue_is_empty_all(void);
/*extern void lockfree_queue_init (void);*/
extern void lockfree_queue_init_callback (void *(*callback)(void *args), void *arguments);
extern void lockfree_queue_insert_item_by_tid (void *tid, void *val);
extern void* lockfree_queue_qsize_watcher();
extern void* lockfree_queue_load_balancer();
extern void* lockfree_queue_remove_item_by_tid (void *tid, int timeout);
extern void* lockfree_queue_remove_all_items ();
extern unsigned long lockfree_queue_size_by_tid (void *tid);
extern unsigned long lockfree_queue_size_total (void);

extern int* find_max_min_element_index(unsigned long *array, unsigned long len);