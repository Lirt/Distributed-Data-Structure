
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

//typedef struct ds_queue_local queue;
extern struct ds_lockfree_queue **queues;
extern int queue_count; //number of created queues

extern pthread_mutex_t *add_mutexes;
extern pthread_mutex_t *rm_mutexes;
extern pthread_mutex_t load_balance_mutex;
extern pthread_cond_t load_balance_cond;
extern pthread_t threads;
extern pthread_t load_balancing_t;
extern pthread_attr_t attr;
extern pthread_mutexattr_t mutex_attr;
extern long **tids;

/********
 * MACROS
 */

//TODO TEST
#define LOCK_LOCAL_QUEUES() \
   for (int it = 0; it < queue_count; it++) { \
      pthread_mutex_lock(&add_mutexes[*tids[it]]); \
      pthread_mutex_lock(&rm_mutexes[*tids[it]]); \
   }

#define UNLOCK_LOCAL_QUEUES() \
   for (int it = 0; it < queue_count; it++) { \
      pthread_mutex_unlock(&add_mutexes[*tids[it]]); \
      pthread_mutex_unlock(&rm_mutexes[*tids[it]]); \
   }

#define LOCK_LOAD_BALANCER() \
   pthread_mutex_lock(&load_balance_mutex);
   
//pthread_mutex_lock(&qsize_watcher_mutex);

/***********
 * FUNCTIONS
 */

//extern void lockfree_queue_destroy(void);
extern void lockfree_queue_free(void *tid);
//extern unsigned long global_size();
extern bool lockfree_queue_is_empty_local (void *tid);
extern bool lockfree_queue_is_empty_all_local (void);
extern bool lockfree_queue_is_empty_all_consistent_local(void);
//extern void lockfree_queue_init_callback(void *(*callback)(void *args), void *arguments, int queue_count, int thread_count);
/*void lockfree_queue_init_callback ( void* (*callback)(void *args), void* arguments, 
  unsigned int queue_count_arg, unsigned int thread_count_arg, 
  bool qw_thread_enable_arg, double local_lb_threshold_dynamic, double global_lb_threshold_dynamic, 
  unsigned long local_lb_threshold_static, unsigned long global_lb_threshold_static, local_lb_type );*/
//extern void lockfree_queue_insert_item(void *val);
extern void lockfree_queue_insert_item_by_tid(void *tid, void *val);
extern void lockfree_queue_insert_item_by_tid_no_lock(void *tid, void *val);
//extern void lockfree_queue_insert_N_items(void** values, int item_count);
extern void* lockfree_queue_load_balancer();
extern void lockfree_queue_move_items(int q_id_from, int q_id_to, unsigned long count);
extern unsigned long lockfree_queue_size_by_tid(void *tid);
//extern unsigned long lockfree_queue_size_total(void);
extern unsigned long* lockfree_queue_size_total_consistent_allarr(void);
extern void* lockfree_queue_qsize_watcher();
//extern void lockfree_queue_stop_watcher(void);
//extern void* lockfree_queue_remove_item(int timeout);
extern void* lockfree_queue_remove_item_by_tid(void *tid, int timeout);
extern void* lockfree_queue_remove_item_by_tid_no_lock(void *tid, int timeout);
extern void* lockfree_queue_remove_all_items();
extern void* comm_listener_global_balance();
extern void* comm_listener_global_size();

extern int  getInsertionTid();
extern int  getRemovalTid();
extern int* find_max_min_element_index(unsigned long *array, unsigned long len);