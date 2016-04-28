
#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
   #include <pthread.h>
#endif

#define ONE_TO_ONE 1
#define TWO_TO_ONE 2

struct q_args {
   void* args;
   long* tid;
   int q_count;
   int t_count;
};


extern void lockfree_queue_destroy(void);

extern unsigned long global_size(bool consistency);

pthread_t* lockfree_queue_init_callback ( void* (*callback)(void *args), void* arguments, size_t item_size_arg,
  unsigned int queue_count_arg, unsigned int thread_count_arg, bool qw_thread_enable_arg, 
  double local_lb_threshold_dynamic, double global_lb_threshold_dynamic, unsigned long local_lb_threshold_static, 
  unsigned long global_lb_threshold_static, unsigned int local_lb_type, unsigned int local_balance_type_arg, 
  bool hook_arg, unsigned long max_qsize );
extern int lockfree_queue_insert_item(void *val);
extern void lockfree_queue_insert_item_by_tid (void *t, void* val);
extern void lockfree_queue_insert_item_by_tid_no_lock(void *t, void* val);
extern void lockfree_queue_insert_N_items(void** values, int item_count);

extern int lockfree_queue_remove_item(void* buffer);
extern int lockfree_queue_remove_item_by_tid (void* t, void* buffer);

extern unsigned long lockfree_queue_size_total(void);
extern unsigned long lockfree_queue_size_total_consistent(void);
