
#define ONE_TO_ONE 1
#define TWO_TO_ONE 2

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif


//TODO REMOVE WHEN IT WILL NOT BE NEEDED
struct q_args {
   void* args;
   long* tid;
   int q_count;
   int t_count;
};

/**************
 * OFFICIAL API
 */
extern void lockfree_queue_destroy(void);
extern unsigned long global_size(); 
//extern void lockfree_queue_init_callback(void *(*callback)(void *args), void *arguments, int queue_count, int thread_count);
void lockfree_queue_init_callback ( void* (*callback)(void *args), void* arguments, 
  unsigned int queue_count_arg, unsigned int thread_count_arg, 
  bool qw_thread_enable_arg, double local_lb_threshold_dynamic, double global_lb_threshold_dynamic, 
  unsigned long local_lb_threshold_static, unsigned long global_lb_threshold_static, unsigned int local_lb_type );
extern void lockfree_queue_insert_item(void *val);
extern void lockfree_queue_insert_N_items(void** values, int item_count);
extern void* lockfree_queue_remove_item(int timeout);

/******************
 * TESTING PURPOSES
 * //TODO MOVE TO PROGRAMMERS API AFTER TESTS ARE FINISHED
 */
extern void lockfree_queue_stop_watcher(void);
extern unsigned long lockfree_queue_size_total_consistent(void);

