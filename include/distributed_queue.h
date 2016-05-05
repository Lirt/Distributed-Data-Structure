
#ifndef TIME_H
   #define TIME_H
   #include <time.h>
#endif 

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

#ifndef STDLIB_H
   #define STDLIB_H
   #include <stdlib.h>
#endif

#define ONE_TO_ONE 1
#define TWO_TO_ONE 2
 
/*
 * Queue item
 */

#ifndef DQ_ITEM
  #define DQ_ITEM
	struct dq_item {
		void* val;
    struct dq_item *next;
	};
#endif
 
#ifndef DQ_ARGS
  #define DQ_ARGS
  struct q_args {
    void* args;
    long* tid;
    int q_count;
    int t_count;
  };
#endif

/*
 * Distributed queue struct
 */

#ifndef DQ_QUEUE
  #define DQ_QUEUE
	struct dq_queue {
    size_t item_size;
    atomic_ulong a_qsize;
    unsigned long max_size;
    struct dq_item *head;
    struct dq_item *tail;
    struct dq_item *divider;
	};
#endif 

/*
 * Variables
 */
extern struct dq_queue **queues;
extern int queue_count;

extern pthread_mutex_t local_queue_struct_mutex;
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

#define LOCK_LOCAL_QUEUES() \
  pthread_mutex_lock(&local_queue_struct_mutex); \
  for (int it = 0; it < queue_count; it++) { \
    pthread_mutex_lock(&add_mutexes[*tids[it]]); \
    pthread_mutex_lock(&rm_mutexes[*tids[it]]); \
  }

#define UNLOCK_LOCAL_QUEUES() \
  for (int it = 0; it < queue_count; it++) { \
    pthread_mutex_unlock(&add_mutexes[*tids[it]]); \
    pthread_mutex_unlock(&rm_mutexes[*tids[it]]); \
  } \
  pthread_mutex_unlock(&local_queue_struct_mutex);

#define LOCK_LOAD_BALANCER() \
  pthread_mutex_lock(&load_balance_mutex);

#define UNLOCK_LOAD_BALANCER() \
  pthread_mutex_unlock(&load_balance_mutex);

/***********
 * FUNCTIONS
 */

struct timespec time_diff_dds(struct timespec *start, struct timespec *end) {
  struct timespec result; 
  if ( ( end->tv_nsec - start->tv_nsec ) < 0 ) {
    result.tv_sec = end->tv_sec - start->tv_sec - 1;
    result.tv_nsec = 1000000000 + end->tv_nsec - start->tv_nsec;
  } 
  else {
    result.tv_sec = end->tv_sec - start->tv_sec;
    result.tv_nsec = end->tv_nsec - start->tv_nsec;
  }

  return result;
}

struct load_balancer_struct {
  unsigned long *qsize_history;
  long src_q;
};

typedef int (*load_balancer_strategy)(void* arg);
extern int dq_load_balancer_pair(void* lb_struct);
extern int dq_load_balancer_all(void* lb_struct);
extern load_balancer_strategy lbs;

typedef void* (*qsize_watcher_strategy)();
extern void* dq_qsize_watcher_min_max();
extern void* dq_qsize_watcher_local_threshold();
extern qsize_watcher_strategy qw_strategy;

typedef struct qsize_struct_sorted {
  unsigned long size;
  long index;
} qsizes;

typedef struct work_to_send_struct {
  long send_count;  //how many dst nodes
  long *dst_node_ids;  //dst node ids
  unsigned long *item_counts;  //amount of items to send to node i
} work_to_send;

extern void dq_destroy(void);
extern void dq_queue_free(void *tid);

pthread_t* dq_init ( void* (*callback)(void *args), void* arguments, size_t item_size_arg,
  unsigned int queue_count_arg, unsigned int thread_count_arg, bool qw_thread_enable_arg, 
  double local_lb_threshold_dynamic, double global_lb_threshold_dynamic, unsigned long local_lb_threshold_static, 
  unsigned long global_lb_threshold_static, unsigned int local_lb_type, unsigned int local_balance_type_arg, 
  bool hook_arg, unsigned long max_qsize );

extern bool dq_is_queue_empty (void *tid);
extern bool dq_is_local_ds_empty (void);
extern bool dq_is_local_ds_empty_consistent(void);

extern int dq_insert_item(void *val);
extern void dq_insert_item_by_tid(void *tid, void *val);
extern void dq_insert_item_by_tid_no_lock(void *tid, void *val);
extern void dq_insert_N_items_no_lock_by_tid(void** values, int item_count, void* qid);

extern unsigned long dq_queue_size_by_tid(void *tid);
extern unsigned long dq_local_size(void);
extern unsigned long* dq_local_size_allarr_consistent(void);
extern qsizes* dq_local_size_allarr_sorted();

extern unsigned long dq_local_size(void);
extern unsigned long dq_local_size_consistent(void);
extern unsigned long dq_global_size(bool consistency);

extern void dq_move_items(int q_id_from, int q_id_to, unsigned long count);

extern int dq_remove_item(void* buffer);
extern int dq_remove_item_by_tid(void *tid, void* buffer);
extern int dq_remove_item_by_tid_no_balance (void* t, void* buffer);
extern int dq_remove_Nitems_by_tid_no_lock(long qid, long item_cnt, void** buffer);

extern int dq_global_balance(long tid);
extern void* dq_comm_listener_global_balance();
extern void* dq_comm_listener_global_size();
extern int dq_send_data_to_node(int dst, unsigned long count);

extern int  dq_get_insertion_tid();
extern int  dq_get_removal_tid();

extern double dq_util_sum_time(time_t sec, long nsec);
extern void* dq_per_time_statistics_reseter(void *arg);
extern void* dq_local_struct_cleanup();

extern unsigned long dq_util_sum_arr(unsigned long *arr, unsigned long len);
extern int dq_util_node_receive_count(int node_id, work_to_send *wts);
extern long dq_util_maxdiff_q(void* qid);
extern long dq_util_find_largest_q();
extern long dq_util_find_smallest_q();
extern long dq_util_find_max_element_index(unsigned long *array, unsigned long len);
extern int dq_util_find_max_min_element_index(unsigned long *array, unsigned long len, int* index_max_min);
extern int dq_util_qsize_comparator(const void *a, const void *b);


