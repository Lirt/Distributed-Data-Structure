
#ifndef _DEFAULT_SOURCE
   #define _DEFAULT_SOURCE
#endif

#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_QUEUE_H
   #define DS_QUEUE_H
  #include "../include/distributed_queue.h"
#endif

#ifndef DS_QUEUE_OFFICIAL_H
   #define DS_QUEUE_OFFICIAL_H
   #include "../include/distributed_queue_api.h"
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
  #include <pthread.h>
#endif

#ifndef STDLIB_H
   #define STDLIB_H
  #include <stdlib.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

#ifndef UNISTD_H
   #define UNISTD_H
   #include <unistd.h>
#endif

#ifndef SYSINFO_H
   #define SYSINFO_H
   #include <sys/sysinfo.h>
#endif

#ifndef STDATOMIC_H
   #define STDATOMIC_H
   #include <stdatomic.h>
#endif

#ifndef LIMITS_H
   #define LIMITS_H
   #include <limits.h>
#endif

#ifndef MATH_H
   #define MATH_H
   #include <math.h>
#endif

#ifndef TIME_H
   #define TIME_H
   #include <time.h>
#endif

#ifndef MPI_H
   #define MPI_H
   #include "/usr/include/mpich-x86_64/mpi.h"
#endif

#include "../include/uthash.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>

#ifndef THREAD_STACK_SIZE
  //#define THREAD_STACK_SIZE  65536
  #define THREAD_STACK_SIZE  1048576  //min. 1MB
#endif


typedef struct tid_hash_struct {
    unsigned long id;      /* key */
    long tid;
    UT_hash_handle hh;     /* makes this structure hashable */
} tid_hash_struct;

/*****
 * Lock-free queue
 ***/
/*
 * GLOBAL VARIABLES
 */
/*****
 *
 * MPI GLOBAL VARIABLES
 */
int comm_size, comm_rank, master_id;


tid_hash_struct *tid_insertion_hashes = NULL;
tid_hash_struct *tid_removal_hashes = NULL;

struct dq_queue **queues;
int queue_count = 0;
int thread_count = 0;
int thread_to_queue_ratio = 0;
struct q_args **q_args_t;

pthread_attr_t attr;
pthread_mutexattr_t mutex_attr;
pthread_t *callback_threads;
long **tids;

pthread_mutex_t local_queue_struct_mutex;
pthread_mutex_t *add_mutexes;
pthread_mutex_t *rm_mutexes;
pthread_mutex_t insertionTidMutex;  //For creating hash table
pthread_mutex_t removalTidMutex;    //For creating hash table
pthread_mutex_t uthash_mutex;    //For hash table writes
pthread_rwlock_t uthash_ins_rwlock;
pthread_rwlock_t uthash_rm_rwlock;


pthread_t load_balancing_t;
pthread_mutex_t load_balance_mutex;
pthread_cond_t load_balance_cond;

pthread_t qsize_watcher_t;

bool qsize_watcher_t_running_flag;
bool qsize_watcher_t_enable;
bool flag_watcher_graceful_stop;
bool flag_graceful_stop;

unsigned long *qsize_history = NULL;
unsigned long qsize_wait_time;
double local_threshold_percent = 20.00;
double global_threshold_percent = 20.00;
double local_threshold_static = 20000;
double global_threshold_static = 100000;
unsigned int len_s = 2;
unsigned int threshold_type = 0;

atomic_ulong rm_count;         //stores per queue amount of removes per second
atomic_ulong rm_count_last;    //stores per queue amount of removes from last second
atomic_ulong ins_count;         //stores per queue amount of removes per second
atomic_ulong ins_count_last;    //stores per queue amount of removes from last second
pthread_t per_time_statistics_reseter_t;       //swaps rm_count of every queue every second to rm_count_last and nulls rm_count
pthread_t local_struct_cleanup_t;       //periodically cleans up queues

long *load_bal_src;
long *load_bal_dest;
long *load_bal_amount;
//TODO free ptrs

FILE *file_pid;
FILE *log_file_lb;
FILE *log_file_qw;
FILE *log_file_debug;
FILE *log_file_global_comm;
FILE *log_file_global_bal;

/*
 * Statistical variables
 */

atomic_ulong moved_items_log;
atomic_ulong moved_items_global_log;
atomic_ulong load_balancer_call_count_watcher;
atomic_ulong load_balancer_call_count_remove;
atomic_ulong load_balancer_call_count_global;
atomic_ulong global_size_call_count;
atomic_ulong global_size_call_count_in_wait;
atomic_ulong global_balance_executed_as_master_count; //only for master if balance is executed
atomic_ulong global_balance_rejected_as_master_count;
atomic_ulong global_balance_call_count;
atomic_ulong global_balance_call_count_in_wait;
time_t total_rt_lb_time_sec;
long total_rt_lb_time_nsec;
time_t total_thr_lb_time_sec;
long total_thr_lb_time_nsec;
time_t total_rt_global_size_time_sec;
long total_rt_global_size_time_nsec;
time_t total_thr_global_size_time_sec;
long total_thr_global_size_time_nsec;
time_t total_rt_global_balance_time_sec;
long total_rt_global_balance_time_nsec;
time_t total_thr_global_balance_time_sec;
long total_thr_global_balance_time_nsec;
/*
 * GLOBAL BALANCING
 */

load_balancer_strategy lbs;
qsize_watcher_strategy qw_strategy;
pthread_t listener_global_size_t;
pthread_t listener_global_balance_t;
pthread_mutex_t load_balance_global_mutex; //To lock thread until global operation as dq_global_size or dq_global_balance is done
pthread_rwlock_t load_balance_global_rwlock; //To lock thread until global operation as dq_global_size or dq_global_balance is done
pthread_cond_t load_balance_global_cond;
pthread_rwlock_t global_size_rwlock;
pthread_rwlock_t global_size_executing_rwlock;


unsigned int global_size_receive_timeout = 10000; //timeout for receive
unsigned long last_global_size;

bool global_balancing_enable; //True enables global balancing, False disables global balancing
struct timespec *last_global_rebalance_call_send_time;   //For elimination of flooding network with global rebalance requests
struct timespec *last_global_rebalance_done_time;
struct timespec *last_local_rebalance_time;

unsigned long local_balance_wait_timer;
unsigned long local_balance_last_balance_threshold;

int debug_wait = 0;

/****
 * 
 * Functions
 * 
 **/

int dq_get_insertion_tid() {
  /*
  * Returns thread id of insertion thread. 
  * If thread is not mapped, this function will create mapping in hash table
  */

  //TODO DO NOT USE, USE INSERT/REMOVE_BY_TID FUNCTIONS

  struct tid_hash_struct *ths;
  pthread_t pt = pthread_self();

  if (pthread_rwlock_rdlock(&uthash_ins_rwlock) != 0) {
    LOG_ERR_T( (long) -1, "Can't acquire read lock\n");
    exit(-1);
  }
  HASH_FIND_INT( tid_insertion_hashes, &pt, ths );
  pthread_rwlock_unlock(&uthash_ins_rwlock);

  if (ths == NULL) {

    pthread_mutex_lock(&insertionTidMutex);
    unsigned int c = HASH_COUNT(tid_insertion_hashes);
    
    tid_hash_struct *tid_hash = NULL;
    tid_hash = (tid_hash_struct*) malloc(sizeof (struct tid_hash_struct) );
    if (tid_hash == NULL) {
      LOG_ERR_T( (long) pt, "Malloc failed\n");
    }

    tid_hash->id = pt;
    tid_hash->tid = c;
    LOG_DEBUG_TD( (long) c, "Insertion thread %ld mapped to Q%d\n", pt, c);

    if (pthread_rwlock_wrlock(&uthash_ins_rwlock) != 0) {
      LOG_ERR_T( (long) -1, "Can't acquire write lock\n");
      exit(-1);
    }
    HASH_ADD_INT( tid_insertion_hashes, id, tid_hash );
    pthread_rwlock_unlock(&uthash_ins_rwlock);    

    if (pthread_rwlock_rdlock(&uthash_ins_rwlock) != 0) {
      LOG_ERR_T( (long) -1, "Can't acquire read lock\n");
      exit(-1);
    }
    HASH_FIND_INT( tid_insertion_hashes, &pt, ths );
    pthread_rwlock_unlock(&uthash_ins_rwlock);

    pthread_mutex_unlock(&insertionTidMutex);

  }
  return ths->tid;
}

int dq_get_removal_tid() {

  /*
  * Returns thread id of removal thread. 
  * If thread is not mapped, this function will create mapping in hash table
  */

  struct tid_hash_struct *ths;
  pthread_t pt = pthread_self();

  if (pthread_rwlock_rdlock(&uthash_rm_rwlock) != 0) {
    LOG_ERR_T( (long) -1, "Can't acquire read lock\n");
    exit(-1);
  }
  HASH_FIND_INT( tid_removal_hashes, &pt, ths );
  pthread_rwlock_unlock(&uthash_rm_rwlock);

  if (ths == NULL) {
    pthread_mutex_lock(&removalTidMutex);
    unsigned int c = HASH_COUNT(tid_removal_hashes);

    tid_hash_struct *tid_hash = NULL;
    tid_hash = (tid_hash_struct*) malloc(sizeof (struct tid_hash_struct));
    if (tid_hash == NULL) {
      LOG_ERR_T( (long) pt, "Malloc failed\n");
    }

    tid_hash->id = pt;
    tid_hash->tid = c;

    if (pthread_rwlock_wrlock(&uthash_rm_rwlock) != 0) {
    LOG_ERR_T( (long) -1, "Can't acquire write lock\n");
      exit(-1);
    }
    HASH_ADD_INT( tid_removal_hashes, id, tid_hash );
    pthread_rwlock_unlock(&uthash_rm_rwlock);


    if (pthread_rwlock_rdlock(&uthash_rm_rwlock) != 0) {
      LOG_ERR_T( (long) -1, "Can't acquire read lock\n");
      exit(-1);
    }
    HASH_FIND_INT( tid_removal_hashes, &pt, ths );
    pthread_rwlock_unlock(&uthash_rm_rwlock);

    pthread_mutex_unlock(&removalTidMutex);
  }

  return ths->tid;

}

void dq_destroy() {
   
 /*
  * Call only after all threads finished (pthread_joined)
  * http://pubs.opengroup.org/onlinepubs/000095399/functions/pthread_cleanup_pop.html
  */
  
  fprintf(stdout, "Destroying DS\n");
  fflush(stdout);

  MPI_Status status;
  short buf;
  if ( comm_size > 1 ) {
    if ( comm_rank == master_id ) {
      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Recv(&buf, 1, MPI_SHORT, i, 1000, MPI_COMM_WORLD, &status);
        }
      }
      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Send(&buf, 1, MPI_SHORT, i, 1001, MPI_COMM_WORLD);
        }
      }
    }
    else {
      MPI_Send(&buf, 1, MPI_SHORT, master_id, 1000, MPI_COMM_WORLD);
      MPI_Recv(&buf, 1, MPI_SHORT, master_id, 1001, MPI_COMM_WORLD, &status);
    }
  }

  if ( qsize_watcher_t_enable == true ) {
    flag_watcher_graceful_stop = true;
    pthread_join(qsize_watcher_t, NULL);
  }

  flag_graceful_stop = true;
  #ifdef COUNTERS
    pthread_join(per_time_statistics_reseter_t, NULL);
  #endif
  pthread_join(listener_global_size_t, NULL);
  pthread_join(listener_global_balance_t, NULL);
  pthread_join(local_struct_cleanup_t, NULL);

  if (pthread_rwlock_destroy(&uthash_ins_rwlock) != 0) {
    fprintf(stderr, "lock destroy failed\n");
    LOG_ERR_T((long) -1, "lock destroy failed\n");
  }
  if (pthread_rwlock_destroy(&uthash_rm_rwlock) != 0) {
    fprintf(stderr,"lock destroy failed\n");
    LOG_ERR_T((long) -1, "lock destroy failed\n");
  }

  pthread_attr_destroy(&attr);
  pthread_mutexattr_destroy(&mutex_attr);

  pthread_cond_destroy(&load_balance_cond);
  pthread_mutex_destroy(&load_balance_mutex);

  LOG_INFO_TD("STATISTICS: \n\t \
    Qsize watcher was called %lu times\n\t \
    Load balance was called from remove %lu times\n\t \
    Global balance was initiated %lu times\n\t \
    Global size was executed %lu times\n\t \
    Global size only read last value %lu times\n\t \
    Local balance moved %lu items\n\t \
    Global balance was executed as master %lu times\n\t \
    Global balance was rejected as master %lu times\n\t \
    Global balance moved %lu items\n\t", 
    atomic_load(&load_balancer_call_count_watcher), atomic_load(&load_balancer_call_count_remove), atomic_load(&load_balancer_call_count_global), atomic_load(&global_size_call_count), atomic_load(&global_size_call_count_in_wait), atomic_load(&moved_items_log), atomic_load(&global_balance_executed_as_master_count), atomic_load(&global_balance_rejected_as_master_count), atomic_load(&moved_items_global_log));

  //double sum_rt_time = dq_util_sum_time(total_rt_lb_time_sec, total_rt_lb_time_nsec);
  /*LOG_INFO_TD("\tTotal realtime spent in load balancer: %lf seconds\
    \n\tTotal Thread time spent in load balancer: %lf seconds\n", sum_rt_time, sum_thr_time);*/
  double sum_thr_time = dq_util_sum_time(total_thr_lb_time_sec, total_thr_lb_time_nsec);
  LOG_INFO_TD("\tTotal Thread time spent in load balancer: %lf seconds\n", sum_thr_time);
   
  double sum_rt_time = dq_util_sum_time(total_rt_global_size_time_sec, total_rt_global_size_time_nsec);
  sum_thr_time = dq_util_sum_time(total_thr_global_size_time_sec, total_thr_global_size_time_nsec);
  LOG_INFO_TD("\tTotal realtime spent in global size: %lf seconds\
    \n\tTotal thread time spent in global size: %lf seconds\n", sum_rt_time, sum_thr_time);

  sum_rt_time = dq_util_sum_time(total_rt_global_balance_time_sec, total_rt_global_balance_time_nsec);
  sum_thr_time = dq_util_sum_time(total_thr_global_balance_time_sec, total_thr_global_balance_time_sec);
  LOG_INFO_TD("\tTotal realtime spent in global balance: %lf seconds\
    \n\tTotal thread time spent in global balance: %lf seconds\n", sum_rt_time, sum_thr_time);

  free(last_global_rebalance_call_send_time);
  free(last_global_rebalance_done_time);
  free(last_local_rebalance_time);

  free(callback_threads);

  for (int i = 0; i < queue_count; i++) {
    struct dq_queue *q = queues[ i ];
    struct dq_item *tmp;
    while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
    }
    free(q->head->val);
    free(q->head);
  }

  for (int i = 0; i < queue_count; i++) {
    free(queues[i]);

    pthread_mutex_destroy(&add_mutexes[i]);
    pthread_mutex_destroy(&rm_mutexes[i]);
  }
  for (int i = 0; i < thread_count; i++) {
    free(tids[i]);
  }

  free(queues);
  free(tids);
  free(add_mutexes);
  free(rm_mutexes);

  fclose(log_file_lb);
  fclose(log_file_qw);
  fclose(log_file_debug);
  fclose(log_file_global_comm);
  fclose(log_file_global_bal);

  for (int i = 0; i < thread_count; i++) {
    if (q_args_t[i]->args != NULL) {
      free(q_args_t[i]->args);
    }
    free(q_args_t[i]);
  }
  free(q_args_t);

  MPI_Finalize();

}


bool dq_is_queue_empty(void *queue_id) {
   
   long *tid = queue_id; 
   
   struct dq_queue *q = queues[ *tid ];

   if ( atomic_load( &(q->a_qsize) ) == 0 ) {
      return true;
   }
   
   return false;
   
}


bool dq_is_local_ds_empty () {

   for (int i = 0 ; i < queue_count; i++) {
      if ( atomic_load( &(queues[i]->a_qsize) ) != 0 ) {
         return false;
      }
   }
   
   return true;
   
}

bool dq_is_local_ds_empty_consistent() {

   bool retval = true; 

   LOCK_LOCAL_QUEUES();

   for (int i = 0 ; i < queue_count; i++) {
      if ( atomic_load( &(queues[i]->a_qsize) ) != 0 ) {
         retval = false;
         break;
      }
   }

   UNLOCK_LOCAL_QUEUES();
   return retval;
   
}

pthread_t* dq_init ( void* (*callback)(void *args), void* arguments, size_t item_size_arg,
  unsigned int queue_count_arg, unsigned int thread_count_arg, 
  bool qw_thread_enable_arg, double local_lb_threshold_percent, double global_lb_threshold_percent, 
  unsigned long local_lb_threshold_static, unsigned long global_lb_threshold_static, unsigned int threshold_type_arg, 
  unsigned int local_balance_type_arg, bool hook_arg, unsigned long max_qsize ) {

  /*
   * Local load balance type values are:
   * '0' for STATIC
   * '1' for PERCENT
   * '2' for DYNAMIC
   */

  //TODO documentation must contain struct used for arguments in thread callback

  pid_t pid = getpid();
  int pid_int = (int) pid;

  char pid_str[8];
  sprintf(pid_str, "%d", pid_int);
  printf("pid=%d\n", pid_int);


  if ( hook_arg == true ) {
    //LOG_INFO_TD("Hook enabled\n");
    printf("DS: Hook enabled, enter 'set var debug_wait=1 to start program'\n");
    while (debug_wait == 0) {
      sleep(1);
    }
  }

  char hostname[256];
  gethostname(hostname, sizeof(hostname));

  /*
   * Init debug files
   */
  struct stat st = {0};

  if (stat("/tmp/distributed_queue", &st) == -1) {
    mkdir("/tmp/distributed_queue", 0777);
  }

  char filename_log_debug[50] = "/tmp/distributed_queue/log_debug_";
  strcat(filename_log_debug, pid_str);
  log_file_debug = fopen(filename_log_debug, "wb");
  if (log_file_debug == NULL) {
    fprintf(stderr, "ERROR: Failed to open debug file '%s'\n", filename_log_debug);
  }
  LOG_DEBUG_TD( (long) -1, "Debug log file opened\n");

  char filename_log_lb[50] = "/tmp/distributed_queue/log_debug_lb_";
  strcat(filename_log_lb, pid_str);
  log_file_lb = fopen(filename_log_lb, "wb");
  if (log_file_lb == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open debug file '%s'\n", filename_log_lb);
  }
  LOAD_BALANCE_LOG_DEBUG_TD("Load balancer log file opened\n");

  char filename_log_qw[50] = "/tmp/distributed_queue/log_debug_qw_";
  strcat(filename_log_qw, pid_str);
  log_file_qw = fopen(filename_log_qw, "wb");
  if (log_file_qw == NULL) {
    LOG_ERR_T((long) -1, "Failed to open debug file '%s'\n", filename_log_qw);
  }
  QSIZE_WATCHER_LOG_DEBUG_TD("Qsize Watcher log file opened\n");

  char filename_pid[50] = "/tmp/distributed_queue/dds.pid";
  file_pid = fopen(filename_pid, "wb");
  if (file_pid == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open pid file '%s'\n", filename_pid);
  }
  fprintf(file_pid, "%d", pid_int);
  fclose(file_pid);

  char filename_log_global_comm[60] = "/tmp/distributed_queue/log_debug_global_size";
  strcat(filename_log_global_comm, pid_str);
  log_file_global_comm = fopen(filename_log_global_comm, "wb");
  if (log_file_global_comm == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open debug file '%s'\n", filename_log_global_comm);
  }
  GLOBAL_COMM_LOG_DEBUG_TD( -1, "Global communication log file opened\n");

  char filename_log_global_bal[60] = "/tmp/distributed_queue/log_debug_global_bal_";
  strcat(filename_log_global_bal, pid_str);
  log_file_global_bal = fopen(filename_log_global_bal, "wb");
  if (log_file_global_bal == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open debug file '%s'\n", filename_log_global_bal);
  }
  GLOBAL_BALANCE_LOG_DEBUG_TD( comm_rank, "Global balance log file opened\n");

  LOG_INFO_TD("-----------CONFIGURATION-----------\nPID: %d\nNode: %s\n", pid, hostname);
  /*
   * MPI CONFIG
   */
  int rc;
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int processor_name_length;
  int required = MPI_THREAD_MULTIPLE;
  int provided;
  int claimed;

  /*
   * MPI Init with multiple thread support
   */
  //rc = MPI_Init_thread(&argc, &argv, required, &provided);
  rc = MPI_Init_thread(NULL, NULL, required, &provided);
  if (rc != MPI_SUCCESS) {
    LOG_ERR_T( (long) -1, "Error in MPI thread init\n");
    exit(-1);
  }

  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
  rc = MPI_Get_processor_name(processor_name, &processor_name_length);
  if (rc != MPI_SUCCESS) {
    LOG_ERR_T((long) -1, "Error in getting processor name\n");
    exit(-1);
  }
  LOG_INFO_TD("COMM_SIZE(Task_count): %d\nCOMM_RANK: %d\nProcessor_name: '%s'\nRequired_thread_level: %d\n", 
    comm_size, comm_rank, processor_name, required);
  fprintf(stdout, "COMM_SIZE(Task_count): %d\nCOMM_RANK: %d\nProcessor_name: '%s'\nRequired_thread_level: %d\n", 
    comm_size, comm_rank, processor_name, required);

  rc = MPI_Query_thread(&claimed);
  if (rc != MPI_SUCCESS) {
    LOG_ERR_T((long) -1, "Error query thread\n");
    exit(-1);
  }
  LOG_INFO_TD("Provided_thread_level: %d\n", provided);

  /*
   * Variables initialization
   */

  atomic_init(&moved_items_log, 0);
  atomic_init(&moved_items_global_log, 0);
  atomic_init(&load_balancer_call_count_watcher, 0);
  atomic_init(&load_balancer_call_count_remove, 0);
  atomic_init(&load_balancer_call_count_global, 0);
  atomic_init(&global_size_call_count, 0);
  atomic_init(&global_size_call_count_in_wait, 0);
  atomic_init(&global_balance_executed_as_master_count, 0); //only for master if balance is executed
  atomic_init(&global_balance_rejected_as_master_count, 0);
  atomic_init(&global_balance_call_count, 0);
  atomic_init(&global_balance_call_count_in_wait, 0);
  atomic_init(&rm_count, 0);
  atomic_init(&rm_count_last, 0);
  total_rt_lb_time_sec = 0;
  total_rt_lb_time_nsec = 0;
  total_thr_lb_time_sec = 0;
  total_thr_lb_time_nsec = 0;

  total_rt_global_size_time_sec = 0;
  total_rt_global_size_time_nsec = 0;
  total_thr_global_size_time_sec = 0;
  total_thr_global_size_time_nsec = 0;

 /*
  * ID of master node on start of program is 0 and is saved in master_id variable
  */
  master_id = 0;
  printf("Master ID: %d\n", master_id);

  /*
   * Queue initialization
   */

  unsigned long max_qsize_tmp;
  if ( max_qsize == 0 ) {
    max_qsize_tmp = ULONG_MAX;  //If max_qsize == 0, queue has infinite size --> ULONG_MAX
  }
  else {
    max_qsize_tmp = max_qsize;
  }
  LOG_INFO_TD("MAX QUEUE SIZE: %lu items\n", max_qsize_tmp);  
  fprintf(stdout, "MAX QUEUE SIZE: %lu items\n", max_qsize_tmp);  

  LOG_INFO_TD("Size of queue items is set to %lu\n", item_size_arg);
  fprintf(stdout, "Size of queue items is set to %lu\n", item_size_arg);

  if ( queue_count_arg == 0 ) 
    queue_count = get_nprocs();
  else 
    queue_count = queue_count_arg;

  LOG_INFO_TD("CPU_COUNT: %d\nQUEUE_COUNT: %d\n", get_nprocs(), queue_count);

  queues = (struct dq_queue**) malloc ( queue_count * sizeof(struct dq_queue) );
  for (int i = 0; i < queue_count; i++) {
    queues[i] = (struct dq_queue*) malloc ( sizeof(struct dq_queue) );
  }

  for (int i = 0; i < queue_count; i++) {
    queues[i]->head = (struct dq_item*) malloc (sizeof(struct dq_item));
    queues[i]->head->val = malloc(sizeof(void*));
    queues[i]->head->next = NULL;
    queues[i]->tail = queues[i]->head;
    queues[i]->divider = queues[i]->head;
    queues[i]->item_size = item_size_arg;
    queues[i]->max_size = max_qsize_tmp;
    atomic_init( &(queues[i]->a_qsize), 0 );
  }   

  /*
   * Setup mutexes
   */

  int *oldstate = NULL;
  int *oldtype = NULL;
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, oldstate);
  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, oldtype);

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  //pthread_attr_setstacksize(&attrs, THREAD_STACK_SIZE);
  pthread_mutexattr_init(&mutex_attr);

  /*
   * For creating hash table with thread id mapping
   */
  pthread_mutex_init(&insertionTidMutex, &mutex_attr);
  pthread_mutex_init(&removalTidMutex, &mutex_attr);
  pthread_mutex_init(&uthash_mutex, &mutex_attr);
  if (pthread_rwlock_init(&uthash_ins_rwlock, NULL) != 0) {
    fprintf(stderr,"Lock init of uthash_ins_rwlock failed\n");
    exit(-1);
  }
  if (pthread_rwlock_init(&uthash_rm_rwlock, NULL) != 0) {
    fprintf(stderr,"Lock init of uthash_rm_rwlock failed\n");
    exit(-1);
  }
  if (pthread_rwlock_init(&global_size_rwlock, NULL) != 0) {
    fprintf(stderr,"Lock init of global_size_rwlock failed\n");
    exit(-1);
  }
  if (pthread_rwlock_init(&global_size_executing_rwlock, NULL) != 0) {
    fprintf(stderr,"Lock init of global_size_executing_rwlock failed\n");
    exit(-1);
  }
  if (pthread_rwlock_init(&load_balance_global_rwlock, NULL) != 0) {
    fprintf(stderr,"Lock init of global_balance_rwlock failed\n");
    exit(-1);
  }

  //pthread_condattr_init (attr);
  pthread_cond_init (&load_balance_cond, NULL);
  pthread_mutex_init(&load_balance_mutex, &mutex_attr);
  pthread_mutex_init(&load_balance_global_mutex, &mutex_attr);
  pthread_cond_init(&load_balance_global_cond, NULL);
  qsize_watcher_t_running_flag = false;

  pthread_mutex_init(&local_queue_struct_mutex, &mutex_attr);
  add_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
  rm_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
  for (int i = 0; i < queue_count; i++) {
    pthread_mutex_init(&add_mutexes[i], &mutex_attr);
    pthread_mutex_init(&rm_mutexes[i], &mutex_attr);
  }
   

  /*
   * Global balance listener
   */
  last_global_rebalance_done_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, last_global_rebalance_done_time);

  last_global_rebalance_call_send_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, last_global_rebalance_call_send_time);
  last_local_rebalance_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, last_local_rebalance_time);
  local_balance_wait_timer = 6000; // in microseconds
  local_balance_last_balance_threshold = 20 * 1000; // in nanoseconds, if less than "" do rebalance


  global_balancing_enable = true; //enables or disables global balancing
  if (global_balancing_enable) {
    rc = pthread_create(&listener_global_balance_t, &attr, dq_comm_listener_global_balance, NULL);
    if (rc) {
      fprintf(stderr, "ERROR: (init) return code from pthread_create() on global balance listener is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
  }
   
  /*
   * Global size listener
   */

  rc = pthread_create(&listener_global_size_t, &attr, dq_comm_listener_global_size, NULL);
  if (rc) {
    LOG_ERR_T( (long) -1, "Pthread create failed\n");
    exit(-1);
  }

  /*
   * Initialize threads to callback function
   */   
  thread_count = thread_count_arg;
  if ( (thread_count_arg != ONE_TO_ONE) && (thread_count_arg != TWO_TO_ONE) ) {
    LOG_ERR_T( (long) -1, "Thread count argument is invalid\n");
    exit(-1);
  }
  if (thread_count_arg == ONE_TO_ONE)
    thread_count = queue_count;
  if (thread_count_arg == TWO_TO_ONE)
    thread_count = 2 * queue_count;

  LOG_INFO_TD("THREAD_MAPPING: %d\n", thread_count);
  LOG_INFO_TD("THREAD_COUNT: %d\n", thread_count);
  thread_to_queue_ratio = thread_count / queue_count;

  callback_threads = (pthread_t*) malloc (thread_count * sizeof(pthread_t));
  tids = (long**) malloc (thread_count * sizeof(long));

  for (int i = 0; i < thread_count; i++) {
    tids[i] = (long*) malloc ( sizeof(long) );
    *tids[i] = i;
  }


 /*
  * LOAD BALANCE TYPE
  */

  lbs = &dq_load_balancer_all; //Default LB strategy(necessary for f.remove item)
  if ( local_balance_type_arg == 1 ) {
    //LB ALL EQUAL
    qsize_wait_time = 7000; //in microseconds
    qw_strategy = &dq_qsize_watcher_local_threshold;
    lbs = &dq_load_balancer_all;
    LOG_INFO_TD("Load balancer strategy is equal balance\n");
  }
  else if ( local_balance_type_arg == 2 ) {
    //LB PAIR
    qsize_wait_time = 4000; //in microseconds
    qw_strategy = &dq_qsize_watcher_min_max;
    lbs = &dq_load_balancer_pair;
    LOG_INFO_TD("Load balancer strategy is pair balance\n");
  }
  /*else {
    LOG_ERR_T( (long) -1, "Bad argument for load balance type\n");
    exit(-1);
  }*/

  /*
   * Initialize qsize watcher
   */

  qsize_watcher_t_enable = qw_thread_enable_arg;
  flag_watcher_graceful_stop = false;
  flag_graceful_stop = false;
  if ( qsize_watcher_t_enable ) {

    //SET THRESHOLD
    if ( threshold_type_arg == 1 ) {
      local_threshold_static = local_lb_threshold_static;
      global_threshold_static = global_lb_threshold_static;
      threshold_type = 1;
      LOG_INFO_TD("Qsize watcher threshold type is STATIC\n");
    }
    else if ( threshold_type_arg == 2 ) {
      local_threshold_percent = local_lb_threshold_percent;
      global_threshold_percent = global_lb_threshold_percent;
      threshold_type = 2;
      LOG_INFO_TD("Qsize watcher threshold type is PERCENT\n");
    }
    else if ( threshold_type_arg == 3 ) {
      LOG_INFO_TD("Qsize watcher threshold type is DYNAMIC\n");
      threshold_type = 3;
    }
    else if ( threshold_type_arg == 0 ) {
      LOG_INFO_TD("Qsize watcher has no load balancing type --> threshold_type_arg is NULL\n");
    }
    else {
      LOG_ERR_T( (long) -1, "Bad argument for threshold_type_arg\n");
    }

    rc = pthread_create(&qsize_watcher_t, &attr, qw_strategy, NULL);
    if (rc) {
      fprintf(stderr, "ERROR: (init qsize_watcher) return code from pthread_create() is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
    else {
       LOG_INFO_TD("QSIZE_THREAD: enabled\n");
       qsize_watcher_t_running_flag = true;
    }
  }
  else { 
    LOG_INFO_TD("QSIZE_THREAD: disabled\n");
  }

  LOG_INFO_TD("-----------CONFIGURATION-----------\n");

  /*
   * Init remove count nuller
   */

  #ifdef COUNTERS
    rc = pthread_create(&per_time_statistics_reseter_t, &attr, dq_per_time_statistics_reseter, NULL);
    if (rc) {
      //fprintf(stderr, "ERROR: (init dq_per_time_statistics_reseter) return code from pthread_create() is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
  #endif

  
  rc = pthread_create(&local_struct_cleanup_t, &attr, dq_local_struct_cleanup, NULL);
  if (rc) {
    //fprintf(stderr, "ERROR: (init cleanup thread) return code from pthread_create() is %d\n", rc);
    LOG_ERR_T( (long) -1, "Pthread create failed\n");
    exit(-1);
  }

  /*
   * Settings for queue argument structure and thread mapping to queues
   */

  q_args_t = (struct q_args**) malloc (thread_count * sizeof(struct q_args));

  for (int i = 0; i < thread_count; i++) {

    q_args_t[i] = (struct q_args*) malloc (sizeof(struct q_args));
    q_args_t[i]->args = arguments;
    q_args_t[i]->tid = tids[i];
    q_args_t[i]->q_count = queue_count;
    q_args_t[i]->t_count = thread_count;
    
    rc = pthread_create(&callback_threads[i], &attr, callback, q_args_t[i]);
    if (rc) {
      fprintf(stderr, "ERROR: (init callback threads) return code from pthread_create() is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
  }

  return callback_threads;

}

void dq_queue_free(void *queue_id) {
   
  /*
   * Frees queue indexed by q_id
   */
  //TODO Check if queues are init. as well in other functions.
  //Test

  long *q_id = queue_id;

  struct dq_queue *q = queues[ *q_id ]; //modulo ok?

  struct dq_item *item;
  struct dq_item *item_tmp;

  item = q->head;
  while (item != NULL) {
    free(item->val); //Can cause segmentation fault when malloc was used in main program ??
    item_tmp = item;
    item = item->next;
    free(item_tmp);
  }

  free(q);
   
}

int dq_insert_item (void* val) {

 /*
  * Free is done in function
  * User must free only his value *val
  */

  long tid = dq_get_insertion_tid();
  struct dq_queue *q = queues[ tid ];

  struct dq_item *item = (struct dq_item*) malloc (sizeof(struct dq_item));
  if (item == NULL) {
    fprintf(stdout, "ERROR: Malloc failed\n");
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return -1;
  }
  struct dq_item *tmp;

  item->val = malloc(sizeof(void*));
  if (item->val == NULL) {
    fprintf(stdout, "ERROR: Malloc failed\n");
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return -1;
  }
  memcpy(item->val, val, q->item_size);

  item->next = NULL;

  //while ( atomic_load(&(q->a_qsize)) >= q->max_size ) {
  while ( q->a_qsize >= q->max_size ) {
    usleep(100);
  }

  pthread_mutex_lock(&add_mutexes[tid]);

  //set next and swap pointer to tail
  q->tail->next = item;

  //q-tail setting is critical section
  q->tail = q->tail->next;

  atomic_fetch_add( &(q->a_qsize), 1);
  #ifdef COUNTERS
  atomic_fetch_add( &ins_count, 1);
  #endif

  //cleanup
  while ( q->head != q->divider ) {
    tmp = q->head;
    q->head = q->head->next;
    free(tmp->val);
    free(tmp);
  }

  pthread_mutex_unlock(&add_mutexes[tid]);
  return 0;

}

void lockfree_queue_insert_item_no_lock (void* val) {

  long tid = dq_get_insertion_tid();
  struct dq_queue *q = queues[ tid ];

  struct dq_item *item = (struct dq_item*) malloc (sizeof(struct dq_item));
  if (item == NULL) {
    LOG_ERR_T( (long) tid, "Malloc failed\n");
  }
  struct dq_item *tmp;
  //item->val = val;
  item->val = malloc(sizeof(void*));
  memcpy(item->val, val, q->item_size);
  item->next = NULL;
   
  //while ( atomic_load(&(q->a_qsize)) >= q->max_size ) {
  while ( q->a_qsize >= q->max_size ) {
    usleep(100);
  }

  //set next and swap pointer to tail
  q->tail->next = item;

  //q-tail setting is critical section
  q->tail = q->tail->next;

  //increment Q size
  atomic_fetch_add( &(q->a_qsize), 1);
  #ifdef COUNTERS
  atomic_fetch_add( &ins_count, 1);
  #endif

  //cleanup
  while ( q->head != q->divider ) {
    tmp = q->head;
    q->head = q->head->next;
    free(tmp->val);
    free(tmp);
  }

}

void dq_insert_item_by_tid (void *t, void* val) {

  long *tid = t;
  struct dq_queue *q = queues[ *tid ];

  struct dq_item *item = (struct dq_item*) malloc (sizeof(struct dq_item));

  if (item == NULL) {
    LOG_ERR_T( (long) tid, "Malloc failed\n");
  }

  struct dq_item *tmp;
  item->val = malloc(sizeof(q->item_size));
  memcpy(item->val, val, q->item_size);
  item->next = NULL;
  pthread_mutex_lock(&add_mutexes[*tid]);

  //while ( atomic_load(&(q->a_qsize)) >= q->max_size ) {
  while ( q->a_qsize >= q->max_size ) {
    usleep(50);
  }

  //set next and swap pointer to tail
  q->tail->next = item;

  //q-tail setting is critical section
  q->tail = q->tail->next;

  //increment Q size
  atomic_fetch_add( &(q->a_qsize), 1);
  #ifdef COUNTERS
    atomic_fetch_add( &ins_count, 1);
  #endif

  //cleanup
  while ( q->head != q->divider ) {
    tmp = q->head;
    q->head = q->head->next;
    free(tmp->val);
    free(tmp);
  }

  pthread_mutex_unlock(&add_mutexes[*tid]);
}

void dq_insert_item_by_tid_no_lock (void *t, void* val) {

   long *tid = t;
   struct dq_queue *q = queues[ *tid ];

   struct dq_item *item = (struct dq_item*) malloc (sizeof(struct dq_item));
   
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }
   struct dq_item *tmp;
   //item->val = val;
   item->val = malloc(sizeof(void*));
   memcpy(item->val, val, q->item_size);
   item->next = NULL;
   
  //while ( atomic_load(&(q->a_qsize)) >= q->max_size ) {
  while ( q->a_qsize >= q->max_size ) {
    usleep(100);
  }

   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   #ifdef COUNTERS
    atomic_fetch_add( &ins_count, 1);
   #endif

   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
}

void lockfree_queue_insert_N_items (void** values, int item_count) {

   long tid = dq_get_insertion_tid();

   if ( item_count == 0 ) {
      return;
   }
   if ( values == NULL ) {
      return;
   }

   struct dq_queue *q = queues[ tid ];

   struct dq_item *item;
   struct dq_item *item_tmp;
   struct dq_item *item_first;

   item = (struct dq_item*) malloc (sizeof(struct dq_item));
   
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
      return;
   }

   //item->val = values[0];
   item->val = malloc(sizeof(void*));
   memcpy(item->val, values[0], q->item_size);
   item_first = item;

   for (int i = 1; i < item_count; i++) {
      item_tmp = (struct dq_item*) malloc (sizeof(struct dq_item));
      if (item_tmp == NULL) {
         LOG_ERR_T( (long) tid, "Malloc failed\n");
         return;
      }
      //item_tmp->val = values[i];
      item_tmp->val = malloc(sizeof(void*));
      if (item_tmp->val == NULL) {
         LOG_ERR_T( (long) tid, "Malloc failed\n");
         return;
      }
      memcpy(item_tmp->val, values[i], q->item_size);
      item->next = item_tmp;
      item = item->next;
   }
   item->next = NULL;
   
  //while ( (atomic_load(&(q->a_qsize)) + item_count) >= q->max_size ) {
  while ( q->a_qsize >= q->max_size ) {
    usleep(100);
  }

   pthread_mutex_lock(&add_mutexes[tid]);
   
   //set next
   q->tail->next = item_first;
   
   //swap pointer to tail
   q->tail = item;
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), item_count);
  #ifdef COUNTERS
    atomic_fetch_add(&ins_count, item_count);
  #endif

   //cleanup   
   struct dq_item *tmp;
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[tid]);
   
}

void dq_insert_N_items_no_lock_by_tid (void** values, int item_count, void *qid) {

  if ( item_count == 0 ) {
    return;
  }
  if ( values == NULL ) {
    return;
  }

  long *tid = qid;
  struct dq_queue *q = queues[ *tid ];

  struct dq_item *item;
  struct dq_item *item_tmp;
  struct dq_item *item_first;

  item = (struct dq_item*) malloc (sizeof(struct dq_item));

  if (item == NULL) {
    fprintf(stdout, "ERROR: Malloc failed in insert_N_items_no_lock_by_tid\n");
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return;
  }

  item->val = malloc(sizeof(void*));
  memcpy(item->val, values[0], q->item_size);
  item_first = item;

  for (int i = 1; i < item_count; i++) {
    item_tmp = (struct dq_item*) malloc (sizeof(struct dq_item));
    if (item_tmp == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
      return;
    }
    item_tmp->val = malloc(sizeof(void*));
    if (item_tmp->val == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
      return;
    }
    memcpy(item_tmp->val, values[i], q->item_size);
    item->next = item_tmp;
    item = item->next;
  }
  item->next = NULL;

  /*while ( atomic_load(&(q->a_qsize)) >= q->max_size ) {
    usleep(100);
  }*/

  //set next
  q->tail->next = item_first;

  //swap pointer to tail
  q->tail = item;

  //increment Q size

  atomic_fetch_add( &(q->a_qsize), item_count);
  #ifdef COUNTERS
    atomic_fetch_add(&ins_count, item_count);
  #endif

  //cleanup   
  struct dq_item *tmp;
  while ( q->head != q->divider ) {
    tmp = q->head;
    q->head = q->head->next;
    free(tmp->val);
    free(tmp);
  }
   
}

int dq_load_balancer_pair(void* lb_struct_arg) {

  struct load_balancer_struct *lb_struct = lb_struct_arg;

  if (queue_count == 1) {
    pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
    return 0;
  }

  struct timespec *current_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);
  clock_gettime(CLOCK_REALTIME, current_time);

  if ( (time_diff_dds(last_local_rebalance_time, current_time).tv_sec == 0) && 
        time_diff_dds(last_local_rebalance_time, current_time).tv_nsec < local_balance_last_balance_threshold ) {
    ;
  }
  else {
    LOAD_BALANCE_LOG_DEBUG_TD("Last local balance was %ld sec and %ld nsec ago\n", 
      time_diff_dds(last_local_rebalance_time, current_time).tv_sec, time_diff_dds(last_local_rebalance_time, current_time).tv_nsec);
    usleep(local_balance_wait_timer);    
  }
  clock_gettime(CLOCK_REALTIME, last_local_rebalance_time);

  long maxdiff_q_id = dq_util_maxdiff_q(&(lb_struct->src_q));
  if ( maxdiff_q_id == -1 ) {
    //No local queue to trade with
    free(current_time);
    free(tp_thr_start);
    free(tp_thr_end);
    pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
    return -1;
  }

  pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
  pthread_mutex_lock(&local_queue_struct_mutex);

  pthread_mutex_lock(&rm_mutexes[lb_struct->src_q]);  
  pthread_mutex_lock(&rm_mutexes[maxdiff_q_id]);
  pthread_mutex_lock(&add_mutexes[maxdiff_q_id]);
  pthread_mutex_lock(&add_mutexes[lb_struct->src_q]);

  unsigned long qsize_src = dq_queue_size_by_tid(&maxdiff_q_id);
  unsigned long qsize_dst = dq_queue_size_by_tid(&(lb_struct->src_q));

  unsigned long estimated_size = (qsize_src + qsize_dst) / 2;
  unsigned long items_to_send;

  if ( qsize_src > qsize_dst ) {
    items_to_send = qsize_src - estimated_size;
    dq_move_items(maxdiff_q_id, lb_struct->src_q, items_to_send);
  }
  else if ( qsize_src < qsize_dst ) {
    items_to_send = qsize_dst - estimated_size;
    dq_move_items(lb_struct->src_q, maxdiff_q_id, items_to_send);
  }
  LOAD_BALANCE_LOG_DEBUG_TD("Balancing Queue[%ld] with %ld items with Queue[%ld] with %ld items -- sending %ld items\n", 
    lb_struct->src_q, qsize_src, maxdiff_q_id, qsize_dst, items_to_send);

  if ( lb_struct->qsize_history != NULL ) {
    unsigned long* qsize_history = lb_struct->qsize_history;
    for (int j = 0; j < queue_count; j++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], dq_queue_size_by_tid(tids[j]));
      qsize_history[j] = dq_queue_size_by_tid(tids[j]);;
    }
  }

  pthread_mutex_unlock(&add_mutexes[maxdiff_q_id]);
  pthread_mutex_unlock(&add_mutexes[lb_struct->src_q]);
  pthread_mutex_unlock(&rm_mutexes[maxdiff_q_id]);
  pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
  pthread_mutex_unlock(&local_queue_struct_mutex);

  //clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  //LOAD_BALANCE_LOG_DEBUG_TD("[LB-Pair]: Final realtime local LB time = '%lu.%lu'\n", 
  //  time_diff_dds(tp_rt_start, tp_rt_end).tv_sec, time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec );
  LOAD_BALANCE_LOG_DEBUG_TD("[LB-Pair]: Final thread local LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end).tv_sec, time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec );
  //total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end).tv_sec;
  //total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec;
  total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end).tv_sec;
  total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec;

  free(current_time);
  free(tp_thr_start);
  free(tp_thr_end);

  return 0;

}

int dq_load_balancer_all(void* arg) {

 /*
  * Counts estimated size
  * Count who should give whom what amount of items
  * Relocates data
  */

  struct load_balancer_struct *lb_struct = arg;

  if (queue_count == 1) {
    pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
    return 0;
  }

  //struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  //struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *current_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  //clock_gettime(CLOCK_REALTIME, tp_rt_start);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);
  clock_gettime(CLOCK_REALTIME, current_time);

  if ( (time_diff_dds(current_time, last_local_rebalance_time).tv_sec == 0) && 
        time_diff_dds(current_time, last_local_rebalance_time).tv_nsec < local_balance_last_balance_threshold ) {
    ;
  }
  else {
    LOAD_BALANCE_LOG_DEBUG_TD("Last local balance was %ld sec and %ld nsec ago\n", 
      time_diff_dds(last_local_rebalance_time, current_time).tv_sec, time_diff_dds(last_local_rebalance_time, current_time).tv_nsec);
    usleep(local_balance_wait_timer);    
  }
  clock_gettime(CLOCK_REALTIME, last_local_rebalance_time);

  //GET LOCKS
  if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {
    if ( lb_struct->src_q == -1 ) {
      //IF QSIZE WATCHER CALLS BALANCE
      LOCK_LOCAL_QUEUES();
    }
    else { 
      //LOCK_LOCAL_QUEUES_EXCEPT_RM(lb_struct->src_q);
      pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
      LOCK_LOCAL_QUEUES();
    }
  }
  else {
    free(current_time);
    free(tp_thr_start);
    free(tp_thr_end);
    return -1;
  }

  LOAD_BALANCE_LOG_DEBUG_TD("LB TIME START: Thread Time: %lu sec and %lu nsec\n", 
    tp_thr_start->tv_sec, tp_thr_start->tv_nsec);

  //Do not use function lockfree_queue_size_total_consistent(), because mutexes are already locked. 
  unsigned long items_to_send;
  unsigned long total = dq_local_size();
  unsigned long estimated_size = total / queue_count;

  int *indexes = (int*) malloc ( 2 * sizeof(int));
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  if ( q_sizes == NULL ) {
    LOG_ERR_T( (long) -2, "Malloc failed\n");
    free(current_time);
    free(tp_thr_start);
    free(tp_thr_end);
    return -1;
  }

  //If items can not be balanced, just take one item to calling queue
  if ( estimated_size == 0 ) {
    LOAD_BALANCE_LOG_DEBUG_TD("Estimated size = 0\n");
    for (int j = 0; j < queue_count; j++) {
      q_sizes[j] = dq_queue_size_by_tid(tids[j]);
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
    }
    dq_util_find_max_min_element_index(q_sizes, queue_count, indexes);
    LOAD_BALANCE_LOG_DEBUG_TD("Max: Q%d with %lu --- Min: Q%d with %lu  ---  Sending: 1 item\n", indexes[0], q_sizes[indexes[0]], 
      indexes[1], q_sizes[indexes[1]]);
    if ( indexes[0] != lb_struct->src_q ) {
      dq_move_items(indexes[0], (int) lb_struct->src_q, (unsigned long) 1);
    }
  }
  else {
    for (int i = 0 ; i < queue_count - 1; i++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Load balance round %d\n", i);
      for (int j = 0; j < queue_count; j++) {
        q_sizes[j] = dq_queue_size_by_tid(tids[j]);
        LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      }
      
      dq_util_find_max_min_element_index(q_sizes, queue_count, indexes);

      if ( (q_sizes[indexes[0]] - (abs(q_sizes[indexes[1]] - estimated_size))) >= estimated_size )
         items_to_send = abs(q_sizes[indexes[1]] - estimated_size);
      else
         items_to_send = q_sizes[indexes[0]] - estimated_size;

      LOAD_BALANCE_LOG_DEBUG_TD("Max: Q%d with %lu --- Min: Q%d with %lu  ---  Sending: %lu items\n", indexes[0], q_sizes[indexes[0]], 
        indexes[1], q_sizes[indexes[1]], items_to_send);

      /*
       * remove N items from queue queues[indexes[0]] (queue with more items)
       * add N items to queue queues[indexes[1]]]
       */
      dq_move_items(indexes[0], indexes[1], items_to_send);
    }
  }

  if ( lb_struct->qsize_history != NULL ) {
    unsigned long* qsize_history = lb_struct->qsize_history;
    for (int j = 0; j < queue_count; j++) {
      q_sizes[j] = dq_queue_size_by_tid(tids[j]);
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      qsize_history[j] = q_sizes[j];
    }
  }
   
  free(indexes);

  UNLOCK_LOCAL_QUEUES();
  pthread_mutex_unlock(&load_balance_mutex);

  //clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  //LOAD_BALANCE_LOG_DEBUG_TD("Final realtime local LB time = '%lu sec and %lu nsec'\n", 
  //  time_diff_dds(tp_rt_start, tp_rt_end).tv_sec, time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec );
  LOAD_BALANCE_LOG_DEBUG_TD("Final thread local LB time = '%lu sec and %lu nsec'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end).tv_sec, time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec );

  //total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end).tv_sec;
  //total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec;
  total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end).tv_sec;
  total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec;

  free(current_time);
  free(tp_thr_start);
  free(tp_thr_end);
  free(q_sizes);

  return 0;

}

void dq_move_items(int q_id_src, int q_id_dest, unsigned long count) {

 /*
  * Does not lock queues
  */

  if ( count == 0 ) {
    return;
  }

  struct dq_queue *q_src = queues[ q_id_src ];
  struct dq_queue *q_dst = queues[ q_id_dest ];

  unsigned long q_size_src = atomic_load( &(q_src->a_qsize) );

  struct dq_item *tmp_div;
  struct dq_item *tmp_div_next;
  tmp_div_next = q_src->divider->next;
  tmp_div = q_src->divider;

  //printf("Count=%ld, Q_SRC_SIZE=%ld, Q_DST_SIZE=%ld\n", count, q_size_src, q_size_dest);

  if ( count > q_size_src ) {
    //LOG_ERR_T( (long) -2, "Cannot move more items(%ld) than queue size(%ld)\n", count, q_size_src);
    LOAD_BALANCE_LOG_DEBUG_TD("Cannot move more items(%ld) than queue size(%ld)\n", count, q_size_src);
    return;
  }
  else {
    for (int i = 0; i < count; i++) {
      tmp_div = tmp_div->next;
    }
    q_src->divider->next = tmp_div->next;
    if ( q_src->divider->next == NULL ) {
      q_src->tail = q_src->divider;
    }

    tmp_div->next = NULL;
    q_dst->tail->next = tmp_div_next;
    q_dst->tail = tmp_div;
  }

  atomic_fetch_sub(&(q_src->a_qsize), count);
  atomic_fetch_add(&(q_dst->a_qsize), count);
  atomic_fetch_add(&moved_items_log, count);
  LOAD_BALANCE_LOG_DEBUG_TD("Moved items count = %ld\n", atomic_load(&moved_items_log));

}

void *dq_qsize_watcher_min_max() {

  bool balance_flag;
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  unsigned long total_qsize;
  long last_q = 0;
  struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  
  unsigned long *qsize_history = (unsigned long*) malloc ( queue_count * sizeof (unsigned long));
  for (int i = 0; i < queue_count; i++) {
    qsize_history[i] = 0;
  }

  QSIZE_WATCHER_LOG_DEBUG_TD("Qsize watcher started\n");

  unsigned long call_count = 0;
  while(1) {
    call_count++;
    total_qsize = 0;
    balance_flag = false;
    
    //Sleep(microseconds) for a while for less overhead
    usleep(qsize_wait_time);

    if ( flag_watcher_graceful_stop ) {
      break;
    }

    for (int i = 0; i < queue_count; i++) {
      q_sizes[i] = dq_queue_size_by_tid(tids[i]);
      total_qsize += q_sizes[i];
    }
    
    if ( threshold_type == 1 ) {
      //Static threshold type
      for (int i = 0; i < queue_count; i++) {
        int id = (last_q + i + 1) % queue_count;
        if ( (q_sizes[id] < local_threshold_static) && (qsize_history[id] >= local_threshold_static) ) {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
          last_q = i;
          balance_flag = true;
          break;
        }
        /*else {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
        }*/
      }
    }
    else if ( threshold_type == 2 ) {
      //Percentage threshold type
      unsigned long threshold = (total_qsize / 100) * local_threshold_percent;
      for (int i = 0; i < queue_count; i++) {
        int id = (last_q + i + 1) % queue_count;
        if ( (q_sizes[id] < threshold) && (qsize_history[id] >= threshold) ) {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
          last_q = i;
          balance_flag = true;
          break;
        }
        /*else {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
        }*/
      }
    }
    else if ( threshold_type == 3 ) {
      //Dynamic threshold type
      unsigned long threshold = ( (atomic_load(&rm_count_last) / queue_count) * len_s);
      QSIZE_WATCHER_LOG_DEBUG_TD("Dynamic threshold set to %ld\n", threshold);
      for (int i = 0; i < queue_count; i++) {
        int id = (last_q + i + 1) % queue_count;
        if ( (q_sizes[id] < threshold) && (qsize_history[id] >= threshold) ) {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
          last_q = i;
          balance_flag = true;
          break;
        }
        /*else {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[id], id, qsize_history[id], id, q_sizes[id]);
        }*/
      }
    }

    if (!balance_flag) {
       continue;
    }
 
    QSIZE_WATCHER_LOG_DEBUG_TD("Balancing queues\n");
    lb_struct->qsize_history = qsize_history;
    lb_struct->src_q = last_q;

    pthread_mutex_lock(&rm_mutexes[last_q]);  //need to lock this because in remove it already has lock
    if ( lbs(lb_struct) != -1 ) {  
      
      atomic_fetch_add(&load_balancer_call_count_watcher, 1);
      QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Qsize history after rebalance is:\n");
      for (int i = 0; i < queue_count; i++) {
        QSIZE_WATCHER_LOG_DEBUG_TD("Q%d-%ld items\n", i, qsize_history[i]);
      }
    }
    else {
      pthread_mutex_unlock(&rm_mutexes[last_q]); 
      QSIZE_WATCHER_LOG_DEBUG_TD("Load Balancer already running\n");
    }
  }

  LOG_INFO_TD("Qsize watcher min max strategy loop count = %ld\n", call_count);

  free(q_sizes);
  free(lb_struct->qsize_history);
  free(lb_struct);

  return NULL;

}

void* dq_qsize_watcher_local_threshold() {
   
  bool balance_flag;
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  unsigned long total_qsize;
  unsigned long *qsize_history = (unsigned long*) malloc ( queue_count * sizeof (unsigned long));
  for (int i = 0; i < queue_count; i++) {
    qsize_history[i] = 0;
  }
  struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  
  QSIZE_WATCHER_LOG_DEBUG_TD("Qsize watcher started\n");
   
  unsigned long call_count = 0;
  while(1) {
    call_count++;
    total_qsize = 0;
    balance_flag = false;

    //Sleep(microseconds) for a while for less overhead
    usleep(qsize_wait_time);

    if ( flag_watcher_graceful_stop ) {
      //return NULL;
      break;
    }

    for (int i = 0; i < queue_count; i++) {
      q_sizes[i] = dq_queue_size_by_tid(tids[i]);
      total_qsize += q_sizes[i];
    }
    QSIZE_WATCHER_LOG_DEBUG_TD("Total qsize = %ld\n", total_qsize);
  
    if (total_qsize == 0) {
      continue;
    }
    
    /*
     * Look if at least one of queue sizes fell under threshold after last check
     * According to argument threshold_type is chosen threshold deciding strategy
     */

    if ( threshold_type == 1 ) {
      //Static threshold type
      for (int i = 0; i < queue_count; i++) {
        if ( (q_sizes[i] < local_threshold_static) && (qsize_history[i] >= local_threshold_static) ) {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
          balance_flag = true;
        }
        else {
          QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
            Old Q%d size was %ld\n\
            New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
        }
      }
    }
    else if ( threshold_type == 2 ) {
      //Percentage threshold type
      unsigned long threshold = (total_qsize / 100) * local_threshold_percent;
      for (int i = 0; i < queue_count; i++) {
        if ( (q_sizes[i] < threshold) && (qsize_history[i] >= threshold) ) {
           QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
              Old Q%d size was %ld\n\
              New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
           balance_flag = true;
        }
        else {
           QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
              Old Q%d size was %ld\n\
              New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
        }
      }
    }
    else if ( threshold_type == 3 ) {
      //Dynamic threshold type
      unsigned long threshold = ( (atomic_load(&rm_count_last) / queue_count) * len_s);
      QSIZE_WATCHER_LOG_DEBUG_TD("Dynamic threshold set to %ld\n", threshold);
      for (int i = 0; i < queue_count; i++) {
        if ( (q_sizes[i] < threshold) && (qsize_history[i] >= threshold) ) {
           QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld REBALANCE:\n\
              Old Q%d size was %ld\n\
              New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
           balance_flag = true;
        }
        else {
           QSIZE_WATCHER_LOG_DEBUG_TD("Q%ld OK:\n\
              Old Q%d size was %ld\n\
              New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
        }
      }
    }
    
    //Write new qsize history
    for (int i = 0; i < queue_count; i++)
       qsize_history[i] = q_sizes[i];

    if (!balance_flag) {
       continue;
    }
 
    QSIZE_WATCHER_LOG_DEBUG_TD("Queues turned below threshold\n");
    lb_struct->qsize_history = qsize_history;
    lb_struct->src_q = -1;
    if ( lbs(lb_struct) != -1 ) {

      atomic_fetch_add(&load_balancer_call_count_watcher, 1);
      QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Qsize history after rebalance is:\n");

      for (int i = 0; i < queue_count; i++) {
        QSIZE_WATCHER_LOG_DEBUG_TD("Q%d-%ld items\n", i, qsize_history[i]);
      }

    }
    else {
       QSIZE_WATCHER_LOG_DEBUG_TD("Load Balancer already running\n");
       continue;
    }
  }

  LOG_INFO_TD("Qsize watcher all equal check loop count = %ld\n", call_count);

  free(q_sizes);
  free(lb_struct->qsize_history);
  free(lb_struct);

  return NULL;

}

int lockfree_queue_remove_item (void* buffer) {

 /*
  * timeout is in microseconds
  */

  void* val = NULL;
  long tid = dq_get_removal_tid();
   
  struct dq_queue *q = queues[ tid ];
   
  pthread_mutex_lock(&rm_mutexes[tid]);
   
  if ( q->divider != q->tail ) {
    val = q->divider->next->val;
    q->divider = q->divider->next;
    atomic_fetch_sub( &(q->a_qsize), 1);
  }
  else {
    pthread_mutex_unlock(&rm_mutexes[tid]);
    while(1) {
      pthread_mutex_lock(&rm_mutexes[tid]);
      if ( q->divider != q->tail ) {
        val = q->divider->next->val;
        q->divider = q->divider->next;
        atomic_fetch_sub( &(q->a_qsize), 1);
        break;
      }

      if ( dq_local_size() == 0 ) {
        pthread_mutex_unlock(&rm_mutexes[tid]);
        if ( dq_global_size(false) != 0 ) {
          dq_global_balance(tid);
          continue;
        }
        else {
          pthread_mutex_lock(&rm_mutexes[tid]);
          break;
        }
      }
      else {
        struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc(sizeof(struct load_balancer_struct));
        lb_struct->qsize_history = NULL;
        lb_struct->src_q = tid;
        if ( lbs(lb_struct) != -1 ) {
          atomic_fetch_add(&load_balancer_call_count_remove, 1);
          free(lb_struct);
        }
        else {
          free(lb_struct);
          pthread_mutex_unlock(&rm_mutexes[tid]);
          usleep(10);
        }
      }
    }
  }
   
  pthread_mutex_unlock(&rm_mutexes[tid]);

  #ifdef COUNTERS
    if (val != NULL) {
      atomic_fetch_add(&rm_count, 1);
    }
  #endif

  if (val != NULL) {
    memcpy(buffer, val, q->item_size);
    return 0;
  }
  else {
    return -1;
  } 
   
}

int dq_remove_item_by_tid (void* t, void* buffer) {

  void* val = NULL;
  long* tid = t;

  struct dq_queue *q = queues[ *tid ]; 

  if ( q->a_qsize == 0 ) {
    usleep(50);
  }

  pthread_mutex_lock(&rm_mutexes[*tid]);

  if ( q->divider != q->tail ) {
    val = q->divider->next->val;
    q->divider = q->divider->next;
    atomic_fetch_sub( &(q->a_qsize), 1);
  }
  else {
    pthread_mutex_unlock(&rm_mutexes[*tid]);
    while(1) {
      pthread_mutex_lock(&rm_mutexes[*tid]);
      if ( q->divider != q->tail ) {
        val = q->divider->next->val;
        q->divider = q->divider->next;
        atomic_fetch_sub( &(q->a_qsize), 1);
        break;
      }

      if ( dq_local_size() == 0 ) {
        pthread_mutex_unlock(&rm_mutexes[*tid]);
        if ( dq_global_size(false) != 0 ) {
          dq_global_balance(*tid);
          continue;
        }
        else {
          pthread_mutex_lock(&rm_mutexes[*tid]);
          break;
        }
      }
      else {
        struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc(sizeof(struct load_balancer_struct));
        lb_struct->qsize_history = NULL;
        lb_struct->src_q = *tid;
        if ( lbs(lb_struct) != -1 ) {
          atomic_fetch_add(&load_balancer_call_count_remove, 1);
          free(lb_struct);
        }
        else {
          free(lb_struct);
          pthread_mutex_unlock(&rm_mutexes[*tid]);
          usleep(10);
        }
      }
    }
  }
   
  pthread_mutex_unlock(&rm_mutexes[*tid]);
  #ifdef COUNTERS
    if (val != NULL) {
      atomic_fetch_add(&rm_count, 1);
    }
  #endif

  if (val != NULL) {
    memcpy(buffer, val, q->item_size);
    return 0;
  }
  else {
    return -1;
  } 
   
}

int lockfree_queue_remove_Nitems (unsigned long N, void** buffer) {
   
 /*
  * N is amount of items to be taken from Q
  */

  long tid = dq_get_removal_tid();
   
  struct dq_queue *q = queues[ tid ];
   
  pthread_mutex_lock(&rm_mutexes[tid]);
  
  unsigned long qsize = atomic_load( &(q->a_qsize) );
  if ( qsize < N ) {
    //printf("Not enough items in queue %ld. There are %ld but was requested %ld.\n", tid, qsize, N);
    LOG_DEBUG_TD(tid, "Not enough items in queue %ld. There are %ld but was requested %ld.\n", tid, qsize, N);
    pthread_mutex_unlock(&rm_mutexes[tid]);
    return -1;
  }
   
  unsigned long i = 0;
  for (i = 0; i < N; i++) {
    if ( q->divider != q->tail ) {
      memcpy(buffer[i], q->divider->next->val, q->item_size);
      q->divider = q->divider->next;
    }
    else {
      break;
    }
  }

  if (i != N) {
    //printf("Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", tid, i);
    LOG_DEBUG_TD(tid, "Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", tid, i);
    pthread_mutex_unlock(&rm_mutexes[tid]);
    return -1;
  }

  atomic_fetch_sub( &(q->a_qsize), N);
  pthread_mutex_unlock(&rm_mutexes[tid]);

  return 0;
   
}

int dq_remove_Nitems_by_tid_no_lock(long qid, long item_cnt, void** buffer) {
   
  struct dq_queue *q = queues[ qid ];

  unsigned long qsize = atomic_load( &(q->a_qsize) );
  if ( qsize < item_cnt ) {
    //printf("Not enough items in queue %ld. There are %ld but was requested %ld.\n", qid, qsize, item_cnt);
    LOG_DEBUG_TD(qid, "Not enough items in queue %ld. There are %ld but was requested %ld.\n", qid, qsize, item_cnt);
    return -1;
  }
   
  unsigned long i;
  for (i = 0; i < item_cnt; i++) {
    if ( q->divider != q->tail ) {
      memcpy(buffer[i], q->divider->next->val, q->item_size);
      q->divider = q->divider->next;
    }
    else {
      break;
    }
  }

  if ( i != item_cnt ) {
    //printf("Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", qid, i);
    LOG_DEBUG_TD(qid, "Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", qid, i);
    return -1;
  }
  
  atomic_fetch_sub( &(q->a_qsize), item_cnt);
  
  return 0;

}

unsigned long dq_global_size(bool consistency) {
   
  /*****
  * send message 'init_global_size'(900) to master
  * all nodes must listen to master 'stop_work' message(901)... so they listens (at least) to master
  * master listens to messages from compute nodes in separate thread
  * on message dq_global_size master stops all compute nodes -> send message to go to barrier
  * after this message, all threads must go to method MPI_Barrier(comm) or global comm_size thread must lock all queues ?
  * after stop, thread sends their queue structure comm_size to master(902)
  * master counts the sum and returns it to user(903)
  * master goes to barrier and unlock all nodes
  */

  if (comm_size <= 1) {
    return dq_local_size();
  }

  if ( pthread_rwlock_trywrlock(&global_size_rwlock) != 0 ) {
    /*
     * Can not get lock, someone has it
     * Wait until he finish job and get global size from his computing
     */    
    GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Can't acquire global size lock, waiting for result of other thread\n");
    if ( pthread_rwlock_rdlock(&global_size_rwlock) != 0 ) {
      return ULONG_MAX;
    }
    else {
      pthread_rwlock_unlock(&global_size_rwlock);
      atomic_fetch_add(&global_size_call_count_in_wait, 1);
      return last_global_size;
    }
  }

  atomic_fetch_add(&global_size_call_count, 1);

  struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, tp_rt_start);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);

  MPI_Status status;
  short consistency_type;
  short buf;

  if ( consistency ) {
    consistency_type = 1;
  }
  else {
    consistency_type = 0;
  }
  
  MPI_Send(&consistency_type, 1, MPI_SHORT, master_id, 900, MPI_COMM_WORLD);

  unsigned long global_size_val = 0;

  MPI_Recv(&buf, 1, MPI_SHORT, master_id, 905, MPI_COMM_WORLD, &status);

  MPI_Recv(&global_size_val, 1, MPI_UNSIGNED_LONG, master_id, 903, MPI_COMM_WORLD, &status);
  last_global_size = global_size_val;

  MPI_Barrier(MPI_COMM_WORLD);
  pthread_rwlock_unlock(&global_size_executing_rwlock);
  pthread_rwlock_unlock(&global_size_rwlock);
  
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "NODE %d: Global structure size is %ld\n", comm_rank, global_size_val);
  
  clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final realtime global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_rt_start, tp_rt_end).tv_sec, time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec );
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final thread global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end).tv_sec, time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec );

  total_rt_global_size_time_sec += time_diff_dds(tp_rt_start, tp_rt_end).tv_sec;
  total_rt_global_size_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end).tv_nsec;
  total_thr_global_size_time_sec += time_diff_dds(tp_thr_start, tp_thr_end).tv_sec;
  total_thr_global_size_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end).tv_nsec;

  free(tp_rt_start);
  free(tp_rt_end);
  free(tp_thr_start);
  free(tp_thr_end);

  return global_size_val;
   
}

void* dq_comm_listener_global_size() {

  MPI_Request *requests_901 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
  MPI_Request *requests_902 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
  MPI_Status *statuses_901 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
  MPI_Status *statuses_902 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
  MPI_Request request_test;
  MPI_Status status_test;
  int test_flag = 0;

  while(1) {

    if (comm_rank == master_id) {
     /*
      * MASTER
      */

      int consistency_type = 0;
      int *consistency_type_src_id = (int*) malloc (2 * sizeof(int));
      
      MPI_Irecv(&consistency_type, 1, MPI_INT, MPI_ANY_SOURCE, 900, MPI_COMM_WORLD, &request_test);

      while(1) {
        MPI_Test(&request_test, &test_flag, &status_test);
        if (!test_flag) {
          //No request for global size
          usleep(global_size_receive_timeout); //in microseconds
        }
        else {
          //Got request for global size
          break;
        }
        if (flag_graceful_stop) {
          //Cleanup on exit
          printf("Gracefully stopping global size listener thread in node %d\n", comm_rank);
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global size listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request_test);
          free(requests_901); free(requests_902); free(statuses_901); free(statuses_902);
          free(consistency_type_src_id);
          return NULL;
        }
      }

      int source_node = status_test.MPI_SOURCE;
      //int recv_tag = status_test.MPI_TAG;
      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Got message with consistency_type=%hd from node %d\n", consistency_type, source_node);
      int cnt = 0;

      //Send doing global size for you message
      short code_905 = 905;
      MPI_Send(&code_905, 1, MPI_SHORT, source_node, 905, MPI_COMM_WORLD);
      
      consistency_type_src_id[0] = consistency_type;
      consistency_type_src_id[1] = source_node;
      for(int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Isend(consistency_type_src_id, 2, MPI_INT, i, 901, MPI_COMM_WORLD, &requests_901[cnt]);
          cnt++;
        }
      }

      MPI_Waitall(comm_size - 1, requests_901, statuses_901);

      unsigned long *node_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
      unsigned long global_struct_size_total = 0;
      cnt = 0;

      if ( consistency_type_src_id[0] == 0 ) {
        //Get inconsistent global size

        unsigned long master_struct_size = dq_local_size();
        for (int i = 0; i < comm_size; i++) {
          if (i != comm_rank) {
            MPI_Irecv(&node_sizes[i], 1, MPI_UNSIGNED_LONG, i, 902, MPI_COMM_WORLD, &requests_902[cnt]);
            cnt++;
          }
          else {
            node_sizes[i] = master_struct_size;
          }
        }

        MPI_Waitall(comm_size - 1, requests_902, statuses_902);

        for (int i = 0; i < comm_size; i++) {
          GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Node %d has %ld items\n", i, node_sizes[i]);
          global_struct_size_total += node_sizes[i];
        }
        GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Global size is %ld\n", global_struct_size_total);

        MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);

        if ( consistency_type_src_id[1] != comm_rank ) {
          MPI_Barrier(MPI_COMM_WORLD);
        }

      }
      else if ( consistency_type_src_id[0] == 1 ) {
        //Get consistent global size
        
        LOCK_LOAD_BALANCER();
        LOCK_LOCAL_QUEUES();

        unsigned long master_struct_size = dq_local_size();
        for (int i = 0; i < comm_size; i++) {
          if (i != comm_rank) {
            MPI_Irecv(&node_sizes[i], 1, MPI_UNSIGNED_LONG, i, 902, MPI_COMM_WORLD, &requests_902[cnt]);
            cnt++;
          }
          else {
            node_sizes[i] = master_struct_size;
          }
        }

        MPI_Waitall(comm_size - 1, requests_902, statuses_902);

        for (int i = 0; i < comm_size; i++) {
          GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Node %d has %ld items\n", i, node_sizes[i]);
          global_struct_size_total += node_sizes[i];
        }
        GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Global size is %ld\n", global_struct_size_total);

        MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);

        if ( consistency_type_src_id[1] != comm_rank ) {
          MPI_Barrier(MPI_COMM_WORLD);
        }

        UNLOCK_LOCAL_QUEUES();
        UNLOCK_LOAD_BALANCER();

      }

      free(consistency_type_src_id);
      free(node_sizes);

    }
    else { 
      /*
       * SLAVE
       */

      int test_flag = 0;
      int *consistency_type_src_id = (int*) malloc (2 * sizeof(int));
      consistency_type_src_id[0] = 0;

      MPI_Irecv(consistency_type_src_id, 2, MPI_INT, master_id, 901, MPI_COMM_WORLD, &request_test);

      while(1) {
        MPI_Test(&request_test, &test_flag, &status_test);
        if (!test_flag) {
          //No request for global size
          usleep(global_size_receive_timeout); //in microseconds
        }
        else {
          //Got request for global size
          break;
        }
        if (flag_graceful_stop) {
          //Cleanup on exit
          free(requests_901); free(requests_902); free(statuses_901); free(statuses_902);
          free(consistency_type_src_id);
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global size listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request_test);
          return NULL;
        }
      }

      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "COMPUTE NODE: Received 901 - consistency %d, src_node %d\n", consistency_type_src_id[0], consistency_type_src_id[1]);
      if ( consistency_type_src_id[0] == 0 ) {

        unsigned long local_size = dq_local_size();
        MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

        if ( consistency_type_src_id[1] != comm_rank ) {
          MPI_Barrier(MPI_COMM_WORLD);
        }

      }
      else if ( consistency_type_src_id[0] == 1 ) {
        LOCK_LOAD_BALANCER();
        LOCK_LOCAL_QUEUES();

        unsigned long local_size = dq_local_size();
        MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

        if ( consistency_type_src_id[1] != comm_rank ) {
          MPI_Barrier(MPI_COMM_WORLD);
        }

        UNLOCK_LOAD_BALANCER();
        UNLOCK_LOCAL_QUEUES();

      }

      free(consistency_type_src_id);

    }
  }

  //

}

int dq_global_balance(long tid) {

  if (comm_size <= 1) {
    return 0;
  }

  struct timespec *balance_start_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *balance_end_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *balance_start_thr_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *balance_end_thr_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, balance_start_time);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, balance_start_thr_time);
  
  if ( pthread_rwlock_trywrlock(&load_balance_global_rwlock) == 0 ) {
    //Got lock
    if ( time_diff_dds(last_global_rebalance_call_send_time, balance_start_time).tv_sec > 0 ) {
      //Last rebalance was more than one second ago, do rebalance again
      atomic_fetch_add(&global_balance_call_count, 1);
      clock_gettime(CLOCK_REALTIME, last_global_rebalance_call_send_time);
      GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Starting global balance in %ld.%ld\n", last_global_rebalance_call_send_time->tv_sec, last_global_rebalance_call_send_time->tv_nsec);
      atomic_fetch_add(&load_balancer_call_count_global, 1);

      short buf;
      MPI_Status status;
      short code_800 = 800;
      MPI_Send(&code_800, 1, MPI_SHORT, master_id, 800, MPI_COMM_WORLD);
      MPI_Recv(&buf, 1, MPI_INT, master_id, 805, MPI_COMM_WORLD, &status); // Balancing finished

      if (buf == 0) {
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node %d: Balancing finished\n", comm_rank);
      } 
      else if (buf == 1) {
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node %d: Balancing has not run, because last balance was too early\n", comm_rank);
        atomic_fetch_add(&global_balance_call_count_in_wait, 1);
      }

      pthread_rwlock_unlock(&load_balance_global_rwlock);

    }
    else {
      pthread_rwlock_unlock(&load_balance_global_rwlock);
      atomic_fetch_add(&global_balance_call_count_in_wait, 1);
      free(balance_start_time);
      free(balance_end_time);
      free(balance_start_thr_time);
      free(balance_end_thr_time);
      return 1; //Rebalance time not more than 1 second
    }

  }
  else {
    GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Global balance already locked, waiting to finish\n");
    if ( pthread_rwlock_rdlock(&load_balance_global_rwlock) != 0 ) {
      atomic_fetch_add(&global_balance_call_count_in_wait, 1);
      free(balance_start_time);
      free(balance_end_time);
      free(balance_start_thr_time);
      free(balance_end_thr_time);
      return -1;  // Error in rdlock
    }
    else {
      GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Node %d: Global balance finished, got rdlock and returning\n", comm_rank);
      pthread_rwlock_unlock(&load_balance_global_rwlock);
      atomic_fetch_add(&global_balance_call_count_in_wait, 1);
      free(balance_start_time);
      free(balance_end_time);
      free(balance_start_thr_time);
      free(balance_end_thr_time);
      return 2; //Rebalance already started, waiting for results and returning
    }
  }

  clock_gettime(CLOCK_REALTIME, balance_end_time);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, balance_end_thr_time);
  GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Global balance Realtime time was %lu sec %lu nsec\n", 
    time_diff_dds(balance_start_time, balance_end_time).tv_sec, time_diff_dds(balance_start_time, balance_end_time).tv_nsec);
  GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Global balance Thread time was %lu sec %lu nsec\n", 
    time_diff_dds(balance_start_thr_time, balance_end_thr_time).tv_sec, time_diff_dds(balance_start_thr_time, balance_end_thr_time).tv_nsec);

  total_rt_global_balance_time_sec += time_diff_dds(balance_start_time, balance_end_time).tv_sec;
  total_rt_global_balance_time_nsec += time_diff_dds(balance_start_time, balance_end_time).tv_nsec;
  total_thr_global_balance_time_sec += time_diff_dds(balance_start_thr_time, balance_end_thr_time).tv_sec;
  total_thr_global_balance_time_nsec += time_diff_dds(balance_start_thr_time, balance_end_thr_time).tv_nsec;

  free(balance_start_time);
  free(balance_end_time);
  free(balance_start_thr_time);
  free(balance_end_thr_time);

  return 0; //Rebalance finished successfuly

}

void* dq_comm_listener_global_balance() {

 /*
  * Send message that local structure is empty(800) to master.
  * Wait for reply if other queues are empty as well(code 801)
  * reply message number is 0 -> queues are not empty if message is 1, queues are empty)
  * if reply is that they are empty, return NULL
  * if queues are not empty, wait for master to rebalance (barrier?)
  * waiting for rebalancing is done with separate thread which wait for message (802)
  * after rebalancing get size of queue or read message with amount of items send
  * if amount = 0; return NULL
  * if not, return item.
  */

  if ( comm_rank == master_id ) {
   /*
    * MASTER
    */
    struct timespec *current_time = (struct timespec*) malloc (sizeof (struct timespec));

    MPI_Request *requests = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
    MPI_Request *requests_801 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
    MPI_Status *statuses = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
    MPI_Status *statuses_801 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));

    while(1) {
      short buf;
      int source_node;
        
      MPI_Request request;
      MPI_Irecv(&buf, 1, MPI_SHORT, MPI_ANY_SOURCE, 800, MPI_COMM_WORLD, &request); //request for global balance
      while(1) {
        MPI_Status status;
        int test_flag;
        MPI_Test(&request, &test_flag, &status);
        if (!test_flag) {
          //No request for global balance
          usleep(global_size_receive_timeout); //in microseconds
        }
        else {
          //Got request for global balance
          source_node = status.MPI_SOURCE;
          break;
        }
        if (flag_graceful_stop) {
          //Cleanup on exit
          free(current_time);
          free(requests); free(requests_801); free(statuses); free(statuses_801);
          printf("Gracefully stopping global balance listener thread in node %d\n", comm_rank);
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global balance listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request);
          return NULL;
        }
      }

      //End if rebalance is called too early
      clock_gettime(CLOCK_REALTIME, current_time);
      if ( time_diff_dds(last_global_rebalance_done_time, current_time).tv_sec < 1 ) {
        //return 1 is that global balance was not done because timeout was not reached
        int code_805 = 1; 
        //rebalance finished unsuccessfuly
        MPI_Send(&code_805, 1, MPI_INT, source_node, 805, MPI_COMM_WORLD); 
        atomic_fetch_add(&global_balance_rejected_as_master_count, 1);
        continue;
      }
      atomic_fetch_add(&global_balance_executed_as_master_count, 1);

      short msg = 0;
      int cnt = 0;
      for(int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Isend(&msg, 1, MPI_SHORT, i, 801, MPI_COMM_WORLD, &requests_801[cnt]);
          cnt++;
        }
      }
      MPI_Waitall(comm_size - 1, requests_801, statuses_801);
      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER GB: sent all rebalance requests\n");

      unsigned long *node_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
      cnt = 0;

      LOCK_LOAD_BALANCER();
      LOCK_LOCAL_QUEUES();

      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Irecv(&node_sizes[i], 1, MPI_UNSIGNED_LONG, i, 802, MPI_COMM_WORLD, &requests[cnt]);
          cnt++;
        }
        else {
          node_sizes[i] = dq_local_size();
        }
      }
      MPI_Waitall(comm_size - 1, requests, statuses);
      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER GB: received all struct sizes\n");

      //COUNT RELOCATION
      unsigned long items_to_send;
      unsigned long estimated_size = dq_util_sum_arr(node_sizes, comm_size) / comm_size;
      work_to_send *wts = (work_to_send*) malloc(comm_size * sizeof(work_to_send));

      for (int i = 0 ; i < comm_size; i++) {
        wts[i].send_count = 0;
        wts[i].dst_node_ids = NULL;
        wts[i].item_counts = NULL;
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node[%d] size is %ld\n", i, node_sizes[i]);
      }

      int *indexes = (int*) malloc ( 2 * sizeof(int));
      for (int i = 0 ; i < comm_size - 1; i++) {
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Global Load balance round %d\n", i);
        
        dq_util_find_max_min_element_index(node_sizes, comm_size, indexes);

        if ( (node_sizes[indexes[0]] - (abs(node_sizes[indexes[1]] - estimated_size))) >= estimated_size )
           items_to_send = abs(node_sizes[indexes[1]] - estimated_size);
        else
           items_to_send = node_sizes[indexes[0]] - estimated_size;

        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Max: Node[%d] with %lu --- Min: Node[%d] with %lu  ---  Sending: %lu items\n", indexes[0], 
          node_sizes[indexes[0]], indexes[1], node_sizes[indexes[1]], items_to_send);
        
        if (indexes[0] == indexes[1]) {
          continue;
        }

        node_sizes[indexes[0]] -= items_to_send;
        node_sizes[indexes[1]] += items_to_send;

        wts[indexes[0]].send_count += 1;
        wts[indexes[0]].dst_node_ids = (long*) realloc(wts[indexes[0]].dst_node_ids, wts[indexes[0]].send_count * sizeof(long));          
        wts[indexes[0]].item_counts = (unsigned long*) realloc(wts[indexes[0]].item_counts, wts[indexes[0]].send_count * sizeof(unsigned long));
        wts[indexes[0]].dst_node_ids[wts[indexes[0]].send_count - 1] = indexes[1];
        wts[indexes[0]].item_counts[wts[indexes[0]].send_count - 1] = items_to_send;
        
        for (int k = 0; k < wts[indexes[0]].send_count; k++) {
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "wts%d send_count=%ld; dst_node_id %ld; item_count %ld\n", indexes[0], wts[indexes[0]].send_count, wts[indexes[0]].dst_node_ids[k], wts[indexes[0]].item_counts[k]);
        }
      }
      free(indexes);

      //Send 803 - work to send
      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          //(index %2 == 0) is node_id; (index %2 == 1) is amount of items to send

          if ( wts[i].send_count > 0 ) {
            unsigned long *arr = (unsigned long*) malloc(wts[i].send_count * 2 * sizeof(unsigned long));
            for (int j = 0; j < wts[i].send_count; j++) {
              arr[j * 2] = wts[i].dst_node_ids[j];
              arr[(j * 2) + 1] = wts[i].item_counts[j];
              GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node[%d]: Must send to N%ld - %ld items\n", i, arr[j * 2], arr[(j*2)+1]);
            }
            if ( i != master_id ) {
              MPI_Send(arr, (wts[i].send_count) * 2, MPI_UNSIGNED_LONG, i, 803, MPI_COMM_WORLD);
            }
            free(arr);
          }
          else {
            unsigned long receive_cnt = dq_util_node_receive_count(i, wts);
            GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node[%d]: must receive %lu messages\n", i, receive_cnt);
            MPI_Send(&receive_cnt, 1, MPI_UNSIGNED_LONG, i, 803, MPI_COMM_WORLD);
          }
          cnt++;
        }
        else {
          ;
        }
      }

      //Send master data if there is any
      for (int j = 0; j < wts[master_id].send_count; j++) {
        if ( wts[master_id].dst_node_ids[j] !=  master_id ) {
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Master sending to N%ld %lu items\n", wts[master_id].dst_node_ids[j], 
            wts[master_id].item_counts[j]);
          dq_send_data_to_node(wts[master_id].dst_node_ids[j], wts[master_id].item_counts[j]);
        }
      }

      int rcv_cnt = dq_util_node_receive_count(master_id, wts);
      if ( rcv_cnt > 0 ) {
        // Master waits for data receives
        for (int i = 0; i < rcv_cnt; i++) {
          int receive_item_count;
          MPI_Status status;
          MPI_Probe(MPI_ANY_SOURCE, 804, MPI_COMM_WORLD, &status);
          MPI_Get_count(&status, MPI_INT, &receive_item_count);

          int *data = malloc (receive_item_count * sizeof(int));
          void **data2 = malloc (receive_item_count * sizeof(void*));
          int src_id = status.MPI_SOURCE;
          
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER: receive_item_count to receive=%d from node %d\n", receive_item_count, src_id);
          MPI_Recv(data, receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
          
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER: received these items\n");
          for (int j = 0; j < receive_item_count; j++) {
            data2[j] = malloc(sizeof(void*));
            data2[j] = &data[j];
          }

          long smallest_qid = dq_util_find_smallest_q();
          dq_insert_N_items_no_lock_by_tid(data2, receive_item_count, &smallest_qid);
          free(data);
          free(data2);
        }
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER: Receiving work done\n");
      }

      //wait for other nodes to finish
      for ( int i = 0; i < comm_size; i++) {
        if ( comm_rank != i ) {
          MPI_Status status;
          MPI_Recv(&buf, 1, MPI_SHORT, i, 806, MPI_COMM_WORLD, &status);
        }
      }

      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "MASTER GB: GB done!\n");
      int code_805 = 0; //0 is that Global balance was finished successfuly
      MPI_Send(&code_805, 1, MPI_INT, source_node, 805, MPI_COMM_WORLD); //rebalance finished
      UNLOCK_LOAD_BALANCER();
      UNLOCK_LOCAL_QUEUES();
      
      clock_gettime(CLOCK_REALTIME, last_global_rebalance_done_time);
      free(node_sizes);

      for (int i = 0; i < comm_size; i++) {
        free(wts[i].dst_node_ids);
        free(wts[i].item_counts);
      }
      free(wts);

    }
  }
  else {
   /*
    * SLAVE
    */
    while(1) {
      
      MPI_Request request;
      MPI_Status status;
      int test_flag = 0;
      short buf;

      MPI_Irecv(&buf, 1, MPI_SHORT, master_id, 801, MPI_COMM_WORLD, &request);
      while(1) {
        MPI_Test(&request, &test_flag, &status);
        if (!test_flag) {
          //No request for global balance
          usleep(global_size_receive_timeout); //in microseconds
        }
        else {
          //Got request for global balance
          break;
        }
        if (flag_graceful_stop) {
          //Cleanup on exit
          printf("Gracefully stopping global balance listener thread in node %d\n", comm_rank);
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global balance listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request);
          return NULL;
        }
      }

      LOCK_LOAD_BALANCER();
      LOCK_LOCAL_QUEUES();
      unsigned long qsize = dq_local_size();
      int receive_messages_count;
      int receive_item_count;

      MPI_Send(&qsize, 1, MPI_UNSIGNED_LONG, master_id, 802, MPI_COMM_WORLD);
      MPI_Probe(master_id, 803, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_UNSIGNED_LONG, &receive_messages_count);

      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave%d: receive_messages_count=%d\n", comm_rank, receive_messages_count);
      unsigned long *work_arr = (unsigned long*) malloc(receive_messages_count * sizeof(unsigned long));

      if (receive_messages_count == 1) {
        // If only 1 message is send in 803 (item count in 803), slave only receives items
        // Slave only receives data
        // In work_arr[0] is number of receives
        MPI_Recv(work_arr, receive_messages_count, MPI_UNSIGNED_LONG, master_id, 803, MPI_COMM_WORLD, &status);

        if (work_arr[0] == 0) {
          // Nothing to receive
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: has nothing to receive\n", comm_rank);
        }
        else {
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: has to receive %lu times\n", comm_rank, work_arr[0]);
          
          // Wait for data receives
          for (int i = 0; i < work_arr[0]; i++) {
            MPI_Probe(MPI_ANY_SOURCE, 804, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &receive_item_count);
            
            int src_id = status.MPI_SOURCE;
            int *data = malloc (receive_item_count * sizeof(int));
            void **data2 = malloc (receive_item_count * sizeof(void*));
            
            GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave%d: receive_item_count to receive=%d from source %d\n", comm_rank, receive_item_count, src_id);
            MPI_Recv(data, receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
            
            GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: received these items...\n", comm_rank);
            for (int j = 0; j < receive_item_count; j++) {
              data2[j] = malloc(sizeof(void*));
              data2[j] = &data[j];
            }

            long smallest_qid = dq_util_find_smallest_q();
            dq_insert_N_items_no_lock_by_tid(data2, receive_item_count, &smallest_qid);
            free(data);
            free(data2);
          }
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: Receiving work done\n", comm_rank);
        }
      }
      else {
        //Receive work and send it to peers
        //if count of itesm in 803 > 1, index[0] says destination and index[1] says amount of items to send
        MPI_Recv(work_arr, receive_messages_count, MPI_UNSIGNED_LONG, master_id, 803, MPI_COMM_WORLD, &status);


        for (int i = 0; i < receive_messages_count / 2; i++) {
          unsigned long dst_node_id = work_arr[i * 2];
          unsigned long send_item_count = work_arr[(i * 2) + 1];
          
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave%d: Processing message %d\n", comm_rank, i);
          GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave%d: has to send %lu items to node %lu\n", comm_rank, send_item_count, dst_node_id);
          
          dq_send_data_to_node(dst_node_id, send_item_count);
        }
        GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: Sending work done\n", comm_rank);
      }

      short work_finished = 0;
      MPI_Send(&work_finished, 1, MPI_SHORT, master_id, 806, MPI_COMM_WORLD);  //send to master sending of work is done
      UNLOCK_LOAD_BALANCER();
      UNLOCK_LOCAL_QUEUES();
      free(work_arr);
      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Slave[%d]: Slave finished work\n", comm_rank);

    }
  }

  return NULL;

}

unsigned long dq_queue_size_by_tid (void *tid) {
   
  long *t = tid;
  //return atomic_load( &(queues[ *t ]->a_qsize) );
  return queues[*t]->a_qsize;
   
}


unsigned long dq_local_size() {

  unsigned long size = 0;
  for (int i = 0; i < queue_count; i++) {
    //size += atomic_load( &(queues[i]->a_qsize) );
    size += (queues[i]->a_qsize);
  }

  return size;
   
}

unsigned long* lockfree_queue_size_total_allarr() {
   
  unsigned long *sizes = (unsigned long*) malloc(queue_count * sizeof(unsigned long));
  for (int i = 0; i < queue_count; i++) {
    //sizes[i] = atomic_load( &(queues[i]->a_qsize) );
    sizes[i] = queues[i]->a_qsize;
  }

  return sizes;
   
}

unsigned long lockfree_queue_size_total_consistent () {

   unsigned long size = 0;
   
   LOCK_LOCAL_QUEUES();

   for (int i = 0; i < queue_count; i++) {
      size += atomic_load( &(queues[i]->a_qsize) );
   }

   UNLOCK_LOCAL_QUEUES();
   return size;
}

unsigned long* dq_local_size_allarr_consistent () {

   unsigned long* sizes = (unsigned long*) malloc(queue_count * sizeof(unsigned long));

   LOCK_LOCAL_QUEUES();

   for (int i = 0; i < queue_count; i++) {
      sizes[i] = atomic_load( &(queues[i]->a_qsize) );
   }

   UNLOCK_LOCAL_QUEUES();
   return sizes;
}

qsizes* dq_local_size_allarr_sorted () {

  qsizes *qss = (qsizes*) malloc(queue_count * sizeof(qsizes));

  for (int i = 0; i < queue_count; i++) {
    qss[i].size = atomic_load( &(queues[i]->a_qsize) );
    qss[i].index = i;
  }

  qsort(qss, queue_count, sizeof(qsizes), dq_util_qsize_comparator);

  return qss;
}

int dq_util_qsize_comparator(const void *a, const void *b) {

  /* index 0 will be lowest size queue */
  qsizes *s1 = (qsizes*) a;
  qsizes *s2 = (qsizes*) b;
  
  return s1->size - s2->size;

} 

bool lockfree_queue_same_size() {

   unsigned long size;
   unsigned long size_ref = atomic_load( &(queues[*tids[0]]->a_qsize) );
   
   for (int i = 0; i < queue_count; i++) {
      size = atomic_load( &(queues[*tids[i]]->a_qsize) );
      if ( size != size_ref ) {
         return false;
      }
   }
   
   return true;
}

int dq_util_node_receive_count(int node_id, work_to_send *wts) {

  int send_count = 0;

  for (int i = 0; i < comm_size; i++) {
    for (int j = 0; j < wts[i].send_count; j++) {
      if ( wts[i].dst_node_ids[j] == node_id ) {
        send_count++;
      }
    }
  }

  return send_count;

}

int dq_send_data_to_node(int dst, unsigned long count) {

 /*
  * Sends 'count' items to 'dst' node
  * Data is taken from biggest queues
  * TODO: Make function work with generic data types according to size_t. 
  *       Use void pointers and correct MPI_Datatype according to size_t.
  *       Or use protobuf
  */

  GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "dq_send_data_to_node: sending %lu data to %d\n", count, dst);

  long count_tmp = count;
  unsigned long *sizes = lockfree_queue_size_total_allarr();
  unsigned long total_size = dq_local_size();
  unsigned long q_remainder = total_size - count;
  unsigned long balanced_size = q_remainder / queue_count;

  GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "\tTotal size=%lu\n\tQ remainder=%lu\n\tBalanced size=%lu\n", total_size, q_remainder, balanced_size);

  unsigned long displ = 0;

  int* int_arr = malloc(count * sizeof(int));

  for (int i = 0; i < queue_count; i++) {
    long items_to_take = sizes[i] - balanced_size;

    if ( items_to_take > 0 ) {
      if ( count_tmp - items_to_take < 0 ) {
        items_to_take = count_tmp;
        count_tmp -= items_to_take;
      }
      else {
        count_tmp -= items_to_take;
      }

      GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Items to take=%ld; Need to get %ld more items\n", items_to_take, count_tmp);
      void** values = malloc(items_to_take * sizeof(void*));
      for (int j = 0; j < items_to_take; j++) {
        values[j] = malloc(sizeof(void*));
      }
      dq_remove_Nitems_by_tid_no_lock((long) i, items_to_take, values);

      for (int j = 0; j < items_to_take; j++) {
        int *v = values[j];
        memcpy(&int_arr[j], v, queues[0]->item_size);
      }

      displ += items_to_take;
      for (int j = 0; j < items_to_take; j++) {
        free(values[j]);
      }
      free(values);
    }
  }

  GLOBAL_BALANCE_LOG_DEBUG_TD(comm_rank, "Node[%d] is sending %lu data to N%d\n", comm_rank, count, dst);
  MPI_Send(int_arr, count, MPI_INT, dst, 804, MPI_COMM_WORLD);
  
  free(int_arr);
  free(sizes);
  atomic_fetch_add(&moved_items_global_log, count);

  return 0;

}

long dq_util_maxdiff_q(void* qid) {

 /*
  * TODO: Check variance of queue sizes and set threshold according to it
  */

  long *q = qid;
  unsigned long diff_max = 0;
  unsigned long diff_max_id = 0;
  unsigned long src_q_size = dq_queue_size_by_tid(q);

  for (long i = 0; i < queue_count; i++) {
    unsigned long diff = abs(dq_queue_size_by_tid(&i) - src_q_size);
    if (diff > diff_max) {
      diff_max_id = i;
      diff_max = diff;
    }
  }

  if (*q == diff_max_id) {
    return -1;
  }
  else {
    return diff_max_id;
  }

}

long dq_util_find_largest_q() {

  long index = 0;
  unsigned long max = 0;
   
  for (long i = 0; i < queue_count; i++) {
    unsigned long size = dq_queue_size_by_tid(&i);
    if ( size > max ) {
      max = size;
      index = i;
    }
  }

  return index;

}

long dq_util_find_smallest_q() {

  long index = 0;
  unsigned long min = ULONG_MAX;
   
  for (long i = 0; i < queue_count; i++) {
    unsigned long size = dq_queue_size_by_tid(&i);
    if ( size < min ) {
      min = size;
      index = i;
    }
  }

  return index;

}

int dq_util_find_max_min_element_index(unsigned long *array, unsigned long len, int* index_max_min) {

 /*
  * index_max_min[0] is index of element with max number
  * index_max_min[1] is index of element with min number
  */
   
  index_max_min[0] = 0;
  index_max_min[1] = 0;

  unsigned long max = 0;
  unsigned long min = ULONG_MAX;

  for (int i = 0; i < len; i++) {
    if ( array[i] > max ) {
      max = array[i];
      index_max_min[0] = i;
    }
    if ( array[i] < min ) {
      min = array[i];
      index_max_min[1] = i;
    }
  }

  return 0;
   
}

long dq_util_find_max_element_index(unsigned long *array, unsigned long len) {

   long index = 0;
   unsigned long max = 0;
   
   for (int i = 0; i < len; i++) {
      if ( array[i] > max ) {
         max = array[i];
         index = i;
      }
   }

   return index;
}

void* dq_per_time_statistics_reseter(void *arg) {

  while(1) {
    usleep(500000);
    if (flag_graceful_stop) {
      break;
    }

    atomic_store(&rm_count_last, atomic_load(&rm_count));
    atomic_store(&ins_count_last, atomic_load(&ins_count));

    LOG_INFO_TD("Per seconds statistics time=%d\n", (int) time(NULL));
    for (long i = 0; i < queue_count; i++) {
      fprintf(stdout, "\tQueue[%ld] size is %ld\n", i, dq_queue_size_by_tid(&i));
      LOG_INFO_TD("\tQueue[%ld] size is %ld\n", i, dq_queue_size_by_tid(&i));
    }
    fprintf(stdout, "\n");
    LOG_INFO_TD("\tTotal removes per second=%ld\n", atomic_load(&rm_count_last));
    LOG_INFO_TD("\tTotal inserts per second=%ld\n", atomic_load(&ins_count_last));

    atomic_store(&rm_count, 0);
    atomic_store(&ins_count, 0);
  }
  return NULL;
}

void* dq_local_struct_cleanup() {

  while(1) {

    if (flag_graceful_stop) {
      break;
    }
 
    sleep(10);
    //unsigned long items_cleaned = 0;

    for (int i = 0; i < queue_count; i++) {
      struct dq_queue *q = queues[i];
      struct dq_item *tmp;
      pthread_mutex_lock(&add_mutexes[i]);
    
      while ( q->head != q->divider ) {
        tmp = q->head;
        q->head = q->head->next;
        free(tmp->val); //allocated in main - can not free here
        free(tmp);
        //items_cleaned++;
      }
      pthread_mutex_unlock(&add_mutexes[i]);
    }
    //LOG_DEBUG_TD((long) 0, "cleaned %ld items\n", items_cleaned);

  }

  return NULL;

}

double dq_util_sum_time(time_t sec, long nsec) {

  double final_time = (double) 0.00;
  final_time += (double) sec;
  final_time += (double) nsec / (double) 1000000000;
  return final_time;

}

unsigned long dq_util_sum_arr(unsigned long *arr, unsigned long len) {

  unsigned long result = 0;
  for (int i = 0; i < len; i++) {
    result += arr[i];
  }

  return result;

}
