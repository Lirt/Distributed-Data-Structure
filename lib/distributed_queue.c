
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

struct ds_lockfree_queue **queues;
int queue_count = 0;
int thread_count = 0;
int thread_to_queue_ratio = 0;

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
pthread_mutex_t qsize_watcher_mutex;   //NOT USED SO FAR

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

/*
 * Statistical variables
 */

atomic_ulong moved_items_log;
atomic_ulong load_balancer_call_count_watcher;
atomic_ulong load_balancer_call_count_remove;
atomic_ulong load_balancer_call_count_global;
atomic_ulong global_size_call_count;
atomic_ulong global_size_call_count_in_wait;
time_t total_rt_lb_time_sec;
long total_rt_lb_time_nsec;
time_t total_thr_lb_time_sec;
long total_thr_lb_time_nsec;
time_t total_rt_global_size_time_sec;
long total_rt_global_size_time_nsec;
time_t total_thr_global_size_time_sec;
long total_thr_global_size_time_nsec;

/*
 * GLOBAL BALANCING
 */

load_balancer_strategy lbs;
qsize_watcher_strategy qw_strategy;
pthread_t listener_global_size_t;
pthread_t listener_global_balance_t;
pthread_mutex_t load_balance_global_mutex; //To lock thread until global operation as global_size or global_balance is done
pthread_rwlock_t load_balance_global_rwlock; //To lock thread until global operation as global_size or global_balance is done
pthread_cond_t load_balance_global_cond;
pthread_rwlock_t global_size_rwlock;
pthread_rwlock_t global_size_executing_rwlock;
//pthread_rwlockattr_t attr;

unsigned int global_size_receive_timeout = 10000; //timeout for receive
unsigned long last_global_size;

bool global_balancing_enable; //True enables global balancing, False disables global balancing
struct timespec *last_global_rebalance_time;   //For elimination of flooding network with global rebalance requests
struct timespec *last_local_rebalance_time;

unsigned long local_balance_wait_timer;
unsigned long local_balance_last_balance_threshold;

int debug_wait = 0;

/****
 * 
 * Functions
 * 
 **/

int getInsertionTid() {
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

int getRemovalTid() {

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

void lockfree_queue_destroy() {
   
  //TODO test
  //TODO handle errors and return bool true/false
  //TODO decide which thread will destroy it and set tid 
  //TODO After changes need to be recoded
  //DONT USE 

  //pthread_kill(tid, 0);
  //No signal is sent, but error checking is still performed so you can use that to check existence of tid.

  //http://pubs.opengroup.org/onlinepubs/000095399/functions/pthread_cleanup_pop.html

  int t_tmp = getInsertionTid();
  //int t_tmp = getRemovalTid();
  int t = t_tmp;

  if (pthread_rwlock_destroy(&uthash_ins_rwlock) != 0) {
    fprintf(stderr,"lock destroy failed\n");
    exit(-1);
  }
  if (pthread_rwlock_destroy(&uthash_rm_rwlock) != 0) {
    fprintf(stderr,"lock destroy failed\n");
    exit(-1);
  }

   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_lock(&add_mutexes[i]);
      pthread_mutex_lock(&rm_mutexes[i]);
   }*/
   LOCK_LOCAL_QUEUES();
   
   //if (load_balancing_t_running_flag)
   //   pthread_cancel(load_balancing_t);
   if (qsize_watcher_t_running_flag)
      pthread_cancel(qsize_watcher_t);

   for (int i = 0; i < queue_count; i++) {
      if (i != t)
         pthread_cancel(callback_threads[i]);
   }
   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_unlock(&add_mutexes[i]);
      pthread_mutex_unlock(&rm_mutexes[i]);      
   }*/
   UNLOCK_LOCAL_QUEUES();

   pthread_attr_destroy(&attr);
   pthread_mutexattr_destroy(&mutex_attr);

   for (int i = 0; i < queue_count; i++) {
      lockfree_queue_free(tids[i]);
   }

   for (int i = 0; i < queue_count; i++) {
      pthread_mutex_destroy(&add_mutexes[i]);
      pthread_mutex_destroy(&rm_mutexes[i]);
   }

   //pthread_condattr_destroy (attr);
   pthread_cond_destroy (&load_balance_cond);
   pthread_mutex_destroy (&load_balance_mutex);
   pthread_mutex_destroy (&qsize_watcher_mutex);

      
   free(queues);
   queue_count = 0;
   
   free(add_mutexes);
   free(rm_mutexes);
   free(callback_threads);
   
   free(tids);

   fclose(log_file_lb);
   fclose(log_file_qw);
   fclose(log_file_debug);
   fclose(log_file_global_comm);
   fclose(file_pid);

   MPI_Finalize();

   //pthread_cancel(callback_threads[t]);
   pthread_exit(&callback_threads[t]);
   
}


bool lockfree_queue_is_empty_local(void *queue_id) {
   
   long *tid = queue_id; 
   
   struct ds_lockfree_queue *q = queues[ *tid ];

   if ( atomic_load( &(q->a_qsize) ) == 0 ) {
      return true;
   }
   
   return false;
   
}


bool lockfree_queue_is_empty_all_local () {

   for (int i = 0 ; i < queue_count; i++) {
      if ( atomic_load( &(queues[i]->a_qsize) ) != 0 ) {
         return false;
      }
   }
   
   return true;
   
}

bool lockfree_queue_is_empty_all_consistent_local() {

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

pthread_t* lockfree_queue_init_callback ( void* (*callback)(void *args), void* arguments, size_t item_size_arg,
  unsigned int queue_count_arg, unsigned int thread_count_arg, 
  bool qw_thread_enable_arg, double local_lb_threshold_percent, double global_lb_threshold_percent, 
  unsigned long local_lb_threshold_static, unsigned long global_lb_threshold_static, unsigned int threshold_type_arg, 
  unsigned int local_balance_type_arg, bool hook_arg ) {

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

  char filename_log_debug[50] = "/tmp/distributed_queue/log_debug";
  strcat(filename_log_debug, pid_str);
  log_file_debug = fopen(filename_log_debug, "wb");
  if (log_file_debug == NULL) {
    fprintf(stderr, "ERROR: Failed to open debug file '%s'\n", filename_log_debug);
  }
  LOG_DEBUG_TD( (long) -1, "Debug log file opened\n");

  char filename_log_lb[50] = "/tmp/distributed_queue/log_debug_lb";
  strcat(filename_log_lb, pid_str);
  log_file_lb = fopen(filename_log_lb, "wb");
  if (log_file_lb == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open debug file '%s'\n", filename_log_lb);
  }
  LOAD_BALANCE_LOG_DEBUG_TD("Load balancer log file opened\n");

  char filename_log_qw[50] = "/tmp/distributed_queue/log_debug_qw";
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

  char filename_log_global_comm[60] = "/tmp/distributed_queue/log_debug_global_comm";
  strcat(filename_log_global_comm, pid_str);
  log_file_global_comm = fopen(filename_log_global_comm, "wb");
  if (log_file_global_comm == NULL) {
    LOG_ERR_T( (long) -1, "Failed to open debug file '%s'\n", filename_log_global_comm);
  }
  GLOBAL_COMM_LOG_DEBUG_TD( -1, "Global communication log file opened\n");


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
  atomic_init(&load_balancer_call_count_watcher, 0);
  atomic_init(&load_balancer_call_count_remove, 0);
  atomic_init(&load_balancer_call_count_global, 0);
  atomic_init(&global_size_call_count, 0);
  atomic_init(&global_size_call_count_in_wait, 0);
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
  * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
  */

  if ( queue_count_arg == 0 ) 
    queue_count = get_nprocs();
  else 
    queue_count = queue_count_arg;

  LOG_INFO_TD("CPU_COUNT: %d\nQUEUE_COUNT: %d\n", get_nprocs(), queue_count);

  queues = (struct ds_lockfree_queue**) malloc ( queue_count * sizeof(struct ds_lockfree_queue) );
  for (int i = 0; i < queue_count; i++) {
    queues[i] = (struct ds_lockfree_queue*) malloc ( sizeof(struct ds_lockfree_queue) );
  }

  /*
   * Queue initialization
   */
  for (int i = 0; i < queue_count; i++) {
    queues[i]->head = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
    queues[i]->head->next = NULL;
    queues[i]->tail = queues[i]->head;
    queues[i]->divider = queues[i]->head;
    queues[i]->item_size = item_size_arg;
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
  pthread_mutex_init(&qsize_watcher_mutex, &mutex_attr);
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

  last_global_rebalance_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, last_global_rebalance_time);
  last_local_rebalance_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, last_local_rebalance_time);
  local_balance_wait_timer = 6000; // in microseconds
  local_balance_last_balance_threshold = 20 * 1000; // in nanoseconds, if less than "" do rebalance


  global_balancing_enable = true; //enables or disables global balancing
  if (global_balancing_enable) {
    rc = pthread_create(&listener_global_balance_t, &attr, comm_listener_global_balance, NULL);
    if (rc) {
      fprintf(stderr, "ERROR: (init) return code from pthread_create() on global balance listener is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
  }
   
  /*
   * Global size listener
   */

  rc = pthread_create(&listener_global_size_t, &attr, comm_listener_global_size, NULL);
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

  lbs = &load_balancer_all_balance; //Default LB strategy(necessary for f.remove item)
  struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  if ( local_balance_type_arg == 1 ) {
    //LB ALL EQUAL
    qsize_wait_time = 7000; //in microseconds
    qw_strategy = &qsize_watcher_local_threshold_strategy;
    lbs = &load_balancer_all_balance;
    LOG_INFO_TD("Load balancer strategy is equal balance\n");
  }
  else if ( local_balance_type_arg == 2 ) {
    //LB PAIR
    qsize_wait_time = 4000; //in microseconds
    qw_strategy = &qsize_watcher_min_max_strategy;
    lbs = &load_balancer_pair_balance;
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

    rc = pthread_create(&qsize_watcher_t, &attr, qw_strategy, lb_struct);
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
    rc = pthread_create(&per_time_statistics_reseter_t, &attr, per_time_statistics_reseter, NULL);
    if (rc) {
      //fprintf(stderr, "ERROR: (init per_time_statistics_reseter) return code from pthread_create() is %d\n", rc);
      LOG_ERR_T( (long) -1, "Pthread create failed\n");
      exit(-1);
    }
  #endif

  /*rc = pthread_create(&local_struct_cleanup_t, &attr, local_struct_cleanup, NULL);
  if (rc) {
    //fprintf(stderr, "ERROR: (init cleanup thread) return code from pthread_create() is %d\n", rc);
    LOG_ERR_T( (long) -1, "Pthread create failed\n");
    exit(-1);
  }*/


  /*
   * Settings for queue argument structure and thread mapping to queues
   */
  struct q_args **q_args_t;
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

void lockfree_queue_free(void *queue_id) {
   
  /*
   * Frees queue indexed by q_id
   */
  //TODO Check if queues are init. as well in other functions.
  //Test

  long *q_id = queue_id;

  struct ds_lockfree_queue *q = queues[ *q_id ]; //modulo ok?

  struct lockfree_queue_item *item;
  struct lockfree_queue_item *item_tmp;

  item = q->head;
  while (item != NULL) {
    free(item->val); //Can cause segmentation fault when malloc was used in main program ??
    item_tmp = item;
    item = item->next;
    free(item_tmp);
  }

  free(q);
   
}

int lockfree_queue_insert_item (void* val) {

 /*
  * Free is done in function
  * User must free only his value *val
  */

  long tid = getInsertionTid();
  struct ds_lockfree_queue *q = queues[ tid ];

  struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
  if (item == NULL) {
    fprintf(stdout, "ERROR: Malloc failed\n");
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return -1;
  }
  struct lockfree_queue_item *tmp;

  item->val = malloc(sizeof(void*));
  if (item->val == NULL) {
    fprintf(stdout, "ERROR: Malloc failed\n");
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return -1;
  }
  memcpy(item->val, val, q->item_size);

  item->next = NULL;
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
    //free(tmp->val); //allocated in main - can not free here
    free(tmp);
  }

  pthread_mutex_unlock(&add_mutexes[tid]);
  return 0;

}

void lockfree_queue_insert_item_no_lock (void* val) {

   long tid = getInsertionTid();
   struct ds_lockfree_queue *q = queues[ tid ];

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }
   struct lockfree_queue_item *tmp;
   //item->val = val;
   item->val = malloc(sizeof(void*));
   memcpy(item->val, val, q->item_size);
   item->next = NULL;
   
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
      //free(tmp->val); //allocated in main - can not free here
      free(tmp);
   }

}

void lockfree_queue_insert_item_by_tid (void *t, void* val) {

   long *tid = t;
   struct ds_lockfree_queue *q = queues[ *tid ];

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));

   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }

   struct lockfree_queue_item *tmp;
   //item->val = val;
   item->val = malloc(sizeof(void*));
   memcpy(item->val, val, q->item_size);
   item->next = NULL;
   pthread_mutex_lock(&add_mutexes[*tid]);
   
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
      //free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[*tid]);
}

void lockfree_queue_insert_item_by_tid_no_lock (void *t, void* val) {

   long *tid = t;
   struct ds_lockfree_queue *q = queues[ *tid ];

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }
   struct lockfree_queue_item *tmp;
   //item->val = val;
   item->val = malloc(sizeof(void*));
   memcpy(item->val, val, q->item_size);
   item->next = NULL;
   
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
      //free(tmp->val);
      free(tmp);
   }
   
}

void lockfree_queue_insert_N_items (void** values, int item_count) {

   long tid = getInsertionTid();

   if ( item_count == 0 ) {
      return;
   }
   if ( values == NULL ) {
      return;
   }

   struct ds_lockfree_queue *q = queues[ tid ];

   struct lockfree_queue_item *item;
   struct lockfree_queue_item *item_tmp;
   struct lockfree_queue_item *item_first;

   item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
      return;
   }

   //item->val = values[0];
   item->val = malloc(sizeof(void*));
   memcpy(item->val, values[0], q->item_size);
   item_first = item;

   for (int i = 1; i < item_count; i++) {
      item_tmp = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      if (item_tmp == NULL) {
         LOG_ERR_T( (long) tid, "Malloc failed\n");
         return;
      }
      //item_tmp->val = values[i];
      item_tmp->val = malloc(sizeof(void*));
      memcpy(item_tmp->val, values[i], q->item_size);
      item->next = item_tmp;
      item = item->next;
   }
   item->next = NULL;
   
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
   struct lockfree_queue_item *tmp;
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      //free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[tid]);
   
}

void lockfree_queue_insert_N_items_no_lock_by_tid (void** values, int item_count, void *qid) {

  if ( item_count == 0 ) {
    return;
  }
  if ( values == NULL ) {
    return;
  }

  fprintf(stdout, "InsertNItems(%d): \n", item_count);
  for (int i = 0; i < item_count; i++) {
    int *v = values[i];
    fprintf(stdout, "%p--%d\n", v, *v);
    fflush(stdout);
  }

  long *tid = qid;
  struct ds_lockfree_queue *q = queues[ *tid ];

  struct lockfree_queue_item *item;
  struct lockfree_queue_item *item_tmp;
  struct lockfree_queue_item *item_first;

  item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));

  if (item == NULL) {
    fprintf(stdout, "ERROR: Malloc failed in insert_N_items_no_lock_by_tid\n");
    fflush(stdout);
    LOG_ERR_T( (long) tid, "Malloc failed\n");
    return;
  }

  //item->val = values[0];
  item->val = malloc(sizeof(void*));
  memcpy(item->val, values[0], q->item_size);
  item_first = item;

  for (int i = 1; i < item_count; i++) {
    item_tmp = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
    if (item_tmp == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
      return;
    }
    //item_tmp->val = values[i];
    item_tmp->val = malloc(sizeof(void*));
    memcpy(item_tmp->val, values[i], q->item_size);
    item->next = item_tmp;
    item = item->next;
  }
  item->next = NULL;

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
  struct lockfree_queue_item *tmp;
  while ( q->head != q->divider ) {
    tmp = q->head;
    q->head = q->head->next;
    //free(tmp->val);
    free(tmp);
  }
   
}

int load_balancer_pair_balance(void* lb_struct_arg) {

  struct load_balancer_struct *lb_struct = lb_struct_arg;

  //struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  //struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *current_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  //clock_gettime(CLOCK_REALTIME, tp_rt_start);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);

  if (queue_count == 1) {
    pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
    return 0;
  }

  clock_gettime(CLOCK_REALTIME, current_time);
  if ( (time_diff_dds(last_local_rebalance_time, current_time)->tv_sec == 0) && 
        time_diff_dds(last_local_rebalance_time, current_time)->tv_nsec < local_balance_last_balance_threshold ) {
    ;
  }
  else {
    LOAD_BALANCE_LOG_DEBUG_TD("Last local balance was %ld sec and %ld nsec ago\n", 
      time_diff_dds(last_local_rebalance_time, current_time)->tv_sec, time_diff_dds(last_local_rebalance_time, current_time)->tv_nsec);
    usleep(local_balance_wait_timer);    
  }
  clock_gettime(CLOCK_REALTIME, last_local_rebalance_time);

  //qsizes *sorted_queue_sizes = lockfree_queue_size_total_allarr_sorted();
  long maxdiff_q_id = maxdiff_q(&(lb_struct->src_q));
  if ( maxdiff_q_id == -1 ) {
    //No local queue to trade with
    return -1;
  }

  pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
  pthread_mutex_lock(&local_queue_struct_mutex);

  pthread_mutex_lock(&rm_mutexes[lb_struct->src_q]);  
  pthread_mutex_lock(&rm_mutexes[maxdiff_q_id]);
  pthread_mutex_lock(&add_mutexes[maxdiff_q_id]);
  pthread_mutex_lock(&add_mutexes[lb_struct->src_q]);

  unsigned long qsize_src = lockfree_queue_size_by_tid(&maxdiff_q_id);
  unsigned long qsize_dst = lockfree_queue_size_by_tid(&(lb_struct->src_q));

  unsigned long estimated_size = (qsize_src + qsize_dst) / 2;
  unsigned long items_to_send;

  if ( qsize_src > qsize_dst ) {
    items_to_send = qsize_src - estimated_size;
    lockfree_queue_move_items(maxdiff_q_id, lb_struct->src_q, items_to_send);
  }
  else if ( qsize_src < qsize_dst ) {
    items_to_send = qsize_dst - estimated_size;
    lockfree_queue_move_items(lb_struct->src_q, maxdiff_q_id, items_to_send);
  }
  LOAD_BALANCE_LOG_DEBUG_TD("Balancing Queue[%ld] with %ld items with Queue[%ld] with %ld items -- sending %ld items\n", 
    lb_struct->src_q, qsize_src, maxdiff_q_id, qsize_dst, items_to_send);

  if ( lb_struct->qsize_history != NULL ) {
    unsigned long* qsize_history = lb_struct->qsize_history;
    for (int j = 0; j < queue_count; j++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], lockfree_queue_size_by_tid(tids[j]));
      qsize_history[j] = lockfree_queue_size_by_tid(tids[j]);;
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
  //  time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec );
  LOAD_BALANCE_LOG_DEBUG_TD("[LB-Pair]: Final thread local LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec, time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec );
  //total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  //total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;
  /*printf("Local balance: \n\trt_start=%ld.%ld --- \n\trt_end=%ld.%ld --- \n\ttime_diff=%ld.%ld ---\n", 
      tp_rt_start->tv_sec, tp_rt_start->tv_nsec, tp_rt_end->tv_sec, tp_rt_end->tv_nsec, 
      time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec);*/
  return 0;

}

int load_balancer_all_balance(void* arg) {

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

  if ( (time_diff_dds(current_time, last_local_rebalance_time)->tv_sec == 0) && 
        time_diff_dds(current_time, last_local_rebalance_time)->tv_nsec < local_balance_last_balance_threshold ) {
    ;
  }
  else {
    LOAD_BALANCE_LOG_DEBUG_TD("Last local balance was %ld sec and %ld nsec ago\n", 
      time_diff_dds(last_local_rebalance_time, current_time)->tv_sec, time_diff_dds(last_local_rebalance_time, current_time)->tv_nsec);
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
      //LOCK_LOCAL_QUEUES_EXCEPT_RM( lb_struct->src_q);
      pthread_mutex_unlock(&rm_mutexes[lb_struct->src_q]);
      LOCK_LOCAL_QUEUES();
    }
  }
  else {
    return -1;
  }

  LOAD_BALANCE_LOG_DEBUG_TD("LB TIME START: Thread Time: %lu sec and %lu nsec\n", 
    tp_thr_start->tv_sec, tp_thr_start->tv_nsec);

  //Do not use function lockfree_queue_size_total_consistent(), because mutexes are already locked. 
  unsigned long items_to_send;
  unsigned long total = lockfree_queue_size_total();
  unsigned long estimated_size = total / queue_count;

  int *indexes = (int*) malloc (2 * sizeof(int));
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  if ( indexes == NULL ) {
    LOG_ERR_T( (long) -2, "Malloc failed\n");
  }
  if ( q_sizes == NULL ) {
    LOG_ERR_T( (long) -2, "Malloc failed\n");
  }

  //If items can not be balanced, just take one item to calling queue
  if ( estimated_size == 0 ) {
    LOAD_BALANCE_LOG_DEBUG_TD("Estimated size = 0\n");
    for (int j = 0; j < queue_count; j++) {
      q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
    }
    indexes = find_max_min_element_index(q_sizes, queue_count);
    LOAD_BALANCE_LOG_DEBUG_TD("Max: Q%d with %lu --- Min: Q%d with %lu  ---  Sending: 1 item\n", indexes[0], q_sizes[indexes[0]], 
      indexes[1], q_sizes[indexes[1]]);
    if ( indexes[0] != lb_struct->src_q ) {
      lockfree_queue_move_items(indexes[0], (int) lb_struct->src_q, (unsigned long) 1);
    }
  }
  else {
    for (int i = 0 ; i < queue_count - 1; i++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Load balance round %d\n", i);
      for (int j = 0; j < queue_count; j++) {
        q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
        LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      }
      
      indexes = find_max_min_element_index(q_sizes, queue_count);

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
      lockfree_queue_move_items(indexes[0], indexes[1], items_to_send);

    }
  }

  if ( lb_struct->qsize_history != NULL ) {
    unsigned long* qsize_history = lb_struct->qsize_history;
    for (int j = 0; j < queue_count; j++) {
      q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      qsize_history[j] = q_sizes[j];
    }
  }
   
  free(indexes);

  UNLOCK_LOCAL_QUEUES();
  pthread_mutex_unlock(&load_balance_mutex);

  //clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  //LOAD_BALANCE_LOG_DEBUG_TD("LB TIME END: \n\tRealtime: %lu sec and %lu nsec\n\tThread Time: %lu sec and %lu nsec\n", 
  //  tp_rt_end->tv_sec, tp_rt_end->tv_nsec, tp_thr_end->tv_sec, tp_thr_end->tv_nsec);
  //LOAD_BALANCE_LOG_DEBUG_TD("Final realtime local LB time = '%lu sec and %lu nsec'\n", 
  //  time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec );
  LOAD_BALANCE_LOG_DEBUG_TD("Final thread local LB time = '%lu sec and %lu nsec'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec, time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec );

  //total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  //total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;

  return 0;

}

void lockfree_queue_move_items(int q_id_src, int q_id_dest, unsigned long count) {

 /*
  * Does not lock queues
  */

  if ( count == 0 ) {
    return;
  }

  struct ds_lockfree_queue *q_src = queues[ q_id_src ];
  struct ds_lockfree_queue *q_dst = queues[ q_id_dest ];

  unsigned long q_size_src = atomic_load( &(q_src->a_qsize) );

  struct lockfree_queue_item *tmp_div;
  struct lockfree_queue_item *tmp_div_next;
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

void *qsize_watcher_min_max_strategy() {

  //TODO Maybe change to real min max strategy without threshold

  bool balance_flag;
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  unsigned long total_qsize;
  bool *q_lb_flags = (bool*) malloc ( queue_count * sizeof(bool) );
  for (int i = 0; i < queue_count; i++) {
    q_lb_flags[i] = 0;
  }
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
      q_sizes[i] = lockfree_queue_size_by_tid(tids[i]);
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
  return NULL;

}

void* qsize_watcher_local_threshold_strategy() {
   
  bool balance_flag;
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  unsigned long total_qsize;
  unsigned long *qsize_history = (unsigned long*) malloc ( queue_count * sizeof (unsigned long));
  for (int i = 0; i < queue_count; i++) {
    qsize_history[i] = 0;
  }
  struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  
  /*struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);*/
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
      q_sizes[i] = lockfree_queue_size_by_tid(tids[i]);
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
  return NULL;

}

int lockfree_queue_remove_item (void* buffer) {

 /*
  * timeout is in microseconds
  */

  void* val = NULL;
  long tid = getRemovalTid();
   
  struct ds_lockfree_queue *q = queues[ tid ];
   
  pthread_mutex_lock(&rm_mutexes[tid]);
   
  //TODO timeout spin will try if q->divider is != q-> tail in while, but needs to be timed to nano or microseconds
  //if (timeout > 0)
  //   usleep(timeout);

  if ( q->divider != q->tail ) {
    val = q->divider->next->val;
    q->divider = q->divider->next;
    atomic_fetch_sub( &(q->a_qsize), 1);
  }
  else {
    pthread_mutex_unlock(&rm_mutexes[tid]);
    while(1) {
      pthread_mutex_lock(&rm_mutexes[tid]);
      if ( q->divider != q->tail ) {   //atomic reads?
        val = q->divider->next->val;
        q->divider = q->divider->next;
        atomic_fetch_sub( &(q->a_qsize), 1);
        break;
      }

      if ( lockfree_queue_size_total() == 0 ) {
        if ( global_balancing_enable == false ) {
          break;
        }
        else {
          pthread_mutex_unlock(&rm_mutexes[tid]);
          if ( global_size(false) != 0 ) {
            global_balance(tid);
            continue;
          }
          else {
            pthread_mutex_lock(&rm_mutexes[tid]);
            break;
          }
        }
      }
      else {
        struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc(sizeof(struct load_balancer_struct));
        lb_struct->qsize_history = NULL;
        lb_struct->src_q = tid;
        if ( lbs(lb_struct) != -1 ) {
          atomic_fetch_add(&load_balancer_call_count_remove, 1);
        }
        else {
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

int lockfree_queue_remove_item_by_tid (void* t, void* buffer) {

  void* val = NULL;
  long* tid = t;

  struct ds_lockfree_queue *q = queues[ *tid ]; 

  pthread_mutex_lock(&rm_mutexes[*tid]);

  //TODO timeout spin
  //if (timeout > 0)
  //   usleep(timeout);

  if ( q->divider != q->tail ) {
    val = q->divider->next->val;
    q->divider = q->divider->next;
    atomic_fetch_sub( &(q->a_qsize), 1);
  }
  else {
    pthread_mutex_unlock(&rm_mutexes[*tid]);
    while(1) {
      pthread_mutex_lock(&rm_mutexes[*tid]);
      if ( q->divider != q->tail ) {   //atomic reads?
        val = q->divider->next->val;
        q->divider = q->divider->next;
        atomic_fetch_sub( &(q->a_qsize), 1);
        break;
      }

      if ( lockfree_queue_size_total() == 0 ) {
        if ( global_balancing_enable == false ) {
          break;
        }
        else {
          pthread_mutex_unlock(&rm_mutexes[*tid]);
          if ( global_size(false) != 0 ) {
            global_balance(*tid);
            continue;
          }
          else {
            pthread_mutex_lock(&rm_mutexes[*tid]);
            break;
          }
        }
      }
      else {
        struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc(sizeof(struct load_balancer_struct));
        lb_struct->qsize_history = NULL;
        lb_struct->src_q = *tid;
        if ( lbs(lb_struct) != -1 ) {
          atomic_fetch_add(&load_balancer_call_count_remove, 1);
        }
        else {
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

int lockfree_queue_remove_item_by_tid_no_lock (void* t, void* buffer) {

   void* val = NULL;
   long* tid = t;

   struct ds_lockfree_queue *q = queues[ *tid ];
   
   //TODO timeout spin
   //if (timeout > 0)
   //   usleep(timeout);
   
   if ( q->divider != q->tail ) {
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
   }
   else {
      //TODO
      ;
   }
   
  //buffer = malloc(sizeof(void*));
  memcpy(buffer, val, q->item_size);

  return 0;

}


int lockfree_queue_remove_Nitems (unsigned long N, void** buffer) {
   
 /*
  * N is amount of items to be taken from Q
  */
  //TODO TEST
   
  long tid = getRemovalTid();
  void **val_arr = malloc(N * sizeof(void*));
  if (val_arr == NULL) {
    printf("removeN malloc failed\n");
    LOG_DEBUG_TD( tid, "Malloc failed\n");
    return -1;
  }
   
  struct ds_lockfree_queue *q = queues[ tid ];
   
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
      //val_arr[i] = q->divider->next->val;
      //memcpy(val_arr[i], q->divider->next->val, sizeof(int*));
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

int lockfree_queue_remove_Nitems_no_lock_by_tid(long qid, long item_cnt, void** buffer) {
   
  struct ds_lockfree_queue *q = queues[ qid ];

  unsigned long qsize = atomic_load( &(q->a_qsize) );
  if ( qsize < item_cnt ) {
    //printf("Not enough items in queue %ld. There are %ld but was requested %ld.\n", qid, qsize, item_cnt);
    LOG_DEBUG_TD(qid, "Not enough items in queue %ld. There are %ld but was requested %ld.\n", qid, qsize, item_cnt);
    return -1;
  }

  //void **val_arr = malloc(item_cnt * sizeof(void*));
  //buffer = malloc(item_cnt * sizeof(void*));
  /*if (val_arr == NULL) {
    fprintf(stderr, "removeN malloc failed\n");
    LOG_DEBUG_TD( qid, "Malloc failed\n");
    return -1;
  }*/
   
  unsigned long i;
  for (i = 0; i < item_cnt; i++) {
    if ( q->divider != q->tail ) {
      //val_arr[i] = malloc(sizeof(int*));
      //buffer[i] = malloc(sizeof(void*));
      //memcpy(val_arr[i], q->divider->next->val, q->item_size);
      //memcpy(val_arr + i, q->divider->next->val, q->item_size);
      //memcpy(val_arr + i, q->divider->next->val, sizeof(int*));
      //memcpy(val_arr[i], q->divider->next->val, sizeof(int*));
      memcpy(buffer[i], q->divider->next->val, q->item_size);
      //memcpy(val_arr + i, q->divider->next->val, sizeof(int*));
      q->divider = q->divider->next;
    }
    else {
      break;
    }
  }

  /*fprintf(stdout, "Removed items(i): %lu\n", i);
  fflush(stdout);
  for (i = 0; i < item_cnt; i++) {
    int *v = val_arr[i];
    fprintf(stdout, "%d\n", *v);
  }
  fflush(stdout);*/

  if ( i != item_cnt ) {
    printf("Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", qid, i);
    LOG_DEBUG_TD(qid, "Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", qid, i);
    return -1;
  }
  
  atomic_fetch_sub( &(q->a_qsize), item_cnt);
  
  return 0;

}

unsigned long global_size(bool consistency) {
   
  /*****
  * send message 'init_global_size'(900) to master
  * all nodes must listen to master 'stop_work' message(901)... so they listens (at least) to master
  * master listens to messages from compute nodes in separate thread
  * on message global_size master stops all compute nodes -> send message to go to barrier
  * after this message, all threads must go to method MPI_Barrier(comm) or global comm_size thread must lock all queues ?
  * after stop, thread sends their queue structure comm_size to master(902)
  * master counts the sum and returns it to user(903)
  * master goes to barrier and unlock all nodes
  */

  //TODO co ak viac nodov posle ziadost o global size. pri architekture s mastrom/P2P
  //last_global_size_time
  //alebo global_size_not_consistent
  //TODO Test viac nodov

  if (comm_size <= 1) {
    return lockfree_queue_size_total();
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
  /*GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "GET GLOBAL SIZE START: \n\tRealtime- %lu.%lu seconds\n\tThread Time- %lu.%lu seconds\n", 
    tp_rt_start->tv_sec, tp_rt_start->tv_nsec, tp_thr_start->tv_sec, tp_thr_start->tv_nsec);*/

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
  //printf("900 send from %d\n", comm_rank);

  unsigned long global_size_val = 0;

  MPI_Recv(&buf, 1, MPI_SHORT, master_id, 905, MPI_COMM_WORLD, &status);
  /*if ( pthread_rwlock_trywrlock(&global_size_executing_rwlock) != 0 ) {
    //lock in listener
    MPI_Recv(&global_size_val, 1, MPI_UNSIGNED_LONG, master_id, 903, MPI_COMM_WORLD, &status);
    last_global_size = global_size_val;
    printf("Rank %d called for GB and received 903\n", comm_rank);

    pthread_rwlock_unlock(&global_size_rwlock);
  }*/
  //else {
    //lock here
    MPI_Recv(&global_size_val, 1, MPI_UNSIGNED_LONG, master_id, 903, MPI_COMM_WORLD, &status);
    last_global_size = global_size_val;
    //printf("Rank %d called for GB and received 903\n", comm_rank);

    MPI_Barrier(MPI_COMM_WORLD);
    pthread_rwlock_unlock(&global_size_executing_rwlock);
    pthread_rwlock_unlock(&global_size_rwlock);
  //}


  
  //printf("Rank %d called for GB and is beyond barrier\n", comm_rank);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "NODE %d: Global structure size is %ld\n", comm_rank, global_size_val);
  //printf("NODE %d: Global structure size is %ld\n", comm_rank, global_size_val);
  
  clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final realtime global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec );
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final thread global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec, time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec );

  total_rt_global_size_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  total_rt_global_size_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  total_thr_global_size_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  total_thr_global_size_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;
  /*printf("NODE %d: Global size: \n\trt_start=%ld.%ld --- \n\trt_end=%ld.%ld --- \n\ttime_diff=%ld.%ld ---\n", comm_rank,
      tp_rt_start->tv_sec, tp_rt_start->tv_nsec, tp_rt_end->tv_sec, tp_rt_end->tv_nsec, 
      time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec);*/
  return global_size_val;
   
}

void* comm_listener_global_size() {

  //int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag,
  //          MPI_Comm comm, MPI_Status *status)
  //int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source,
  //          int tag, MPI_Comm comm, MPI_Request *request)
  //int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
  //          MPI_Comm comm, MPI_Request *request)
  //int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
  //          MPI_Comm comm)
  //int MPI_Waitall(int count, MPI_Request array_of_requests[], 
  //          MPI_Status array_of_statuses[])
  //MPI_ANY_SOURCE
  //status.MPI_SOURCE
  //status.MPI_TAG

  MPI_Request* requests_901;
  MPI_Request* requests_902;
  MPI_Status *statuses_901;
  MPI_Status *statuses_902;

  while(1) {

    if (comm_rank == master_id) {
      requests_901 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
      requests_902 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
      statuses_901 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
      statuses_902 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
      int consistency_type;
      
      //For testing on request and implementation of timeout for gracefull exit on receive
      MPI_Request request_test;
      MPI_Status status_test;
      int test_flag = 0;
      
      //MPI_Irecv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
      //MPI_Recv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      //MPI_Recv(&buf, 1, MPI_SHORT, comm_rank, 900, MPI_COMM_WORLD, &status);
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
          //TODO CLEANUP MUST NOT DEADLOCK
          printf("Gracefully stopping global size listener thread in node %d\n", comm_rank);
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global size listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request_test);
          return NULL;
        }
      }

      //status.MPI_SOURCE
      //status.MPI_TAG
      int source_node = status_test.MPI_SOURCE;
      //int recv_tag = status_test.MPI_TAG;
      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Got message with consistency_type=%hd from node %d\n", consistency_type, source_node);
      //printf("MASTER: Got message with consistency_type=%hd from node %d\n", consistency_type, source_node);
      int cnt = 0;

      //Send doing global size for you message
      short code_905 = 905;
      MPI_Send(&code_905, 1, MPI_SHORT, source_node, 905, MPI_COMM_WORLD);
      
      int *consistency_type_src_id = (int*) malloc (2 * sizeof(int));
      consistency_type_src_id[0] = consistency_type;
      consistency_type_src_id[1] = source_node;
      for(int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Isend(consistency_type_src_id, 2, MPI_INT, i, 901, MPI_COMM_WORLD, &requests_901[cnt]);
          cnt++;
        }
      }

      MPI_Waitall(comm_size - 1, requests_901, statuses_901);
      //printf("MASTER: Got all 901 from %d\n", source_node);
      
      unsigned long *node_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
      unsigned long global_struct_size_total = 0;
      cnt = 0;

      if ( consistency_type_src_id[0] == 0 ) {
        //Get inconsistent global size
        unsigned long master_struct_size = lockfree_queue_size_total();
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
        //printf("MASTER: Got all 902 from %d\n", source_node);

        for (int i = 0; i < comm_size; i++) {
          GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Node %d has %ld items\n", i, node_sizes[i]);
          global_struct_size_total += node_sizes[i];
        }
        GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Global size is %ld\n", global_struct_size_total);

        //IF master asks himself on global size
        //TODO remove printfs
        //printf("source_node=%d\n", source_node);
        //if ( pthread_rwlock_trywrlock(&global_size_rwlock) != 0 ) {
        //if ( pthread_rwlock_trywrlock(&global_size_executing_rwlock) != 0 ) {
        if ( consistency_type_src_id[1] == comm_rank ) {
          //someone is in global_size function and has lock, just unlock queues, he will barrier
          //printf("MASTER: send no barrier\n");
          MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
        }
        else {
          //nobody is in global size function, I must barrier
          //printf("MASTER: send and barrier\n");
          MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
          MPI_Barrier(MPI_COMM_WORLD);
          //pthread_rwlock_unlock(&global_size_rwlock);
          //pthread_rwlock_unlock(&global_size_executing_rwlock);
        }
      }
      else if ( consistency_type_src_id[0] == 1 ) {
        //Get consistent global size
        
        LOCK_LOAD_BALANCER();
        LOCK_LOCAL_QUEUES();

        unsigned long master_struct_size = lockfree_queue_size_total();
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
        //printf("MASTER: Got all 902 from %d\n", source_node);

        for (int i = 0; i < comm_size; i++) {
          GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Node %d has %ld items\n", i, node_sizes[i]);
          global_struct_size_total += node_sizes[i];
        }
        GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Global size is %ld\n", global_struct_size_total);

        //IF master asks himself on global size
        //printf("source_node=%d\n", source_node);
        //if ( pthread_rwlock_trywrlock(&global_size_rwlock) != 0 ) {
        //if ( pthread_rwlock_trywrlock(&global_size_executing_rwlock) != 0 ) {
        if ( consistency_type_src_id[1] == comm_rank ) {
          //someone is in global_size function and has lock, just unlock queues, he will barrier
          //printf("MASTER: send no barrier\n");
          MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
          UNLOCK_LOCAL_QUEUES();
          UNLOCK_LOAD_BALANCER();
        }
        else {
          //nobody is in global size function, I must barrier
          //printf("MASTER: send and barrier\n");
          //printf("MASTER: Sending 903 to %d\n", source_node);
          MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
          MPI_Barrier(MPI_COMM_WORLD);
          //printf("MASTER: finished req from %d\n",source_node);
          UNLOCK_LOCAL_QUEUES();
          UNLOCK_LOAD_BALANCER();
          //pthread_rwlock_unlock(&global_size_rwlock);
          //pthread_rwlock_unlock(&global_size_executing_rwlock);
        }
      }
      free(requests_901); free(requests_902); free(statuses_901); free(statuses_902); free(node_sizes);
    }
    else { 
      //For testing on request and implementation of timeout for gracefull exit on receive
      MPI_Request request_test;
      MPI_Status status_test;
      int test_flag = 0;
      int *consistency_type_src_id = (int*) malloc (2 * sizeof(int));

      //MPI_Irecv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
      //MPI_Recv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      //MPI_Recv(&buf, 1, MPI_SHORT, master_id, 901, MPI_COMM_WORLD, &status);
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
          LOG_DEBUG_TD( (long) -3, "Gracefully stopping global size listener thread in node %d\n", comm_rank);
          MPI_Cancel(&request_test);
          return NULL;
        }
      }

      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "COMPUTE NODE: Received 901 - consistency %d, src_node %d\n", consistency_type_src_id[0], consistency_type_src_id[1]);
      if ( consistency_type_src_id[0] == 0 ) {

        unsigned long local_size = lockfree_queue_size_total();
        MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

        //if ( pthread_rwlock_trywrlock(&global_size_rwlock) != 0 ) {
        //if ( pthread_rwlock_trywrlock(&global_size_executing_rwlock) != 0 ) {
        if ( consistency_type_src_id[1] == comm_rank ) {
          //someone is in global_size function and has lock, just unlock queues, he will barrier
          //printf("Slave no barrier\n");
        }
        else {
          //nobody is in global size function, I must barrier
          //printf("Slave barrier\n");
          MPI_Barrier(MPI_COMM_WORLD);
          //pthread_rwlock_unlock(&global_size_rwlock);
          //pthread_rwlock_unlock(&global_size_executing_rwlock);
        }
      }
      else if ( consistency_type_src_id[0] == 1 ) {
        LOCK_LOAD_BALANCER();
        LOCK_LOCAL_QUEUES();

        unsigned long local_size = lockfree_queue_size_total();
        MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

        //TODO MUST WAIT FOR GLOBAL BALANCE TO FINISH IF IT IS EXECUTING --> HAS WLOCK
        //if ( pthread_rwlock_trywrlock(&global_size_rwlock) != 0 ) {
        //if ( pthread_rwlock_trywrlock(&global_size_executing_rwlock) != 0 ) {
        if ( consistency_type_src_id[1] == comm_rank ) {
          //someone is in global_size function and has lock, just unlock queues, he will barrier
          //printf("Slave no barrier\n");
          UNLOCK_LOCAL_QUEUES();
          UNLOCK_LOAD_BALANCER();
        }
        else {
          //nobody is in global size function, I must barrier
          //printf("Slave barrier\n");
          MPI_Barrier(MPI_COMM_WORLD);
          UNLOCK_LOAD_BALANCER();
          UNLOCK_LOCAL_QUEUES();
          //pthread_rwlock_unlock(&global_size_rwlock);
          //pthread_rwlock_unlock(&global_size_executing_rwlock);
        }
      }
    }
  }
}

int global_balance(long tid) {

  if (comm_size <= 1) {
    return 0;
  }

  struct timespec *balance_start_time = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *balance_end_time = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, balance_start_time);
  
  if ( pthread_rwlock_trywrlock(&load_balance_global_rwlock) == 0 ) {
    //Got lock
    //printf("Node %d: Got lock on global balance\n", comm_rank);
    if ( time_diff_dds(last_global_rebalance_time, balance_start_time)->tv_sec > 0 ) {
      //Last rebalance was more than one second ago, do rebalance again
      printf("Node %d: Last GB was MORE than 1sec ago\n", comm_rank);
      clock_gettime(CLOCK_REALTIME, last_global_rebalance_time);
      GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Starting global balance in %ld.%ld\n", last_global_rebalance_time->tv_sec, last_global_rebalance_time->tv_nsec);
      printf("Node %d: Starting global balance in %ld.%ld\n", comm_rank, last_global_rebalance_time->tv_sec, last_global_rebalance_time->tv_nsec);
      atomic_fetch_add(&load_balancer_call_count_global, 1);

      short buf;
      MPI_Status status;
      short code_800 = 800;
      MPI_Send(&code_800, 1, MPI_SHORT, master_id, 800, MPI_COMM_WORLD);
      MPI_Recv(&buf, 1, MPI_INT, master_id, 805, MPI_COMM_WORLD, &status); // Balancing finished
      printf("Node %d: Balancing finished, unlock RWLock\n", comm_rank);
      pthread_rwlock_unlock(&load_balance_global_rwlock);

    }
    else {
      //printf("Node %d: GB was LESS than 1sec ago\n", comm_rank);
      pthread_rwlock_unlock(&load_balance_global_rwlock);
      return 1; //Rebalance time not more than 1 second
    }
  }
  else {
    //printf("Node %d: Global balance already locked, waiting to finish\n", comm_rank);
    GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Global balance already locked, waiting to finish\n");
    if ( pthread_rwlock_rdlock(&load_balance_global_rwlock) != 0 ) {
      return -1;  // Error in rdlock
    }
    else {
      printf("Node %d: Global balance finished, got rdlock and returning\n", comm_rank);
      pthread_rwlock_unlock(&load_balance_global_rwlock);
      return 2; //Rebalance already started, waiting for results and returning
    }
  }


  clock_gettime(CLOCK_REALTIME, balance_end_time);
  GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Global balance time was %lu.%lu\n", 
    time_diff_dds(balance_start_time, balance_end_time)->tv_sec, time_diff_dds(balance_start_time, balance_end_time)->tv_nsec);
  printf("Node %d: Global balance time was %lu.%lu\n", comm_rank,
    time_diff_dds(balance_start_time, balance_end_time)->tv_sec, time_diff_dds(balance_start_time, balance_end_time)->tv_nsec);

  return 0; //Rebalance finished successfuly

}

void* comm_listener_global_balance() {

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
    //MASTER
    printf("Master listening on comm_rank %d\n", comm_rank);
    while(1) {
      
      MPI_Request request;
      MPI_Status status;
      int test_flag = 0;
      short buf;

      MPI_Irecv(&buf, 1, MPI_SHORT, MPI_ANY_SOURCE, 800, MPI_COMM_WORLD, &request); //request for global balance
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
      
      MPI_Request *requests_801 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));;
      MPI_Status *statuses_801 = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));;
      int source_node = status.MPI_SOURCE;
      printf("MASTER GB: Got GB request from N[%d]\n", source_node);
      short msg = 0;
      int cnt = 0;
      //MPI_Bcast(&buf, 1, MPI_SHORT, comm_rank, 801, MPI_COMM_WORLD);
      for(int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Isend(&msg, 1, MPI_SHORT, i, 801, MPI_COMM_WORLD, &requests_801[cnt]);
          cnt++;
        }
      }
      MPI_Waitall(comm_size - 1, requests_801, statuses_801);
      printf("MASTER GB: sent all rebalance requests\n");

      MPI_Request *requests =  (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
      MPI_Status *statuses = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
      unsigned long *node_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
      cnt = 0;

      LOCK_LOAD_BALANCER();
      LOCK_LOCAL_QUEUES();
      unsigned long master_struct_size = lockfree_queue_size_total();

      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          MPI_Irecv(&node_sizes[i], 1, MPI_UNSIGNED_LONG, i, 802, MPI_COMM_WORLD, &requests[cnt]);
          cnt++;
        }
        else {
          node_sizes[i] = master_struct_size;
        }
      }
      MPI_Waitall(comm_size - 1, requests, statuses);
      printf("MASTER GB: received all struct sizes\n");

      //COUNT RELOCATION
      unsigned long items_to_send;
      unsigned long estimated_size = sum_arr(node_sizes, comm_size) / comm_size;
      work_to_send *wts = (work_to_send*) malloc(comm_size * sizeof(work_to_send));

      for (int i = 0 ; i < comm_size; i++) {
        wts[i].send_count = 0;
        wts[i].dst_node_ids = NULL;
        wts[i].item_counts = NULL;
        GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Node[%d] size is %ld\n", i, node_sizes[i]);
        printf("MASTER GB: Node[%d] size is %ld\n", i, node_sizes[i]);
      }

      for (int i = 0 ; i < comm_size - 1; i++) {
        LOAD_BALANCE_LOG_DEBUG_TD("Global Load balance round %d\n", i);
        
        int *indexes = find_max_min_element_index(node_sizes, comm_size);

        if ( (node_sizes[indexes[0]] - (abs(node_sizes[indexes[1]] - estimated_size))) >= estimated_size )
           items_to_send = abs(node_sizes[indexes[1]] - estimated_size);
        else
           items_to_send = node_sizes[indexes[0]] - estimated_size;

        printf("Max: Node[%d] with %lu --- Min: Node[%d] with %lu  ---  Sending: %lu items\n", indexes[0], node_sizes[indexes[0]], indexes[1], node_sizes[indexes[1]], items_to_send);
        LOAD_BALANCE_LOG_DEBUG_TD("Max: Node[%d] with %lu --- Min: Node[%d] with %lu  ---  Sending: %lu items\n", indexes[0], node_sizes[indexes[0]], indexes[1], node_sizes[indexes[1]], items_to_send);
        
        if (indexes[0] == indexes[1]) {
          continue;
        }

        wts[indexes[0]].send_count += 1;
        wts[indexes[0]].dst_node_ids = (long*) realloc(wts[indexes[0]].dst_node_ids, wts[indexes[0]].send_count * sizeof(long));
        wts[indexes[0]].dst_node_ids[wts[indexes[0]].send_count - 1] = indexes[1];
        wts[indexes[0]].item_counts = (unsigned long*) realloc(wts[indexes[0]].item_counts, 
          wts[indexes[0]].send_count * sizeof(unsigned long));
        wts[indexes[0]].item_counts[wts[indexes[0]].send_count - 1] = items_to_send;
        
        for (int k = 0; k < wts[indexes[0]].send_count; k++) {
          printf("wts%d send_count=%ld; dst_node_id %ld; item_count %ld\n", indexes[0], wts[indexes[0]].send_count, 
            wts[indexes[0]].dst_node_ids[k], wts[indexes[0]].item_counts[k]);
          fflush(stdout);
        }

      }

      //Send 803 - work to send
      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
          //index %2==0 is node_id, index %2==1 is amount of items to send

          if ( wts[i].send_count > 0 ) {
            /*int blocks[3]={1, wts[i].send_count, wts[i].send_count};
            MPI_Datatype types[3]={MPI_LONG, MPI_LONG, MPI_UNSIGNED_LONG};
            MPI_Aint displacements[3];
            MPI_Datatype wts_mpi_struct_type;
            MPI_Aint longex, unsigned_longex;
            MPI_Type_extent(MPI_LONG, &longex);
            MPI_Type_extent(MPI_UNSIGNED_LONG, &unsigned_longex);
            displacements[0] = static_cast<MPI_Aint>(0);
            displacements[1] = longex;
            displacements[2] = longex + longex;
            MPI_Type_struct(3, blocks, displacements, types, &wts_mpi_struct_type);
            MPI_Send(&wts, 1, wts_mpi_struct_type, i, 803, MPI_COMM_WORLD);*/

            unsigned long *arr = (unsigned long*) malloc(wts[i].send_count * 2 * sizeof(unsigned long));
            for (int j = 0; j < wts[i].send_count; j++) {
              arr[j * 2] = wts[i].dst_node_ids[j];
              arr[(j * 2) + 1] = wts[i].item_counts[j];
              GLOBAL_COMM_LOG_INFO_TD(comm_rank, "Node[%d]: Must send to N%ld - %ld items\n", i, arr[j * 2], arr[(j*2)+1]);
              printf("MASTER GB: Node[%d]: Must send to N%ld - %ld items\n", i, arr[j * 2], arr[(j*2)+1]);
            }
            if ( i != master_id ) {
              MPI_Send(arr, (wts[i].send_count) * 2, MPI_UNSIGNED_LONG, i, 803, MPI_COMM_WORLD);
            }

          }
          else {
            int receive_cnt = node_receive_count(i, wts);
            GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Node[%d]: must receive %d messages\n", i, receive_cnt);
            printf("MASTER GB: Node[%d]: must receive %d messages\n", i, receive_cnt);
            //if ( receive_cnt > 0 ) {
              MPI_Send(&receive_cnt, 1, MPI_UNSIGNED_LONG, i, 803, MPI_COMM_WORLD);
            //}
            //else ( receive_cnt == 0 ) {
            //  MPI_Send(&receive_cnt, 1, MPI_UNSIGNED_LONG, i, 803, MPI_COMM_WORLD);
            //}
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
          GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Master sending to N%ld %lu items\n", wts[master_id].dst_node_ids[j], 
            wts[master_id].item_counts[j]);
          printf("MASTER GB: Master sending to N%ld %lu items\n", wts[master_id].dst_node_ids[j], wts[master_id].item_counts[j]);
          send_data(wts[master_id].dst_node_ids[j], wts[master_id].item_counts[j]);
        }
      }

      int rcv_cnt = node_receive_count(master_id, wts);
      if ( rcv_cnt > 0 ) {
        // Master waits for data receives
        for (int i = 0; i < rcv_cnt; i++) {
          int receive_item_count;

          MPI_Probe(MPI_ANY_SOURCE, 804, MPI_COMM_WORLD, &status);
          MPI_Get_count(&status, MPI_INT, &receive_item_count);

          int *data = malloc (receive_item_count * sizeof(int));
          void **data2 = malloc (receive_item_count * sizeof(void*));
          int src_id = status.MPI_SOURCE;
          
          fprintf(stdout, "MASTER: receive_item_count to receive=%d\n", receive_item_count);
          fflush(stdout);

          //void* data = (void*) malloc();
          //data[i] = (int*) malloc(receive_item_count * sizeof(int));
          //MPI_Recv(data[i], receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
          MPI_Recv(data, receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
          
          fprintf(stdout, "MASTER: received these items\n");
          fflush(stdout);
          for (int j = 0; j < receive_item_count; j++) {
            //int v = data[i][j];
            //fprintf(stdout, "%d\n", v);
            //fprintf(stdout, "%d\n", data[j]);
            data2[j] = &data[j];
            int *v = data2[j];
            fprintf(stdout, "%d\n", *v);
          }
          fflush(stdout);

          //lockfree_queue_insert_N_items((void**) &data[i], receive_send_count);
          long smallest_qid = find_smallest_q();
          //lockfree_queue_insert_N_items_no_lock_by_tid((void**) &data, receive_item_count, &smallest_qid);
          lockfree_queue_insert_N_items_no_lock_by_tid(data2, receive_item_count, &smallest_qid);
          free(data);
          free(data2);
        }
        printf("MASTER: Receiving work done\n");
      }

      //wait for other nodes to finish
      for ( int i = 0; i < comm_size; i++) {
        if ( comm_rank != i ) {
          MPI_Recv(&buf, 1, MPI_SHORT, i, 806, MPI_COMM_WORLD, &status);
        }
      }

      printf("MASTER GB: GB done!\n");
      int code_805 = 0;
      MPI_Send(&code_805, 1, MPI_INT, source_node, 805, MPI_COMM_WORLD); //rebalance finished
      UNLOCK_LOAD_BALANCER();
      UNLOCK_LOCAL_QUEUES();

    }
  }
  else {
    //SLAVE
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
      unsigned long qsize = lockfree_queue_size_total();
      MPI_Send(&qsize, 1, MPI_UNSIGNED_LONG, master_id, 802, MPI_COMM_WORLD);

      MPI_Probe(master_id, 803, MPI_COMM_WORLD, &status);
      int receive_send_count;
      int receive_item_count;
      MPI_Get_count(&status, MPI_UNSIGNED_LONG, &receive_send_count);
      fprintf(stdout, "Slave%d: receive_send_count=%d\n", comm_rank, receive_send_count);
      fflush(stdout);
      unsigned long *work_arr = (unsigned long*) malloc(receive_send_count * sizeof(unsigned long));
      if (receive_send_count == 1) {
        // Slave only receives data
        // In work_arr[0] is number of receives
        MPI_Recv(work_arr, receive_send_count, MPI_UNSIGNED_LONG, master_id, 803, MPI_COMM_WORLD, &status);
        if (work_arr[0] == 0) {
          // Nothing to receive
          fprintf(stdout, "Slave[%d]: has nothing to receive\n", comm_rank);
          fflush(stdout);
        }
        else {
          fprintf(stdout, "Slave[%d]: has to receive %d times\n", comm_rank, receive_send_count);
          fflush(stdout);
          // Wait for data receives
          for (int i = 0; i < work_arr[0]; i++) {
            MPI_Probe(MPI_ANY_SOURCE, 804, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &receive_item_count);
            int src_id = status.MPI_SOURCE;
            int *data = malloc (receive_item_count * sizeof(int));
            void **data2 = malloc (receive_item_count * sizeof(void*));
            
            fprintf(stdout, "Slave%d: receive_item_count to receive=%d\n", comm_rank, receive_item_count);
            fflush(stdout);

            //void* data = (void*) malloc();
            //data[i] = (int*) malloc(receive_item_count * sizeof(int));
            //MPI_Recv(data[i], receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
            MPI_Recv(data, receive_item_count, MPI_INT, src_id, 804, MPI_COMM_WORLD, &status);
            
            fprintf(stdout, "Slave[%d]: received these items\n", comm_rank);
            //fflush(stdout);
            /*for (int j = 0; j < receive_item_count; j++) {
              int v = data[i][j];
              fprintf(stdout, "%d\n", v);
            }*/
            for (int j = 0; j < receive_item_count; j++) {
              //fprintf(stdout, "%d\n", data[j]);
              /*int* d = malloc(sizeof(int));
              *d = data[j];*/
              data2[j] = &data[j];
              int *v = data2[j];
              fprintf(stdout, "%d\n", *v);
            }
            fflush(stdout);

            long smallest_qid = find_smallest_q();
            //lockfree_queue_insert_N_items_no_lock_by_tid((void**) &data[i], receive_item_count, &smallest_qid);
            //lockfree_queue_insert_N_items_no_lock_by_tid((void**) &data, receive_item_count, &smallest_qid);
            lockfree_queue_insert_N_items_no_lock_by_tid(data2, receive_item_count, &smallest_qid);
            free(data);
            free(data2);
          }
          printf("Slave[%d]: Receiving work done\n", comm_rank);
        }
      }
      else {
        //Receive work and send it to peers
        MPI_Recv(work_arr, receive_send_count, MPI_UNSIGNED_LONG, master_id, 803, MPI_COMM_WORLD, &status);

        for (int i = 0; i < receive_send_count / 2; i++) {
          unsigned long dst_node_id = work_arr[i * 2];
          unsigned long send_item_count = work_arr[(i * 2) + 1];
          fprintf(stdout, "Slave%d: has to send %lu items to node %lu\n", comm_rank, send_item_count, dst_node_id);
          fflush(stdout);
          send_data(dst_node_id, send_item_count);
        }
        printf("Slave[%d]: Sending work done\n", comm_rank);
      }

      free(work_arr);
      short work_finished = 0;
      MPI_Send(&work_finished, 1, MPI_SHORT, master_id, 806, MPI_COMM_WORLD);  //send to master sending of work is done
      UNLOCK_LOAD_BALANCER();
      UNLOCK_LOCAL_QUEUES();
      printf("Slave[%d]: Slave finished work\n", comm_rank);

    }
  }

  return NULL;

}

unsigned long lockfree_queue_size_by_tid (void *tid) {
   
  long *t = tid;
  //return atomic_load( &(queues[ *t ]->a_qsize) );
  return queues[*t]->a_qsize;
   
}


unsigned long lockfree_queue_size_total() {
   
  //TODO nepouzivat pri nekonzistentnej celkovej velkosti atomicke premenne

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

unsigned long* lockfree_queue_size_total_consistent_allarr () {

   unsigned long* sizes = (unsigned long*) malloc(queue_count * sizeof(unsigned long));

   LOCK_LOCAL_QUEUES();

   for (int i = 0; i < queue_count; i++) {
      sizes[i] = atomic_load( &(queues[i]->a_qsize) );
   }

   UNLOCK_LOCAL_QUEUES();
   return sizes;
}

qsizes* lockfree_queue_size_total_allarr_sorted () {

  qsizes *qss = (qsizes*) malloc(queue_count * sizeof(qsizes));

  for (int i = 0; i < queue_count; i++) {
    qss[i].size = atomic_load( &(queues[i]->a_qsize) );
    qss[i].index = i;
  }

  qsort(qss, queue_count, sizeof(qsizes), qsize_comparator);

  //TODO DELETE AFTER TEST
  /*printf("Sorted array: \n");
  for (int i = 0; i < queue_count; i++) {
    printf("\tQ[%ld] - %lu\n", qss[i].index, qss[i].size);
  }*/

  return qss;
}

int qsize_comparator(const void *a, const void *b) {

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

void lockfree_queue_stop_watcher() {

  //int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag,
  //          MPI_Comm comm, MPI_Status *status)
  //int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source,
  //          int tag, MPI_Comm comm, MPI_Request *request)
  //int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
  //          MPI_Comm comm, MPI_Request *request)
  //int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
  //          MPI_Comm comm)

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
  flag_graceful_stop = true;  //for remove count nuller
  #ifdef COUNTERS
    pthread_join(per_time_statistics_reseter_t, NULL);
  #endif
  pthread_join(listener_global_size_t, NULL);
  pthread_join(listener_global_balance_t, NULL);
  //pthread_join(local_struct_cleanup_t, NULL);

  LOG_INFO_TD("STATISTICS: \n\tQsize watcher was called %lu times\n\tLoad balance was called from remove %lu times\
    \n\tGlobal balance was initiated %lu times\n\tGlobal size was executed %lu times\n\tGlobal size only read last value %lu times\n\tLocal balance moved %lu items\n", atomic_load(&load_balancer_call_count_watcher), atomic_load(&load_balancer_call_count_remove), atomic_load(&load_balancer_call_count_global), 
    atomic_load(&global_size_call_count), atomic_load(&global_size_call_count_in_wait), atomic_load(&moved_items_log));

  //double sum_rt_time = sum_time(total_rt_lb_time_sec, total_rt_lb_time_nsec);
  double sum_thr_time = sum_time(total_thr_lb_time_sec, total_thr_lb_time_nsec);
  /*LOG_INFO_TD("\tTotal realtime spent in load balancer: %lf seconds\
    \n\tTotal Thread time spent in load balancer: %lf seconds\n", sum_rt_time, sum_thr_time);*/
  LOG_INFO_TD("\tTotal Thread time spent in load balancer: %lf seconds\n", sum_thr_time);
   
  double sum_rt_time = sum_time(total_rt_global_size_time_sec, total_rt_global_size_time_nsec);
  sum_thr_time = sum_time(total_thr_global_size_time_sec, total_thr_global_size_time_nsec);
  LOG_INFO_TD("\tTotal realtime spent in global size: %lf seconds\
    \n\tTotal thread time spent in global size: %lf seconds\n", sum_rt_time, sum_thr_time);

  //pthread_cancel(listener_global_size_t);



  //if ( pthread_cancel(listener_global_size_t) != 0 )
  //   printf("Pthread cancel on listener_global_size_t failed\n");
  //pthread_cancel(listener_global_balance_t);
  //MPI_Finalize();   //TODO remove this function from here

}

int node_receive_count(int node_id, work_to_send *wts) {

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

int send_data(int dst, unsigned long count) {

  fprintf(stdout, "Send_Data: sending %lu data to %d\n", count, dst);
  fflush(stdout);
  long count_tmp = count;
  unsigned long *sizes = lockfree_queue_size_total_allarr();
  unsigned long total_size = lockfree_queue_size_total();
  unsigned long q_remainder = total_size - count;
  unsigned long balanced_size = q_remainder / queue_count;

  fprintf(stdout, "Q sizes:\n");
  for (int i = 0; i < queue_count; i++) {
    fprintf(stdout, "Q[%d] size = %lu\n", i, sizes[i]);
  }
  fprintf(stdout, "Total size=%lu\n", total_size);
  fprintf(stdout, "Q remainder=%lu\n", q_remainder);
  fprintf(stdout, "Balanced size=%lu\n", balanced_size);
  fflush(stdout);

  unsigned long displ = 0;
  void** all_items = malloc(count * sizeof(void*));

  if (all_items == NULL) {
    fprintf(stdout, "ERROR: Malloc failed in send_data\n");
    fflush(stdout);
  }

  for (int i = 0; i < queue_count; i++) {
    long items_to_take = sizes[i] - balanced_size;
    fprintf(stdout, "Items to take in i=%d is %ld\n", i, items_to_take);
    fflush(stdout);

    if ( items_to_take > 0 ) {
      if ( count_tmp - items_to_take < 0 ) {
        items_to_take = count_tmp;
        count_tmp -= items_to_take;
      }
      else {
        count_tmp -= items_to_take;
      }
      //TODO test retype void* to array of pointers to int 
      fprintf(stdout, "Items to take after comp=%ld; need to get %ld more items\n", items_to_take, count_tmp);
      fflush(stdout);
      void** values = malloc(items_to_take * sizeof(void*));
      for (int j = 0; j < items_to_take; j++) {
        values[j] = malloc(sizeof(void*));
      }
      lockfree_queue_remove_Nitems_no_lock_by_tid((long) i, items_to_take, values);

      /*printf("Removed items in send_items: \n");
      for (int j = 0; j < items_to_take; j++) {
        int *v = values[j];
        printf("%d\n", *v);
      }*/

      fprintf(stdout, "Copying %ld items with size %lu with displacement %lu\n", items_to_take, queues[0]->item_size, displ );
      fflush(stdout);
      //memcpy(all_items + displ, values, queues[0]->item_size * items_to_take);
      memcpy(all_items + displ, values, sizeof(int*) * items_to_take);
      displ += items_to_take;
      //free(values);
    }
  }
  
  /*fprintf(stdout, "Sending All_items(%lu): \n", count);
  for (int j = 0; j < count; j++) {
    int *v = all_items[j];
    fprintf(stdout, "%d\n", *v);
    fflush(stdout);
  }*/

  //fprintf(stdout, "Recopied items: \n");
  int* int_arr = malloc(count * sizeof(int));
  for (int i = 0; i < count; i++) {
    int *v = all_items[i];
    int_arr[i] = *v;
    //fprintf(stdout, "%d\n", int_arr[i]);
    //fflush(stdout);
    //memcpy(int_arr, all_items[i], sizeof(int) * count);
  }

  //TODO GET ITEM SIZE BY size_t AND PASS IT TO MPI. USE PROTOBUF? OR SEND BYTES
  //MPI_Send(*all_items, count, queues[0]->item_size, dst, MPI_COMM_WORLD);
  printf("Node[%d] is sending data to N%d\n", comm_rank, dst);
  MPI_Send(int_arr, count, MPI_INT, dst, 804, MPI_COMM_WORLD);

  return 0;

}

long maxdiff_q(void* qid) {

  long *q = qid;
  unsigned long diff_max = 0;
  unsigned long diff_max_id = 0;
  unsigned long src_q_size = lockfree_queue_size_by_tid(q);

  //TODO sledovat statisticku hodnotu ako velmi sa lisia rady a podla nej volit threshold? (variancia?)
  for (long i = 0; i < queue_count; i++) {
    unsigned long diff = abs(lockfree_queue_size_by_tid(&i) - src_q_size);
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

long find_largest_q() {

  long index = 0;
  unsigned long max = 0;
   
  for (long i = 0; i < queue_count; i++) {
    unsigned long size = lockfree_queue_size_by_tid(&i);
    if ( size > max ) {
      max = size;
      index = i;
    }
  }

  return index;

}

long find_smallest_q() {

  long index = 0;
  unsigned long min = ULONG_MAX;
   
  for (long i = 0; i < queue_count; i++) {
    unsigned long size = lockfree_queue_size_by_tid(&i);
    if ( size < min ) {
      min = size;
      index = i;
    }
  }

  return index;

}

int* find_max_min_element_index(unsigned long *array, unsigned long len) {

    /*
     * index_max_min[0] is index of element with max number
     * index_max_min[1] is index of element with min number
     */
   
   unsigned long *arr_max_min = (unsigned long*) malloc ( 2 * sizeof(unsigned long));
   arr_max_min[0] = 0;
   arr_max_min[1] = ULONG_MAX;
   
   int *index_max_min = (int*) malloc ( 2 * sizeof(int));
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
   
   free(arr_max_min);
   return index_max_min;
   
}

long find_max_element_index(unsigned long *array, unsigned long len) {

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

void* per_time_statistics_reseter(void *arg) {

  while(1) {
    sleep(1);
    if (flag_graceful_stop) {
      break;
    }

    atomic_store(&rm_count_last, atomic_load(&rm_count));
    atomic_store(&ins_count_last, atomic_load(&ins_count));

    //TODO remove prints after debugging
    LOG_INFO_TD("Per seconds statistics time=%d\n", (int) time(NULL));
    for (long i = 0; i < queue_count; i++) {
      LOG_INFO_TD("\tQueue[%ld] size is %ld\n", i, lockfree_queue_size_by_tid(&i));
    }
    LOG_INFO_TD("\tTotal removes per second=%ld\n", atomic_load(&rm_count_last));
    LOG_INFO_TD("\tTotal inserts per second=%ld\n", atomic_load(&ins_count_last));

    atomic_store(&rm_count, 0);
    atomic_store(&ins_count, 0);
  }
  return NULL;
}

void* local_struct_cleanup() {

  while(1) {

    if (flag_graceful_stop) {
      break;
    }
 
    sleep(1);
    unsigned long items_cleaned = 0;

    for (int i = 0; i < queue_count; i++) {
      struct ds_lockfree_queue *q = queues[i];
      struct lockfree_queue_item *tmp;
      pthread_mutex_lock(&add_mutexes[i]);
    
      //cleanup
      while ( q->head != q->divider ) {
        tmp = q->head;
        q->head = q->head->next;
        //free(tmp->val); //allocated in main - can not free here
        free(tmp);
        items_cleaned++;
      }
      pthread_mutex_unlock(&add_mutexes[i]);
    }
    //printf("cleaned %ld items\n", items_cleaned);

  }

  return NULL;

}

double sum_time(time_t sec, long nsec) {

  double final_time = (double) 0.00;
  final_time += (double) sec;
  final_time += (double) nsec / (double) 1000000000;
  return final_time;

}

unsigned long sum_arr(unsigned long *arr, unsigned long len) {

  unsigned long result = 0;
  for (int i = 0; i < len; i++) {
    result += arr[i];
  }

  return result;

}
