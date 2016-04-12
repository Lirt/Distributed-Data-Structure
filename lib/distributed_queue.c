
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

bool load_balancing_t_running_flag;
bool qsize_watcher_t_running_flag;
bool qsize_watcher_t_enable;
bool flag_watcher_graceful_stop;
bool flag_graceful_stop;

unsigned long *qsize_history = NULL;
unsigned long qsize_wait_time = 20000; //in microseconds
double local_threshold_percent = 20.00;
double global_threshold_percent = 20.00;
double local_threshold_static = 20000;
double global_threshold_static = 100000;
unsigned int len_s = 3;
unsigned int threshold_type = 0;

atomic_ulong rm_count;         //stores per queue amount of removes per second
atomic_ulong rm_count_last;    //stores per queue amount of removes from last second
atomic_ulong ins_count;         //stores per queue amount of removes per second
atomic_ulong rm_count_last;    //stores per queue amount of removes from last second
pthread_t per_time_statistics_reseter_t;       //swaps rm_count of every queue every second to rm_count_last and nulls rm_count

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

pthread_t listener_global_size_t;
pthread_t listener_global_balance_t;
pthread_mutex_t load_balance_global_mutex; //To lock thread until global operation as global_size or global_balance is done
pthread_cond_t load_balance_global_cond;
pthread_rwlock_t global_size_rwlock;
//pthread_rwlockattr_t attr;

unsigned int global_size_receive_timeout = 10000; //100ms timeout for receive
unsigned long last_global_size;

bool global_balancing_enable; //True enables global balancing, False disables global balancing
double last_rebalance_time;   //For elimination of flooding network with global rebalance requests


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
  struct tid_hash_struct *ths;
  pthread_t pt = pthread_self();

  if (pthread_rwlock_rdlock(&uthash_rm_rwlock) != 0) {
    LOG_ERR_T( (long) -1, "Can't acquire read lock\n");
    exit(-1);
  }
  HASH_FIND_INT( tid_insertion_hashes, &pt, ths );
  pthread_rwlock_unlock(&uthash_rm_rwlock);

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
  printf("%d\n", t);

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
   
   if (load_balancing_t_running_flag)
      pthread_cancel(load_balancing_t);
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

pthread_t* lockfree_queue_init_callback ( void* (*callback)(void *args), void* arguments, 
  unsigned int queue_count_arg, unsigned int thread_count_arg, 
  bool qw_thread_enable_arg, double local_lb_threshold_percent, double global_lb_threshold_percent, 
  unsigned long local_lb_threshold_static, unsigned long global_lb_threshold_static, unsigned int threshold_type_arg, 
  unsigned int local_balance_type_arg ) {

  /*
   * Local load balance type values are:
   * '0' for STATIC
   * '1' for PERCENT
   * '2' for DYNAMIC
   */

  //TODO documentation must contain struct used for arguments in thread callback

  char hostname[256];
  pid_t pid = getpid();
  int pid_int = (int) pid;
  gethostname(hostname, sizeof(hostname));

  /*
   * Init debug files
   */
  struct stat st = {0};

  if (stat("/tmp/distributed_queue", &st) == -1) {
    mkdir("/tmp/distributed_queue", 0777);
  }

  char pid_str[8];
  sprintf(pid_str, "%d", pid_int);
  printf("pid=%d\n", pid_int);

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
  LOG_INFO_TD("Task_count: %d\nCOMM_RANK: %d\nProcessor_name: '%s'\nRequired_thread_level: %d\n", 
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

  //pthread_condattr_init (attr)
  pthread_cond_init (&load_balance_cond, NULL);
  pthread_mutex_init(&load_balance_mutex, &mutex_attr);
  pthread_mutex_init(&load_balance_global_mutex, &mutex_attr);
  pthread_cond_init(&load_balance_global_cond, NULL);
  pthread_mutex_init(&qsize_watcher_mutex, &mutex_attr);
  load_balancing_t_running_flag = false;
  qsize_watcher_t_running_flag = false;

  add_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
  rm_mutexes = (pthread_mutex_t*) malloc ( queue_count * sizeof(pthread_mutex_t) );
  for (int i = 0; i < queue_count; i++) {
    pthread_mutex_init(&add_mutexes[i], &mutex_attr);
    pthread_mutex_init(&rm_mutexes[i], &mutex_attr);
  }
   

  /*
   * Global balance listener
   */
  global_balancing_enable = false; //enables or disables global balancing
  last_rebalance_time = 0;
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

  rc = pthread_create(&listener_global_size_t, &attr, comm_listener_global_size_consistent, NULL);
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
   * Initialize qsize watcher
   */

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

  struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  qsize_watcher_strategy qw_strategy;
  if ( local_balance_type_arg == 1 ) {
    //all
    qw_strategy = (qsize_watcher_local_threshold_strategy());
    lbs = (load_balancer_pair_balance(NULL));
    LOG_INFO_TD("Load balancer strategy is pair balance\n");
  }
  else if ( local_balance_type_arg == 2 ) {
    //pair
    qw_strategy = (qsize_watcher_min_max_strategy());
    lbs = (load_balancer_all_balance(NULL));
    LOG_INFO_TD("Load balancer strategy is equal balance\n");
  }
  else {
    LOG_ERR_T( (long) -1, "Bad argument for qsize_watcher and load balance type\n");
    exit(-1);
  }

  qsize_watcher_t_enable = qw_thread_enable_arg;
  flag_watcher_graceful_stop = false;
  flag_graceful_stop = false;
  if ( qsize_watcher_t_enable ) {
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

  rc = pthread_create(&per_time_statistics_reseter_t, &attr, per_time_statistics_reseter, NULL);
  if (rc) {
    fprintf(stderr, "ERROR: (init per_time_statistics_reseter) return code from pthread_create() is %d\n", rc);
    LOG_ERR_T( (long) -1, "Pthread create failed\n");
    exit(-1);
  }


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

void lockfree_queue_insert_item (void* val) {

   long tid = getInsertionTid();
   struct ds_lockfree_queue *q = queues[ tid ];

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }
   struct lockfree_queue_item *tmp;
   item->val = val;
   item->next = NULL;
   pthread_mutex_lock(&add_mutexes[tid]);
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   atomic_fetch_add(&ins_count, 1);

   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      //free(tmp->val); //allocated in main - can not free here
      free(tmp);
   }

   pthread_mutex_unlock(&add_mutexes[tid]);
}

void lockfree_queue_insert_item_by_tid (void *t, void* val) {

   long *tid = t;
   struct ds_lockfree_queue *q = queues[ *tid ];

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));

   if (item == NULL) {
      LOG_ERR_T( (long) tid, "Malloc failed\n");
   }

   struct lockfree_queue_item *tmp;
   item->val = val;
   item->next = NULL;
   pthread_mutex_lock(&add_mutexes[*tid]);
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
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
   item->val = val;
   item->next = NULL;
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
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

   item->val = values[0];
   item_first = item;

   for (int i = 1; i < item_count; i++) {
      item_tmp = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      if (item_tmp == NULL) {
         LOG_ERR_T( (long) tid, "Malloc failed\n");
         return;
      }
      item_tmp->val = values[i];
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
  atomic_fetch_add(&ins_count, item_count);

   //cleanup   
   struct lockfree_queue_item *tmp;
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[tid]);
   
}

void* load_balancer_pair_balance(void* lb_struct_arg) {

  struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, tp_rt_start);

  struct load_balancer_struct *lb_struct = lb_struct_arg;

  pthread_mutex_lock(&add_mutexes[lb_struct->pair[0]]);
  pthread_mutex_lock(&add_mutexes[lb_struct->pair[1]]);
  pthread_mutex_lock(&rm_mutexes[lb_struct->pair[0]]);
  pthread_mutex_lock(&rm_mutexes[lb_struct->pair[1]]);

  unsigned long qsize_src = lockfree_queue_size_by_tid(&lb_struct->pair[0]);
  unsigned long qsize_dst = lockfree_queue_size_by_tid(&lb_struct->pair[1]);

  unsigned long estimated_size = (qsize_src + qsize_dst) / 2;
  unsigned long items_to_send;

  if ( qsize_src > qsize_dst ) {
    items_to_send = qsize_src - estimated_size;
    lockfree_queue_move_items(lb_struct->pair[0], lb_struct->pair[1], items_to_send);
  }
  else if ( qsize_src < qsize_dst ) {
    items_to_send = qsize_dst - estimated_size;
    lockfree_queue_move_items(lb_struct->pair[1], lb_struct->pair[0], items_to_send);
  }

  if ( lb_struct->qsize_history != NULL ) {
    unsigned long* qsize_history = lb_struct->qsize_history;
    for (int j = 0; j < queue_count; j++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], lockfree_queue_size_by_tid(tids[j]));
      qsize_history[j] = lockfree_queue_size_by_tid(tids[j]);;
    }
  }

  pthread_mutex_unlock(&add_mutexes[lb_struct->pair[0]]);
  pthread_mutex_unlock(&add_mutexes[lb_struct->pair[1]]);
  pthread_mutex_unlock(&rm_mutexes[lb_struct->pair[0]]);
  pthread_mutex_unlock(&rm_mutexes[lb_struct->pair[1]]);



  clock_gettime(CLOCK_REALTIME, tp_rt_end);
  total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  //total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  //total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;

  return NULL;

}

void* load_balancer_all_balance(void* arg) {

 /*
  * Counts estimated size
  * Count who should give whom what amount of items
  * Relocates data
  */

  LOCK_LOCAL_QUEUES();

  //#ifdef COUNTERS
    struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
    struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
    struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
    struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
    clock_gettime(CLOCK_REALTIME, tp_rt_start);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);
  //#endif
  LOAD_BALANCE_LOG_DEBUG_TD("LB TIME START: \n\tRealtime - %lu.%lu seconds\n\tThread Time- %lu.%lu seconds\n", 
    tp_rt_start->tv_sec, tp_rt_start->tv_nsec, tp_thr_start->tv_sec, tp_thr_start->tv_nsec);

  //Do not use function lockfree_queue_size_total_consistent(), because mutexes are already locked. 
  unsigned int total = lockfree_queue_size_total();
  unsigned int estimated_size = total / queue_count;

  int *indexes = (int*) malloc (2 * sizeof(int));
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  if ( indexes == NULL ) {
    LOG_ERR_T( (long) -2, "Malloc failed\n");
  }
  if ( q_sizes == NULL ) {
    LOG_ERR_T( (long) -2, "Malloc failed\n");
  }
  unsigned long items_to_send;

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

    LOAD_BALANCE_LOG_DEBUG_TD("LB: Sizes after load balance round %d\n", i);
  }

  if ( arg != NULL ) {
    unsigned long* qsize_history = arg;
    for (int j = 0; j < queue_count; j++) {
      q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
      LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      qsize_history[j] = q_sizes[j];
    }
  }
   
  free(indexes);

  UNLOCK_LOCAL_QUEUES();
  load_balancing_t_running_flag = false;

  //#ifdef COUNTERS
    clock_gettime(CLOCK_REALTIME, tp_rt_end);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  //#endif
  LOAD_BALANCE_LOG_DEBUG_TD("LB TIME END: \n\tRealtime - %lu.%lu seconds\n\tThread Time- %lu.%lu seconds\n", 
    tp_rt_end->tv_sec, tp_rt_end->tv_nsec, tp_thr_end->tv_sec, tp_thr_end->tv_nsec);
  LOAD_BALANCE_LOG_DEBUG_TD("Final realtime local LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec );
  LOAD_BALANCE_LOG_DEBUG_TD("Final thread local LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec, time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec );

  total_rt_lb_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  total_rt_lb_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  total_thr_lb_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  total_thr_lb_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;

  return NULL;

}

void lockfree_queue_move_items(int q_id_src, int q_id_dest, unsigned long count) {

   /*
    * Does not lock queues
    */

   if ( count == 0 ) {
      return;
   }

   struct ds_lockfree_queue *q_src = queues[ q_id_src ];
   struct ds_lockfree_queue *q_dest = queues[ q_id_dest ];

   unsigned long q_size_src = atomic_load( &(q_src->a_qsize) );

   struct lockfree_queue_item *tmp_div;
   struct lockfree_queue_item *tmp_div_next;
   tmp_div_next = q_src->divider->next;
   tmp_div = q_src->divider;

   //printf("Count=%ld, Q_SRC_SIZE=%ld, Q_DST_SIZE=%ld\n", count, q_size_src, q_size_dest);

   if ( count > q_size_src ) {
      LOG_ERR_T( (long) -2, "Cannot move more items(%ld) than queue size(%ld)\n", count, q_size_src);
      return;
   }
   else {
      for (int i = 0; i < count; i++) {
         tmp_div = tmp_div->next;
      }
      q_src->divider->next = tmp_div->next;

      tmp_div->next = NULL;
      q_dest->tail->next = tmp_div_next;
      q_dest->tail = tmp_div;
   }

    atomic_fetch_sub(&(q_src->a_qsize), count);
    atomic_fetch_add(&(q_dest->a_qsize), count);
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
  
  /*struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);*/
  QSIZE_WATCHER_LOG_DEBUG_TD("Qsize watcher started\n");

  while(1) {
    total_qsize = 0;
    balance_flag = false;
    
    //Sleep(microseconds) for a while for less overhead
    //usleep(qsize_wait_time);

    if ( flag_watcher_graceful_stop ) {
      return NULL;
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
    if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {
      
      load_balancing_t_running_flag = true;
      
      lb_struct->pair[0] = find_max_element_index(q_sizes, queue_count);  //max index
      lb_struct->pair[1] = last_q;  //min index
      lb_struct->qsize_history = qsize_history;

      QSIZE_WATCHER_LOG_DEBUG_TD("Starting min-max load balancer.\n");
      lbs(lb_struct);
      atomic_fetch_add(&load_balancer_call_count_watcher, 1);

      pthread_mutex_unlock(&load_balance_mutex);

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

}

void* qsize_watcher_local_threshold_strategy() {
   
  bool balance_flag;
  unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
  unsigned long total_qsize;
  unsigned long *qsize_history = (unsigned long*) malloc ( queue_count * sizeof (unsigned long));
  for (int i = 0; i < queue_count; i++) {
    qsize_history[i] = 0;
  }
  //struct load_balancer_struct *lb_struct = (struct load_balancer_struct*) malloc (sizeof(struct load_balancer_struct));
  
  /*struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);*/
  QSIZE_WATCHER_LOG_DEBUG_TD("Qsize watcher started\n");
   
  while(1) {
    total_qsize = 0;
    balance_flag = false;

    if ( flag_watcher_graceful_stop ) {
      return NULL;
    }

    for (int i = 0; i < queue_count; i++) {
      q_sizes[i] = lockfree_queue_size_by_tid(tids[i]);
      total_qsize += q_sizes[i];
    }
    QSIZE_WATCHER_LOG_DEBUG_TD("Total qsize = %ld\n", total_qsize);
  
    if (total_qsize == 0) {
      //Sleep(microseconds) for a while for less overhead
      usleep(qsize_wait_time);
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
    
    if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {

      load_balancing_t_running_flag = true;
      QSIZE_WATCHER_LOG_DEBUG_TD("Starting load balancer thread.\n");

      //lb_struct->qsize_history = qsize_history;
      lbs(qsize_history);
      //load_balancer_all_balance(qsize_history);

      atomic_fetch_add(&load_balancer_call_count_watcher, 1);
      QSIZE_WATCHER_LOG_DEBUG_TD("Waiting for load balance thread to finish\n");

      pthread_mutex_unlock(&load_balance_mutex);

      QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Got signal from load balancer -> returning to work\n\
        QSIZE WATCHER: Qsize history after rebalance is:\n");
      for (int i = 0; i < queue_count; i++) {
        QSIZE_WATCHER_LOG_DEBUG_TD("Q%d-%ld items\n", i, qsize_history[i]);
      }
    }
    else {
       QSIZE_WATCHER_LOG_DEBUG_TD("Load Balancer already running\n");
       continue;
    }
  }  
}

void* lockfree_queue_remove_item (int timeout) {

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
            //printf("Queues are empty\n");
            
            //TODO Send message that queue is empty(800) to master
            //wait for reply if other queues are empty as well(code 801)
            //reply message number is 0 -> queues are not empty if message is 1, queues are empty)
            //if reply is that they are empty, return NULL
            //if queues are not empty, wait for master to rebalance (barrier?)
            //waiting for rebalancing is done with separate thread which wait for message (802)
            //after rebalancing get size of queue or read message with amount of items send
            //if amount = 0; return NULL
            //if not, return item.

            //TODO how to eliminate flooding with rebalance requests to master
            //create variable with timer of last rebalance request
            //allow node ask for rebalancing every X second/milisecond

            if (!global_balancing_enable) {
               break;
            }
            else {
               atomic_fetch_add(&load_balancer_call_count_global, 1);

               short buf;
               MPI_Status status;
               short code_800 = 800;
               MPI_Send(&code_800, 1, MPI_SHORT, master_id, 800, MPI_COMM_WORLD);
               MPI_Recv(&buf, 1, MPI_SHORT, master_id, 801, MPI_COMM_WORLD, &status);
               if ( buf == 0 ) {
                  printf("REMOVE: Global balancing -> other queues are not empty\n");
                  /*
                   * other local_structures are not empty
                   * wait for signal that network is rebalanced and then get item
                   */
                  pthread_cond_wait(&load_balance_global_cond, &load_balance_global_mutex);
                  pthread_mutex_unlock(&load_balance_global_mutex);
                  //if ( lockfree_queue_size_total() == 0 ) {
                  //   break;
                  //}
                  //else {
                  //TODO FIRST CHECK divider and tail then release lock?
                  //TODO also check total size... other queues may have items
                  //During load balancing if nothing can be send to queue but there are items, relocate item to queue which called for balancing
                  if ( q->divider != q->tail ) {   //atomic reads?
                     val = q->divider->next->val;
                     q->divider = q->divider->next;
                     atomic_fetch_sub( &(q->a_qsize), 1);
                     break;
                  }
                  else {
                     break;
                  }
               } 
               else {
                  //other local_structures are empty
                  break;
               }
            }
         }
         else {
            //pthread_mutex_trylock returns 0 if lock is acquired
            if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {
              load_balancing_t_running_flag = true;

              //pthread_attr_t attr_lb;
              //pthread_attr_init(&attr_lb);
              //pthread_attr_setdetachstate(&attr_lb, PTHREAD_CREATE_JOINABLE);
              int rc = pthread_create(&load_balancing_t, &attr, load_balancer_all_balance, NULL);
              if (rc) {
                printf("ERROR: (remove item) return code from pthread_create() of load_balancing_t is %d\n", rc);
                load_balancing_t_running_flag = false;
                pthread_mutex_unlock(&rm_mutexes[tid]);
                pthread_mutex_unlock(&load_balance_mutex);
                continue;
              }
              else {
                atomic_fetch_add(&load_balancer_call_count_remove, 1);
                //Rm mutex for this thread must be unlocked. May cause harm in other parts of code... 
                pthread_mutex_unlock(&rm_mutexes[tid]);
                pthread_join(load_balancing_t, NULL);
                pthread_mutex_unlock(&load_balance_mutex);
                //pthread_attr_destroy(attr_lb);
              }
            }
            else {
               pthread_mutex_unlock(&rm_mutexes[tid]);
               usleep(50);   //TODO timer
            }
         }
      }
   }
   
   pthread_mutex_unlock(&rm_mutexes[tid]);
   atomic_fetch_add(&rm_count, 1);
   return val;
   
}

void* lockfree_queue_remove_item_by_tid (void* t, int timeout) {

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
      //TODO
      ;
   }
   
   pthread_mutex_unlock(&rm_mutexes[*tid]);
   return val;
   
}

void* lockfree_queue_remove_item_by_tid_no_lock (void* t, int timeout) {

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
   
   return val;
}


void** lockfree_queue_remove_Nitems (unsigned long N, int timeout) {
   
   /*
    * N is amount of items to be taken from Q
    */
   
   long tid = getRemovalTid();
   void **val_arr = malloc(N * sizeof(void*));
   if (val_arr == NULL)
      LOG_DEBUG_TD( tid, "Malloc failed\n");
   
   volatile struct ds_lockfree_queue *q = queues[ tid ];
   
   pthread_mutex_lock(&rm_mutexes[tid]);
   
   unsigned long item_count = atomic_load( &(q->a_qsize) ); 
   if ( atomic_load( &(q->a_qsize) ) < N ) {
      printf("Not enough items in queue %ld. There are %ld but was requested %ld.\n", tid, item_count, N);
      pthread_mutex_unlock(&rm_mutexes[tid]);
      return NULL;
   }
   
   unsigned long i = 0;
   for (i = 0; i < N; i++) {
      if ( q->divider != q->tail ) {
         val_arr[i] = q->divider->next->val;
         q->divider = q->divider->next;
      }
      else {
         break;
      }
   }

   if (i != N-1) {
      printf("Function did not return requested numbers from queue %ld. number of returned values is %ld.\n", tid, i);
      pthread_mutex_unlock(&rm_mutexes[tid]);
      return NULL;
   }
   
   atomic_fetch_sub( &(q->a_qsize), N);
   pthread_mutex_unlock(&rm_mutexes[tid]);
   return val_arr;
   
}


unsigned long lockfree_queue_size_by_tid (void *tid) {
   
   long *t = tid;
   return atomic_load( &(queues[ *t ]->a_qsize) );
   
}


unsigned long lockfree_queue_size_total() {
   
   unsigned long size = 0;
   for (int i = 0; i < queue_count; i++) {
      size += atomic_load( &(queues[i]->a_qsize) );
   }

   return size;
   
}

unsigned long global_size() {
   
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
      return last_global_size;
    }
  }

  struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr_end = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, tp_rt_start);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_start);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "GET GLOBAL SIZE START: \n\tRealtime- %lu.%lu seconds\n\tThread Time- %lu.%lu seconds\n", 
    tp_rt_start->tv_sec, tp_rt_start->tv_nsec, tp_thr_start->tv_sec, tp_thr_start->tv_nsec);

  MPI_Status status;
  unsigned long global_size = 0;
  short code_900 = 900;
  
  
  MPI_Send(&code_900, 1, MPI_SHORT, master_id, 900, MPI_COMM_WORLD);

  /*if (pthread_rwlock_rwlock(&global_size_rwlock) != 0) {
    LOG_ERR_T( (long) -1, "Can't acquire global size lock\n");
    return ULONG_MAX;
  }*/

  MPI_Recv(&global_size, 1, MPI_UNSIGNED_LONG, master_id, 903, MPI_COMM_WORLD, &status);
  last_global_size = global_size;
  pthread_rwlock_unlock(&global_size_rwlock);
  
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "NODE %d: Global structure size is %ld\n", comm_rank, global_size);
  
  clock_gettime(CLOCK_REALTIME, tp_rt_end);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr_end);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "GET GLOBAL SIZE END: \n\tRealtime- %lu.%lu seconds\n\tThread Time- %lu.%lu seconds\n", 
    tp_rt_end->tv_sec, tp_rt_end->tv_nsec, tp_thr_end->tv_sec, tp_thr_end->tv_nsec);
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final realtime global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec, time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec );
  GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "Final thread global LB time = '%lu.%lu'\n", 
    time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec, time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec );

  total_rt_global_size_time_sec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_sec;
  total_rt_global_size_time_nsec += time_diff_dds(tp_rt_start, tp_rt_end)->tv_nsec;
  total_thr_global_size_time_sec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_sec;
  total_thr_global_size_time_nsec += time_diff_dds(tp_thr_start, tp_thr_end)->tv_nsec;

  return global_size;
   
}

//void* control_message_listener() {
   //TODO can be used as listener to every message and deciding what to do after that message is received
   //can be implemented using ANY_SOURCE and MPI_ANY_TAG
//}
void* comm_listener_global_size_consistent() {

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

  short buf;
  //short code_900 = 900;
  short code_901 = 901;
  MPI_Request* requests_901;
  MPI_Request* requests_902;
  MPI_Status status;
  MPI_Status *statuses;

  while(1) {

    if (comm_rank == master_id) {
      requests_901 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
      requests_902 = (MPI_Request*) malloc( (comm_size - 1) * sizeof(MPI_Request));
      statuses = (MPI_Status*) malloc( (comm_size - 1) * sizeof(MPI_Status));
      
      //For testing on request and implementation of timeout for gracefull exit on receive
      MPI_Request request_test;
      MPI_Status status_test;
      int test_flag = 0;
      
      //MPI_Irecv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
      //MPI_Recv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      //MPI_Recv(&buf, 1, MPI_SHORT, comm_rank, 900, MPI_COMM_WORLD, &status);
      MPI_Irecv(&buf, 1, MPI_SHORT, MPI_ANY_SOURCE, 900, MPI_COMM_WORLD, &request_test);

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

      //status.MPI_SOURCE
      //status.MPI_TAG
      int source_node = status.MPI_SOURCE;
      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Got message with buf=%hd from node %d\n", buf, source_node);
      int cnt = 0;
      if ( buf == 900 ) {
        for(int i = 0; i < comm_size; i++) {
           if (i != comm_rank) {
              MPI_Isend(&code_901, 1, MPI_SHORT, i, 901, MPI_COMM_WORLD, &requests_901[cnt]);
              cnt++;
           }
        }
      }

      LOCK_LOAD_BALANCER();
      LOCK_LOCAL_QUEUES();

      unsigned long master_struct_size = lockfree_queue_size_total();
      unsigned long *node_sizes;
      node_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
      cnt = 0;
      for (int i = 0; i < comm_size; i++) {
        if (i != comm_rank) {
           MPI_Irecv(&node_sizes[i], 1, MPI_UNSIGNED_LONG, i, 902, MPI_COMM_WORLD, &requests_902[cnt]);
           cnt++;
        }
        else {
           node_sizes[i] = master_struct_size;
        }
      }

      MPI_Waitall(comm_size - 1, requests_902, statuses);

      unsigned long global_struct_size_total = 0;
      for (int i = 0; i < comm_size; i++) {
        GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Node %d has %ld items\n", i, node_sizes[i]);
        global_struct_size_total += node_sizes[i];
      }
      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "MASTER: Global size is %ld\n", global_struct_size_total);

      MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
      MPI_Barrier(MPI_COMM_WORLD);

      UNLOCK_LOCAL_QUEUES();
      UNLOCK_LOAD_BALANCER();
      
      free(requests_901); free(requests_902); free(statuses); free(node_sizes);
    }
    else { 
      //For testing on request and implementation of timeout for gracefull exit on receive
      MPI_Request request_test;
      MPI_Status status_test;
      int test_flag = 0;

      //MPI_Irecv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
      //MPI_Recv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      //MPI_Recv(&buf, 1, MPI_SHORT, master_id, 901, MPI_COMM_WORLD, &status);
      MPI_Irecv(&buf, 1, MPI_SHORT, master_id, 901, MPI_COMM_WORLD, &request_test);

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

      GLOBAL_COMM_LOG_DEBUG_TD(comm_rank, "COMPUTE NODE: Received message with buf=%hd\n", buf);
      if ( code_901 == 901 ) {
        LOCK_LOAD_BALANCER();
        LOCK_LOCAL_QUEUES();

        unsigned long local_size = lockfree_queue_size_total();
        MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

        MPI_Barrier(MPI_COMM_WORLD);
        UNLOCK_LOAD_BALANCER();
        UNLOCK_LOCAL_QUEUES();
      }
    }
  }
}

void* comm_listener_global_balance() {

   //TODO Send message that queue is empty(800) to master
   //wait for reply if other queues are empty as well(code 801)
   //reply message number is 0 -> queues are not empty if message is 1, queues are empty)
   //if reply is that they are empty, return NULL
   //if queues are not empty, wait for master to rebalance (barrier?)
   //waiting for rebalancing is done with separate thread which wait for message (802)
   //after rebalancing get size of queue or read message with amount of items send
   //if amount = 0; return NULL
   //if not, return item.

   //TODO how to eliminate flooding with rebalance requests to master
   //create variable with timer of last rebalance request
   //allow node ask for rebalancing every X second/milisecond

   //TODO REBALANCING
   //Get all total sizes of nodes
   //continue as for load balance in local mode -> count how many items to send from where to whom
   //then send message(700) to node how many items it has to send to which node
   //that node then must remove items from queues in the way, they will stay balanced 
   //then send items to node
   //wait for reply that sending was OK
   //receiver must add these items to queues in way they stay balanced, or rebalance queues after adding items

   return NULL;

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

  if ( qsize_watcher_t_enable == true ) {
    flag_watcher_graceful_stop = true;
    pthread_join(qsize_watcher_t, NULL);
  }
  flag_graceful_stop = true;  //for remove count nuller
  pthread_join(per_time_statistics_reseter_t, NULL);
  pthread_join(listener_global_size_t, NULL);

  LOG_INFO_TD("STATISTICS: \n\tQsize watcher was called %lu times\n\tLoad balance was called from remove %lu times\
    \n\tGlobal balance was initiated %lu times\n\tLocal balance moved %lu items\n", atomic_load(&load_balancer_call_count_watcher),
    atomic_load(&load_balancer_call_count_remove), atomic_load(&load_balancer_call_count_global), 
    atomic_load(&moved_items_log));

  double sum_rt_time = sum_time(total_rt_lb_time_sec, total_rt_lb_time_nsec);
  double sum_thr_time = sum_time(total_thr_lb_time_sec, total_thr_lb_time_nsec);
  LOG_INFO_TD("\tTotal realtime spent in load balancer: %lf seconds\
    \n\tTotal Thread time spent in load balancer: %lf seconds\n", sum_rt_time, sum_thr_time);
   
  sum_rt_time = sum_time(total_rt_global_size_time_sec, total_rt_global_size_time_nsec);
  sum_thr_time = sum_time(total_rt_global_size_time_sec, total_rt_global_size_time_nsec);
  LOG_INFO_TD("\tTotal realtime spent in global size: %lf seconds\
    \n\tTotal thread time spent in global size: %lf seconds\n", sum_rt_time, sum_thr_time);

  //pthread_cancel(listener_global_size_t);



  //if ( pthread_cancel(listener_global_size_t) != 0 )
  //   printf("Pthread cancel on listener_global_size_t failed\n");
  //pthread_cancel(listener_global_balance_t);
  //MPI_Finalize();   //TODO remove this function from here

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

  //TODO TEST

  while(1) {
    sleep(1);
    if (flag_graceful_stop) {
      break;
    }
    atomic_store(&rm_count_last, atomic_load(&rm_count));
    atomic_store(&ins_count_last, atomic_load(&ins_count));
    for (int i = 0; i < queue_count; i++) {
      LOG_INFO_TD("Queue[%d] size is %ld", (long) i, lockfree_queue_size_by_tid(i));
    }
    LOG_INFO_TD("Total removes per second=%ld in time=%d\n", atomic_load(&rm_count_last), (int) time(NULL));
    LOG_INFO_TD("Total inserts per second=%ld in time=%d\n", atomic_load(&ins_count_last), (int) time(NULL));
    atomic_store(&rm_count, 0);
    atomic_store(&ins_count, 0);
  }
  return NULL;
}

double sum_time(time_t sec, long nsec) {

  double final_time = (double) 0;
  final_time += (double) sec;
  final_time += (double) nsec / (double) 1000000000;
  return final_time;

}