
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

/*#ifndef UTHASH_H
   #define UTHASH_H
   #include "../uthash/src/uthash.h"
#endif*/

#include "../include/uthash.h"

#include <sys/types.h>
#include <sys/stat.h>

/*
 * TODO
 * 1. Scitat procesorovy cas pre kazde vlakno a vypisat statistiku
 * 2. Rozbehnut MPI a vytvorit vlakno, ktore pocuva docasneho mastra na scitanie velkosti globalnej DS
 */

typedef struct tid_hash_struct {
    unsigned long id;      /* key */
    int tid;
    UT_hash_handle hh;     /* makes this structure hashable */
} tid_hash_struct;

/*****
 * Lock-free queue
 ***
 *
 *
 * GLOBAL VARIABLES
 */

/*****
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

pthread_t load_balancing_t;
pthread_mutex_t load_balance_mutex;
pthread_cond_t load_balance_cond;

pthread_t qsize_watcher_t;
pthread_mutex_t qsize_watcher_mutex;   //NOT USED SO FAR

bool load_balancing_t_running_flag;
bool qsize_watcher_t_running_flag;

unsigned long *qsize_history = NULL;
long q_threshold = 1000;

//http://stackoverflow.com/questions/19197836/algorithm-to-evenly-distribute-values-into-containers
//http://stackoverflow.com/questions/15258908/best-strategy-to-distribute-number-into-groups-evenly
//https://en.wikipedia.org/wiki/Partition_problem
long *load_bal_src;
long *load_bal_dest;
long *load_bal_amount;
//TODO free ptrs

FILE *log_file_lb;
FILE *log_file_qw;
FILE *log_file_debug;

/*
 * Statistical variables
 */
unsigned long moved_items_log;
unsigned long load_balancer_call_count_watcher;
unsigned long load_balancer_call_count_remove;
unsigned long load_balancer_call_count_global;
//TODO THIS

/*
 * GLOBAL BALANCING
 */

pthread_t listener_global_size_t;
pthread_t listener_global_balance_t;
pthread_mutex_t load_balance_global_mutex; //To lock thread until global operation as global_size or global_balance is done
pthread_cond_t load_balance_global_cond;

bool global_balancing_enable; //True enables global balancing, False disables global balancing
double last_rebalance_time;   //For elimination of flooding network with global rebalance requests
//TODO find functions to get time with miliseconds

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
   HASH_FIND_INT( tid_insertion_hashes, &pt, ths );
   if (ths == NULL) {

      pthread_mutex_lock(&insertionTidMutex);
      unsigned int c = HASH_COUNT(tid_insertion_hashes);
      
      tid_hash_struct *tid_hash = NULL;
      tid_hash = (tid_hash_struct*) malloc(sizeof (struct tid_hash_struct));

      tid_hash->id = pt;
      tid_hash->tid = c;
      printf("Insertion thread %ld mapped to Q%d\n", pt, c);
      HASH_ADD_INT( tid_insertion_hashes, id, tid_hash );

      HASH_FIND_INT( tid_insertion_hashes, &pt, ths );
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
   HASH_FIND_INT( tid_removal_hashes, &pt, ths );
   if (ths == NULL) {

      pthread_mutex_lock(&removalTidMutex);
      unsigned int c = HASH_COUNT(tid_removal_hashes);

      tid_hash_struct *tid_hash = NULL;
      tid_hash = (tid_hash_struct*) malloc(sizeof (struct tid_hash_struct));

      tid_hash->id = pt;
      tid_hash->tid = c;
      printf("Removal thread %ld mapped to Q%d\n", pt, c);
      HASH_ADD_INT( tid_removal_hashes, id, tid_hash );

      HASH_FIND_INT( tid_removal_hashes, &pt, ths );
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

   MPI_Finalize();

   //pthread_cancel(callback_threads[t]);
   pthread_exit(&callback_threads[t]);
   
}


bool lockfree_queue_is_empty_local(void *queue_id) {
   
   int *q_id = queue_id; //tid == queue index
   
   struct ds_lockfree_queue *q = queues[ *q_id ];

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

void lockfree_queue_init_callback (void* (*callback)(void *args), void* arguments, unsigned int queue_count_arg, unsigned int thread_count_arg) {

   //TODO documentation must contain struct used for arguments in thread callback
   
   /*
    * DEBUG START FOR GDB
    * Uncomment while loop for enabling waiting for gdb attach
    * Then in gdb do: "set var debug_wait = 1" to unlock while loop and start program
    */
   
   int debug_wait = 0;
   char hostname[256];
   pid_t pid = getpid();
   int pid_int = (int) pid;
   gethostname(hostname, sizeof(hostname));
   printf("PID %d on %s ready for attach\n", pid, hostname);
   fflush(stdout);
   //while (debug_wait == 0)
   //   sleep(1);
   //}


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
      //TODO write MPI_Error_string on error
      printf("ERROR: Error in MPI thread init\n");
      exit(-1);
   }

   MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
   MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
   rc = MPI_Get_processor_name(processor_name, &processor_name_length);
   if (rc != MPI_SUCCESS) {
      printf("ERROR: Node %d: Error in getting processor name\n", comm_rank);
      exit(-1);
   }
   printf("Hello, number of tasks is '%d', my comm_rank is '%d', my processor is '%s'\
      and required thread level is %d\n", comm_size, comm_rank, processor_name, required);

   rc = MPI_Query_thread(&claimed);
   if (rc != MPI_SUCCESS) {
      printf("ERROR: Node %d: Error query thread\n", comm_rank);
      exit(-1);
   }
   printf("Node %d: Query thread level '%d', level of threads provided '%d'\n", comm_rank, claimed, provided);

   /*
    * Init debug files
    */
   
   struct stat st = {0};

   if (stat("/tmp/distributed_queue", &st) == -1) {
      mkdir("/tmp/distributed_queue", 0777);
   }

   char pid_str[8];
   sprintf(pid_str, "%d", pid_int);

   char filename_log_lb[50] = "/tmp/distributed_queue/log_debug_lb";
   strcat(filename_log_lb, pid_str);
   log_file_lb = fopen(filename_log_lb, "wb");
   if (log_file_lb == NULL)
      printf("ERROR: Failed to open debug file '%s'\n", filename_log_lb);
   LOAD_BALANCE_LOG_DEBUG_TD("Load balancer log file opened\n");

   char filename_log_qw[50] = "/tmp/distributed_queue/log_debug_qw";
   strcat(filename_log_qw, pid_str);
   log_file_qw = fopen(filename_log_qw, "wb");
   if (log_file_qw == NULL)
      printf("ERROR: Failed to open debug file '%s'\n", filename_log_qw);
   QSIZE_WATCHER_LOG_DEBUG_TD("Qsize Watcher log file opened\n");

   char filename_log_debug[50] = "/tmp/distributed_queue/log_debug";
   strcat(filename_log_debug, pid_str);
   log_file_debug = fopen(filename_log_debug, "wb");
   if (log_file_debug == NULL)
      printf("ERROR: Failed to open debug file '%s'\n", filename_log_debug);
   LOG_DEBUG_TD((unsigned long) 0, "Debug log file opened\n");

   /*
    * Variables initialization
    */

   moved_items_log = 0;
   load_balancer_call_count_watcher;
   load_balancer_call_count_remove;
   load_balancer_call_count_global;

   global_balancing_enable = false; //enables or disables global balancing
   last_rebalance_time = 0;
   
   /*
    * ID of master node on start of program is 0 and is saved in master_id variable
    */
   master_id = 0;

   /*
    * get_nprocs counts hyperthreads as separate CPUs --> 2 core CPU with HT has 4 cores
    */
   
   if (queue_count_arg == 0) 
      queue_count = get_nprocs();
   else 
      queue_count = queue_count_arg;
  
   LOG_DEBUG_TD((unsigned long) 0, "Number of cpus by get_nprocs is : %d\nCreating %d queues\n", get_nprocs(), queue_count);
   
   queues = (struct ds_lockfree_queue**) malloc ( queue_count * sizeof(struct ds_lockfree_queue) );
   for (i = 0; i < queue_count; i++) {
      queues[i] = (struct ds_lockfree_queue*) malloc ( sizeof(struct ds_lockfree_queue) );
   }
   
   /*
    * Queue initialization
    */
   for (i = 0; i < queue_count; i++) {
      queues[i]->head = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      queues[i]->head->next = NULL;
      queues[i]->tail = queues[i]->head;
      queues[i]->divider = queues[i]->head;
      atomic_init( &(queues[i]->a_qsize), 0 );
   }   
   LOG_DEBUG_TD((unsigned long) 0, "Queues initialized\n");
   
   /*
    * Setup mutexes
    */

   int *oldstate = NULL;
   int *oldtype = NULL;
   pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, oldstate);
   pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, oldtype);

   pthread_attr_init(&attr);
   pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   pthread_mutexattr_init(&mutex_attr);

   /*
    * For creating hash table with thread id mapping
    */
   pthread_mutex_init(&insertionTidMutex, &mutex_attr);
   pthread_mutex_init(&removalTidMutex, &mutex_attr);

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
    * Initialize listener threads
    */

   /*
    * Global balance listener
    */
   if (global_balancing_enable) {
      rc = pthread_create(&listener_global_balance_t, &attr, comm_listener_global_balance, NULL);
      if (rc) {
         printf("ERROR: (init) return code from pthread_create() on global balance listener is %d\n", rc);
         exit(-1);
      }
   }
   
   /*
    * Global size listener
    */
   //TODO uncomment after function is done
   /*rc = pthread_create(&listener_global_size_t, &attr, comm_listener_global_size, NULL);
   if (rc) {
      printf("ERROR: (init) return code from pthread_create() on global size listener is %d\n", rc);
      exit(-1);
   }*/

   /*
    * Initialize threads to callback function
    */   
   thread_count = thread_count_arg;
   if ( (thread_count_arg != ONE_TO_ONE) && (thread_count_arg != TWO_TO_ONE) ) {
      printf("ERROR: Thread count argument is invalid\n");
      exit(-1);
   }
   if (thread_count_arg == ONE_TO_ONE)
      thread_count = queue_count;
   if (thread_count_arg == TWO_TO_ONE)
      thread_count = 2 * queue_count;
   
   LOG_DEBUG_TD((unsigned long) 0, "Thread count is %d\n", thread_count);
   thread_to_queue_ratio = thread_count / queue_count;

   callback_threads = (pthread_t*) malloc (thread_count * sizeof(pthread_t));
   tids = (long**) malloc (thread_count * sizeof(long));

   /*
    * Settings for queue argument structure and thread mapping to queues
    */
   struct q_args **q_args_t;
   q_args_t = (struct q_args**) malloc (thread_count * sizeof(struct q_args));
   
   for (int i = 0; i < thread_count; i++) {

      tids[i] = (long*) malloc ( sizeof(long));
      *tids[i] = i;
      q_args_t[i] = (struct q_args*) malloc (sizeof(struct q_args));
      q_args_t[i]->args = arguments;
      q_args_t[i]->tid = tids[i];
      q_args_t[i]->q_count = queue_count;
      q_args_t[i]->t_count = thread_count;
      
      rc = pthread_create(&callback_threads[i], &attr, callback, q_args_t[i]);
      if (rc) {
         printf("ERROR: (init callback threads) return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
      
      LOG_DEBUG_TD((unsigned long) 0, "Created thread with ID %ld\n", callback_threads[i]);
   }
   
   LOG_DEBUG_TD((unsigned long) 0, "%d Threads to callbacks initialized\n", thread_count);
   
   /*
    * Initialize load balancer
    */

   rc = pthread_create(&qsize_watcher_t, &attr, lockfree_queue_qsize_watcher, NULL);
   if (rc) {
      printf("ERROR: (init qsize_watcher) return code from pthread_create() is %d\n", rc);
      exit(-1);
   }
   else {
         LOG_DEBUG_TD((unsigned long) 0, "QSize watcher thread initialized\n");
         qsize_watcher_t_running_flag = true;
   }

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

   int tid = getInsertionTid();
   struct ds_lockfree_queue *q = queues[ tid ]; //volatile?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   if (item == NULL) {
      printf("ERROR: Malloc failed in lockfree_queue_insert_item\n");
      //LOG_DEBUG_TD((unsigned long) tid, "ERROR: Malloc failed\n");
   }
   struct lockfree_queue_item *tmp;
   item->val = val;
   item->next = NULL;   //TODO ?
   pthread_mutex_lock(&add_mutexes[tid]);
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      //free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[tid]);
}

void lockfree_queue_insert_item_by_tid (void *t, void* val) {

   unsigned long *tid = t;
   struct ds_lockfree_queue *q = queues[ *tid ]; //volatile?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));

   if (item == NULL) {
      printf("ERROR: Malloc failed\n");
      //LOG_DEBUG_TD(*tid, "ERROR: Malloc failed\n");
   }

   struct lockfree_queue_item *tmp;
   item->val = val;
   item->next = NULL;   //TODO ?
   pthread_mutex_lock(&add_mutexes[*tid]);
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
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

   unsigned long *tid = t;
   struct ds_lockfree_queue *q = queues[ *tid ]; //volatile?

   struct lockfree_queue_item *item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   
   if (item == NULL) {
      printf("ERROR: Malloc failed\n");
      //LOG_DEBUG_TD(*tid, "Malloc failed\n");
   }
   struct lockfree_queue_item *tmp;
   item->val = val;
   item->next = NULL;   //TODO ?
   
   //set next and swap pointer to tail
   q->tail->next = item;
   
   //q-tail setting is critical section
   q->tail = q->tail->next;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), 1);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      //free(tmp->val);
      free(tmp);
   }
   
}


void lockfree_queue_insert_Nitems_by_tid (void** values, int item_count) {

   int tid = getInsertionTid();
   volatile struct ds_lockfree_queue *q = queues[ tid ]; //volatile?

   struct lockfree_queue_item *item;
   struct lockfree_queue_item *item_tmp;
   struct lockfree_queue_item *item_first_new;
   struct lockfree_queue_item *tmp;

   item = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
   
   if (item == NULL) {
      printf("ERROR: Malloc failed\n");
      //LOG_DEBUG_TD((unsigned long) tid, "Malloc failed\n");
   }

   //item->val = *values[0]; //TODO FIX -- TRY COMPILE
   item_first_new = item;
   for (int i = 1; i < item_count; i++) {
      item_tmp = (struct lockfree_queue_item*) malloc (sizeof(struct lockfree_queue_item));
      if (item_tmp == NULL)
         LOG_DEBUG_TD((unsigned long) tid, "Malloc failed\n");
      item_tmp->val = values[i];
      item->next = item_tmp;
      item = item->next;
   }
   
   pthread_mutex_lock(&add_mutexes[tid]);
   
   //set next
   q->tail->next = item_first_new;   
   
   //swap pointer to tail
   //q-tail setting is critical section
   q->tail = item;   //use cmp_and_swp?
   
   //increment Q size
   atomic_fetch_add( &(q->a_qsize), item_count);
   
   //cleanup
   while ( q->head != q->divider ) {
      tmp = q->head;
      q->head = q->head->next;
      //free(tmp->val);
      free(tmp);
   }
   
   pthread_mutex_unlock(&add_mutexes[tid]);
   
}


void* lockfree_queue_load_balancer(void* arg) {
   
   //TODO len premiestnit smernik na polozku, od ktorej je pocet potrebnych premiestnenych poloziek do druheho radu

   pthread_self();
   LOAD_BALANCE_LOG_DEBUG_TD("Load balance thread started successfuly\n");
   //printf("QSIZE WATCHER: Load balance thread started successfuly\n");

   pthread_mutex_lock(&load_balance_mutex);

   LOCK_LOCAL_QUEUES();
   
   /*
    * relocate data
    * count estimated size
    * count who should give whom what amount of items
    * TODO strategy pattern
    * TODO dynamic threshold setting -> to qsize watcher
    */
   
   //Do not use function lockfree_queue_size_total_consistent(), because mutexes are already locked. 
   unsigned int total = lockfree_queue_size_total();  
   unsigned int estimated_size = total / queue_count;
   
   int *indexes = (int*) malloc (2 * sizeof(int)); //indexes for largest[0] and smallest[1] queue
   unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
   if ( indexes == NULL ) {
      LOG_DEBUG_TD((unsigned long) 0, "ERROR: Malloc failed\n");
   }
   if ( q_sizes == NULL ) {

   }
   unsigned long items_to_send;

   //TODO update condition, relocate data (queue_count - 1) loops?
   for (int i = 0 ; i < queue_count; i++) {
      LOAD_BALANCE_LOG_DEBUG_TD("Load balance round %d\n", i);
      //printf("Load balance round %d\n", i);
      for (int j = 0; j < queue_count; j++) {
         q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
         LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
         //printf("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
      }
      
      indexes = find_max_min_element_index(q_sizes, queue_count);

      if ( (q_sizes[indexes[0]] - (abs(q_sizes[indexes[1]] - estimated_size))) >= estimated_size )
         items_to_send = abs(q_sizes[indexes[1]] - estimated_size);
      else
         items_to_send = q_sizes[indexes[0]] - estimated_size;

      //printf("Max: Q%d with %lu --- Min: Q%d with %lu  ---  Sending: %lu items\n", indexes[0], q_sizes[indexes[0]], 
      //  indexes[1], q_sizes[indexes[1]], items_to_send);
      LOAD_BALANCE_LOG_DEBUG_TD("Max: Q%d with %lu --- Min: Q%d with %lu  ---  Sending: %lu items\n", indexes[0], q_sizes[indexes[0]], 
         indexes[1], q_sizes[indexes[1]], items_to_send);

      /*
       * remove N items from queue queues[indexes[0]] (queue with more items)
       * add N items to queue queues[indexes[1]]]
       */
      lockfree_queue_move_items(indexes[0], indexes[1], items_to_send);

      //printf("LB: Sizes after load balance round %d\n", i);
      LOAD_BALANCE_LOG_DEBUG_TD("LB: Sizes after load balance round %d\n", i);
      
      if ( arg != NULL ) {
         unsigned long* qsize_history = arg;
         for (int j = 0; j < queue_count; j++) {
            q_sizes[j] = lockfree_queue_size_by_tid(tids[j]);
            LOAD_BALANCE_LOG_DEBUG_TD("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
            //printf("Queue %ld size is %lu\n", *tids[j], q_sizes[j]);
            qsize_history[j] = q_sizes[j];
         }
      }
   }
   
   free(indexes);

   //unlock Qs
   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_unlock(&add_mutexes[*tids[i]]);
      pthread_mutex_unlock(&rm_mutexes[*tids[i]]);
   }*/
   UNLOCK_LOCAL_QUEUES();

   pthread_cond_broadcast(&load_balance_cond);
   load_balancing_t_running_flag = false;
   pthread_mutex_unlock(&load_balance_mutex);
   LOAD_BALANCE_LOG_DEBUG_TD("Load balancing thread returning\n");
   //printf("Load balancing thread returning\n");
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
   //unsigned long q_size_dest = atomic_load( &(q_dest->a_qsize) );

   struct lockfree_queue_item *tmp_div;
   struct lockfree_queue_item *tmp_div_next;
   tmp_div_next = q_src->divider->next;
   tmp_div = q_src->divider;

   //printf("Count=%ld, Q_SRC_SIZE=%ld, Q_DST_SIZE=%ld\n", count, q_size_src, q_size_dest);

   if ( count > q_size_src ) {
      printf("ERROR: Cannot move more items(%ld) than queue size(%ld)\n", count, q_size_src);
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

   atomic_fetch_sub( &(q_src->a_qsize), count);
   atomic_fetch_add( &(q_dest->a_qsize), count);
   moved_items_log += count;
   LOAD_BALANCE_LOG_DEBUG_TD("Moved items count = %ld\n", moved_items_log);

}

void* lockfree_queue_qsize_watcher() {
   
   //TODO complete function, test
   
   unsigned long threshold = 20000;
   unsigned long *q_sizes = (unsigned long*) malloc (queue_count * sizeof(unsigned long));
   unsigned long total_qsize;
   unsigned long *qsize_history = NULL;
   
   bool balance_flag;

   QSIZE_WATCHER_LOG_DEBUG_TD("Watching queues\n");
   //printf("Watching queues\n");
   
   while(1) {

      //pthread_mutex_lock(&qsize_watcher_mutex);
      //TODO pause variable for watcher to not waste processor time
      //Set variables to null values each loop
      total_qsize = 0;
      balance_flag = false;
      
      //Sleep(microseconds) for a while for less overhead
      usleep(50000);

      bool lbft = false;

      if (load_balancing_t_running_flag) {
         lbft = true;
      }
      pthread_cond_wait(&load_balance_cond, &load_balance_mutex); //TODO May cause deadlock
      pthread_mutex_unlock(&load_balance_mutex);
      /*while(load_balancing_t_running_flag) {
         ;
      }*/
      if (lbft) {
         //printf("Load balancing ended in REMOVE, continuing in work\n");
         QSIZE_WATCHER_LOG_DEBUG_TD("Load balancing ended in REMOVE, continuing in work\n");
      }

      //Get qsizes (non-consistent)
      //total_qsize = lockfree_queue_size_total();
      for (int i = 0; i < queue_count; i++) {
         q_sizes[i] = lockfree_queue_size_by_tid(tids[i]);
         total_qsize += q_sizes[i];
      }
      //printf("QSIZE WATCHER: total qsize = %ld\n", total_qsize);
      QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: total qsize = %ld\n", total_qsize);
      
      if ( qsize_history == NULL ) {
         /*
          * If we got no history of qsizes, make one and go to next iteration of watching
          */
         qsize_history = (unsigned long*) malloc ( queue_count * sizeof (unsigned long));
         for (int i = 0; i < queue_count; i++) {
            qsize_history[i] = q_sizes[i];
         }
         continue;
      }
      else {



         if (total_qsize == 0) {
            continue;
         }
         /*
          * Look if at least one of queue sizes fell under threshold after last check
          */
         for (int i = 0; i < queue_count; i++) {
            if ( (q_sizes[i] < threshold) && (qsize_history[i] >= threshold) ) {
               QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Q%ld REBALANCE:\n\
                  Old Q%d size was %ld\n\
                  New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
               //printf("QSIZE WATCHER: Q%ld REBALANCE:\n", *tids[i]);
               //printf("Old Q%d size was %ld\n", i, qsize_history[i]);
               //printf("New Q%d size is %ld\n", i, q_sizes[i]);
               balance_flag = true;
            }
            else {
               QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Q%ld OK:\n\
                  Old Q%d size was %ld\n\
                  New Q%d size is %ld\n", *tids[i], i, qsize_history[i], i, q_sizes[i]);
               //printf("QSIZE WATCHER: Q%ld OK:\n", *tids[i]);
               //printf("Old Q%d size was %ld\n", i, qsize_history[i]);
               //printf("New Q%d size is %ld\n", i, q_sizes[i]);
            }
         }
      }
      
      //Write new qsize history
      for (int i = 0; i < queue_count; i++)
         qsize_history[i] = q_sizes[i];

      if (!balance_flag) {
         //printf("QSIZE WATCHER: No need for rebalancing\n\n");
         continue;
      }
   
      //printf("QSIZE WATCHER: Queues turned below threshold - starting load balancer thread.\n");
      QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Queues turned below threshold\n");
      
      if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {
         load_balancing_t_running_flag = true;
         QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Starting load balancer thread.\n");
         int rc = pthread_create(&load_balancing_t, &attr, lockfree_queue_load_balancer, qsize_history);
         if (rc) {
            printf("ERROR: (in qsize watcher) return code from pthread_create() load_balancing_t is %d\n", rc);
            exit(-1);
         }

         QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Waiting for load balance thread to finish\n");
         //printf("QSIZE WATCHER: Waiting for load balance thread to finish\n");
         /*
          * TODO Recommended to use cond_wait in while loop
          * https://computing.llnl.gov/tutorials/pthreads/#ConditionVariables
          */
         pthread_cond_wait (&load_balance_cond, &load_balance_mutex);
         pthread_mutex_unlock(&load_balance_mutex);

         QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Got signal from load balancer -> returning to work\n\
            QSIZE WATCHER: Qsize history after rebalance is:\n");
         //printf("QSIZE WATCHER: Got signal from load balancer -> returning to work\n");
         //printf("QSIZE WATCHER: Qsize history after rebalance is:\n");
         for (int i = 0; i < queue_count; i++) {
            QSIZE_WATCHER_LOG_DEBUG_TD("Q%d-%ld items\n", i, qsize_history[i]);
            //printf("Q%d-%ld items\n", i, qsize_history[i]);
         }
      }
      else {
         //printf("Load Balancer already running in remove\n");
         QSIZE_WATCHER_LOG_DEBUG_TD("Load Balancer already running in remove\n");
         continue;
      }
   }
   qsize_watcher_t_running_flag = false;
   
}


void* lockfree_queue_remove_all_items () {
   //TODO implementation
   
   return NULL;
   
}

void* lockfree_queue_remove_item (int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */

   void* val = NULL;
   int tid = getRemovalTid();
   
   //struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   struct ds_lockfree_queue *q = queues[ tid ]; //modulo ok?
   

   pthread_mutex_lock(&rm_mutexes[tid]);
   
   //TODO timeout spin will try if q->divider is != q-> tail in while, but needs to be timed to nano or microseconds
   //if (timeout > 0)
   //   usleep(timeout);

   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
   }
   else {
      pthread_mutex_unlock(&rm_mutexes[tid]);
      while(1) {
         /*
          * pthread_mutex_trylock returns 0 if lock is acquired
          */
         
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

         if ( pthread_mutex_trylock(&load_balance_mutex) == 0 ) {
            
            load_balancing_t_running_flag = true;
            int rc = pthread_create(&load_balancing_t, &attr, lockfree_queue_load_balancer, NULL);
            if (rc) {
               printf("ERROR: (remove item) return code from pthread_create() of load_balancing_t is %d\n", rc);
               exit(-1);
            }
            else {
               //QSIZE_WATCHER_LOG_DEBUG_TD("QSIZE WATCHER: Waiting for load balance thread to finish\n");
               //printf("REMOVE T%d: Waiting for load balance thread to finish\n", tid);
               /*
                * TODO Recommended to use cond_wait in while loop
                * https://computing.llnl.gov/tutorials/pthreads/#ConditionVariables
                */

               //TODO rm mutex for this thread must be unlocked
               //may cause harm in other parts of code... 
               pthread_mutex_unlock(&rm_mutexes[tid]);
               pthread_cond_wait(&load_balance_cond, &load_balance_mutex);
               pthread_mutex_unlock(&load_balance_mutex);
            }
         }
         else {
            pthread_mutex_unlock(&rm_mutexes[tid]);
            if (load_balancing_t_running_flag == true) {
               continue;
            }
         }
      }
   }
   
   pthread_mutex_unlock(&rm_mutexes[tid]);
   return val;
   
}

void* lockfree_queue_remove_item_by_tid (void* t, int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */

   void* val = NULL;
   long* tid = t;

   if ( lockfree_queue_is_empty_local (tid) ) {
      //printf("Queue %ld is empty\n", *t);
      return val;
   }
   
   //struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   struct ds_lockfree_queue *q = queues[ *tid ]; //modulo ok?
   

   pthread_mutex_lock(&rm_mutexes[*tid]);
   
   //TODO timeout spin
   //if (timeout > 0)
   //   usleep(timeout);
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
   }
   else {
      //TODO check other queues and relocate data to queues to be same sized
      //Or set special variable which signalize which Q was empty and wanted to get item to 1
   }
   
   pthread_mutex_unlock(&rm_mutexes[*tid]);
   return val;
   
}

void* lockfree_queue_remove_item_by_tid_no_lock (void* t, int timeout) {

   /*
    * tid should be tid of inserting thread - 1
    * timeout is in microseconds
    */

   void* val = NULL;
   long* tid = t;

   if ( lockfree_queue_is_empty_local(tid) ) {
      //printf("Queue %ld is empty\n", *t);
      return val;
   }
   
   //struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   struct ds_lockfree_queue *q = queues[ *tid ]; //modulo ok?
   
   //TODO timeout spin
   //if (timeout > 0)
   //   usleep(timeout);
   
   if ( q->divider != q->tail ) {   //atomic reads?
      val = q->divider->next->val;
      q->divider = q->divider->next;
      atomic_fetch_sub( &(q->a_qsize), 1);
   }
   else {
      //TODO check other queues and relocate data to queues to be same sized
      //Or set special variable which signalize which Q was empty and wanted to get item to 1
   }
   
   return val;
   
}


void** lockfree_queue_remove_Nitems_by_tid (unsigned long N, int timeout) {
   
   /*
    * N is amount of items to be taken from Q
    */
   
   int tid = getRemovalTid();
   void **val_arr = malloc(N * sizeof(void*));
   if (val_arr == NULL)
      LOG_DEBUG_TD((unsigned long) tid, "Malloc failed\n");
   
   //volatile struct ds_lockfree_queue *q = queues[*t % queue_count]; //modulo ok?
   volatile struct ds_lockfree_queue *q = queues[ tid ]; //modulo ok?
   
   pthread_mutex_lock(&rm_mutexes[tid]);
   
   unsigned long item_count = atomic_load( &(q->a_qsize) ); 
   if ( atomic_load( &(q->a_qsize) ) < N ) {
      printf("Not enough items in queue %d. There are %ld but was requested %ld.\n", tid, item_count, N);
      pthread_mutex_unlock(&rm_mutexes[tid]);
      return NULL;
   }
   
   unsigned long i = 0;
   for (i = 0; i < N; i++) {
      if ( q->divider != q->tail ) {   //atomic reads?
         val_arr[i] = q->divider->next->val;
         q->divider = q->divider->next;
      }
      else {
         break;
      }
   }

   if (i != N-1) {
      printf("Function did not return requested numbers from queue %d. number of returned values is %ld.\n", tid, i);
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

   unsigned long global_size = 0;
   short code_900 = 900;
   MPI_Send(&code_900, 1, MPI_SHORT, master_id, 900, MPI_COMM_WORLD);

   MPI_Status status;
   MPI_Recv(&global_size, 1, MPI_UNSIGNED_LONG, master_id, 903, MPI_COMM_WORLD, &status);
   printf("NODE %d: Global structure size is %ld\n", comm_rank, global_size);
   return global_size;
   
}

//void* control_message_listener() {
   //TODO can be used as listener to every message and deciding what to do after that message is received
//}
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
         //MPI_Irecv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
         //MPI_Recv(code, 1, MPI_SHORT, comm_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
         MPI_Recv(&buf, 1, MPI_SHORT, comm_rank, 900, MPI_COMM_WORLD, &status);
         //status.MPI_SOURCE
         //status.MPI_TAG
         int source_node = status.MPI_SOURCE;
         printf("Master got message with buf=%hd from node %d\n", buf, source_node);
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

         //TODO lock load balancer + watcher thread
         unsigned long master_struct_size = lockfree_queue_size_total();

         unsigned long *local_struct_sizes;
         local_struct_sizes = (unsigned long*) malloc(comm_size * sizeof(unsigned long));
         cnt = 0;
         for (int i = 0; i < comm_size; i++) {
            if (i != comm_rank) {
               MPI_Irecv(&local_struct_sizes[i], 1, MPI_UNSIGNED_LONG, i, 902, MPI_COMM_WORLD, &requests_902[cnt]);
               cnt++;
            }
            else {
               local_struct_sizes[i] = master_struct_size;
            }
         }

         MPI_Waitall(comm_size - 1, requests_902, statuses);

         unsigned long global_struct_size_total = 0;
         for (int i = 0; i < comm_size; i++) {
            printf("MASTER: Node %d has %ld items\n", i, local_struct_sizes[i]);
            global_struct_size_total += local_struct_sizes[i];
         }
         printf("MASTER: Total sum is %ld\n", global_struct_size_total);

         MPI_Send(&global_struct_size_total, 1, MPI_UNSIGNED_LONG, source_node, 903, MPI_COMM_WORLD);
         MPI_Barrier(MPI_COMM_WORLD);
         UNLOCK_LOCAL_QUEUES();
         free(requests_901); free(requests_902); free(statuses); free(local_struct_sizes);
      }
      else { 
         //MPI_Irecv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
         //MPI_Recv(code, 1, MPI_SHORT, master_id, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
         MPI_Recv(&buf, 1, MPI_SHORT, master_id, 901, MPI_COMM_WORLD, &status);
         printf("COMPUTE NODE %d: Received message with buf=%hd\n", comm_rank, buf);
         if ( code_901 == 901 ) {
            LOCK_LOAD_BALANCER();
            LOCK_LOCAL_QUEUES();
            //TODO lock load balancer + watcher thread
            //TODO UNLOCK LOAD BALANCER?

            unsigned long local_size = lockfree_queue_size_total();
            MPI_Send(&local_size, 1, MPI_UNSIGNED_LONG, master_id, 902, MPI_COMM_WORLD);

            MPI_Barrier(MPI_COMM_WORLD);
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
   
   //Lock mutexes for adding and removing threads
   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_lock(&add_mutexes[i]);
      pthread_mutex_lock(&rm_mutexes[i]);
   }*/
   LOCK_LOCAL_QUEUES();

   for (int i = 0; i < queue_count; i++) {
      size += atomic_load( &(queues[i]->a_qsize) );
   }

   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_unlock(&add_mutexes[i]);
      pthread_mutex_unlock(&rm_mutexes[i]);
   }*/
   UNLOCK_LOCAL_QUEUES();

   return size;
   
}

unsigned long* lockfree_queue_size_total_consistent_allarr () {

   unsigned long* sizes = (unsigned long*) malloc(queue_count * sizeof(unsigned long));
   
   //Lock mutexes for adding and removing threads
   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_lock(&add_mutexes[i]);
      pthread_mutex_lock(&rm_mutexes[i]);
   }*/
   LOCK_LOCAL_QUEUES();

   for (int i = 0; i < queue_count; i++) {
      sizes[i] = atomic_load( &(queues[i]->a_qsize) );
   }

   /*for (int i = 0; i < queue_count; i++) {
      pthread_mutex_unlock(&add_mutexes[i]);
      pthread_mutex_unlock(&rm_mutexes[i]);
   }*/
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

   pthread_cancel(qsize_watcher_t);
   //if ( pthread_cancel(listener_global_size_t) != 0 )
   //   printf("Pthread cancel on listener_global_size_t failed\n");
   //pthread_cancel(listener_global_balance_t);
   //MPI_Finalize();   //TODO remove this function from here

}

int* find_max_min_element_index(unsigned long *array, unsigned long len) {
   
   /*
    * TODO can find max min, save to array ordered from max to min with values and then after relocation
    * only update values which were relocated and update positions if they changed
    */

    /*
     * index_max_min[0] is index of element with max number
     * index_max_min[1] is index of element with min number
     */

     //apply strategy pattern to sorting
   
   
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
