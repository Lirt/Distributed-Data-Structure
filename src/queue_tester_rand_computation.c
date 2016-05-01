
#ifndef _DEFAULT_SOURCE
   #define _DEFAULT_SOURCE
#endif

#include <math.h>
#include <stdio.h>
#include <string.h>

#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_QUEUE_OFFICIAL_H
   #define DS_QUEUE_OFFICIAL_H
   #include "../include/distributed_queue_api.h"
#endif

#ifndef STDATOMIC_H
   #define STDATOMIC_H
   #include <stdatomic.h>
#endif 

#ifndef STDLIB_H
   #define STDLIB_H
  #include <stdlib.h>
#endif

#ifndef PTHREAD_H
   #define PTHREAD_H
  #include <pthread.h>
#endif

#ifndef UNISTD_H
   #define UNISTD_H
   #include <unistd.h>
#endif

#ifndef STDBOOL_H
   #define STDBOOL_H
   #include <stdbool.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>

#ifndef TIME_H
   #define TIME_H
   #include <time.h>
#endif

#include <argp.h>

#ifndef MPI_H
   #define MPI_H
   #include "/usr/include/mpich-x86_64/mpi.h"
#endif

#include <gsl/gsl_rng.h>

/*
 * GLOBAL VARIABLES
 */

const char *argp_program_version = "DQS version 0.1";
const char *argp_program_bug_address = "ondrej.vaskoo@gmail.com"; 
static char doc[] = "DQS by Ondrej Vasko";

atomic_ulong finished;
atomic_ulong total_removes;
atomic_int np;
atomic_int nc;

unsigned int program_duration = 10;
unsigned int item_amount = 0;
unsigned int queue_count_arg = 0;
unsigned long max_qsize = 0;
bool load_balance_thread_arg = false;
unsigned int threshold_type_arg = 0;
unsigned int local_balance_type_arg = 0;
double local_lb_threshold_percent = 0.0;
double global_lb_threshold_percent = 0.0;
unsigned long local_lb_threshold_static = 0;
unsigned long global_lb_threshold_static = 0;
unsigned int *q_ins_ratios = NULL;
unsigned int *q_rm_ratios = NULL;
unsigned long computation_load = 0;
bool hook;
pthread_barrier_t barrier;

unsigned long *n_inserted_arr;
unsigned long *n_removed_arr;
unsigned long *sums;

int producers = 0;
int consumers = 0;

/***********
 * FUNCTIONS
 */

unsigned long sum_array(unsigned long* arr) {

  unsigned long res = 0;
  for(int i = 0; i < queue_count_arg; i++) {
    res += arr[i];
  }

  return res;

}

int time_diff(struct timespec *start, struct timespec *end, struct timespec *result) {

  if ( ( end->tv_nsec - start->tv_nsec ) < 0 ) {
    result->tv_sec = end->tv_sec - start->tv_sec - 1;
    result->tv_nsec = 1000000000 + end->tv_nsec - start->tv_nsec;
  } 
  else {
    result->tv_sec = end->tv_sec - start->tv_sec;
    result->tv_nsec = end->tv_nsec - start->tv_nsec;
  }

  return 0;

}

int *generateRandomNumber(int rangeMin, int rangeMax, gsl_rng *r) {

  int *rn;
  rn = (int*) malloc (sizeof(int));
  if (rn == NULL) {
    LOG_ERR_T((long) -1, "Malloc failed\n");
    return NULL;
  }

  //*r = rand() % rangeMax + rangeMin;
  //*r = 15;
  *rn = gsl_rng_uniform_int(r, 20);
  return rn;

}

void *work(void *arg_struct) {

  struct q_args *args = arg_struct;
  long *tid = args->tid;
  long *qid = (long*) malloc(sizeof(long));
  *qid = *tid/2;
  //int q_count = args->q_count;
  //int t_count = args->t_count;

  const gsl_rng_type *T;
  gsl_rng *r;
  T = gsl_rng_taus;
  r = gsl_rng_alloc (T);
  gsl_rng_set(r, *tid);

  struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_start_insert = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end_insert = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_thr = (struct timespec*) malloc (sizeof (struct timespec));
  clock_gettime(CLOCK_REALTIME, tp_rt_start);
  LOG_DEBUG_TD(*tid, "START PROGRAM: \n\tRealtime - %lu.%lu seconds\n", tp_rt_start->tv_sec, tp_rt_start->tv_nsec);
  LOG_DEBUG_TD(*tid, "\tThread %ld has ID %lu, insertion ratio %d and remove ratio %d\n", 
    *tid, pthread_self(), q_ins_ratios[*tid / 2], q_rm_ratios[*tid / 2] );

  struct stat st = {0};
  if (stat("/tmp/distributed_queue", &st) == -1) {
    mkdir("/tmp/distributed_queue", 0777);
  }

  pid_t pid = getpid();
  int pid_int = (int) pid;
  char pid_str[8];
  sprintf(pid_str, "%d", pid_int);

  char filename_ins[60] = "/tmp/distributed_queue/work_";
  char filename_rm[60] = "/tmp/distributed_queue/work_";
  char tid_str[4];
  char ins[5] = "ins_";
  char rm[4] = "rm_";

  strcat(filename_ins, ins);
  strcat(filename_rm, rm);

  sprintf(tid_str, "%ld_", *tid);
  strcat(filename_ins, tid_str);
  strcat(filename_rm, tid_str);
  strcat(filename_ins, pid_str);
  strcat(filename_rm, pid_str);


  FILE *work_file_ins = fopen(filename_ins, "wb");
  if ( work_file_ins == NULL ) {
    LOG_ERR_T(*tid, "ERROR: error in opening file %s\n", filename_ins);
    exit(-1);
  }

  FILE *work_file_rm = fopen(filename_rm, "wb");
  if ( work_file_rm == NULL ) {
    LOG_ERR_T(*tid, "ERROR: error in opening file %s\n", filename_rm);
    exit(-1);
  }

 /*
  * Set ranges for insertion of random numbers according to q_ins_ratios set in command line arguments
  */
  unsigned int lowRange;
  lowRange = 1;
  //unsigned int highRange;
  //highRange = computation_load;


  clock_gettime(CLOCK_REALTIME, tp_rt_start_insert);
  if ( *tid % 2 == 0 ) {
   /*
    * PRODUCER
    */
    atomic_fetch_add(&np, 1);
    if ( atomic_load(&np) > producers ) {
      atomic_fetch_add( &finished, 1);

      free(qid);
      free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);
      fclose(work_file_ins);
      fclose(work_file_rm);

      return NULL;
    }

    int *rn;
    int x = 0;

    if ( item_amount > 0 ) {
      while(1) {
        //Start producing items
        //rn = generateRandomNumber(lowRange, q_ins_ratios[*tid / 2], r);
        rn = generateRandomNumber(lowRange, 20, r);
        if (rn == NULL)
          continue;

        NUMBER_ADD_RM_FPRINTF( work_file_ins, filename_ins, "%d\n", *rn );
        dq_insert_item_by_tid(qid, rn);
        n_inserted_arr[*tid / 2]++;
        free(rn);

        x++;
        if (x == 10000) {
          x = 0;
          if ( sum_array(n_inserted_arr) >= item_amount ) {
            clock_gettime(CLOCK_REALTIME, tp_rt_end_insert);
            struct timespec *result = (struct timespec*) malloc (sizeof (struct timespec));
            time_diff(tp_rt_start_insert, tp_rt_end_insert, result);
            pthread_barrier_wait(&barrier);
            LOG_INFO_TD("Finished inserting, endTime = %ld sec and %ld nsec\n",  result->tv_sec, result->tv_nsec);
            LOG_INFO_TD("\tT[%ld]: Inserted %lu items\n", *tid, n_inserted_arr[*tid/2]);
            LOG_INFO_TD("\tTotal Inserted %lu items\n", sum_array(n_inserted_arr));

            atomic_fetch_add( &finished, 1);
            free(result);
            free(qid);
            free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);
            fclose(work_file_ins);
            fclose(work_file_rm);
            return NULL;
          }
        }
      }
    }
    if ( program_duration > 0 ) {
      while(1) {
        //Start producing items
        //rn = generateRandomNumber(lowRange, q_ins_ratios[*tid / 2], r);
        rn = generateRandomNumber(lowRange, 20, r);
        if (rn == NULL)
          continue;

        NUMBER_ADD_RM_FPRINTF( work_file_ins, filename_ins, "%d\n", *rn );
        dq_insert_item_by_tid(qid, rn);
        n_inserted_arr[*tid / 2]++;
        free(rn);

        x++;
        if (x == 10000) {
          x = 0;
          clock_gettime(CLOCK_REALTIME, tp_rt_end_insert);
          struct timespec *result = (struct timespec*) malloc (sizeof (struct timespec));
          time_diff(tp_rt_start_insert, tp_rt_end_insert, result);
          if ( result->tv_sec >= program_duration ) {
            pthread_barrier_wait(&barrier);
            LOG_INFO_TD("Time is up, endTime = %ld sec and %ld nsec\n", result->tv_sec, result->tv_nsec);
            LOG_INFO_TD("\tT[%ld]: Inserted %lu items\n", *tid, n_inserted_arr[*tid/2]);
            LOG_INFO_TD("\tTotal Inserted %lu items\n", sum_array(n_inserted_arr));

            atomic_fetch_add( &finished, 1);
            free(result);
            free(qid);
            free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);
            fclose(work_file_ins);
            fclose(work_file_rm);
            return NULL;
          }
          else {
            free(result);
          }
        }
      }
    }
  }
  else {
   /*
    * CONSUMER
    */
    atomic_fetch_add(&nc, 1);
    if ( atomic_load(&nc) > consumers ) {

      free(qid);
      free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);
      fclose(work_file_ins);
      fclose(work_file_rm);

      return NULL;
    }

    if ( q_rm_ratios[*tid / 2] == 0 ) {

      free(qid);
      free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);
      fclose(work_file_ins);
      fclose(work_file_rm);

      return NULL;
    }

    int ret;
    while(1) {
      int *retval = (int*) malloc(sizeof(int));
      ret = dq_remove_item_by_tid(qid, retval);

      if (ret == -1) {
        free(retval);
        unsigned long size = dq_local_size();
        if (size == 0) {
          unsigned long global_size_val = dq_global_size(false);
          if ( global_size_val == 0 ) {
            unsigned long fin = atomic_load( &finished );
            if ( fin != queue_count_arg ) {
              continue;
            }
            else {
              clock_gettime(CLOCK_REALTIME, tp_rt_end);
              clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);

              struct timespec *result = (struct timespec*) malloc (sizeof (struct timespec));
              time_diff(tp_rt_start, tp_rt_end, result);
              LOG_DEBUG_TD(*tid, "Total sum of removed items for T[%ld] is %lu\n", *tid, sum_array(sums));
              LOG_DEBUG_TD(*tid, "Total removed items %lu\n", sum_array(n_removed_arr));
              LOG_DEBUG_TD(*tid, "Final realtime program time = %lu.%lu\n", 
                result->tv_sec, result->tv_nsec );

              LOG_INFO_TD("\tT[%ld]: Removed items %lu\n", *tid, n_removed_arr[*tid/2]);
              LOG_INFO_TD("\tT[%ld]: Sum of items %lu\n", *tid, sums[*tid/2]);
              LOG_INFO_TD("Total sum of removed items is %lu\n", sum_array(sums));
              LOG_INFO_TD("Total removed items %lu\n", sum_array(n_removed_arr));
              LOG_INFO_TD("Final realtime program time = %lu.%lu\n", 
                result->tv_sec, result->tv_nsec );

              free(result);
              free(qid);
              free(tp_rt_start); free(tp_rt_start_insert); free(tp_rt_end); free(tp_rt_end_insert); free(tp_thr);

              fclose(work_file_ins);
              fclose(work_file_rm);

              return NULL;
            }
          } //GLOBAL SIZE
        } //LOCAL SIZE
      } //RETVAL NULL
      else {
        NUMBER_ADD_RM_FPRINTF(work_file_rm, filename_rm, "%d\n", *retval);
        n_removed_arr[*tid/2]++;
        for (int i = 0; i < *retval; i++) {
          *retval = log2(*retval);
        }
        //sums[*tid/2] += *retval;
        free(retval);
      }
    }
  }

  return NULL;

}

static int parse_opt (int key, char *arg, struct argp_state *state) {

  int *arg_lb_count = state->input; 
  switch (key) {

    case 'd': {
      char *endptr;
      program_duration = strtoul(arg, &endptr, 10);
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "Cannot convert string to number\n");
        fprintf (stderr, "OPT[ERROR]: 'd' Cannot convert string to number\n");
        exit(-1);
      }
      printf("OPT: Program duration set to %s\n", arg);
      break;
    }

    case 'q': {
      char *endptr;
      queue_count_arg = strtoul(arg, &endptr, 10);
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "Cannot convert string to number\n");
        fprintf (stderr, "OPT[ERROR]: 'q' Cannot convert string to number\n");
        exit(-1);
      }
      if (queue_count_arg > 8) {
        free(q_ins_ratios);
        free(q_rm_ratios);
        q_ins_ratios = (unsigned int*) malloc(queue_count_arg * sizeof(unsigned int));
        q_rm_ratios = (unsigned int*) malloc(queue_count_arg * sizeof(unsigned int));
        for (int i = 0; i < queue_count_arg; i++) {
          q_ins_ratios[i] = 1;
          q_rm_ratios[i] = 1;
        }
      }

      n_inserted_arr = (unsigned long*) malloc(queue_count_arg * sizeof(unsigned long));
      n_removed_arr = (unsigned long*) malloc(queue_count_arg * sizeof(unsigned long));
      free(sums);
      sums = (unsigned long*) malloc(queue_count_arg * sizeof(unsigned long));
      for (int i = 0; i < queue_count_arg; i++) {
        n_inserted_arr[i] = 0;
        n_removed_arr[i] = 0;
        sums[i] = 0;
      }

      printf("OPT: Queue count set to %s\n", arg);
      break;
    }

    case 'l': {
      if ( strcmp(arg, "true") == 0 ) {
        printf("OPT: load balancing thread will be enabled\n");
        load_balance_thread_arg = true;
      }
      else if ( strcmp(arg, "false") == 0 ) {
        printf("OPT: load balancing thread will NOT be enabled\n");
        load_balance_thread_arg = false;
      }
      else {
        //LOG_ERR_T( (long) -1, "Option to load balance argument must be ""true"" or ""false""\n");
        fprintf(stderr, "OPT[ERROR]: Option to load balance argument must be ""true"" or ""false""\n");
        exit(-1);
      }
      break;
    }

    case 'h': {
      printf("OPT: Hook enabled. Enter 'set var debug_wait=1' to start program.\n");
      hook = true;
      break;
    }

    case 'p': {
      char *endptr;
      producers = strtoul(arg, &endptr, 10);
      if ( endptr == arg ) {
        //LOG_ERR_T( (long) -1, "Cannot parse number\n");
        fprintf(stderr, "OPT[ERROR]: \n");
        exit(-1);
      }
      pthread_barrier_init(&barrier, NULL, producers);
      printf("OPT: Number of producer threads set to %d.\n", producers);
      break;
    }

    case 'c': {
      char *endptr;
      consumers = strtoul(arg, &endptr, 10);
      if ( endptr == arg ) {
        //LOG_ERR_T( (long) -1, "Cannot parse number\n");
        fprintf(stderr, "OPT[ERROR]: Number of consumer threads must be number\n");
        exit(-1);
      }
      printf("OPT: Number of consumer threads set to %d.\n", consumers);
      break;
    }

    case 's': {
      char *endptr;
      max_qsize = strtoul(arg, &endptr, 10);
      if ( endptr == arg ) {
        //LOG_ERR_T( (long) -1, "Cannot parse number\n");
        fprintf(stderr, "OPT[ERROR]: Maximum qsize must be number\n");
        exit(-1);
      }
      printf("OPT: Maximum queue size was set to %lu.\n", max_qsize);
      break;
    }

    case 'a': {
      char *endptr;
      item_amount = strtoul(arg, &endptr, 10);
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "Cannot convert string to number\n");
        fprintf (stderr, "OPT[ERROR]: 'a' Cannot convert string to number\n");
        exit(-1);
      }
      printf("OPT: Amount of items to insert set to %s\n", arg);
      break;
    }

    case 150: {
      local_lb_threshold_percent = atof(arg);
      if ( local_lb_threshold_percent == 0.0 ) {
        //LOG_ERR_T( (long) -1, "Cannot parse floating point number\n");
        fprintf(stderr, "OPT[ERROR]: LOCAL THRESHOLD PERCENT: Cannot parse floating point number\n");
        exit(-1);
      }
      printf("OPT: Local LB threshold set to %lf%%\n", local_lb_threshold_percent);
      (*arg_lb_count)++;
      break;
    }

    case 151: {
      global_lb_threshold_percent = atof(arg);
      if ( global_lb_threshold_percent == 0.0 ) {
        //LOG_ERR_T( (long) -1, "Cannot parse floating point number\n");
        fprintf(stderr, "OPT[ERROR]: GLOBAL THRESHOLD PERCENT: Cannot parse floating point number\n");
        exit(-1);
      }
      printf("OPT: Global LB threshold set to %lf%%\n", global_lb_threshold_percent);
      (*arg_lb_count)++;
      break;
    }

    case 152: {
      char *endptr;
      local_lb_threshold_static = strtoul(arg, &endptr, 10);
      if ( endptr == arg ) {
        //LOG_ERR_T( (long) -1, "Cannot parse number\n");
        fprintf(stderr, "OPT[ERROR]: LOCAL THRESHOLD STATIC: Cannot parse number\n");
        exit(-1);
      }
      printf("OPT: Local LB threshold set to %lu items\n", local_lb_threshold_static);
      (*arg_lb_count)++;
      break;
    }

    case 153: {
      char *endptr;
      global_lb_threshold_static = strtoul(arg, &endptr, 10);
      if ( endptr == arg ) {
        //LOG_ERR_T( (long) -1, "Cannot parse number\n");
        fprintf(stderr, "OPT[ERROR]: GLOBAL THRESHOLD STATIC: Cannot parse number\n");
        exit(-1);
      }
      printf("OPT: Global LB threshold set to %lu items\n", global_lb_threshold_static);
      (*arg_lb_count)++;
      break;
    }

    case 154: {
      if ( strcmp(arg, "static") == 0 ) {
        printf("OPT: Threshold type set to static\n");
        threshold_type_arg = 1;
      }
      else if ( strcmp(arg, "percent") == 0 ) {
        printf("OPT: Threshold type set to percent\n");
        threshold_type_arg = 2;
      }
      else if ( strcmp(arg, "dynamic") == 0 ) {
        printf("OPT: Threshold type set to dynamic\n");
        threshold_type_arg = 3;
      }
      else {
        //LOG_ERR_T( (long) -1, "Option to load balance type argument must be ""static"", ""percent"" or ""dynamic""\n");
        fprintf(stderr, "OPT[ERROR]: Option to Threshold type argument must be ""static"", ""percent"" or ""dynamic""\n");
        exit(-1);
      }
      break;
    }

    case 155: {
      if ( strcmp(arg, "all") == 0 ) {
        printf("OPT: Load balancing type set to balance all queues equal\n");
        local_balance_type_arg = 1;
      }
      else if ( strcmp(arg, "pair") == 0 ) {
        printf("OPT: Load balancing type set to balance between pairs of queue\n");
        local_balance_type_arg = 2;
      }
      else {
        //LOG_ERR_T( (long) -1, "Option to load balance type argument must be ""static"", ""percent"" or ""dynamic""\n");
        fprintf(stderr, "OPT[ERROR]: Option to load balance type argument must be ""all"" or ""pair""\n");
        exit(-1);
      }
      break;
    }

    case 220: {
      char *endptr;
      q_ins_ratios[0] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qri1' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q1 insertion ratio set to %u\n", q_ins_ratios[0]);
      break;
    }

    case 221: {
      char *endptr;
      q_ins_ratios[1] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qri2' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q2 insertion ratio set to %u\n", q_ins_ratios[1]);
      break;
    }

    case 222: {
      char *endptr;
      q_ins_ratios[2] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qri3' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q3 insertion ratio set to %u\n", q_ins_ratios[2]);
      break;
    }

    case 223: {
      char *endptr;
      q_ins_ratios[3] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qri4' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q4 insertion ratio set to %u\n", q_ins_ratios[3]);
      break;
    }

    case 225: {
      char *endptr;
      q_rm_ratios[0] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qrm1' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q1 removal ratio set to %u\n", q_rm_ratios[0]);
      break;
    }

    case 226: {
      char *endptr;
      q_rm_ratios[1] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qrm2' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q2 removal ratio set to %u\n", q_rm_ratios[1]);
      break;
    }

    case 227: {
      char *endptr;
      q_rm_ratios[2] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qrm3' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q3 removal ratio set to %u\n", q_rm_ratios[2]);
      break;
    }

    case 228: {
      char *endptr;
      q_rm_ratios[3] = strtoul(arg, &endptr, 10);
      errno = 0;
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "cannot convert string to number\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: 'qrm4' Cannot convert string to number");
        exit(-1);
      }
      printf("OPT: Q4 removal ratio set to %u\n", q_rm_ratios[3]);
      break;
    }

    case 224: {
      char *endptr;
      computation_load = strtoul(arg, &endptr, 10);
      if (endptr == arg) {
        //LOG_ERR_T( (long) -1, "Cannot convert string to number\n");
        fprintf (stderr, "OPT[ERROR]: 'computation_load' Cannot convert string to number\n");
        exit(-1);
      }

      free(q_ins_ratios);
      free(q_rm_ratios);
      q_ins_ratios = (unsigned int*) malloc(queue_count_arg * sizeof(unsigned int));
      q_rm_ratios = (unsigned int*) malloc(queue_count_arg * sizeof(unsigned int));
      for (int i = 0; i < queue_count_arg; i++) {
        q_ins_ratios[i] = computation_load;
        q_rm_ratios[i] = computation_load;
      }

      printf("OPT: Computation load set to %s\n", arg);
      break;
    }

    case ARGP_KEY_END: {
      printf("\n");
      if ( *arg_lb_count < 3 && *arg_lb_count != 0 ) {
        argp_usage (state);
        //LOG_ERR_T( (long) -1, "Too few arguments for LB\n");
        argp_failure (state, 1, 0, "OPT[ERROR]: Too few arguments for LB");
        exit(-1);
      }
      break;
    }

    default: {
      //printf("OPT: Unknown argument '%s'\n", arg);
      return ARGP_ERR_UNKNOWN;
    }
  }

  //If returns non zero value, argp will stop and raise that error value
  return 0;

} 

int main(int argc, char** argv) {

  setbuf(stdout, NULL);

  q_ins_ratios = (unsigned int*) malloc(8 * sizeof(unsigned int));
  q_rm_ratios = (unsigned int*) malloc(8 * sizeof(unsigned int));
  for (int i = 0; i < 8; i++) {
    q_ins_ratios[i] = 1;
    q_rm_ratios[i] = 1;
  }

  sums = (unsigned long*) malloc(8 * sizeof(unsigned long));
  for (int i = 0; i < queue_count_arg; i++) {
    sums[i] = 0;
  }

  //TODO add len_s parameter for dynamic threshold

  struct argp_option options[] = { 
    { "duration",                 'd', "<NUM> in seconds",0, "Sets duration of inserting items in seconds", 1},
    { "amount",                   'a', "<NUM> in items",0, "Sets amount of items to insert", 1},
    { "queue-count",              'q', "<NUM>",           0, "Sets amount of queues on one node", 1},
    { "lb-thread",                'l', "true/false",      0, "Enables or disables dedicated load balancing thread (default false).", 2},
    { "hook",                     'h', NULL,      0, "Waits for gdb hook on pid. In gdb enter 'set var debug_wait=1' \
                                                        and after that 'continue' to start program.", 2},
    { "producers",                'p', "<NUM>",           0, "Amount of producer threads", 4},
    { "consumers",                'c', "<NUM>",           0, "Amount of consumer threads", 4},
    { "max_qsize",                's', "<NUM>",           0, "Maximum size of queue", 4},
    { "local-threshold-percent",  150, "<DECIMAL>",       0, "Sets threshold for local load balancing thread in percentage", 2},
    { "global-threshold-percent", 151, "<DECIMAL>",       0, "Sets threshold for global load balancing thread in percentage", 2},
    { "local-threshold-static",   152, "<NUM>",           0, "Sets static threshold for local load balancing thread in number of items", 2},
    { "global-threshold-static",  153, "<NUM>",           0, "Sets static threshold for global load balancing thread in number of items", 2},
    { "local-threshold-type",     154, "static/percent/dynamic", 0, "Choses local threshold type", 2},
    { "local-balance-type",       155, "all/pair", 0, "Choses local balancing type. Type all balances all queues to same size. \
    Type Pair balances only queue with lowest size with highest size queue.", 2},
    { "q1-ins-ratio",                 220, "<NUM>",           0, "Number of items inserted into queue 1 on a insertion", 3},
    { "q2-ins-ratio",                 221, "<NUM>",           0, "Number of items inserted into queue 2 on a insertion", 3},
    { "q3-ins-ratio",                 222, "<NUM>",           0, "Number of items inserted into queue 3 on a insertion", 3},
    { "q4-ins-ratio",                 223, "<NUM>",           0, "Number of items inserted into queue 4 on a insertion", 3},
    { "q1-rm-ratio",                 225, "<NUM>",           0, "Number of items inserted into queue 1 on a insertion", 3},
    { "q2-rm-ratio",                 226, "<NUM>",           0, "Number of items inserted into queue 2 on a insertion", 3},
    { "q3-rm-ratio",                 227, "<NUM>",           0, "Number of items inserted into queue 3 on a insertion", 3},
    { "q4-rm-ratio",                 228, "<NUM>",           0, "Number of items inserted into queue 4 on a insertion", 3},
    { "computation-load",         224, "<NUM>",           0, "Highest number of random number from 0 to N (0-24). Simulates computation.\
    Table of load times is in root folder of project.", 3},
    { 0 }  
  };

  struct argp argp = { options, parse_opt, 0, doc };
  int arg_lb_count = 4;
  argp_parse(&argp, argc, argv, 0, 0, &arg_lb_count);

  atomic_init(&finished, 0);
  atomic_init(&np, 0);
  atomic_init(&nc, 0);

  pthread_t *cb_threads = dq_init(work, NULL, sizeof(int), queue_count_arg, TWO_TO_ONE, load_balance_thread_arg, 
    local_lb_threshold_percent, global_lb_threshold_percent, local_lb_threshold_static, 
    global_lb_threshold_static,  threshold_type_arg, local_balance_type_arg, hook, max_qsize);

  for (int i = 0; i < (queue_count_arg * TWO_TO_ONE); i++ ) {
    pthread_join(cb_threads[i], NULL);
  }
  dq_destroy();
  pthread_barrier_destroy(&barrier);

  free(q_ins_ratios); free(q_rm_ratios);
  free(n_inserted_arr); free(n_removed_arr); free(sums);

  printf("Main finished\n");
  return 0;

}
