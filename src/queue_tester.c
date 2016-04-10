
#ifndef _DEFAULT_SOURCE
   #define _DEFAULT_SOURCE
#endif

#include <math.h>
#include <stdio.h>
#include <string.h>

/*
#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif
*/

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

/*
int clock_gettime(clockid_t clk_id, struct timespec *tp);
struct timespec {
   time_t   tv_sec;        //seconds
   long     tv_nsec;       //nanoseconds
};
Sufficiently recent versions of GNU libc and the Linux kernel support the following clocks:
CLOCK_REALTIME
   System-wide realtime clock. Setting this clock requires appropriate privileges.
CLOCK_MONOTONIC
   Clock that cannot be set and represents monotonic time since some unspecified starting point.
CLOCK_PROCESS_CPUTIME_ID
   High-resolution per-process timer from the CPU.
CLOCK_THREAD_CPUTIME_ID
   Thread-specific CPU-time clock.
*/

atomic_ulong finished;
atomic_ulong total_inserts;
atomic_ulong total_removes;
atomic_ulong total_sum;

unsigned int program_duration = 10;
unsigned int queue_count_arg = 0;
bool load_balance_thread_arg = false;
unsigned int load_balance_local_type_arg = 0;
double local_lb_threshold_percent = 0.0;
double global_lb_threshold_percent = 0.0;
unsigned long local_lb_threshold_static = 0;
unsigned long global_lb_threshold_static = 0;
unsigned int *q_ratios = NULL;

int *generateRandomNumber(int rangeMin, int rangeMax) {
	
   int *r;
   r = (int*) malloc (sizeof(int));
   if (r == NULL) {
      fprintf(stderr, "ERROR: Malloc failed in function generateRandomNumber\n");
      exit(-1);
   }
	*r = rand() % rangeMax + rangeMin;
	return r;

}

void *work(void *arg_struct) {
   
   //TODO automatic counting of removes and inserts and evaluation

   //SCENARE: 
   //rozne dlzky program duration
   //porovnat ako funguje s vlaknom qsizewatcher a bez, porovnanie load balance metod dlzkou programu / poctom add/rm
   //statistiky, cim menej load balanceu
   //meranie casu

   //TODO load balancovat k vlaknu ktore je prazdne. vyber od koho nahodne, parameter kolko presunut %.
   //TODO spisat algoritmy
   //TODO all printfs to log_debug
   //TODO load balancing lockuje len rady, kde sa presuva

   struct q_args *args = arg_struct;
   long *tid = args->tid;
   int q_count = args->q_count;
   //int t_count = args->t_count;
   unsigned long pthread_tid = pthread_self();
   
   struct timespec *tp_rt = (struct timespec*) malloc (sizeof (struct timespec));
   struct timespec *tp_proc = (struct timespec*) malloc (sizeof (struct timespec));
   struct timespec *tp_thr = (struct timespec*) malloc (sizeof (struct timespec));
   clock_gettime(CLOCK_REALTIME, tp_rt);
   clock_gettime(CLOCK_PROCESS_CPUTIME_ID, tp_proc);
   clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);
   printf("START PROGRAM[T%ld]: \n\tRealtime - %lu.%lu seconds\n\tProcess Time - %lu.%lu seconds\n\tThread Time - %lu.%lu seconds\n", *tid,
    tp_rt->tv_sec, tp_rt->tv_nsec, tp_proc->tv_sec, tp_proc->tv_nsec, tp_thr->tv_sec, tp_thr->tv_nsec);
   printf("\tThread %ld has ID %lu\n", *tid, pthread_tid);

   time_t startTimeTimespecSec = tp_rt->tv_sec;
   long startTimeTimespecNSec = tp_rt->tv_nsec;

   struct stat st = {0};
   if (stat("/tmp/distributed_queue", &st) == -1) {
      mkdir("/tmp/distributed_queue", 0777);
   }

   char filename_ins[40] = "/tmp/distributed_queue/work_";
   char filename_rm[40] = "/tmp/distributed_queue/work_";
   char tid_str[4];
   char ins[4] = "ins";
   char rm[3] = "rm";
   
   sprintf(tid_str, "%ld", *tid);
   strcat(filename_ins, tid_str);
   strcat(filename_rm, tid_str);
   
   strcat(filename_ins, ins);
   FILE *work_file_ins = fopen(filename_ins, "wb");
   if ( work_file_ins == NULL ) {
      fprintf(stderr, "ERROR: error in opening file %s\n", filename_ins);
      exit(-1);
   }
   
   strcat(filename_rm, rm);
   FILE *work_file_rm = fopen(filename_rm, "wb");
   if ( work_file_rm == NULL ) {
      fprintf(stderr, "ERROR: error in opening file %s\n", filename_rm);
      exit(-1);
   }
   
   /*
    * Set ranges for insertion of random numbers according to q_ratios set in command line arguments
    */
   unsigned int lowRange, highRange;
   lowRange = 1;
   highRange = q_ratios[*tid / 2];


   if ( fprintf(work_file_ins, "Hello from insertion work thread - T%ld(id=%lu), my insertion range is %u-%u\n", 
      *tid, pthread_tid, lowRange, highRange) < 0 ) {
      fprintf(stderr, "ERROR: cannot write to file %s\n", filename_ins);
   }
   if ( fprintf(work_file_rm, "Hello from removing work thread - T%ld(id=%lu), my insertion range is %u-%u\n", 
      *tid, pthread_tid, lowRange, highRange) < 0 ) {
      fprintf(stderr, "ERROR: cannot write to file %s\n", filename_ins);
   }
   
   int endTime;
   int startTime = (int) time(NULL);

   if ( *tid % 2 == 0 ) {
      /*
       * PRODUCER
       */
      unsigned long n_inserted = 0;
      unsigned long sum = 0;
      int *rn;
      int **rn_array;

      //If thread has 0 insertion ratio
      if ( lowRange == 0 && highRange == 0 ) {
         endTime = (int) time(NULL) - startTime;
         printf("T%ld: Time is up, endTime = %d\n", *tid, endTime);
         printf("Thread %ld Inserted %lu items\n", *tid, n_inserted);

         atomic_fetch_add( &finished, 1);
         fclose(work_file_ins);
         fclose(work_file_rm);
         pthread_exit(NULL);
      }

      //Start producing items
      while(1) {
         rn = generateRandomNumber(lowRange, highRange);

         if ( (*rn) == 1) {
            sum += *rn;
            if ( fprintf( work_file_ins, "%d\n", *rn) < 0 )
               fprintf(stderr, "ERROR: fprintf failed\n");
            n_inserted++;
            atomic_fetch_add( &total_inserts, 1);
            lockfree_queue_insert_item(rn);
         }
         else {
            sum += (*rn) * (*rn);
            rn_array = (int**) malloc( (*rn) * sizeof(int*) );
            if ( rn_array == NULL ) {
               fprintf(stderr, "ERROR: Malloc rn_array failed\n");
            }
            for(int i = 0; i < *rn; i++) {
               rn_array[i] = (int*) malloc(sizeof(int));
               if ( rn_array[i] == NULL ) {
                  fprintf(stderr, "ERROR: Malloc rn_array[%d] failed\n", i);
               }
               *(rn_array[i]) = *rn;
               if ( fprintf( work_file_ins, "%d\n", *(rn_array[i]) ) < 0 )
                  fprintf(stderr, "ERROR: fprintf failed\n");
            }
            n_inserted += *rn;
            atomic_fetch_add( &total_inserts, *rn);
            lockfree_queue_insert_N_items( (void**) rn_array, *rn);
         }

         //end after "program_duration" seconds
         endTime = (int) time(NULL) - startTime;
         if (endTime >= program_duration) {
            printf("T%ld: Time is up, endTime = %d\n", *tid, endTime);
            printf("Thread %ld Inserted %lu items\n", *tid, n_inserted);
            printf("Sum of insertion thread %ld items is %lu\n", *tid, sum);

            atomic_fetch_add( &finished, 1);
            fclose(work_file_ins);
            fclose(work_file_rm);
            pthread_exit(NULL);
         }
      }
   }
   else {
      /*
       * CONSUMER
       */
      int timeout = 0;
      unsigned long sum = 0;
      unsigned long n_removed = 0;
      int *retval;

      while(1) {
         retval = lockfree_queue_remove_item(timeout);

         if (retval == NULL) {
            unsigned long size = lockfree_queue_size_total_consistent();
            if (size == 0) {
               unsigned long fin = atomic_load( &finished );
               if ( fin != queue_count_arg )
                  continue;
               else {
                  //TODO ADD THREAD JOIN TO INSERTION THREAD FROM MY QUEUE
                  printf("All insertion threads finished. Queues are empty - quitting consumer threads\n");
                  printf("Thread %ld removed %lu numbers\n", *tid, n_removed);
                  printf("Sum of removal thread %ld items is %lu\n", *tid, sum);
                  if (*tid == 1) {
                     printf("Total sum is %lu\n", atomic_load(&total_sum));
                     printf("Total inserted items %lu\n", atomic_load(&total_inserts));
                     printf("Total removed items %lu\n", atomic_load(&total_removes));
                  }
   
                  fclose(work_file_ins);
                  fclose(work_file_rm);

                  clock_gettime(CLOCK_REALTIME, tp_rt);
                  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, tp_proc);
                  clock_gettime(CLOCK_THREAD_CPUTIME_ID, tp_thr);
                  printf("END PROGRAM[T%ld]: \n\tRealtime - %lu.%lu seconds\n\tProcess Time - %lu.%lu seconds\n\tThread Time - %lu.%lu seconds\n", *tid,
                   tp_rt->tv_sec, tp_rt->tv_nsec, tp_proc->tv_sec, tp_proc->tv_nsec, tp_thr->tv_sec, tp_thr->tv_nsec);
                  printf("Final program time = '%lu.%lu'\n", (tp_rt->tv_sec - startTimeTimespecSec), (tp_rt->tv_nsec - startTimeTimespecNSec) );

                  //lockfree_queue_destroy();
                  //TODO Pthread Cleanup and destroy method
                  lockfree_queue_stop_watcher();
                  pthread_exit(NULL);
                  break;
               }
            }
         }
         else {
            if ( fprintf(work_file_rm, "%d\n", *retval) < 0 )
               fprintf(stderr, "ERROR: fprintf failed\n");
            n_removed++;
            atomic_fetch_add( &total_removes, 1);
            sum += *retval;
            atomic_fetch_add( &total_sum, *retval);
            free(retval);
         }
      }
   }

}

static int parse_opt (int key, char *arg, struct argp_state *state) {
   /*
    * 1. The option's unique key, which can be the short option 'd'.  Other noncharacter
    * keys exist
    * 2. The value of the option's argument, as a string.
    * 3. A variable that keeps Argp's state as we repeatedly iterate the callback
    * function. 
    */

   int *arg_lb_count = state->input; 
   switch (key) {
      case 'd': {
         printf("OPT: Program duration set to %s\n", arg);
         program_duration = strtoul(arg, NULL, 0);
         if (program_duration == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            fprintf (stderr, "OPT: [FAILURE] cannot convert string to number\n");
            exit(-1);
         }
         break;
      }
      case 'q': {
         printf("OPT: Queue count set to %s\n", arg);
         queue_count_arg = strtoul(arg, NULL, 0);
         if (queue_count_arg == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            fprintf (stderr, "OPT: [FAILURE] cannot convert string to number\n");
            exit(-1);
         }
         free(q_ratios);
         q_ratios = (unsigned int*) malloc(queue_count_arg * sizeof(unsigned int));
         for (int i = 0; i < queue_count_arg; i++) {
            q_ratios[i] = 1;
         }
         break;
      }
      case 'l': {
         if ( strcmp(arg, "true") == 0 ) {
            printf("OPT: load balancing thread will be enabled\n");
            load_balance_thread_arg = true;
         }
         if ( strcmp(arg, "false") == 0 ) {
            printf("OPT: load balancing thread will NOT be enabled\n");
            load_balance_thread_arg = false;
         }
         else {
            fprintf(stderr, "OPT: [ERROR] Option to load balance argument must be ""true"" or ""false""\n");
            exit(-1);
         }
         break;
      }
      case 150: {
         local_lb_threshold_percent = atof(arg);
         if ( local_lb_threshold_percent == 0.0 ) {
            fprintf(stderr, "OPT: [ERROR] LOCAL THRESHOLD PERCENT: Cannot parse floating point number\n");
            exit(-1);
         }
         printf("OPT: Local LB threshold set to %lf%%\n", local_lb_threshold_percent);
         (*arg_lb_count)++;
         break;
      }
      case 151: {
         global_lb_threshold_percent = atof(arg);
         if ( global_lb_threshold_percent == 0.0 ) {
            fprintf(stderr, "OPT: [ERROR] GLOBAL THRESHOLD PERCENT: Cannot parse floating point number\n");
            exit(-1);
         }
         printf("OPT: Global LB threshold set to %lf%%\n", global_lb_threshold_percent);
         (*arg_lb_count)++;
         break;
      }
      case 152: {
         local_lb_threshold_static = strtoul(arg, NULL, 0);
         if ( local_lb_threshold_static == 0 ) {
            fprintf(stderr, "OPT: [ERROR] LOCAL THRESHOLD STATIC: Cannot parse number\n");
            exit(-1);
         }
         printf("OPT: Local LB threshold set to %lu items\n", local_lb_threshold_static);
         (*arg_lb_count)++;
         break;
      }
      case 153: {
         global_lb_threshold_static = strtoul(arg, NULL, 0);
         if ( global_lb_threshold_static == 0 ) {
            fprintf(stderr, "OPT: [ERROR] GLOBAL THRESHOLD STATIC: Cannot parse number\n");
            exit(-1);
         }
         printf("OPT: Global LB threshold set to %lu items\n", global_lb_threshold_static);
         (*arg_lb_count)++;
         break;
      }
      case 154: {
         if ( strcmp(arg, "static") == 0 ) {
            printf("OPT: Load balancing type set to static\n");
            load_balance_local_type_arg = 1;
         }
         else if ( strcmp(arg, "percent") == 0 ) {
            printf("OPT: Load balancing type set to percent\n");
            load_balance_local_type_arg = 2;
         }
         else if ( strcmp(arg, "dynamic") == 0 ) {
            printf("OPT: Load balancing type set to dynamic\n");
            load_balance_local_type_arg = 3;
         }
         else {
            fprintf(stderr, "OPT: [ERROR] Option to load balance type argument must be ""static"", ""percent"" or ""dynamic""\n");
            exit(-1);
         }
         break;
      }
      case 220: {
         q_ratios[0] = (unsigned int) strtoul(arg, NULL, 0);
         if (q_ratios[0] == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            exit(-1);
         }
         printf("OPT: Q0 ratio set to %u\n", q_ratios[0]);
         break;
      }
      case 221: {
         q_ratios[1] = strtoul(arg, NULL, 0);
         if (q_ratios[1] == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            exit(-1);
         }
         printf("OPT: Q1 ratio set to %u\n", q_ratios[1]);
         break;
      }
      case 222: {
         q_ratios[2] = strtoul(arg, NULL, 0);
         if (q_ratios[2] == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            exit(-1);
         }
         printf("OPT: Q2 ratio set to %u\n", q_ratios[2]);
         break;
      }
      case 223: {
         q_ratios[3] = strtoul(arg, NULL, 0);
         if (q_ratios[3] == 0) {
            argp_failure (state, 1, 0, "OPT: [FAILURE] cannot convert string to number");
            exit(-1);
         }
         printf("OPT: Q3 ratio set to %u\n", q_ratios[3]);
         break;
      }
      case ARGP_KEY_END: {
         printf("\n");
         if ( *arg_lb_count < 3 && *arg_lb_count != 0 ) {
            argp_usage (state);
            argp_failure (state, 1, 0, "OPT: [FAILURE] too few arguments for LB");
            exit(-1);
         }
         break;
      }
      default: {
         printf("OPT: Unknown argument '%s'\n", arg);
         return ARGP_ERR_UNKNOWN;
      }
   }

   //If returns non zero value, argp will stop and raise that error value
   return 0;

} 

int main(int argc, char** argv) {
   
   //error_t argp_parse (const struct argp *argp, int argc, char **argv, unsigned flags, int *arg_index, void *input)
   //*input is additional data to callback
   //Flags: ARGP_PARSE_ARGV0, ARGP_NO_ERRS, ARGP_NO_ARGS, ARGP_IN_ORDER, ARGP_NO_HELP, ARGP_NO_EXIT, ARGP_LONG_ONLY, ARGP_SILENT
   /*The OPTION_ALIAS flag  causes the option to inherit all fields from the previous option 
   except for the long option name (the 1st field), and the key (the 2nd field).*/
   //OPTION_HIDDEN hides an option
   /*
   struct argp {
      const struct argp_option *options,
      argp_parser_t parser,
      const char *args_doc,
      const char *doc,
      const struct argp_child *children,
      char *(*help_filter)(int key, const char *text, void *input),
      const char *argp_domain
   }
   */

   setbuf(stdout, NULL);

   q_ratios = (unsigned int*) malloc(2 * sizeof(unsigned int));
   for (int i = 0; i < 2; i++) {
      q_ratios[i] = 1;
   }

   //TODO len_s parameter for dynamic threshold
   const char *argp_program_version = "DQS 0.1";
   const char *argp_program_bug_address = "ondrej.vaskoo@gmail.com"; 
   //TODO Add argument for threshold type select and option for setting (static <NUM>, percent <NUM>, dynamic)
   struct argp_option options[] = { 
      //{ "duration", 'd', "<NUM> in seconds", OPTION_ARG_OPTIONAL, "Sets duration of inserting items in seconds"},
      //last arument is group
      { "duration",                 'd', "<NUM> in seconds",0, "Sets duration of inserting items in seconds", 1},
      { "queue-count",              'q', "<NUM>",           0, "Sets amount of queues on one node", 1},
      { "lb-thread",                'l', "true/false",      0, "Enables or disables dedicated load balancing thread", 2},
      { "local-threshold-percent",  150, "<DECIMAL>",       0, "Sets threshold for local load balancing thread in percentage", 2},
      { "global-threshold-percent", 151, "<DECIMAL>",       0, "Sets threshold for global load balancing thread in percentage", 2},
      { "local-threshold-static",   152, "<NUM>",           0, "Sets static threshold for local load balancing thread in number of items", 2},
      { "global-threshold-static",  153, "<NUM>",           0, "Sets static threshold for global load balancing thread in number of items", 2},
      { "local-threshold-type",     154, "static/percent/dynamic", 0, "Choses local threshold type", 2},
      { "q1-ratio",                 220, "<NUM>",           0, "Number of items inserted into queue 1 on a insertion", 3},
      { "q2-ratio",                 221, "<NUM>",           0, "Number of items inserted into queue 2 on a insertion", 3},
      { "q3-ratio",                 222, "<NUM>",           0, "Number of items inserted into queue 3 on a insertion", 3},
      { "q4-ratio",                 223, "<NUM>",           0, "Number of items inserted into queue 4 on a insertion", 3},
      { 0 }  
   };
   struct argp argp = { options, parse_opt };
   int arg_lb_count = 4;
   argp_parse(&argp, argc, argv, 0, 0, &arg_lb_count);

   atomic_init(&finished, 0);
   atomic_init(&total_inserts, 0);
   atomic_init(&total_removes, 0);
   atomic_init(&total_sum, 0);

   lockfree_queue_init_callback(work, NULL, queue_count_arg, TWO_TO_ONE, load_balance_thread_arg, 
      local_lb_threshold_percent, global_lb_threshold_percent, local_lb_threshold_static, 
      global_lb_threshold_static, load_balance_local_type_arg);

   pthread_exit(NULL);
	return 0;
}
