#include <math.h>
#include <stdio.h>
#include <time.h>
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

unsigned int program_duration = 5;
unsigned int queue_count_arg = NULL;
bool load_balance_arg = false;
double local_lb_threshold = 20.00;
double global_lb_threshold = 20.00;
unsigned int *q_ratios;

int *generateRandomNumber(int rangeMin, int rangeMax) {
	
   int *r;
   r = (int*) malloc (sizeof(int));
   if (r == NULL) {
      printf("ERROR: Malloc failed in function generateRandomNumber\n");
      exit(-1);
   }
	*r = rand() % rangeMax + rangeMin;
	return r;

}

void *work(void *arg_struct) {
   
   //TODO automatic counting of removes and inserts and evaluation

   //TODO load balancovat k vlaknu ktore je prazdne. vyber od koho nahodne, parameter kolko presunut %.
   //porovnanie load balance metod dlzkou programu / poctom 
   //kolko krat bolo zavolane load balance a od koho
   //load balancing lockuje len rady, kde sa presuva
   //statistiky, cim menej load balanceu
   //porovnat ako funguje s vlaknom qsizewatcher a bez
   //rozne dlzky program duration
   //vyhodnotenie
   //spisat algoritmy
   //TODO all printfs to log_debug
   //meranie casu

   struct timespec *tp_rt;
   struct timespec *tp_proc;
   clock_gettime(CLOCK_REALTIME, tp_rt);
   clock_gettime(CLOCK_PROCESS_CPUTIME_ID, tp_proc);
   printf("Realtime: %lu.%lu seconds\nProcess: %lu.%lu seconds\n", tp_rt->tv_sec, tp_rt->tv_nsec, tp_proc->tv_sec, tp_proc->tv_nsec);

   struct q_args *args = arg_struct;
   long *tid = args->tid;
   //int q_count = args->q_count;
   //int t_count = args->t_count;
   unsigned long pthread_tid = pthread_self();
   printf("Hello from work - T%ld with TID %ld\n", *tid, pthread_tid);
   
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
      printf("ERROR: error in opening file %s\n", filename_ins);
      exit(-1);
   }
   
   strcat(filename_rm, rm);
   FILE *work_file_rm = fopen(filename_rm, "wb");
   if ( work_file_rm == NULL ) {
      printf("ERROR: error in opening file %s\n", filename_rm);
      exit(-1);
   }
   
   int timeout = 0;
   
   //Ranges for random numbers
   unsigned int lowRange, highRange;
   if (q_ratios[*tid] == 0) {
      lowRange = q_ratios[*tid];
      highRange = q_ratios[*tid];
   }
   else {
      lowRange = 1;
      highRange = q_ratios[*tid];
   }
   
   if ( fprintf(work_file_ins, "Hello from insertion work thread - T%ld, my range is %ld-%ld\n", *tid, lowRange, highRange) < 0 )
      printf("ERROR: cannot write to file %s\n", filename_ins);
   if ( fprintf(work_file_rm, "Hello from removing work thread - T%ld, my range is %ld-%ld\n", *tid, lowRange, highRange) < 0 )
      printf("ERROR: cannot write to file %s\n", filename_ins);

   unsigned long sum = 0;
   unsigned long n_inserted = 0;
   unsigned long n_removed = 0;

   int *rn;
   int *retval;
   
   int endTime;
   int startTime = (int) time(NULL);
   printf("Start time for thread %ld is %d\n", *tid, startTime);

   if ( *tid % 2 == 0 ) {
      //PRODUCER
      while(1) {
         rn = generateRandomNumber(lowRange, highRange);

         lockfree_queue_insert_item (rn);
         if ( fprintf(work_file_ins, "%d\n", *rn) < 0 )
            printf("ERROR: fprintf failed\n");
         n_inserted++;

         //end after N seconds
         endTime = (int) time(NULL) - startTime;
         if (endTime >= program_duration) {
            printf("Time is up, endTime = %d\n", endTime);
            printf("Thread %ld Inserted %lu items\n", *tid, n_inserted);

            atomic_fetch_add( &finished, 1);
            fclose(work_file_ins);
            fclose(work_file_rm);
            pthread_exit(NULL);
            //break;
         }
      }

   }
   else {
      //CONSUMER
      printf("Starting consumer (T%ld) at time %d\n", *tid, (int) time(NULL));

      while(1) {
         //retval = (int*) malloc (sizeof(int));
         retval = lockfree_queue_remove_item(timeout);

         if (retval == NULL) {

            unsigned long size = lockfree_queue_size_total_consistent();
            if (size == 0) {
               unsigned long fin = atomic_load( &finished );
               if ( fin != 2 )
                  continue;
               else {
                  printf("All insertion threads finished. Queues are empty - quitting consumer threads\n");
                  break;
               }
            }
         }
         else {
            if ( fprintf(work_file_rm, "%d\n", *retval) < 0 ) 
               printf("ERROR: fprintf failed\n");
            n_removed++;
            sum += *retval;
            free(retval);
         }
      }
   }

   printf("Thread %ld inserted %lu numbers\n", *tid, n_inserted);
   printf("Thread %ld removed %lu numbers\n", *tid, n_removed);
   printf("Sum of thread %ld items is %lu\n", *tid, sum);
   
   fclose(work_file_ins);
   fclose(work_file_rm);

   clock_gettime(CLOCK_REALTIME, tp_rt);
   clock_gettime(CLOCK_PROCESS_CPUTIME_ID, tp_proc);
   printf("Realtime: %lu.%lu seconds\nProcess: %lu.%lu seconds\n", tp_rt->tv_sec, tp_rt->tv_nsec, tp_proc->tv_sec, tp_proc->tv_nsec);

   //lockfree_queue_destroy();
   //TODO Pthread Cleanup and destroy method
   lockfree_queue_stop_watcher();
   pthread_exit(NULL);
   
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
         printf("Program duration set to %s\n", arg);
         program_duration = strtoul(arg);
         break;
      }
      case 'q': {
         printf("Queue count set to %s\n", arg);
         queue_count_arg = strtoul(arg);
         break;
      }
      case 'l': {
         if strcmp(arg, "true") {
            load_balance_arg = true;
         }
         if strcmp(arg, "false") {
            load_balance_arg = false;
         }
         else {
            printf("Option to load balance argument must be ""true"" or ""false""\n");
            exit(-1);
         }
         break;
      }
      case '111': {
         local_lb_threshold = atof(arg);
         if ( local_lb_threshold == 0.0 ) {
            printf("LOCAL THRESHOLD: Cannot parse floating point number\n");
            exit(-1);
         }
         *arg_lb_count++;
         break;
      }
      case '112': {
         global_lb_threshold = atof(arg);
         if ( global_lb_threshold == 0.0 ) {
            printf("GLOBAL THRESHOLD: Cannot parse floating point number\n");
            exit(-1);
         }
         *arg_lb_count++;
         break;
      }
      case '120': {
         q_ratios[0] = strtoul(arg);
         if (q_ratios[0] == 0) {
            argp_failure (state, 1, 0, "FAILURE: cannot convert string to number");
            exit(-1);
         }
         break;
      }
     case '121': {
         q_ratios[1] = strtoul(arg);
         if (q_ratios[1] == 0) {
            argp_failure (state, 1, 0, "FAILURE: cannot convert string to number");
            exit(-1);
         }
         break;
      }
      case '122': {
         q_ratios[2] = strtoul(arg);
         if (q_ratios[2] == 0) {
            argp_failure (state, 1, 0, "FAILURE: cannot convert string to number");
            exit(-1);
         }
         break;
      }
      case '123': {
         q_ratios[3] = strtoul(arg);
         if (q_ratios[3] == 0) {
            argp_failure (state, 1, 0, "FAILURE: cannot convert string to number");
            exit(-1);
         }
         break;
      }
      case ARGP_KEY_END: {
         printf("\n");
         if ( *arg_lb_count < 3 && *arg_lb_count != 0 ) {
            argp_usage (state);
            argp_failure (state, 1, 0, "FAILURE: too few arguments");
            exit(-1);
         }
         break;
      }
      default: {
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
   q_ratios = (unsigned int*) malloc(20 * sizeof(unsigned int));
   for (int i = 0; i < 20; i++) {
      q_ratios[i] = 1;
   }

   const char *argp_program_version = "DQS 0.1";
   const char *argp_program_bug_address = "ondrej.vaskoo@gmail.com"; 
   struct argp_option options[] = { 
      //{ "duration", 'd', "<NUM> in seconds", OPTION_ARG_OPTIONAL, "Sets duration of inserting items in seconds"},
      //last arument is group
      { "duration", 'd', "<NUM> in seconds", 0, "Sets duration of inserting items in seconds", 1},
      { "queue-count", 'q', "<NUM>", 0, "Sets amount of queues on one node", 1},
      { "lb-thread", 'l', "true/false", 0, "Enables or disables dedicated load balancing thread", 2},
      { "local-threshold", '111', "<%%>", 0, "Sets threshold for local load balancing thread in percentage", 2},
      { "global-threshold", '112', "<%%>", 0, "Sets threshold for global load balancing thread in percentage", 2},
      { "q1-ratio", '120', "<NUM>", 0, "Number of items inserted into queue 1 on a insertion", 3},
      { "q2-ratio", '121', "<NUM>", 0, "Number of items inserted into queue 2 on a insertion", 3},
      { "q3-ratio", '122', "<NUM>", 0, "Number of items inserted into queue 3 on a insertion", 3},
      { "q4-ratio", '123', "<NUM>", 0, "Number of items inserted into queue 4 on a insertion", 3},
      { 0 }  
   };
   struct argp argp = { options, parse_opt };
   int arg_lb_count = 4;
   argp_parse(&argp, argc, argv, 0, 0, &arg_lb_count);

   exit(-1);

   setbuf(stdout, NULL);

   atomic_init(&finished, 0);
   atomic_init(&total_inserts, 0);
   atomic_init(&total_removes, 0);
   atomic_init(&total_sum, 0);

   lockfree_queue_init_callback(work, NULL, queue_count_arg, TWO_TO_ONE);
   
   pthread_exit(NULL);
	return 0;
}
