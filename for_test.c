
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef TIME_H
   #define TIME_H
   #include <time.h>
#endif

struct timespec *time_diff(struct timespec *start, struct timespec *end) {
  
  struct timespec *result = (struct timespec*) malloc (sizeof (struct timespec));;
  if ( ( end->tv_nsec - start->tv_nsec ) < 0 ) {
    result->tv_sec = end->tv_sec - start->tv_sec - 1;
    result->tv_nsec = 1000000000 + end->tv_nsec - start->tv_nsec;
  } 
  else {
    result->tv_sec = end->tv_sec - start->tv_sec;
    result->tv_nsec = end->tv_nsec - start->tv_nsec;
  }
  return result;

}

int *generateRandomNumber(int rangeMin, int rangeMax) {
	
   int *r;
   r = (int*) malloc (sizeof(int));
   if (r == NULL) {
      fprintf(stderr, "Malloc failed\n");
      return NULL;
   }
	*r = rand() % rangeMax + rangeMin;
	return r;

}

double sum_time(time_t sec, long nsec) {

  double final_time = (double) 0;
  final_time += (double) sec;
  final_time += (double) nsec / (double) 1000000000;
  return final_time;

}

int main(int argc, char** argv) {

  unsigned int lowRange, highRange;
  lowRange = 1;
  highRange = 50;

  struct timespec *tp_rt_start = (struct timespec*) malloc (sizeof (struct timespec));
  struct timespec *tp_rt_end = (struct timespec*) malloc (sizeof (struct timespec));

	for (int i = 0; i < 50; i++) {
    int *rn = generateRandomNumber(lowRange, highRange);
    clock_gettime(CLOCK_REALTIME, tp_rt_start);

    unsigned long p = (unsigned long) pow(2, i);
		printf("num: %ld\n", p);
    for (int j = 0; j < p; j++) {
    }

    clock_gettime(CLOCK_REALTIME, tp_rt_end);
	  double sum_rt_time = sum_time(time_diff(tp_rt_start, tp_rt_end)->tv_sec, time_diff(tp_rt_start, tp_rt_end)->tv_nsec);
	  printf("Realtime program time for %d^2: %lf seconds\n", sum_rt_time);
    printf("Realtime program time for %d^2 = %lu.%lu\n", i, time_diff(tp_rt_start, tp_rt_end)->tv_sec, time_diff(tp_rt_start, tp_rt_end)->tv_nsec );
    
  }

  return 0;

}	
            

