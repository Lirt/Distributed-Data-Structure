#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "ds_debug.h"
#endif

#include <math.h>
#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

int generateRandomNumber(int rangeMin, int rangeMax) {
	
	int r = rand() % rangeMax + rangeMin;
	return r;

}

void init() {

	srand(time(NULL));
   log_file = fopen(strcat(path,filename), "a");

}

void testLogging() {
   /*
    * tests logging functions
    */
   char* path = "/home/ovasko/Dropbox/Skola/DP/program/log/";
   char* filename = "rng_logging_test.log";
   time_t t;
   
   LOG_DEBUG(ctime(&t), '0', "Just a test DEBUG message %d.", 1);
   LOG_CRIT(ctime(&t), 0, "Critical Error!");
   LOG_ERR(ctime(&t), 1, "Error in ...");
   LOG_INFO("Logging messages works!");
   
}

int main(int argc, char** argv) {

   /*
    * VARIABLES
    */
	int min = 1;            //minimum value for generated random numbers
	int max = 9;            //maximum value for generated random numbers
	int timeout = 500000;	//timeout between sending in microseconds

   /*
    * INIT
    */
   init();
   testLogging();

	/*
    * MPI CONFIG
    */
	int size, rank;
	int rc;
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int processor_name_length;
	//int required = MPI_THREAD_MULTIPLE;	#CAN CAUSE PROBLEMS
	int required = MPI_THREAD_SERIALIZED;	
	int provided;

   /*
    * MPI THREAD SUPPORT INIT
    */
	rc = MPI_Init_thread(&argc, &argv, required, &provided);
	if (rc != MPI_SUCCESS) {
		printf("Error in thread init\n");
		return -1;
	}

	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	rc = MPI_Get_processor_name(processor_name, &processor_name_length);
	if (rc != MPI_SUCCESS) {
		printf("Node %d: Error in getting processor name\n", rank);
		return -1;
	}
	printf("Hello, this is MASTER number of tasks is '%d', my rank is '%d' and my processor is '%s'\n", size, rank, processor_name);
	
	int claimed;
	rc = MPI_Query_thread(&claimed);
	if (rc != MPI_SUCCESS) {
		printf("Node %d: Error query thread\n", rank);
		return -1;
	}
	printf("Node %d: Query thread level '%d', level of threads provided '%d'\n", rank, claimed, provided);


	/*
	 * PROGRAM
	 */
	time_t t = time(NULL);
   
   /*
    * Wait for enter button
    * Then send clients message to stop doing job
    */

   int tag = 0;
   int i;
   char c = 0;
   
   printf("Press enter to stop clients\n");
   while(1) {
      c = getchar();
      if (c == 13)
         break;
   }
   
   while(1) {
      for(i = 0; i < size; i++) {
			if ( i != rank ) {
				printf("Node %d: Sending stop message to node '%d'\n", rank, rn, i);
				MPI_Send(0, 1, MPI_INT, i, tag, MPI_COMM_WORLD);
				//usleep(timeout);
			}
		}
	}


	/*
    * End clients
    */
	for(i = 0; i < size; i++) {
		if ( i != rank ) {
			printf("Node %d: Sending shutdown message to node '%d'\n", rank, i);
			MPI_Send(-1, 1, MPI_INT, i, tag, MPI_COMM_WORLD);
			//usleep(timeout);
		}
	}

   /*
    * FINALIZE
    */
	rc = MPI_Finalize();
	if (rc != MPI_SUCCESS) {
		printf("Node %d: Error in finalize\n", rank);
	}


	return 0;
}


