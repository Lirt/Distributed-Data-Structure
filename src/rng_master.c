#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <unistd.h>

int generateRandomNumber(int rangeMin, int rangeMax) {
	
	int r = rand() % rangeMax + rangeMin;
	return r;

}

void init() {

	srand(time(NULL));

}

int main(int argc, char** argv) {

	int min = 1;
	int max = 9;
	int hostCnt = 0;
	int timeout = 1000000;	//in microseconds
	init();

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
	int rn = 0;
	int buf;
	int count = 1;
	int i = 0;
	while(1) {
		for(i = 0; i < size; i++) {
			rn = generateRandomNumber(min, max);
			if ( i != rank ) {
				printf("Node %d: Sending number '%d' to node '%d'\n", rank, rn, i);
				MPI_Send(&rn, count, MPI_INT, i, 1, MPI_COMM_WORLD);
				usleep(timeout);
			}
		}
	}



	rc = MPI_Finalize();
	if (rc != MPI_SUCCESS) {
		printf("Node %d: Error in finalize\n", rank);
	}


	return 0;
}
