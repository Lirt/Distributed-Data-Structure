#include <math.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#ifndef DS_DEBUG_H
   #define DS_DEBUG_H
   #include "../include/ds_debug.h"
#endif

#ifndef DS_STACK_H
   #define DS_STACK_H
	#include "../include/ds_stack.h"
#endif

#ifndef MPI_H
   #define MPI_H
	#include "mpi.h"
#endif

#ifndef STDLIB_H
   #define STDLIB_H
	#include <stdlib.h>
#endif


int main(int argc, char** argv) {

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
	printf("Hello, number of tasks is '%d', my rank is '%d' and my processor is '%s'\n", size, rank, processor_name);
	
	int claimed;
	rc = MPI_Query_thread(&claimed);
	if (rc != MPI_SUCCESS) {
		printf("Error query thread\n");
		return -1;
	}
	printf("Node %d: Query thread level '%d', level of threads provided '%d'\n", rank, claimed, provided);


	/*
	 * DS_STACK INIT
	 */
	struct ds_stack *stack = init_stack();

   
	/*
	 * THREADS
	 */
	//struct thread_pop_data {
	//	struct *ds_stack;
	//	int buf;
	//}
	pthread_t threads[2];
	//struct thread_pop_data *tpd;
	//tpd = (struct thread_pop_data*) malloc (sizeof(struct thread_pop_data));

	/*
	 * PROGRAM
	 */
	int buf;
	int count = 1;
	int tid = 0;
	MPI_Status status;
	//printf("Node %d: Receiving numbers from node '0'\n", rank);

	printf("Hello Node %d | Thread %d\n", rank, tid);
	while(1) {
		MPI_Recv(&buf, count, MPI_INT, 0, tid, MPI_COMM_WORLD, &status);
		if (buf == -1) {
			//CLIENTS END WHEN RECEIVED NUMBER IS -1
			printf("Node %d: END###\n", rank);
			break;
		}
		printf("Node %d | Thread %d: Number '%d' received\n", rank, tid, buf);
		//tpd->buf = buf;
		//tpd->ds_stack = stack;
		//for (int j = 0; j < 2; j++) {
		//	rc = pthread_create(&threads[i], NULL, push_to_stack, (void*) tpd)
		//}
		push_to_stack(stack, buf);
	}

	//PRINT STACK
	//int *num = (int *) malloc(sizeof(int));
	int num;
	printf("Node %d: stack numbers - ", rank);
	while ( pop_from_stack(stack, &num) != -1 ) {
		printf("%d ", num);
	}
	printf("\n");


	rc = MPI_Finalize();
	if (rc != MPI_SUCCESS) {
		printf("Node %d: Error in finalize\n", rank);
	}


	return 0;
}
