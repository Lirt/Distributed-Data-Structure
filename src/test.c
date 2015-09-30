
#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>

int main (int argc, char *argv[]) {

	int size, rank;
	int rc;
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int processor_name_length;

	//int required = MPI_THREAD_MULTIPLE;
	int required = MPI_THREAD_SERIALIZED;	
	int provided;

	rc = MPI_Init_thread(&argc, &argv, required, &provided);
	if (rc != MPI_SUCCESS)
		printf("Error Init\n");

	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	rc = MPI_Get_processor_name(processor_name, &processor_name_length);
	if (rc != MPI_SUCCESS)
		printf("Error processor name\n");

	printf("Hello, number of tasks is '%d', my rank is '%d' and my processor is '%s'\n", size, rank, processor_name);
	
	int claimed;
	rc = MPI_Query_thread(&claimed);
	if (rc != MPI_SUCCESS)
		printf("Error query thread\n");

	printf("Query thread level '%d', level of threads provided '%d'\n", claimed, provided);

	rc = MPI_Finalize();
	if (rc != MPI_SUCCESS)
		printf("Error Finalize\n");

	return 0;
}
