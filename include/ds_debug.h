
/*
 * COMPILE MAIN PROGRAM WHICH INCLUDES THIS FILE WITH -DDEBUG
 */

#include <errno.h>
#ifndef STDIO_H
   	#define STDIO_H
   	#include <stdio.h>
#endif

extern FILE *log_file_global_comm;
extern FILE *log_file_debug;
extern FILE *log_file_qw;
extern FILE *log_file_lb;

#define GET_TIME_INT() (int) time(NULL)

#ifdef DEBUG
   	#define LOG_DEBUG(TIME, THREAD, MESSAGE, ...) fprintf(log_file_debug, "[DEBUG]: %s %s:%d: Thread[%ld]: %s\n", \
		TIME, __FILE__, __LINE__, THREAD, MESSAGE, ##__VA_ARGS__)
	#define LOG_DEBUG_TD(THREAD, MESSAGE, ...) \
		fprintf(log_file_debug, "[DEBUG]: %d %s:%d: Thread[%ld]: ", \
			GET_TIME_INT(), __FILE__, __LINE__, THREAD); \
		fprintf(log_file_debug, MESSAGE, ##__VA_ARGS__)

	#define LOAD_BALANCE_LOG_DEBUG_T(TIME, MESSAGE, ...) fprintf(log_file_lb, "[LB DEBUG]: (%s %s:%d): %s \n", \
		TIME, __FILE__, __LINE__, MESSAGE, ##__VA_ARGS__)
	#define LOAD_BALANCE_LOG_DEBUG_TD(MESSAGE, ...) \
		fprintf(log_file_lb, "[LB DEBUG]: (%d %s:%d): ", \
			GET_TIME_INT(), __FILE__, __LINE__); \
		fprintf(log_file_lb, MESSAGE, ##__VA_ARGS__)

	#define QSIZE_WATCHER_LOG_DEBUG_T(TIME, MESSAGE, ...) fprintf(log_file_qw, "[LB DEBUG]: (%s %s:%d): %s \n", \
		TIME, __FILE__, __LINE__, MESSAGE, ##__VA_ARGS__)
	#define QSIZE_WATCHER_LOG_DEBUG_TD(MESSAGE, ...) \
		fprintf(log_file_qw, "[LB DEBUG]: (%d %s:%d): ", \
			GET_TIME_INT(), __FILE__, __LINE__); \
		fprintf(log_file_qw, MESSAGE, ##__VA_ARGS__)
	
	#define GLOBAL_COMM_LOG_DEBUG_TD(RANK, MESSAGE, ...) \
		fprintf(log_file_global_comm, "[GLOBAL COMMUNICATION]: (%d %s:%d): RANK[%d]: ", \
			GET_TIME_INT(), __FILE__, __LINE__, RANK); \
		fprintf(log_file_global_comm, MESSAGE, ##__VA_ARGS__)

	#define NUMBER_ADD_RM_FPRINTF(FILE, FILE_NAME, MESSAGE, ...) \
		if ( fprintf(FILE, MESSAGE, ##__VA_ARGS__) < 0 ) \
			fprintf(stderr, "ERROR[%s:%d]: fprintf failed to file %s\n", __FILE__, __LINE__, FILE_NAME);

#else
   	#define LOG_DEBUG(...)
	#define LOG_DEBUG_TD(...)
	#define QSIZE_WATCHER_LOG_DEBUG_TD(MESSAGE, ...)
	#define LOAD_BALANCE_LOG_DEBUG_TD(MESSAGE, ...)
	#define NUMBER_ADD_RM_FPRINTF(...)
	#define GLOBAL_COMM_LOG_DEBUG_TD(...)
#endif

#define CLEAN_ERRNO() (errno == 0 ? "No error message" : strerror(errno))

#define LOG_CRIT(MESSAGE, ...) fprintf(log_file_debug, "[CRITICAL]: (%s %s:%d: Errno: %s): %s \n", \
	GET_TIME_INT(), __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_ERR(MESSAGE, ...) fprintf(log_file_debug, "[ERROR]: (%s %s:%d: Errno: %s): %s\n", \
	GET_TIME_INT(), __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_ERR_T(THREAD, MESSAGE, ...) \
	fprintf(log_file_debug, "[ERROR]: (T[%ld] %d %s:%d: Errno: '%s'): ", \
		THREAD, GET_TIME_INT(), __FILE__, __LINE__, CLEAN_ERRNO()); \
	fprintf(log_file_debug, MESSAGE, ##__VA_ARGS__)


#define LOG_INFO(MESSAGE, ...) fprintf(log_file_debug, "[INFO]: (%s)\n", MESSAGE, ##__VA_ARGS__) 


