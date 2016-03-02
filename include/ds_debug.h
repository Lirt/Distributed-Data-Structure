
/*
 * COMPILE MAIN PROGRAM WHICH INCLUDES THIS FILE WITH -DDEBUG
 */

#include <errno.h>
#ifndef STDIO_H
   	#define STDIO_H
   	#include <stdio.h>
#endif

extern FILE *log_file;
extern FILE *load_balance_log_file;

#ifdef DEBUG
   	#define LOG_DEBUG(TIME, THREAD, MESSAGE, ...) fprintf(log_file, "[DEBUG]: %s %s:%d: Thread '%ld': %s\n", TIME, __FILE__, __LINE__, THREAD, MESSAGE, ##__VA_ARGS__)
	#define LOG_DEBUG_TD(TIME, THREAD, MESSAGE, ...) fprintf(log_file, "[DEBUG]: %d %s:%d: Thread '%ld': %s\n", TIME, __FILE__, __LINE__, THREAD, MESSAGE, ##__VA_ARGS__)
#else
   	#define LOG_DEBUG(TIME, ...)
	#define LOG_DEBUG_TD(TIME, ...)
#endif

#define CLEAN_ERRNO() (errno == 0 ? "No error message" : strerror(errno))

#define LOG_CRIT(TIME, MESSAGE, ...) fprintf(log_file, "[CRITICAL]: (%s %s:%d: Errno: %s): %s \n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_ERR(TIME, MESSAGE, ...) fprintf(log_file, "[ERROR]: (%s %s:%d: Errno: %s): %s\n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_INFO(MESSAGE, ...) fprintf(log_file, "[INFO]: (%s)\n", MESSAGE, ##__VA_ARGS__) 

#define LOAD_BALANCE_LOG_DEBUG_T(TIME, MESSAGE, ...) fprintf(load_balance_log_file, "[LB DEBUG]: (%s %s:%d): %s \n", TIME, __FILE__, __LINE__, MESSAGE, ##__VA_ARGS__)
#define LOAD_BALANCE_LOG_DEBUG_TD(TIME, MESSAGE, ...) fprintf(load_balance_log_file, "[LB DEBUG]: (%d %s:%d): %s \n", TIME, __FILE__, __LINE__, MESSAGE, ##__VA_ARGS__)
