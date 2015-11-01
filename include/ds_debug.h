/*
 * COMPILE MAIN PROGRAM WHICH INCLUDES THIS FILE WITH -DDEBUG
 */
#include <errno.h>
#ifndef STDIO_H
   #define STDIO_H
   #include <stdio.h>
#endif

#ifdef DEBUG
   #define LOG_DEBUG(TIME, THREAD, MESSAGE, ...) fprintf(log_file, "[DEBUG]: %s %s:%d: Thread '%d': %s\n", TIME, __FILE__, __LINE__, THREAD, MESSAGE, ##__VA_ARGS__)
#else
   #define LOG_DEBUG(TIME, ...)
#endif

#define CLEAN_ERRNO() (errno == 0 ? "No error message" : strerror(errno))

#define LOG_CRIT(TIME, MESSAGE, ...) fprintf(log_file, "[CRITICAL]: (%s %s:%d: Errno: %s): %s \n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_ERR(TIME, MESSAGE, ...) fprintf(log_file, "[ERROR]: (%s %s:%d: Errno: %s): %s\n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), MESSAGE, ##__VA_ARGS__)

#define LOG_INFO(MESSAGE, ...) fprintf(log_file, "[INFO]: (%s)\n", MESSAGE, ##__VA_ARGS__) 

extern FILE *log_file;

