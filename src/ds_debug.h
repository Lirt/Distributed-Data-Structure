/*
 * COMPILE MAIN PROGRAM WHICH INCLUDES THIS FILE WITH -DDEBUG
 */

#ifdef DEBUG
   #define LOG_DEBUG(TIME, THREAD, MESSAGE ...) fprintf(log_file, "[DEBUG]: %s %s:%d: Thread%d: " MESSAGE "\n", TIME, __FILE__, __LINE__, THREAD, ##__VA_ARGS__)
#else
   #define LOG_DEBUG(TIME, ...)
#endif

#define CLEAN_ERRNO() (errno == 0 ? "No error message" : strerror(errno))

#define LOG_CRIT(TIME, ...) fprintf(log_file, "[CRITICAL]: (%s %s:%d: Errno: %s) " MESSAGE "\n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), ##__VA_ARGS__)

#define LOG_ERR(TIME, ...) fprintf(log_file, "[ERROR]: (%s %s:%d: Errno: %s) " MESSAGE "\n", TIME, __FILE__, __LINE__, CLEAN_ERRNO(), ##__VA_ARGS__)

#define LOG_INFO(...) fprintf(log_file, "[INFO]: (%s) " MESSAGE "\n", __FILE__, ##__VA_ARGS__) 

extern FILE* log_file;

