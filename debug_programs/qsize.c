#include <stdio.h>
#ifndef STDLIB_H
   #define STDLIB_H
  #include <stdlib.h>
#endif

typedef struct qsize_struct_sorted {
  unsigned long size;
  int index;
} qsizes;

int qsize_comparator(const void *a, const void *b) {

  const struct qsize_struct_sorted *s1 = (const struct qsize_struct_sorted*) a;
  const struct qsize_struct_sorted *s2 = (const struct qsize_struct_sorted*) b;

  return s1->size - s2->size;

} 

int qsize_comparator2(const void *a, const void *b) {

  qsizes *s1 = (qsizes*) a;
  qsizes *s2 = (qsizes*) b;
  printf("Q1[%d] - %ld\nQ2[%d] - %ld\n", s1->index, s1->size, s2->index, s2->size);
  return s1->size - s2->size;

} 

int main(int argc, char** argv) {
  
  struct qsize_struct_sorted **qss = (struct qsize_struct_sorted**) malloc(8 * sizeof(struct qsize_struct_sorted*));
  for (int i = 0; i < 8; i++) {
    qss[i] = (struct qsize_struct_sorted*) malloc(sizeof(struct qsize_struct_sorted));
  }
  
  qsizes *qss2 = (qsizes*) malloc(8 * sizeof(qsizes));
  
  unsigned long arr[8];
  arr[0] = 5;
  arr[1] = 3;
  arr[2] = 4;
  arr[3] = 1;
  arr[4] = 6;
  arr[5] = 2;
  arr[6] = 7;
  arr[7] = 9;
  
  for (int i = 0; i < 8; i++) {
    qss[i]->size = arr[i];
    qss[i]->index = i;
  }
  
  printf("Unsorted array: \n");
  for (int i = 0; i < 8; i++) {
    printf("index[%d] - %ld\n", qss[i]->index, qss[i]->size);
  }
  
  qsort(qss, 8, sizeof(struct qsize_struct_sorted*), qsize_comparator);

  printf("Sorted array: \n");
  for (int i = 0; i < 8; i++) {
    printf("index[%d] - %ld\n", qss[i]->index, qss[i]->size);
  }
  
  //////////////
  
  for (int i = 0; i < 8; i++) {
    qss2[i].size = arr[i];
    qss2[i].index = i;
  }
  
  printf("Unsorted array2: \n");
  for (int i = 0; i < 8; i++) {
    printf("index[%d] - %ld\n", qss2[i].index, qss2[i].size);
  }
  
  qsort(qss2, 8, sizeof(qsizes), qsize_comparator2);

  printf("Sorted array2: \n");
  for (int i = 0; i < 8; i++) {
    printf("index[%d] - %ld\n", qss2[i].index, qss2[i].size);
  }
  
  return 0;
  
}