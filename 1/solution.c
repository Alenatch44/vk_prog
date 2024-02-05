#define _POSIX_C_SOURCE 199309L

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "libcoro.h"

typedef struct sort_arguments 
{
  int *arrToSort;
  int arrSize;
  char *fileToSort;
  uint64_t timeQuant;
} sort_arguments;


uint64_t get_time(void) 
{
  struct timespec spec;
  clock_gettime(CLOCK_MONOTONIC, &spec);
  return spec.tv_sec * 1000000000 + spec.tv_nsec;
}

void my_merge
(
	int arr[], 
	int l, 
	int m, 
	int r
) 
{
  int i, j, k;
  int n1 = m - l + 1;
  int n2 = r - m;

  int L[n1], R[n2];

  for (i = 0; i < n1; i++) L[i] = arr[l + i];
  for (j = 0; j < n2; j++) R[j] = arr[m + 1 + j];

  i = 0;
  j = 0;
  k = l;
  while (i < n1 && j < n2) 
  {
    if (L[i] <= R[j]) {
      arr[k] = L[i];
      i++;
    } else {
      arr[k] = R[j];
      j++;
    }
    k++;
  }

  while (i < n1) 
  {
    arr[k] = L[i];
    i++;
    k++;
  }

  while (j < n2) 
  {
    arr[k] = R[j];
    j++;
    k++;
  }
}

uint64_t my_merge_sort
(
	int arr[], 
	int l, 
	int r, 
	int *yieldNum, 
	uint64_t quant,
	uint64_t *startTime
) 
{
  if (l < r) 
  {
    int m = l + (r - l) / 2;

    uint64_t as1 = my_merge_sort(arr, l, m, yieldNum, quant, startTime);

    uint64_t ts1 = get_time();

    if (ts1 - *startTime >= quant) 
	{
      *yieldNum += 1;
      coro_yield();
      *startTime = get_time();
    }

    uint64_t ts2 = get_time();

    uint64_t as2 = my_merge_sort(arr, m + 1, r, yieldNum, quant, startTime);

    my_merge(arr, l, m, r);
    return ts2 - ts1 + as1 + as2;
  }
  return 0;
}

void my_merge_arrays
(
	int arr1[], 
	int arr2[], 
	int n1, 
	int n2, 
	int arr3[]
) 
{
  int i = 0, j = 0, k = 0;

  while (i < n1 && j < n2) 
  {
    if (arr1[i] < arr2[j])
      arr3[k++] = arr1[i++];
    else
      arr3[k++] = arr2[j++];
  }

  while (i < n1) arr3[k++] = arr1[i++];

  while (j < n2) arr3[k++] = arr2[j++];
}

int count_file_size(char *fileName) 
{
  int temp;
  int count = 0;
  FILE *file = fopen(fileName, "r");
  if (file == NULL) 
  {
    printf("Could not open specified file");
    return -1;
  }

  while (fscanf(file, "%d", &temp) == 1) 
  {
    count++;
  }
  fclose(file);
  return count;
}

static int coroutine_func_f(void *context) 
{
  uint64_t ts1 = get_time();
  uint64_t startTime = get_time();
  int yieldNum = 0;

  struct sort_arguments *fa = (struct sort_arguments *)context;
  FILE *fp = fopen(fa->fileToSort, "r");

  for (int i = 0; i < fa->arrSize; i++) 
  {
    fscanf(fp, "%d ", &(fa->arrToSort)[i]);
  }

  uint64_t lostTime = my_merge_sort(fa->arrToSort, 0, fa->arrSize - 1, &yieldNum, fa->timeQuant, &startTime);

  uint64_t totalTime = get_time() - ts1 - lostTime;

  printf(
      "Coroutine that sorted %s:\n"
      "  Elapsed time: %llu ms\n"
      "  Yielded %d times\n",
      fa->fileToSort, (unsigned long long)(totalTime / 1000000), yieldNum);
  fclose(fp);
  return 0;
}


int main(int argc, char **argv) 
{
  uint64_t ts1 = get_time();

  if (argc <= 1) 
  {
    printf("No files to sort.\n");
    return EXIT_FAILURE;
  }

  int targetLatency = atoi(argv[1]);
  if (targetLatency == 0) 
  {
    printf("Enter a proper target latency.\n");
    return EXIT_FAILURE;
  }

  int coroNum = atoi(argv[2]);
  if (coroNum == 0) 
  {
    printf("Enter a proper number of coroutines.\n");
    return EXIT_FAILURE;
  }
  coroNum = argc - 3;

  coro_sched_init();
  int **arrs = (int **)calloc((argc - 3), sizeof(int *));
  struct sort_arguments *fa = malloc((argc - 3) * sizeof(struct sort_arguments));

  int *sizes = malloc((argc - 3) * sizeof(int));

  for (int i = 0; i < argc - 3; ++i) 
  {
    sizes[i] = count_file_size(argv[i + 3]);
    *(arrs + i) = (int *)calloc(sizes[i], sizeof(int));
    fa[i] = (sort_arguments){*(arrs + i), sizes[i], argv[i + 3], (targetLatency / coroNum) * 1000};
    coro_new(coroutine_func_f, &(fa[i]));
  }

  struct coro *c;
  while ((c = coro_sched_wait()) != NULL) 
  {
    printf("Finished %d\n", coro_status(c));
    coro_delete(c);
  }

  for (int i = 0; i < argc - 3 - 1; ++i) 
  {
    int *ans = malloc((sizes[i] + sizes[i + 1]) * sizeof(int));
    my_merge_arrays(*(arrs + i), *(arrs + i + 1), sizes[i], sizes[i + 1], ans);
    free(*(arrs + i));
    free(*(arrs + i + 1));
    *(arrs + i + 1) = ans;
    sizes[i + 1] += sizes[i];
  }

  FILE *file = fopen("output_file.txt", "w");
  if (file == NULL) 
  {
    printf("Error opening the file.\n");
    return 1;
  }

  for (int i = 0; i < sizes[argc - 3 - 1]; i++) 
  {
    fprintf(file, "%d\n", arrs[argc - 3 - 1][i]);
  }

  fclose(file);
  free(arrs[argc - 3 - 1]);
  free(arrs);
  free(fa);
  free(sizes);

  uint64_t ts2 = get_time();
  printf("Total elapsed time: %llu ms\n", (unsigned long long)((ts2 - ts1) / 1000000));

  return 0;
}