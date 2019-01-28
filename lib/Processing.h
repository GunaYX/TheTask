#ifndef TASK_PINGCAP_MYSORT_H
#define TASK_PINGCAP_MYSORT_H

#include <string>
#include "Includes.h"

namespace myTask {
int MergeSort(const char *inputFileName, const char *outputFileName, int (*compareFunc)(const void *a, const void *b));
int SortAndSum(const char *inputFileName, const char *outputFileName, int (*compareFunc)(const void *, const void *));
int JoinSortedResult(const char *uidIidFilename, const char *iidIpriceFilename, const char *outputFileName);
int ValidateResult(const char *resultFileName, const char *validationFileName, uint64_t factor);
}

#endif //TASK_PINGCAP_MYSORT_H
