#include <cstring>
#include "Includes.h"
#include "Processing.h"
#include "DataGenerator.h"
#include "Log.h"


int main() {
  using namespace myTask;
  if (chdir(path) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "change working dir failed: " + std::string(strerror(errno)));
    return -1;
  }

  std::string sPath(path);
  DataGenerator dataGenerator(sPath);
  dataGenerator.init();
  dataGenerator.excute();
  if (MergeSort(kFileNameUidIid, kFileNameUidIidSorted, compareIidUid) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "merge sort uid iid file failed.");
    return -1;
  }
//  displayFile(kFileNameUidIidSorted);
  if (MergeSort(kFileNameIidIprice, kFileNameIidIpriceSorted, compareIid_inII) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "merge sort uid iid file failed.");
    return -1;
  }
//  displayFile(kFileNameIidIpriceSorted);
  if (JoinSortedResult(kFileNameUidIidSorted, kFileNameIidIpriceSorted, kFileNameJoinResult) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "join failed.");
    return -1;
  }
  if (SortAndSum(kFileNameJoinResult, kFileNameFinalResult, comparUid_inUI) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "sum result failed.");
    return -1;
  }
  if (ValidateResult(kFileNameFinalResult, kFileNameValidate, 10) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "result check failed.");
    return -1;
  }
  printInfo(__FILE__, __FUNCTION__, __LINE__, "result check passed");
  return 0;
}