#include <string>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "DataGenerator.h"
#include "Processing.h"
#include "Log.h"
#include "Includes.h"

static const char kFileNameTemp1[] = "temp1";
static const char kFileNameTemp2[] = "temp2";

int main() {
  using namespace myTask;
  if (chdir(path) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "change working dir failed: " + std::string(strerror(errno)));
    return -1;
  }
  std::string sPath(path);
  DataGenerator dataGenerator(sPath);
  dataGenerator.init();

  dataGenerator.generateIidIprice();
  displayFile(kFileNameIidIprice);
  if (MergeSort(kFileNameIidIprice, kFileNameTemp1, compareIid_inII) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "merge sort failed.");
    return -1;
  }
  displayFile(kFileNameTemp1);
  dataGenerator.generateIidIprice();
  if (SortAndSum(kFileNameIidIprice, kFileNameTemp2, compareIid_inII) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "merge sort failed.");
    return -1;
  }
  displayFile(kFileNameTemp2);
  // if work as expected two files will be same
  if (ValidateResult(kFileNameTemp1, kFileNameTemp2, 1) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "result check failed.");
    return -1;
  }
  printInfo(__FILE__, __FUNCTION__, __LINE__, "result check passed");

  return 0;
}