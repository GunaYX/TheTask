#include "DataGenerator.h"
#include "Log.h"
#include "Includes.h"

int main() {
  using namespace myTask;
  printInfo(__FILE__, __FUNCTION__, __LINE__, "test data generator");
//  myTask::DataGenerator dataGenerator("/mnt/d/Temp/");
  DataGenerator dataGenerator(std::string(path));
  dataGenerator.init();
  dataGenerator.excute();
  displayFile(kFileNameUidIid);
  displayFile(kFileNameIidIprice);

  return 0;

}


