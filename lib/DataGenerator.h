#ifndef TASK_PINGCAP_DATAGENERATOR_H
#define TASK_PINGCAP_DATAGENERATOR_H

#include <string>
#include "Includes.h"

namespace myTask {

class DataGenerator {
 public:
  explicit DataGenerator(std::string Path) : path(Path) {}
  ~DataGenerator() = default;

  int init(); // initialize file descriptor;
  int excute(); // generateData;
  int generateIidIprice(); // generate <Item_id,Item_price> file
  int generateUidIid(); // generate <User_id, Item_id> file, output <User_id, User_cost> file
  int generateValidationSet(); // generate validation set to verify our algorithm

 private:
  DataGenerator();

  void shuffleRecords(Record *p, uint64_t length);

  std::string path;
};

}

#endif //TASK_PINGCAP_DATAGENERATOR_H
