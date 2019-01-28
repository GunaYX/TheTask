#include "DataGenerator.h"

#include <iostream>
#include <array>
#include <algorithm>
#include <utility>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iterator>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <random>

#include "Log.h"
#include "Includes.h"
#include "RecordBuffer.h"
#include "Processing.h"

namespace myTask {

int DataGenerator::init() {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "data generate init");
  // change working directory.
  if (chdir(path.c_str()) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "change working dir failed: " + std::string(strerror(errno)));
    return -1;
  }
  return 0;
}

// generate data if not exists
int DataGenerator::excute() {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "begin generate data");
  generateIidIprice();
  generateUidIid();
  generateValidationSet();
  return 0;
}

/***
 * shuffle generated records
 * @param p shuffle begin position
 * @param length shuffle length
 */
void DataGenerator::shuffleRecords(Record *p, uint64_t length) {
  for (int i = 1; i < length; ++i) {
    std::swap(p[i], p[std::rand() % length]);
  }
}

/***
 * generate unordered Item_id, Item_price file.
 * generate ordered file then use stl shuffle algorithm to make them unordered.
 * for convenience, we set Iprice = 10 * Iid.
 * @return return 0, if success. return -1 otherwise.
 */
int DataGenerator::generateIidIprice() {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "generating <i_id,i_price> file");
  int fd = open(kFileNameIidIprice, O_CREAT | O_RDWR);
  if (fd < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  if (ftruncate(fd, kFileLength) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }

  // map whole file in memory
  void *addr = mmap(nullptr, kFileLength, PROT_WRITE | PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
  if (addr == MAP_FAILED) {
    printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  auto records = (Record *) addr;
  // write ordered records;
  for (uint64_t i = 0; i < kRecordNum; ++i) {
    records[i].r1 = i + 1;// assign i_id
    records[i].r2 = 10 * (i + 1);// assign i_price
  }

  for (int i = 0; i < 100; ++i) {
    std::cout << records[i].r1 << " " << records[i].r2 << std::endl;
  }
  // use stl shuffle algorithm
  shuffleRecords(records, kRecordNum);

  for (int i = 0; i < 100; ++i) {
    std::cout << records[i].r1 << " " << records[i].r2 << std::endl;
  }

  if (msync(addr, kFileLength, MS_SYNC) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  if (munmap(addr, kFileLength) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  fsync(fd);
  close(fd);
  printInfo(__FILE__, __FUNCTION__, __LINE__, "<i_id,i_price> file generated");
  return 0;
}

/***
 * output unordered uid, iid file
 * generate random uid and iid(iid < kRecordNum).
 * @return return 0, if success. return -1 otherwise.
 */
int DataGenerator::generateUidIid() {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "generating <u_id,i_id> file");
  // still need to mmap to accelerate data generation
  int fd = open(kFileNameUidIid, O_CREAT | O_RDWR);
  printInfo(__FILE__, __FUNCTION__, __LINE__, "open file: " + std::string(kFileNameUidIid));
  if (fd < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  displayFile(kFileNameUidIid);
  if (ftruncate(fd, kFileLength) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  displayFile(kFileNameUidIid);

  // map whole file in memory
  void *addr = mmap(nullptr, kFileLength, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
  if (addr == MAP_FAILED) {
    printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  auto records = (Record *) addr;

  for (int i = 0; i < 100; ++i) {
    std::cout << records[i].r1 << " " << records[i].r2 << std::endl;
  }
  // random number generator
  std::random_device rd;
  std::mt19937_64 eng(rd());
  std::uniform_int_distribution<unsigned long long> distr;

  for (uint64_t i = 0; i < kRecordNum; ++i) {
    records[i].r1 = distr(eng) % kUidNum; // random u_id
    if (0 == records[i].r1) {
      records[i].r1 = 1;
    }
    records[i].r2 = distr(eng) % kRecordNum; // i_id must be in i_id i_price file
    if (0 == records[i].r2) {
      records[i].r2 = 1;
    }// zeros are not allowed.
    if (i % 100000 == 0) {
      printInfo(__FILE__, __FUNCTION__, __LINE__, std::to_string(i) + " records written");
    }
  }

  // print records indexed from 1 to 100 to check our shuffled result
  for (int i = 0; i < 100; ++i) {
    std::cout << records[i].r1 << " " << records[i].r2 << std::endl;
  }

  if (msync(addr, kFileLength, MS_SYNC) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  if (munmap(addr, kFileLength) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  fsync(fd);
  close(fd);
  printInfo(__FILE__, __FUNCTION__, __LINE__, "<u_id,i_id> file generated");
  return 0;
}

/***
 * generate validation set from uid iid file
 * sort and merge to get <u_id, u_spent> result file.
 * it's a good practice for our final version of the join algorithm.
 * @return return 0, if success. return -1 otherwise.
 */
int DataGenerator::generateValidationSet() {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "generate validation set");
  // obtain a copy of Uid Iid file;
  int source = open(kFileNameUidIid, O_RDONLY, 0);
  int dest = open(kFileNameUidIidCopy, O_WRONLY | O_CREAT /*| O_TRUNC/**/, 0644);

  // struct required, rationale: function stat() exists also

  sendfile(dest, source, nullptr, FdGetFileSize(source));

  close(source);
  close(dest);

  // sort and merge result;
  SortAndSum(kFileNameUidIidCopy, kFileNameValidate, comparUid_inUI);

  printInfo(__FILE__, __FUNCTION__, __LINE__, "validation set generated");
  return 0;
}

}