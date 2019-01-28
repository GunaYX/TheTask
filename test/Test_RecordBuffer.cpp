//
// Created by guanyx on 2019/1/27.
//
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "RecordBuffer.h"
#include "Log.h"
#include "Includes.h"

static const char fileName[] = "testRecordBuffer";
static const off_t kFileLength = 1024 * 1024;
static const off_t kMapSize = 256 * 1024;
// test put and get in RecordBuffer;
int main() {
  myTask::printInfo(__FILE__, __FUNCTION__, __LINE__, "test record buffer");
  myTask::RecordBuffer recordBuffer;
  int fd = open(fileName, O_CREAT | O_RDWR);
  if (fd < 0) {
    myTask::printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  if (ftruncate(fd, kFileLength) != 0) {
    myTask::printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fd);
    return -1;
  }
  ftruncate(fd, kFileLength);
  recordBuffer.Configure(fd, 0, kFileLength, kMapSize);
  if (recordBuffer.Init() < 0) {
    myTask::printError(__FILE__, __FUNCTION__, __LINE__, "recorde buffer init failed");
    return -1;
  }
  uint64_t numToWrite = 0;
  while (true) {
    myTask::Record record{numToWrite, numToWrite * 10};
    ++numToWrite;
    if (recordBuffer.PutNext(record) == 1) {
      if (recordBuffer.WindowsMoveToNext() == 1) {
        recordBuffer.Free();
        break;
      }
      recordBuffer.PutNext(record);
    }
  }
  myTask::printInfo(__FILE__, __FUNCTION__, __LINE__, "record written: " + std::to_string(numToWrite));
  // read again to check consistency
  if (recordBuffer.Init() < 0) {
    myTask::printError(__FILE__, __FUNCTION__, __LINE__, "recorde buffer init failed");
    return -1;
  }
  uint64_t recordGet = 0;
  while (true) {
    myTask::Record record{};
    ++recordGet;
    if (recordBuffer.GetNext(record) == 1) {
      if (recordBuffer.WindowsMoveToNext() == 1) {
        recordBuffer.Free();
        break;
      }
      recordBuffer.GetNext(record);
    }
    if (recordGet % 100 == 0) {
      myTask::printInfo(__FILE__,
                        __FUNCTION__,
                        __LINE__,
                        "record read: " + std::to_string(record.r1) + " " + std::to_string(record.r2));
    }
  }
  myTask::printInfo(__FILE__, __FUNCTION__, __LINE__, "num of record read: " + std::to_string(recordGet));
  return 0;
}

