#include "Processing.h"

#include <cstring>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "Includes.h"
#include "RecordBuffer.h"
#include "Log.h"

namespace myTask {
/***
 * first pass in merge sort use in memory sort;
 * @param fd
 * @return return -1 when error. on success, return number of ordered bulk
 */
int SortFirstPass(int fd, off_t fileLength, int (*compareFunc)(const void *, const void *)) {
  int orderedBulkNum = 0;
  printInfo(__FILE__, __FUNCTION__, __LINE__, "merge sort first pass");
  // first pass use in memory sort algorithm // TODO: parallel sorting?
  for (off_t lengthProcessed = 0; lengthProcessed < fileLength; lengthProcessed += kMapSize) {
    printInfo(__FILE__, __FUNCTION__, __LINE__, "processed file: " + std::to_string(lengthProcessed) + "bytes");
    off_t curMapSize = kMapSize;
    if (lengthProcessed + kMapSize > fileLength) {
      curMapSize = fileLength - lengthProcessed;
    }
    void *addr = mmap(nullptr, curMapSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, lengthProcessed);
    if (addr == MAP_FAILED) {
      printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
      munmap(addr, curMapSize);
      return -1;
    }
    auto records_out = (Record *) addr;
    // stl internal sort;
    std::qsort(records_out, curMapSize / sizeof(Record), sizeof(Record), compareFunc);

    if (msync(addr, curMapSize, MS_SYNC) < 0) {
      printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
      return -1;
    }
    if (munmap(addr, curMapSize) < 0) {
      printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
      return -1;
    }
    ++orderedBulkNum;
  }
  return orderedBulkNum;
}
/***
 * two pass external sorting
 * first pass use in memory sort
 * second pass use multi way merge sort
 * @param inputFileName file name of input file
 * @param outputFileName file name of output file
 * @param compareFunc compare function for sort
 * @return return 0 on success, return -1 when error occurs.
 */
int MergeSort(const char *inputFileName, const char *outputFileName, int (*compareFunc)(const void *, const void *)) {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "merge sort: " + std::string(inputFileName));

  int fdIn = open(inputFileName, O_RDWR);
  if (fdIn < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  off_t inputFileLength = FdGetFileSize(fdIn);
  uint64_t inputRecordNum = inputFileLength / sizeof(Record);

  int orderedBulkNum = SortFirstPass(fdIn, inputFileLength, compareFunc);
  if (orderedBulkNum < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "first pass in merge sort failed");
    close(fdIn);
    return -1;
  }
  printInfo(__FILE__, __FUNCTION__, __LINE__, "ordered bulk num: " + std::to_string(orderedBulkNum));

  // second pass use multi way merge
  // input buffers share 3GB memory, output buffer use 512MB memory
  RecordBuffer inputBuffers[orderedBulkNum];
  off_t inputBufferSize = kMergeSortInBufferSize;
  off_t singleInputBufferSize = inputBufferSize / orderedBulkNum;
  off_t lengthAssigned = 0;
  for (int i = 0; i < orderedBulkNum; ++i) {
    off_t assignSize = kMapSize;
    if (lengthAssigned + kMapSize > inputFileLength) {
      assignSize = inputFileLength - lengthAssigned;
    }
    inputBuffers[i].Configure(fdIn, lengthAssigned, assignSize, singleInputBufferSize);
    lengthAssigned += assignSize;
    if (inputBuffers[i].Init() < 0) {
      printError(__FILE__, __FUNCTION__, __LINE__, "record buffer init failed: " + std::string(strerror(errno)));
      return -1;
    }
  }
  int fdOut = open(outputFileName, O_CREAT | O_RDWR);
  if (fdOut < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  if (ftruncate(fdOut, kFileLength) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  uint64_t outputCount = 0;
  RecordBuffer outputBuffer;
  outputBuffer.Configure(fdOut, 0, kFileLength, kMergeSortOutBufferSize);
  outputBuffer.Init();

  for (off_t processedBytes = 0; processedBytes < inputFileLength; processedBytes += sizeof(Record)) {
    Record max{0, 0};
    int max_index = 0;
    for (int i = 0; i < orderedBulkNum; ++i) {
      if (inputBuffers[i].IsFileEnd()) {
        continue;
      }
      Record cur{0, 0};
      inputBuffers[i].Get(cur);
      if ((cur.r1 == 0) && (cur.r2 == 0)) {
        printError(__FILE__, __FUNCTION__, __LINE__, "record lost");
        break;
      }
      if (compareFunc(&max, &cur) != 0) {
        max = cur;
        max_index = i;
      }
    }
    inputBuffers[max_index].Next();
    if ((max.r1 == 0) && (max.r2 == 0)) {
      printError(__FILE__, __FUNCTION__, __LINE__, "record lost");
      break;
    }
    // if current output buffer is full, msync and switch to another output buffer.
    outputBuffer.Put(max);
    if (outputBuffer.Next() < 0) {
      outputBuffer.Free();
      printError(__FILE__, __FUNCTION__, __LINE__, "output file full");
      continue;
    }
    outputCount++;
    // TODO: prefetch
  }
  outputBuffer.Free();
  printInfo(__FILE__, __FUNCTION__, __LINE__, "output count: " + std::to_string(outputCount));
  // shrink output file, free used space.
  if (ftruncate(fdOut, outputCount * sizeof(Record)) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  close(fdOut);
  close(fdIn);
  return 0;
}
/***
 * like the former function, but in second pass we sum results for same uid. This save some IO.
 * @param inputFileName
 * @param outputFileName
 * @return return 0 on success, return -1 when error occurs.
 */
int SortAndSum(const char *inputFileName, const char *outputFileName, int (*compareFunc)(const void *, const void *)) {
  printInfo(__FILE__, __FUNCTION__, __LINE__, "sort and sum: " + std::string(inputFileName));

  int fdIn = open(inputFileName, O_RDWR);
  if (fdIn < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  off_t inputFileLength = FdGetFileSize(fdIn);

  int orderedBulkNum = SortFirstPass(fdIn, inputFileLength, compareFunc);
  if (orderedBulkNum <= 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "first pass in merge sort failed");
    close(fdIn);
    return -1;
  }
  printInfo(__FILE__, __FUNCTION__, __LINE__, "ordered bulk num: " + std::to_string(orderedBulkNum));

  // second pass use multi way merge
  // input buffers share 3GB memory, output buffer use 512MB memory
  RecordBuffer inputBuffers[orderedBulkNum];
  off_t inputBufferSize = kMergeSortInBufferSize;
  off_t singleInputBufferSize = inputBufferSize / orderedBulkNum;
  off_t lengthAssigned = 0;
  for (int i = 0; i < orderedBulkNum; ++i) {
    off_t assignSize = kMapSize;
    if (lengthAssigned + kMapSize > inputFileLength) {
      assignSize = inputFileLength - lengthAssigned;
    }
    inputBuffers[i].Configure(fdIn, lengthAssigned, assignSize, singleInputBufferSize);
    lengthAssigned += assignSize;
    if (inputBuffers[i].Init() < 0) {
      printError(__FILE__, __FUNCTION__, __LINE__, "record buffer init failed: " + std::string(strerror(errno)));
      return -1;
    }
  }
  int fdOut = open(outputFileName, O_CREAT | O_RDWR);
  if (fdOut < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  if (ftruncate(fdOut, kFileLength) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  uint64_t outputCount = 0;
  RecordBuffer outputBuffer;
  outputBuffer.Configure(fdOut, 0, kFileLength, kMergeSortOutBufferSize);
  outputBuffer.Init();

  // merge
  Record lastRecord{0, 0};
  for (off_t processedBytes = 0; processedBytes < inputFileLength; processedBytes += sizeof(Record)) {
    Record max{0, 0};
    int max_index;
    for (int i = 0; i < orderedBulkNum; ++i) {
      if (inputBuffers[i].IsFileEnd()) {
        continue;
      }
      Record cur{0, 0};
      inputBuffers[i].Get(cur);
      if (compareFunc(&max, &cur) != 0) {
        max = cur;
        max_index = i;
      }
    }
    inputBuffers[max_index].Next();
    if ((max.r1 == 0) && (max.r2 == 0)) {
      printError(__FILE__, __FUNCTION__, __LINE__, "record lost");
      break;
    }
    if ((lastRecord.r1 == 0) && (lastRecord.r2 == 0)) {
      lastRecord = max;
    } else if (lastRecord.r1 != max.r1) {
      // if current output buffer is full, msync and switch to another output buffer.
      outputBuffer.Put(lastRecord);
      lastRecord = max;
      if (outputBuffer.Next() < 0) {
        outputBuffer.Free();
        printError(__FILE__, __FUNCTION__, __LINE__, "output file full");
        return -1;
      }
      outputCount++;
    } else {
      lastRecord.r2 += max.r2;
    }

    // TODO: prefetch
  }
  // write the last one
  outputBuffer.Put(lastRecord);
  if (outputBuffer.Next() < 0) {
    outputBuffer.Free();
    printError(__FILE__, __FUNCTION__, __LINE__, "output file full");
    return -1;
  }
  outputCount++;
  outputBuffer.Free();
  printInfo(__FILE__, __FUNCTION__, __LINE__, "output count: " + std::to_string(outputCount));
  // shrink output file, free used space.
  if (ftruncate(fdOut, outputCount * sizeof(Record)) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fdIn);
    return -1;
  }
  close(fdOut);
  close(fdIn);
  return 0;
}
/***
 * sort merge join algorithm. output <u_id, i_price> tuples.
 * @param uidIidFilename
 * @param iidIpriceFilename
 * @param outputFileName
 * @return return 0 on success, return -1 when error occurs.
 */
// TODO: parallel prefetch
int JoinSortedResult(const char *uidIidFilename, const char *iidIpriceFilename, const char *outputFileName) {
  printInfo(__FILE__, __FUNCTION__, __LINE__, " ");
  int fd_ui = open(uidIidFilename, O_RDWR);
  if (fd_ui < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd_ui);
    return -1;
  }
  int fd_ii = open(iidIpriceFilename, O_RDWR);
  if (fd_ii < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd_ii);
    return -1;
  }
  off_t inputFileLength = FdGetFileSize(fd_ui);

  int fd_out = open(outputFileName, O_CREAT | O_RDWR);
  if (fd_out < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd_out);
    return -1;
  }
  if (ftruncate(fd_out, inputFileLength) != 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file truncate failed: " + std::string(strerror(errno)));
    close(fd_out);
    return -1;
  }

  RecordBuffer uiBuffer, iiBuffer, outBuffer;
  uiBuffer.Configure(fd_ui, 0, inputFileLength, kMapSize / 3);
  uiBuffer.Init();
  iiBuffer.Configure(fd_ii, 0, inputFileLength, kMapSize / 3);
  iiBuffer.Init();
  outBuffer.Configure(fd_out, 0, inputFileLength, kMapSize / 3);
  outBuffer.Init();

  // TODO: may get some partial sum here reduce IO.
  //  Swaping inner and outer loop might make this procedure easier understood
//  Record lastRecord{0,0};
  Record uiRecord{0, 0}, iiRecord{0, 0};
  while (!uiBuffer.IsFileEnd()) {
    uiBuffer.Get(uiRecord);
    uiBuffer.Next();
    while (iiRecord.r1 != uiRecord.r2) {
      iiBuffer.Get(iiRecord);
      iiBuffer.Next();
      if ((iiBuffer.IsFileEnd()) && (iiRecord.r1 != uiRecord.r2)) { // don't forget last element in ii Record
        printError(__FILE__, __FUNCTION__, __LINE__, "iid not found in file:" + std::to_string(uiRecord.r2));
        return -1;
      }
    }
    uiRecord.r2 = iiRecord.r2;
    outBuffer.Put(uiRecord);
    outBuffer.Next();
  }
  return 0;
}
/***
 * validate the result of our algorithm
 * @param resultFileName result of out join algorithm.
 * @param validationFileName result from data generator
 * @return return 0 if two files are identical, return -1 when error occurs.
 */
int ValidateResult(const char *resultFileName, const char *validationFileName, uint64_t factor) {
  printInfo(__FILE__, __FUNCTION__, __LINE__, " ");

  int fd_result = open(resultFileName, O_RDWR);
  if (fd_result < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd_result);
    return -1;
  }
  off_t fileSize = FdGetFileSize(fd_result);
  int fd_validate = open(validationFileName, O_RDWR);
  if (fd_validate < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd_validate);
    return -1;
  }
  if (fileSize != FdGetFileSize(fd_validate)) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file length not equal");
    return -1;
  }

  RecordBuffer resultBuffer, validateBuffer;
  resultBuffer.Configure(fd_result, 0, fileSize, kMemSize / 2);
  resultBuffer.Init();
  validateBuffer.Configure(fd_validate, 0, fileSize, kMemSize / 2);
  validateBuffer.Init();
  uint64_t countPassed = 0;
  while (!resultBuffer.IsFileEnd()) {
    Record resultRecord, validateRecord;
    resultBuffer.Get(resultRecord);
    resultBuffer.Next();
    validateBuffer.Get(validateRecord);
    validateBuffer.Next();
    if ((resultRecord.r1 != validateRecord.r1) || (resultRecord.r2 != validateRecord.r2 * factor)) {
      std::cout << resultRecord.r1 << ":" << validateRecord.r1 << " " << resultRecord.r2 << ":" << validateRecord.r2
                << std::endl;
      printError(__FILE__,
                 __FUNCTION__,
                 __LINE__,
                 "result check error! " + std::to_string(countPassed) + " have passed.");
      return -1;
    }
    ++countPassed;
    if (countPassed > fileSize / sizeof(Record)) {
      printError(__FILE__, __FUNCTION__, __LINE__, "something wrong");
      return -1;
    }
  }
  return 0;
}
}