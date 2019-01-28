#ifndef TASK_PINGCAP_INCLUDES_H
#define TASK_PINGCAP_INCLUDES_H

#include <cstdint>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>
#include <sys/mman.h>
#include <cstring>
#include <string>

namespace myTask {
// each record consists of <uint64_t, uint64_t>
struct Record {
  uint64_t r1;
  uint64_t r2;
};

// for testing
static const off_t kMapSize = 3 * 1024 * 1024LL;
static const off_t kMemSize = 4 * 1024 * 1024LL; // 4MB memory
static const off_t kFileLength = 12 * 1024 * 1024LL; // single file size 12MB
static const off_t kMergeSortInBufferSize = 3 * 1024 * 1024;
static const off_t kMergeSortOutBufferSize = 512 * 1024;
//static const off_t kMergeSortInBufferSize = 3 * 1024 * 1024 * 1024LL;
//static const off_t kMergeSortOutBufferSize = 512 * 1024 * 1024LL;
//static const off_t kMemSize = 4 * 1024 * 1024 * 1024LL; // 4GB memory
//static const off_t kFileLength = 12 * 1024 * 1024 * 1024LL; // single file size 12GB
//static const off_t kMapSize = 3 * 1024 * 1024 * 1024LL;
static const off_t kUidNum = kFileLength / 1024;
static const off_t kRecordNum = kFileLength / sizeof(Record); // the number of record we have in one file
static const char kFileNameIidIprice[] = "ii.data";
static const char kFileNameUidIid[] = "ui.data";
static const char kFileNameIidIpriceSorted[] = "ii.sorted";
static const char kFileNameUidIidSorted[] = "ui.sorted";
static const char kFileNameUidIidCopy[] = "ui.data.copy";
static const char kFileNameValidate[] = "perfectAnswer.data";
static const char kFileNameJoinResult[] = "join.result";
static const char kFileNameFinalResult[] = "final.result";

//static const char path[] = "/mnt/d/Temp/test/";
static const char path[] = "/mnt/e/"; // path where we store files

// compare functions
int compareIidUid(const void *a, const void *b);
int compareUidIid(const void *a, const void *b);
int comparUid_inUI(const void *a, const void *b);
int compareIid_inII(const void *a, const void *b);

off_t FdGetFileSize(int fd);

void displayFile(const char *fileName);

}
#endif //TASK_PINGCAP_INCLUDES_H
