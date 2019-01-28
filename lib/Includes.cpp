#include "Includes.h"
#include "Log.h"

namespace myTask {
int compareIidUid(const void *a, const void *b) {
  auto ra = (const Record *) a;
  auto rb = (const Record *) b;
  if (ra->r2 != rb->r2) {
    return ra->r2 < rb->r2;
  } else {
    return ra->r1 < rb->r1;
  }
}
int compareUidIid(const void *a, const void *b) {
  auto ra = (const Record *) a;
  auto rb = (const Record *) b;
  if (ra->r1 != rb->r1) {
    return ra->r1 < rb->r1;
  } else {
    return ra->r2 < rb->r2;
  }
}
int comparUid_inUI(const void *a, const void *b) {
  auto ra = (const Record *) a;
  auto rb = (const Record *) b;
  return ra->r1 < rb->r1;
}
int compareIid_inII(const void *a, const void *b) {
  auto ra = (const Record *) a;
  auto rb = (const Record *) b;
  return ra->r1 < rb->r1;
}
off_t FdGetFileSize(int fd) {
  struct stat stat_buf;
  off_t rc = fstat(fd, &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}
void displayFile(const char *fileName) {
  int fd = open(fileName, O_RDWR);
  if (fd < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "file open failed: " + std::string(strerror(errno)));
    close(fd);
    return;
  }

  // map whole file in memory
  void *addr = mmap(nullptr, kFileLength, PROT_WRITE | PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
  if (addr == MAP_FAILED) {
    printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
    close(fd);
    return;
  }
  auto records = (Record *) addr;
  // print records indexed from 1 to 100
  for (int i = 0; i < 100; ++i) {
    std::cout << records[i].r1 << " " << records[i].r2 << std::endl;
  }
  close(fd);
}
}