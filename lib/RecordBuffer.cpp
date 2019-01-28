#include "RecordBuffer.h"

#include <string>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <sys/mman.h>

#include "Log.h"

namespace myTask {
RecordBuffer::RecordBuffer() : fd(-1), isFileEnd(false) {

}
RecordBuffer::~RecordBuffer() {
  if (pBufferBegin != nullptr) {
    msync(pBufferBegin, curWindowLength, MS_SYNC);
    munmap(pBufferBegin, curWindowLength);
  }
}
int RecordBuffer::Configure(int fd, off_t beginFrom, off_t totalLength, off_t inMenSize) {
  this->fd = fd;
  this->totalBegin = beginFrom;
  this->totalLength = totalLength;
  this->inMenSize = inMenSize;
  this->curWindowBegin = 0;
  this->curWindowLength = 0;
  this->recordLimit = curWindowLength / sizeof(Record);
  this->curRecord = 0;
  return 0;
}
int RecordBuffer::Init() {
  curWindowBegin = totalBegin;
  // get window length
  curWindowLength = inMenSize;
  if (curWindowBegin + inMenSize > totalBegin + totalLength) {
    curWindowLength = totalBegin + totalLength - curWindowBegin;
  }
  recordLimit = curWindowLength / sizeof(Record);
  pBufferBegin = mmap(nullptr, curWindowLength, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, totalBegin);
  if (pBufferBegin == MAP_FAILED) {
    printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
    munmap(pBufferBegin, inMenSize);
    close(fd);
    return -1;
  }
  records = (Record *) pBufferBegin;
  curRecord = 0;
//  if (msync(addr, mappedSize, MS_SYNC) < 0) {
//    printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
//    close(fd);
//    return -1;
//  }
//  if (munmap(addr, mappedSize) < 0) {
//    printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
//    close(fd);
//    return -1;
//  }
  return 0;
}
void RecordBuffer::SetCurrentPos(off_t pos) {
  curRecord = pos;
}
int RecordBuffer::GetNext(Record &record) {
  if (curRecord < recordLimit) {
    record = records[curRecord];
    ++curRecord;
    return 0;
  } else {
    return 1;
  }
}
int RecordBuffer::GetAt(Record &record, off_t pos) {
  if (pos < recordLimit) {
    record = records[pos];
    return 0;
  } else {
    return -1;
  }
}
int RecordBuffer::WindowsMoveToNext() {
  if (curWindowBegin + curWindowLength < totalBegin + totalLength) {
    return nextWindow();
  } else {
    isFileEnd = true;
    return 1;
  }
}
int RecordBuffer::PutNext(Record &record) {
  if (curRecord < recordLimit) {
    records[curRecord] = record;
    ++curRecord;
    return 0;
  } else {
    return 1;
  }
}
int RecordBuffer::PutAt(Record record, off_t pos) {
  if (pos < recordLimit) {
    records[pos] = record;
    return 0;
  } else {
    return -1;
  }
}
int RecordBuffer::setCurRecord(off_t pos) {
  return 0;
}
int RecordBuffer::nextWindow() {
  // munmap last window first
  if (msync(pBufferBegin, curWindowLength, MS_SYNC) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
    return -1;
  }
  if (munmap(pBufferBegin, curWindowLength) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
    return -1;
  }
  // get new window begin position
  curWindowBegin += curWindowLength;
  // get new window length
  if (curWindowBegin + inMenSize > totalBegin + totalLength) {
    curWindowLength = totalBegin + totalLength - curWindowBegin;
  }
  recordLimit = curWindowLength / sizeof(Record);
  pBufferBegin = mmap(nullptr, curWindowLength, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, curWindowBegin);
  if (pBufferBegin == MAP_FAILED) {
    printError(__FILE__, __FUNCTION__, __LINE__, "map failed: " + std::string(strerror(errno)));
    munmap(pBufferBegin, curWindowLength);
    return -1;
  }
  records = (Record *) pBufferBegin;
  curRecord = 0;
  return 0;
}
int RecordBuffer::Free() {
  if (msync(pBufferBegin, curWindowLength, MS_SYNC) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "msync failed: " + std::string(strerror(errno)));
    return -1;
  }
  if (munmap(pBufferBegin, curWindowLength) < 0) {
    printError(__FILE__, __FUNCTION__, __LINE__, "munmap failed: " + std::string(strerror(errno)));
    return -1;
  }
  return 0;
}
bool RecordBuffer::IsFileEnd() {
  return isFileEnd;
}
int RecordBuffer::Next() {
  ++curRecord;
  if (curRecord < recordLimit) {
    return 0;
  }
  if (curWindowBegin + curWindowLength < totalBegin + totalLength) {
    return nextWindow();
  } else {
    isFileEnd = true;
    return 1;
  }
}
int RecordBuffer::Get(Record &record) {
  record = records[curRecord];
  return 0;
}
int RecordBuffer::Put(Record record) {
  records[curRecord] = record;
  return 0;
}

}
