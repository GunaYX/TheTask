#ifndef TASK_PINGCAP_RECORDBUFFER_H
#define TASK_PINGCAP_RECORDBUFFER_H

#include "Includes.h"

namespace myTask {
class RecordBuffer {
 public:
  RecordBuffer();
  ~RecordBuffer();

  int Configure(int fd, off_t beginFrom, off_t totalLength, off_t inMenSize);
  int Init();
  int Get(Record &record);
  int Put(Record record);
  int Next();
  void SetCurrentPos(off_t pos);
  int GetNext(Record &record);
  int GetAt(Record &record, off_t pos);
  int WindowsMoveToNext(); // switch to next window
  int PutNext(Record &record);
  int PutAt(Record record, off_t pos);
  int Free();
  int CurrentWindowEnd();
  bool IsFileEnd();

 private:
  int fd;
  off_t inMenSize;
  off_t recordLimit;
  off_t totalBegin, totalLength;
  void *pBufferBegin;
  off_t curWindowBegin, curWindowLength;
  Record *records;
  off_t curRecord; // index of the curRecord;

  bool isFileEnd;

  int setCurRecord(off_t pos);
  int nextWindow();
};
}
#endif //TASK_PINGCAP_RECORDBUFFER_H
