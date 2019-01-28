#ifndef TASK_PINGCAP_LOG_H
#define TASK_PINGCAP_LOG_H

#include <iostream>
#include <string>
namespace myTask {
void printInfo(std::string fileName, std::string functionName, int lineNo, std::string additional);
void printError(std::string fileName, std::string functionName, int lineNo, std::string additional);
}
#endif //TASK_PINGCAP_LOG_H
