#include "Log.h"

#include <iostream>
#include <string>
namespace myTask {
void printInfo(std::string fileName, std::string functionName, int lineNo, std::string additional) {
  std::cout << "[INFO] in file: " << fileName << ", in function:" << functionName << ", at line: " << lineNo
            << ", get: " << additional
            << std::endl;
}
void printError(std::string fileName, std::string functionName, int lineNo, std::string additional) {
  std::cout << "[ERROR] in file: " << fileName << ", in function:" << functionName << ", at line: " << lineNo
            << ", get: " << additional
            << std::endl;
}
}