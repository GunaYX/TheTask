cmake_minimum_required(VERSION 3.13)
project(task_pingcap)

set(CMAKE_CXX_STANDARD 11)
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64")

include_directories(./lib)

add_executable(test_sort test/Test_sort.cpp lib/Processing.h lib/Processing.cpp lib/Includes.h lib/RecordBuffer.cpp lib/RecordBuffer.h lib/DataGenerator.cpp lib/DataGenerator.h lib/Log.h lib/Log.cpp lib/Includes.cpp)
add_executable(test_datagenerator test/Test_DataGenerator.cpp lib/DataGenerator.cpp lib/DataGenerator.h lib/RecordBuffer.cpp lib/RecordBuffer.h lib/Log.h lib/Log.cpp lib/Includes.cpp)
add_executable(test_recordbuffer test/Test_RecordBuffer.cpp lib/RecordBuffer.cpp lib/RecordBuffer.h lib/Log.h lib/Log.cpp lib/Includes.cpp)
add_executable(task_pingcap main.cpp lib/Processing.h lib/Processing.cpp lib/Includes.h lib/RecordBuffer.cpp lib/RecordBuffer.h lib/DataGenerator.cpp lib/DataGenerator.h lib/Log.h lib/Log.cpp lib/Includes.cpp)
