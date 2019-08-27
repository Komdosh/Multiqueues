cmake_minimum_required(VERSION 3.9)
project(Mutiqueues)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS -pthread)

find_package(Boost 1.58.0 COMPONENTS filesystem REQUIRED)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    add_executable(Mutiqueues main.cpp)
    target_link_libraries(Mutiqueues ${Boost_LIBRARIES})
endif()



