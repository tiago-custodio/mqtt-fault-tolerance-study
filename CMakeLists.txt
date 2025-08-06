cmake_minimum_required(VERSION 3.10)
project(middleware)

set(CMAKE_CXX_STANDARD 17)

find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json REQUIRED)

add_executable(middleware middleware.cpp)
target_link_libraries(middleware PahoMqttCpp::PahoMqttCpp nlohmann_json::nlohmann_json)