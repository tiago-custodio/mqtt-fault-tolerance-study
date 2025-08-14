cmake_minimum_required(VERSION 3.10)
project(middleware3)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json REQUIRED)

if(DEFINED EXECUTABLE_NAME AND NOT "${EXECUTABLE_NAME}" STREQUAL "")
  set(TARGET_NAME "${EXECUTABLE_NAME}")
else()
  set(TARGET_NAME "${PROJECT_NAME}")
endif()

find_library(PAHO_MQTT_C_LIB
  NAMES paho-mqtt3as paho-mqtt3a paho-mqtt3cs paho-mqtt3c
  PATHS /usr/local/lib /usr/lib
)

add_executable(${TARGET_NAME} middleware3.cpp)

target_link_libraries(${TARGET_NAME} PRIVATE
  PahoMqttCpp::paho-mqttpp3
  nlohmann_json::nlohmann_json
  ${PAHO_MQTT_C_LIB}
)

set_target_properties(${TARGET_NAME} PROPERTIES
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH "/usr/local/lib"
)
