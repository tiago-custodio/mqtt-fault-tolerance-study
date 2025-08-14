cmake_minimum_required(VERSION 3.10)
project(middleware2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Pacotes com config conhecido
find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json REQUIRED)

# Permite trocar o nome do binário via -DEXECUTABLE_NAME=...
if(DEFINED EXECUTABLE_NAME AND NOT "${EXECUTABLE_NAME}" STREQUAL "")
  set(TARGET_NAME "${EXECUTABLE_NAME}")
else()
  set(TARGET_NAME "${PROJECT_NAME}")
endif()

# Tenta localizar a lib C do Paho (SSL preferido: paho-mqtt3as)
# Fallbacks: paho-mqtt3a (sem SSL), ou variantes com 'c'
find_library(PAHO_MQTT_C_LIB
  NAMES paho-mqtt3as paho-mqtt3a paho-mqtt3cs paho-mqtt3c
  PATHS /usr/local/lib /usr/lib
)

add_executable(${TARGET_NAME} middleware2.cpp)

target_link_libraries(${TARGET_NAME} PRIVATE
  PahoMqttCpp::paho-mqttpp3
  nlohmann_json::nlohmann_json
  ${PAHO_MQTT_C_LIB}
)

# Opcional: garanta /usr/local/lib no rpath, já que instalamos via source
set_target_properties(${TARGET_NAME} PROPERTIES
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH "/usr/local/lib"
)
