cmake_minimum_required(VERSION 3.10)
project(middleware1)

set(CMAKE_CXX_STANDARD 17)

# Encontra as dependências
find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json REQUIRED)

# Cria o executável
add_executable(middleware1 middleware1.cpp)

# Faz o link correto usando o target real do Paho MQTT C++
target_link_libraries(middleware1
    PahoMqttCpp::paho-mqttpp3
    nlohmann_json::nlohmann_json
)
