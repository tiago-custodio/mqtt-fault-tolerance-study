FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# ===============================
# 0️⃣ Instala dependências
# ===============================
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        cmake \
        wget \
        git \
        nlohmann-json3-dev \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# ===============================
# 1️⃣ Compila e instala Paho MQTT C
# ===============================
RUN git clone https://github.com/eclipse/paho.mqtt.c.git /tmp/paho.mqtt.c && \
    cd /tmp/paho.mqtt.c && \
    cmake -Bbuild -H. \
        -DPAHO_BUILD_STATIC=TRUE \
        -DPAHO_BUILD_SHARED=TRUE \
        -DPAHO_WITH_SSL=TRUE && \
    cmake --build build/ --target install && \
    ldconfig && \
    rm -rf /tmp/paho.mqtt.c

# ===============================
# 2️⃣ Compila e instala Paho MQTT C++
# ===============================
RUN git clone https://github.com/eclipse/paho.mqtt.cpp.git /tmp/paho.mqtt.cpp && \
    cd /tmp/paho.mqtt.cpp && \
    cmake -Bbuild -H. \
        -DPAHO_BUILD_STATIC=TRUE \
        -DPAHO_BUILD_SHARED=TRUE && \
    cmake --build build/ --target install && \
    ldconfig && \
    rm -rf /tmp/paho.mqtt.cpp

# ===============================
# 3️⃣ Copia e compila seu middleware
# ===============================
WORKDIR /app
COPY . .

# Cria pasta de build e compila com nome específico para este middleware
RUN mkdir build && \
    cd build && \
    cmake -DEXECUTABLE_NAME=middleware2 .. && \
    make

# ===============================
# 4️⃣ Define o comando de execução
# ===============================
CMD ["./build/middleware2"]
