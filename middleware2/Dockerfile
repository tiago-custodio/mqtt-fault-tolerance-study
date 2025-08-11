FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y build-essential cmake wget git nlohmann-json3-dev libssl-dev libpaho-mqtt-dev && \
    git clone https://github.com/eclipse/paho.mqtt.cpp.git && \
    cd paho.mqtt.cpp && \
    cmake -Bbuild -H. -DPAHO_BUILD_STATIC=TRUE -DPAHO_BUILD_SHARED=TRUE && \
    cmake --build build/ --target install && \
    ldconfig && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN mkdir build && \
    cd build && \
    cmake .. && \
    make

CMD ["./build/middleware"]
