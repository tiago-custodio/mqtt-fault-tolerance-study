// middleware1.cpp
#include <iostream>
#include <queue>
#include <chrono>
#include <thread>
#include <mqtt/async_client.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class CircuitBreaker {
private:
    int failureCount = 0;
    int successCount = 0;
    bool isOpen = false;
    std::chrono::steady_clock::time_point lastFailureTime;
    const int failureThreshold = 3;
    const int successThreshold = 2;
    const std::chrono::seconds resetTimeout = std::chrono::seconds(10);

public:
    bool allowRequest() {
        if (isOpen) {
            auto now = std::chrono::steady_clock::now();
            if (now - lastFailureTime > resetTimeout) {
                isOpen = false;
                return true;
            }
            return false;
        }
        return true;
    }

    void recordFailure() {
        failureCount++;
        successCount = 0;
        lastFailureTime = std::chrono::steady_clock::now();
        
        if (failureCount >= failureThreshold) {
            isOpen = true;
            std::cout << "Circuit breaker OPENED" << std::endl;
        }
    }

    void recordSuccess() {
        successCount++;
        if (successCount >= successThreshold) {
            failureCount = 0;
            std::cout << "Circuit breaker RESET" << std::endl;
        }
    }

    bool isCircuitOpen() const { return isOpen; }
};

class MQTTMiddleware {
private:
    mqtt::async_client client;
    std::queue<std::string> messageQueue;
    CircuitBreaker cb;
    const std::string RECEIVER_TOPIC = "iot/data";

public:
    MQTTMiddleware(const std::string& brokerAddress) 
        : client(brokerAddress, "middleware1") {}

    void start() {
        client.connect()->wait();

        // Ativa o consumo de mensagens
        client.start_consuming();

        client.subscribe("iot/input", 1)->wait(); // Recebe mensagens do sender
        
        while (true) {
            auto msg = client.consume_message();
            
            if (msg) {
                // Novo log para depuração
                std::cout << "[Middleware1] Mensagem recebida no tópico 'iot/input': " 
                          << msg->to_string() << std::endl;

                processMessage(msg->to_string());
            }
            
            retryFailedMessages();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

private:
    void processMessage(const std::string& payload) {
        try {
            if (cb.allowRequest()) {
                if (forwardToReceiverTopic(payload)) {
                    cb.recordSuccess();
                } else {
                    cb.recordFailure();
                    messageQueue.push(payload);
                }
            } else {
                messageQueue.push(payload);
                std::cout << "Circuit open - message queued" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            messageQueue.push(payload);
        }
    }

    bool forwardToReceiverTopic(const std::string& payload) {
        // Publica a mensagem processada no tópico do receiver
        mqtt::message_ptr pubmsg = mqtt::make_message(RECEIVER_TOPIC, payload);
        pubmsg->set_qos(1);
        client.publish(pubmsg)->wait();
        
        // Simula 20% de chance de falha
        static int counter = 0;
        if (++counter % 5 == 0) {
            std::cout << "Simulated receiver failure" << std::endl;
            return false;
        }
        return true;
    }

    void retryFailedMessages() {
        static auto lastRetry = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        
        if (now - lastRetry < std::chrono::seconds(5)) return;
        lastRetry = now;
        
        if (messageQueue.empty()) return;
        
        std::cout << "Retrying " << messageQueue.size() << " queued messages" << std::endl;
        
        while (!messageQueue.empty()) {
            auto msg = messageQueue.front();
            if (forwardToReceiverTopic(msg)) {
                messageQueue.pop();
            } else {
                break;
            }
        }
    }
};

int main() {
    MQTTMiddleware middleware("tcp://mosquitto:1883");
    middleware.start();
    return 0;
}
