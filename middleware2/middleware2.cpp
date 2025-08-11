// middleware2_replication.cpp
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <mqtt/async_client.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class MiddlewareNode {
private:
    std::string id;
    bool isLeader;
    std::vector<std::string> clusterNodes;
    mqtt::async_client client;
    mqtt::async_client sender_client;
    std::mutex mtx;

public:
    MiddlewareNode(const std::string& broker, const std::string& nodeId, 
                  const std::vector<std::string>& nodes)
        : id(nodeId),
          client(broker, "middleware2_" + nodeId),
          sender_client(broker, "middleware2_sender_" + nodeId),
          clusterNodes(nodes) 
    {
        isLeader = (nodeId == "node1"); // Primeiro nó é líder por padrão
    }

    void start() {
        client.connect()->wait();
        sender_client.connect()->wait();

        client.subscribe("iot/input", 1)->wait(); // Escuta igual middleware1
        std::cout << "[Middleware2] Subscribed to topic: iot/input" << std::endl;
        
        if (isLeader) {
            std::cout << "[Middleware2] Starting as LEADER node" << std::endl;
        } else {
            std::cout << "[Middleware2] Starting as FOLLOWER node" << std::endl;
        }

        while (true) {
            auto msg = client.consume_message();
            
            if (msg) {
                std::cout << "[Middleware2] Message received on topic '" 
                          << msg->get_topic() << "': " << msg->to_string() << std::endl;
                processMessage(msg->to_string());
            }
            
            checkLeaderHealth();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

private:
    void processMessage(const std::string& payload) {
        std::lock_guard<std::mutex> lock(mtx);
        
        try {
            if (isLeader) {
                // Processa e replica para os followers
                std::cout << "[Middleware2] Leader processing: " << payload << std::endl;
                
                for (const auto& node : clusterNodes) {
                    if (node != id) {
                        std::cout << "[Middleware2] Replicating to " << node << std::endl;
                    }
                }
                
                // Envio para receiver
                sender_client.publish("iot/data", payload, 1, false)->wait();
                std::cout << "[Middleware2] Forwarded to receiver" << std::endl;
            } else {
                std::cout << "[Middleware2] Forwarding to leader: " << payload << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[Middleware2] Error: " << e.what() << std::endl;
        }
    }

    void checkLeaderHealth() {
        if (!isLeader) {
            static int counter = 0;
            if (++counter % 10 == 0) {
                std::cout << "[Middleware2] Leader failure detected! Starting election..." << std::endl;
                startElection();
            }
        }
    }

    void startElection() {
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& node : clusterNodes) {
            if (node != "node1") {
                isLeader = (node == id);
                if (isLeader) {
                    std::cout << "[Middleware2] Elected as new LEADER" << std::endl;
                    break;
                }
            }
        }
    }
};

int main() {
    std::vector<std::string> clusterNodes = {"node1", "node2", "node3"};
    MiddlewareNode node("tcp://mosquitto:1883", "node1", clusterNodes);
    node.start();
    return 0;
}
