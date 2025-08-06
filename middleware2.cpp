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
    std::mutex mtx;

public:
    MiddlewareNode(const std::string& broker, const std::string& nodeId, 
                  const std::vector<std::string>& nodes)
        : id(nodeId), client(broker, "middleware2_" + nodeId), clusterNodes(nodes) 
    {
        isLeader = (nodeId == "node1"); // Primeiro n� � l�der por padr�o
    }

    void start() {
        client.connect()->wait();
        client.subscribe("iot/data", 1)->wait();
        
        if (isLeader) {
            std::cout << "Starting as LEADER node" << std::endl;
        } else {
            std::cout << "Starting as FOLLOWER node" << std::endl;
        }

        while (true) {
            auto msg = client.consume_message();
            
            if (msg) {
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
                std::cout << "Leader processing: " << payload << std::endl;
                
                // Simula replica��o
                for (const auto& node : clusterNodes) {
                    if (node != id) {
                        std::cout << "Replicating to " << node << std::endl;
                    }
                }
                
                // Simula envio para receiver
                std::cout << "Forwarding to receiver" << std::endl;
            } else {
                // Encaminha para o l�der
                std::cout << "Forwarding to leader: " << payload << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    void checkLeaderHealth() {
        if (!isLeader) {
            // Simula��o: 10% de chance de detectar falha do l�der
            static int counter = 0;
            if (++counter % 10 == 0) {
                std::cout << "Leader failure detected! Starting election..." << std::endl;
                startElection();
            }
        }
    }

    void startElection() {
        std::lock_guard<std::mutex> lock(mtx);
        
        // L�gica simplificada de elei��o RAFT
        // Neste exemplo, o primeiro n� da lista que n�o � o l�der atual se torna o novo l�der
        for (const auto& node : clusterNodes) {
            if (node != "node1") { // node1 era o l�der original
                isLeader = (node == id);
                if (isLeader) {
                    std::cout << "Elected as new LEADER" << std::endl;
                    break;
                }
            }
        }
    }
};

int main() {
    std::vector<std::string> clusterNodes = {"node1", "node2", "node3"};
    
    // Cada inst�ncia deve ter um ID diferente (passado como argumento em produ��o)
    MiddlewareNode node("tcp://mosquitto:1883", "node1", clusterNodes);
    node.start();
    
    return 0;
}