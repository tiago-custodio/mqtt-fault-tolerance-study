// middleware3_pipeline.cpp
#include <iostream>
#include <vector>
#include <functional>
#include <memory>
#include <mqtt/async_client.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class PipelineStage {
public:
    virtual ~PipelineStage() = default;
    virtual std::string process(const std::string& input) = 0;
    virtual bool isHealthy() const { return true; }
};

class ValidationStage : public PipelineStage {
public:
    std::string process(const std::string& input) override {
        try {
            auto j = json::parse(input);
            if (!j.contains("device_id") || !j.contains("temperature")) {
                throw std::runtime_error("Invalid message format");
            }
            return input;
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("Validation failed: ") + e.what());
        }
    }
};

class TransformationStage : public PipelineStage {
private:
    bool simulatedFailure = false;
public:
    std::string process(const std::string& input) override {
        if (simulatedFailure) {
            throw std::runtime_error("Simulated transformation failure");
        }
        
        auto j = json::parse(input);
        j["processed"] = true;
        j["server_timestamp"] = time(nullptr);
        return j.dump();
    }

    bool isHealthy() const override {
        // 20% chance de reportar falha
        static int counter = 0;
        if (++counter % 5 == 0) {
            return false;
        }
        return true;
    }
};

class Supervisor {
public:
    std::unique_ptr<PipelineStage> restartStage(std::unique_ptr<PipelineStage> stage) {
        std::cout << "Restarting failed stage..." << std::endl;
        // Na prática, aqui criaria uma nova instância do estágio
        return std::move(stage);
    }
};

class MQTTMiddleware {
private:
    mqtt::async_client client;
    std::vector<std::unique_ptr<PipelineStage>> pipeline;
    Supervisor supervisor;

public:
    MQTTMiddleware(const std::string& brokerAddress) 
        : client(brokerAddress, "middleware3") 
    {
        // Configura pipeline
        pipeline.push_back(std::make_unique<ValidationStage>());
        pipeline.push_back(std::make_unique<TransformationStage>());
    }

    void start() {
        client.connect()->wait();
        client.subscribe("iot/data", 1)->wait();
        
        while (true) {
            auto msg = client.consume_message();
            
            if (msg) {
                processMessage(msg->to_string());
            }
            
            checkPipelineHealth();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

private:
    void processMessage(const std::string& payload) {
        try {
            std::string processed = payload;
            
            for (auto& stage : pipeline) {
                processed = stage->process(processed);
            }
            
            // Simula envio para receiver
            std::cout << "Processed message: " << processed << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Pipeline error: " << e.what() << std::endl;
        }
    }

    void checkPipelineHealth() {
        for (auto& stage : pipeline) {
            if (!stage->isHealthy()) {
                std::cout << "Stage failed, restarting..." << std::endl;
                stage = supervisor.restartStage(std::move(stage));
            }
        }
    }
};

int main() {
    MQTTMiddleware middleware("tcp://mosquitto:1883");
    middleware.start();
    return 0;
}