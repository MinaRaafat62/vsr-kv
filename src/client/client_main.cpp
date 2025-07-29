#include "io_uring_loop.hpp"
#include "tcp_client.hpp"
#include "vsr_message.hpp"
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <mutex>
#include <atomic>
#include <memory>
#include <chrono>
#include <cstring>

// --- Configuration ---

struct ReplicaInfo {
    int id;
    std::string host;
    int port;
};

const std::vector<ReplicaInfo> CLUSTER_CONFIG = {
    {0, "localhost", 8080},
    {1, "localhost", 8081},
    {2, "localhost", 8082}
};

const std::chrono::seconds RECONNECT_DELAY = std::chrono::seconds(5);

// --- Shared State & Mutexes ---

// Holds the TCP client objects, mapped by replica ID.
std::map<int, std::shared_ptr<TcpClient>> clients;
std::mutex clients_mutex;

// Buffers for handling TCP stream data, mapped by replica ID.
std::map<int, std::vector<char>> client_side_buffers;
std::mutex buffer_mutex;

// For preventing garbled output from multiple threads.
std::mutex cout_mutex;

// The replica we are currently sending requests to.
std::atomic<int> current_target_replica_id = 0;

// --- Helper Functions ---

void print_help() {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\nAvailable commands:\n"
              << "  status           - Show connection status and current target.\n"
              << "  ping             - Send a ping to the current target replica.\n"
              << "  get <key>        - Request a key from the current target.\n"
              << "  set <key> <val>  - Set a key-value via the current target.\n"
              << "  help             - Show this help message.\n"
              << "  exit / quit      - Close the client.\n" << std::endl;
}

// Schedules a reconnect attempt on the network thread after a delay.
void schedule_reconnect(IoUringLoop& loop, std::shared_ptr<TcpClient> client, int replica_id) {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\n[SYSTEM] Scheduling reconnect for Replica " << replica_id << " in " << RECONNECT_DELAY.count() << " seconds." << std::endl;
    std::cout << "> " << std::flush;

    // Post a task to the network loop to set a timeout.
    loop.post([&loop, client, replica_id]() {
        loop.submit_timeout(RECONNECT_DELAY, [client, replica_id](int result) {
            {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "\n[SYSTEM] Attempting to reconnect to Replica " << replica_id << "..." << std::endl;
                std::cout << "> " << std::flush;
            }
            client->connect();
        });
    });
}

// Advances the target to the next replica in a round-robin fashion.
void advance_to_next_replica() {
    int old_target = current_target_replica_id.load();
    int new_target = (old_target + 1) % CLUSTER_CONFIG.size();
    current_target_replica_id.store(new_target);
    
    std::lock_guard<std::mutex> cout_lock(cout_mutex);
    std::cout << "\n[FAILOVER] Connection to target " << old_target << " is down. Advancing to replica " << new_target << "." << std::endl;
    std::cout << "> " << std::flush;
}

// Sends a message to the current target, handling failover if disconnected.
void send_message_to_target(const vsr_message& msg) {
    std::shared_ptr<TcpClient> client_to_use;
    int target_id = current_target_replica_id.load();
    bool needs_failover = false;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        auto it = clients.find(target_id);
        if (it == clients.end() || !it->second->is_connected()) {
            needs_failover = true;
        } else {
            client_to_use = it->second;
        }
    }

    if (client_to_use) {
        auto serialized_msg = msg.serialize();
        std::vector<char> char_vec(serialized_msg.begin(), serialized_msg.end());
        client_to_use->send(char_vec);
    } else if (needs_failover) {
        advance_to_next_replica();
    }
}


int main() {
    IoUringLoop loop;
    std::thread network_thread([&loop]() {
        try {
            loop.run();
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Critical network loop error: " << e.what() << std::endl;
        }
    });

    std::cout << "[SYSTEM] Client starting. Automatically connecting to all replicas..." << std::endl;

    // --- Automatic Connection Setup ---
    for (const auto& replica_info : CLUSTER_CONFIG) {
        auto client = std::make_shared<TcpClient>(loop, replica_info.host, replica_info.port);
        int replica_id = replica_info.id;

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            clients[replica_id] = client;
        }

        client->set_on_connect([replica_id](auto conn) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "\n[INFO] Successfully connected to Replica " << replica_id << "." << std::endl;
            std::cout << "> " << std::flush;
        });

        auto on_failure = [&loop, client, replica_id](auto conn) {
            {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "\n[INFO] Disconnected from Replica " << replica_id << "." << std::endl;
            }
             {
                std::lock_guard<std::mutex> lock(buffer_mutex);
                client_side_buffers.erase(replica_id);
            }
            if (current_target_replica_id.load() == replica_id) {
                advance_to_next_replica();
            }
            schedule_reconnect(loop, client, replica_id);
        };
        
        client->set_on_disconnect(on_failure);
        client->set_on_connect_failed([&loop, client, replica_id]() {
             {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "\n[INFO] Failed to connect to Replica " << replica_id << "." << std::endl;
            }
            schedule_reconnect(loop, client, replica_id);
        });

        client->set_on_message([replica_id](const std::vector<char>& data) {
            std::lock_guard<std::mutex> lock(buffer_mutex);
            auto& buffer = client_side_buffers[replica_id];
            buffer.insert(buffer.end(), data.begin(), data.end());

            while (true) {
                if (buffer.size() < sizeof(vsr_header)) break;

                vsr_header header;
                std::memcpy(&header, buffer.data(), sizeof(vsr_header));
                if (header.size == 0 || header.size > 65536) {
                    buffer.clear();
                    break;
                }

                if (buffer.size() < header.size) break;
                
                std::vector<char> full_message(buffer.begin(), buffer.begin() + header.size);
                buffer.erase(buffer.begin(), buffer.begin() + header.size);

                vsr_message msg;
                {
                    std::lock_guard<std::mutex> cout_lock(cout_mutex);
                    std::cout << "\n[RECV from Replica " << replica_id << "] ";
                    if (vsr_message::deserialize(full_message, msg)) {
                        std::cout << "Cmd: " << static_cast<int>(msg.header.command_)
                                  << ", Op: " << msg.header.op;
                        if (!msg.payload.empty()) {
                            std::string p(msg.payload.begin(), msg.payload.end());
                            std::cout << ", Payload: '" << p << "'";
                        }
                    } else {
                        std::cout << "Deserialization Error!";
                    }
                    std::cout << std::endl << "> " << std::flush;
                }
            }
        });
        
        // Initial connection attempt
        client->connect();
    }

    print_help();

    // --- User Command Loop ---
    std::string line;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "> " << std::flush;
        }
        
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string command;
        ss >> command;

        if (command == "exit" || command == "quit") {
            break;
        } else if (command == "help") {
            print_help();
        } else if (command == "status") {
            std::lock_guard<std::mutex> lock(clients_mutex);
            std::lock_guard<std::mutex> cout_lock(cout_mutex);
            std::cout << "--- Client Status ---" << std::endl;
            std::cout << "  Current Target: Replica " << current_target_replica_id.load() << std::endl;
            for(const auto& config : CLUSTER_CONFIG) {
                auto it = clients.find(config.id);
                bool connected = (it != clients.end() && it->second->is_connected());
                std::cout << "  Replica " << config.id << ": " << (connected ? "CONNECTED" : "DISCONNECTED") << std::endl;
            }
            std::cout << "---------------------" << std::endl;
        } else if (command == "ping") {
            vsr_message msg;
            msg.header.command_ = command::ping;
            send_message_to_target(msg);
        } else if (command == "get") {
            std::string key;
            if (!(ss >> key)) {
                std::cerr << "Usage: get <key>" << std::endl;
            } else {
                vsr_message msg;
                msg.header.command_ = command::request;
                msg.header.operation_ = operation::get;
                msg.payload.assign(key.begin(), key.end());
                send_message_to_target(msg);
            }
        } else if (command == "set") {
            std::string key, value;
            if (!(ss >> key >> value)) {
                std::cerr << "Usage: set <key> <value>" << std::endl;
            } else {
                vsr_message msg;
                msg.header.command_ = command::request;
                msg.header.operation_ = operation::set;
                std::string payload_str = key + " " + value;
                msg.payload.assign(payload_str.begin(), payload_str.end());
                send_message_to_target(msg);
            }
        } else {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Unknown command: '" << command << "'. Type 'help' for a list of commands." << std::endl;
        }
    }

    // A clean exit is difficult with a detached thread, so we'll just exit.
    // In a production app, you would signal the network thread to shut down gracefully.
    std::cout << "Exiting client." << std::endl;
    // Detaching the thread means we don't wait for it. The OS will clean it up.
    network_thread.detach(); 
    return 0;
}