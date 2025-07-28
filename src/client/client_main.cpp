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

struct ReplicaInfo {
    int id;
    std::string host;
    int port;
};

// --- Shared State ---
std::map<int, std::shared_ptr<TcpClient>> clients;
std::atomic<int> current_target_replica_id = 0; // Start by targeting replica 0
std::mutex clients_mutex;
std::mutex cout_mutex;

// --- Helper Functions ---

void print_help() {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\nAvailable commands:\n"
              << "  connect          - Attempt to connect to all known replicas.\n"
              << "  status           - Show connection status and current target.\n"
              << "  ping             - Send a ping to the current target replica.\n"
              << "  get <key>        - Request a key from the current target.\n"
              << "  set <key> <val>  - Set a key-value via the current target.\n"
              << "  help             - Show this help message.\n"
              << "  exit / quit      - Close the client.\n" << std::endl;
}

// Advances the target to the next replica in a round-robin fashion.
void advance_to_next_replica(int total_replicas) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    int old_target = current_target_replica_id.load();
    int new_target = (old_target + 1) % total_replicas;
    current_target_replica_id.store(new_target);
    
    std::lock_guard<std::mutex> cout_lock(cout_mutex);
    std::cout << "\n[FAILOVER] Target " << old_target << " is down. Advancing to target replica " << new_target << "." << std::endl;
    std::cout << "> " << std::flush;
}

// Sends a message to the CURRENT target replica, handling failover.
void send_message(const vsr_message& msg, int total_replicas) {
    std::shared_ptr<TcpClient> client_to_use;
    int target_id = current_target_replica_id.load();

    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        auto it = clients.find(target_id);
        if (it == clients.end() || !it->second->is_connected()) {
            // The target is already known to be disconnected, advance immediately.
            // We call the advance function outside the lock to avoid recursive locking.
        } else {
            client_to_use = it->second;
        }
    }

    if (client_to_use) {
        auto serialized_msg = msg.serialize();
        std::vector<char> char_vec(serialized_msg.begin(), serialized_msg.end());
        client_to_use->send(char_vec);
    } else {
        // If we couldn't get a client, it means it's disconnected.
        advance_to_next_replica(total_replicas);
    }
}

// --- Main Application ---

int main() {
    const std::vector<ReplicaInfo> cluster_config = {
        {0, "localhost", 8080},
        {1, "localhost", 8081},
        {2, "localhost", 8082}
    };
    const int total_replicas = cluster_config.size();

    IoUringLoop loop;
    std::thread network_thread([&loop]() {
        try { loop.run(); } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Critical network loop error: " << e.what() << std::endl;
        }
    });

    print_help();

    std::string line;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "> " << std::flush;
        }
        
        if (!std::getline(std::cin, line)) break;

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
            for(const auto& config : cluster_config) {
                auto it = clients.find(config.id);
                bool connected = (it != clients.end() && it->second->is_connected());
                std::cout << "  Replica " << config.id << ": " << (connected ? "CONNECTED" : "DISCONNECTED") << std::endl;
            }
            std::cout << "---------------------" << std::endl;
        } else if (command == "connect") {
            std::lock_guard<std::mutex> lock(clients_mutex);
            std::cout << "Attempting to connect to all replicas..." << std::endl;
            for (const auto& replica : cluster_config) {
                auto client = std::make_shared<TcpClient>(loop, replica.host, replica.port);
                clients[replica.id] = client;
                int replica_id = replica.id;

                client->set_on_connect([replica_id](auto conn) {
                    std::lock_guard<std::mutex> cout_lock(cout_mutex);
                    std::cout << "\n[INFO] Successfully connected to Replica " << replica_id << std::endl;
                    std::cout << "> " << std::flush;
                });

                // FIX: On disconnect, check if it was our target. If so, failover.
                client->set_on_disconnect([replica_id, total_replicas](auto conn) {
                    std::lock_guard<std::mutex> cout_lock(cout_mutex);
                    std::cout << "\n[INFO] Disconnected from Replica " << replica_id << std::endl;
                    if (current_target_replica_id.load() == replica_id) {
                        // We can't call advance_to_next_replica directly as it would lock the same mutex.
                        // Instead, we just print a message. The next send will trigger the advance.
                        std::cout << "[FAILOVER] Current target has disconnected." << std::endl;
                    }
                    std::cout << "> " << std::flush;
                });

                client->set_on_message([replica_id](const std::vector<char>& data) {
                    vsr_message msg;
                    std::lock_guard<std::mutex> cout_lock(cout_mutex);
                    std::cout << "\n[RECV from Replica " << replica_id << "] ";
                    if (vsr_message::deserialize(data, msg)) {
                        std::cout << "Cmd: " << static_cast<int>(msg.header.command_)
                                  << ", Op: " << msg.header.op;
                        if (!msg.payload.empty()) {
                            std::string payload_str(msg.payload.begin(), msg.payload.end());
                            std::cout << ", Payload: '" << payload_str << "'";
                        }
                        std::cout << std::endl;
                    } else {
                        std::cerr << "Failed to deserialize message." << std::endl;
                    }
                    std::cout << "> " << std::flush;
                });

                client->connect();
            }
        } else if (command == "ping") {
            vsr_message msg;
            msg.header.command_ = command::ping;
            send_message(msg, total_replicas);
        } else if (command == "get") {
            std::string key;
            if (!(ss >> key)) {
                std::cerr << "Usage: get <key>" << std::endl;
            } else {
                vsr_message msg;
                msg.header.command_ = command::request;
                msg.header.operation_ = operation::get;
                msg.payload.assign(key.begin(), key.end());
                send_message(msg, total_replicas);
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
                send_message(msg, total_replicas);
            }
        } else if (!command.empty()) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Unknown command: '" << command << "'. Type 'help' for a list of commands." << std::endl;
        }
    }

    network_thread.detach();
    std::cout << "Exiting client." << std::endl;
    return 0;
}