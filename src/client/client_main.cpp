#include "io_uring_loop.hpp"
#include "tcp_client.hpp"
#include "vsr_message.hpp"
#include "utilities.hpp"

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

const std::vector<ReplicaInfo> CLUSTER_CONFIG = {
    {0, "localhost", 8080},
    {1, "localhost", 8081},
    {2, "localhost", 8082}
};


std::map<int, std::shared_ptr<TcpClient>> clients;
std::atomic<int> current_target_replica_id = 0;
std::mutex clients_mutex;
std::mutex cout_mutex;

utilities::uint128_t self_client_id = {0xDEADBEEF, 0xCAFEF00D};
std::atomic<uint32_t> next_request_number = 1;


void send_message(vsr_message& msg, int total_replicas);


void print_help() {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\nAvailable commands:\n"
              << "  status           - Show connection status and current target.\n"
              << "  ping             - Send a ping to the current target replica.\n"
              << "  get <key>        - Request a key from the cluster.\n"
              << "  set <key> <val>  - Set a key-value via the cluster.\n"
              << "  help             - Show this help message.\n"
              << "  exit / quit      - Close the client.\n" << std::endl;
}

void advance_to_next_replica(int total_replicas) {
    // This function is now only called from within send_message, which handles the output
    int old_target = current_target_replica_id.load();
    int new_target = (old_target + 1) % total_replicas;
    current_target_replica_id.store(new_target);
}

// Sends a message to the cluster, handling failover and retries automatically.
void send_message(vsr_message& msg, int total_replicas) {
    // Assign a unique request number once before any attempts are made.
    // This ensures the operation is idempotent if retried on a different server.
    msg.header.client = self_client_id;
    msg.header.request = next_request_number++;

    // Loop to attempt sending the message, once for each possible replica.
    for (int attempt = 0; attempt < total_replicas; ++attempt) {
        std::shared_ptr<TcpClient> client_to_use;
        int target_id = current_target_replica_id.load();

        { // Scope for the mutex lock
            std::lock_guard<std::mutex> lock(clients_mutex);
            auto it = clients.find(target_id);
            if (it != clients.end() && it->second->is_connected()) {
                client_to_use = it->second;
            }
        }

        if (client_to_use) {
            // If we found a connected client, send the message and exit the function.
            auto serialized_msg = msg.serialize();
            std::vector<char> char_vec(serialized_msg.begin(), serialized_msg.end());
            client_to_use->send(char_vec);
            return; // Success!
        } else {
            // The current target is not connected. Announce failover and try the next one.
            {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "\n[FAILOVER] Target " << target_id << " is disconnected. Advancing..." << std::endl;
                std::cout << "> " << std::flush;
            }
            advance_to_next_replica(total_replicas);
            // The loop will now try again with the new target_id.
        }
    }

    // If the loop completes without returning, it means we have tried every replica and failed.
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cerr << "\n[ERROR] Could not send message. All replicas appear to be down." << std::endl;
    std::cout << "> " << std::flush;
}



void handle_user_input() {
    const int total_replicas = CLUSTER_CONFIG.size();
    print_help();

    std::string line;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "> " << std::flush;
        }

        if (!std::getline(std::cin, line) || line == "exit" || line == "quit") {
            break;
        }

        std::stringstream ss(line);
        std::string command;
        ss >> command;

        if (command.empty()) {
            continue;
        }

        if (command == "help") {
            print_help();
        } else if (command == "status") {
            std::lock_guard<std::mutex> lock(clients_mutex);
            std::lock_guard<std::mutex> cout_lock(cout_mutex);
            std::cout << "--- Client Status ---" << std::endl;
            std::cout << "  Current Target: Replica " << current_target_replica_id.load() << std::endl;
            for(const auto& config : CLUSTER_CONFIG) {
                auto it = clients.find(config.id);
                bool connected = (it != clients.end() && it->second->is_connected());
                std::cout << "  Replica " << config.id << " (" << config.host << ":" << config.port << "): "
                          << (connected ? "CONNECTED" : "DISCONNECTED") << std::endl;
            }
            std::cout << "---------------------" << std::endl;
        } else if (command == "ping") {
            vsr_message msg;
            msg.header.command_ = command::ping;
            send_message(msg, total_replicas);
        } else if (command == "get") {
            std::string key;
            if (!(ss >> key)) {
                std::lock_guard<std::mutex> lock(cout_mutex);
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
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cerr << "Usage: set <key> <value>" << std::endl;
            } else {
                vsr_message msg;
                msg.header.command_ = command::request;
                msg.header.operation_ = operation::set;
                std::string payload_str = key + " " + value;
                msg.payload.assign(payload_str.begin(), payload_str.end());
                send_message(msg, total_replicas);
            }
        } else {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Unknown command: '" << command << "'. Type 'help' for a list of commands." << std::endl;
        }
    }
}

int main() {
    IoUringLoop loop;
    std::thread network_thread([&loop]() {
        try { loop.run(); } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Critical network loop error: " << e.what() << std::endl;
        }
    });

    std::cout << "[SYSTEM] Client starting. Automatically connecting to all replicas..." << std::endl;
    for (const auto& replica_info : CLUSTER_CONFIG) {
        auto client = std::make_shared<TcpClient>(loop, replica_info.host, replica_info.port);

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            clients[replica_info.id] = client;
        }

        int replica_id = replica_info.id;
        int total_replicas = CLUSTER_CONFIG.size();

        client->set_on_connect([replica_id](auto) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "\n[INFO] Successfully connected to Replica " << replica_id << "." << std::endl;
            std::cout << "> " << std::flush;
        });

        client->set_on_disconnect([replica_id](auto) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "\n[INFO] Disconnected from Replica " << replica_id << "." << std::endl;
            std::cout << "> " << std::flush;
        });

        client->set_on_message([replica_id](const std::vector<char>& data) {
            vsr_message msg;
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "\n[RECV from Replica " << replica_id << "] ";
            if (vsr_message::deserialize(data, msg)) {
                std::string payload_str(msg.payload.begin(), msg.payload.end());
                std::cout << "Reply: '" << payload_str << "'" << std::endl;
            } else {
                std::cerr << "Failed to deserialize message." << std::endl;
            }
            std::cout << "> " << std::flush;
        });

        client->connect();
    }

    handle_user_input();

    std::cout << "\nExiting client." << std::endl;
    // It's generally better to let the program exit cleanly, which will stop all threads.
    // Detaching can sometimes hide shutdown issues. Let's make it joinable if possible,
    // but for a simple client, exiting the main thread is sufficient to terminate the process.
    if (network_thread.joinable()) {
        // In a more complex app, you would signal the loop to stop and then join.
        // For this example, we'll just detach as the program is ending anyway.
        network_thread.detach();
    }

    return 0;
}