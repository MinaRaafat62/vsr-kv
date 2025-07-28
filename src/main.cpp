#include "io_uring_loop.hpp"
#include "tcp_server.hpp"
#include "replica_manager.hpp"
#include "protocol_handler.hpp"
#include "vsr_message.hpp"
#include "tcp_connection.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <map>

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <self_replica_id>" << std::endl;
        return 1;
    }

    int self_id = std::stoi(argv[1]);

    std::map<int, std::pair<std::string, int>> cluster_config = {
        {0, {"localhost", 8080}},
        {1, {"localhost", 8081}},
        {2, {"localhost", 8082}}};

    if (cluster_config.find(self_id) == cluster_config.end())
    {
        std::cerr << "Replica ID " << self_id << " not found in cluster configuration." << std::endl;
        return 1;
    }

    int self_port = cluster_config[self_id].second;

    std::vector<Peer> peers_to_connect;
    for (const auto &[id, config] : cluster_config)
    {
        if (id != self_id)
        {
            peers_to_connect.push_back({id, config.first, config.second});
        }
    }

    try
    {
        IoUringLoop loop;
        TcpServer server(loop, self_port);
        auto manager = std::make_unique<ReplicaManager>(loop, server, self_id, std::move(peers_to_connect));
        
        // Create the protocol handler, which links itself to the server.
        protocol_handler handler(server);

        // The application layer now sets a single callback on the protocol_handler.
        handler.set_on_message_received([&handler, self_id](std::shared_ptr<TcpConnection> conn, vsr_message& msg)
        {
            // The application can now inspect the connection state to decide what to do.
            if (conn->get_state() == TcpConnection::State::CLIENT)
            {
                std::cout << "[Replica " << self_id << "] Received from CLIENT: " 
                          << "Command=" << static_cast<int>(msg.header.command_) 
                          << ", Op=" << msg.header.op << std::endl;
                
            }
            else if (conn->get_state() == TcpConnection::State::PEER)
            {
                std::cout << "[Replica " << self_id << "] Received from PEER: " 
                          << "Command=" << static_cast<int>(msg.header.command_)
                          << ", Op=" << msg.header.op << std::endl;
                
            }
        });

        server.set_replica_manager(std::move(manager));
        server.start();
        loop.run();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Critical error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}