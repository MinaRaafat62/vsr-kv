#include "io_uring_loop.hpp"
#include "tcp_server.hpp"
#include "tcp_connection.hpp"
#include "replica_manager.hpp"
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

        server.set_on_client_message([&server, self_id](std::shared_ptr<TcpConnection> conn, const std::vector<char> &data)
        {
        std::string message(data.begin(), data.end());
        std::cout << "[Replica " << self_id << "] Received message from CLIENT: '" << message << "'" << std::endl;
        std::cout << "[Replica " << self_id << "] Broadcasting client message to peers..." << std::endl;
        server.broadcast_to_peers(data); 
        });

        // This handler is for messages from other REPLICAS
        server.set_on_peer_message([self_id](std::shared_ptr<TcpConnection> conn, const std::vector<char> &data)
        {
            std::string message(data.begin(), data.end());
            std::cout << "[Replica " << self_id << "] Received message from PEER: '" << message << "'" << std::endl;

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