#include "replica.hpp"
#include <iostream>
#include <vector>

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <self_replica_id>" << std::endl;
        return 1;
    }

    int self_id = std::stoi(argv[1]);

    // Define the cluster configuration
    std::vector<peer_config> cluster_config = {
        {0, "localhost", 8080},
        {1, "localhost", 8081},
        {2, "localhost", 8082}
    };

    bool id_found = false;
    for(const auto& peer : cluster_config) {
        if (peer.id == self_id) {
            id_found = true;
            break;
        }
    }

    if (!id_found) {
        std::cerr << "Replica ID " << self_id << " not found in cluster configuration." << std::endl;
        return 1;
    }

    try {
        // Create the main Replica object.
        Replica replica(self_id, cluster_config);
        
        // Run the replica. This will start the network loop and begin processing.
        replica.run();
    } catch (const std::exception &e) {
        std::cerr << "Critical error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}