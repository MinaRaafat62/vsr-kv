#ifndef REPLICA_STATE_HPP
#define REPLICA_STATE_HPP
#include "vsr_message.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <vector>


struct log_entry {
    uint32_t view;
    vsr_message msg;
};

struct client_table_entry {
    uint32_t request_number = 0;
    bool executed = false;
    std::vector<byte> result;
};


enum class ReplicaStatus {
    NORMAL,
    VIEW_CHANGE,
    RECOVERY,
};

struct peer_config {
    int id;
    std::string host;
    int port;
};


class ReplicaState {
public:
    const int id_;
    const int fault_tolerance_f_;
    std::map<int, peer_config> cluster_config_;

    ReplicaStatus status_ = ReplicaStatus::NORMAL;
    uint32_t view_ = 0;
    uint64_t op_ = 0;
    uint64_t commit_ = 0;

    std::map<uint64_t, log_entry> log_;
    std::map<utilities::uint128_t, client_table_entry> client_table_;
    
    utilities::uint128_t recovery_nonce_ = {0,0};

    ReplicaState(int id, std::vector<peer_config> config)
        : id_(id),
          fault_tolerance_f_((config.size() - 1) / 2)
    {
        for (const auto& peer : config) {
            cluster_config_[peer.id] = peer;
        }
    }

    int get_primary_id() const {
        return view_ % (2 * fault_tolerance_f_ + 1);
    }

    bool is_primary() const {
        return id_ == get_primary_id();
    }
};


#endif // REPLICA_STATE_HPP