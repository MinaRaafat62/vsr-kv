#ifndef REPLICA_STATE_HPP
#define REPLICA_STATE_HPP
#include "vsr_message.hpp"
#include "replica_types.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include <set>





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
    uint64_t last_executed_op_ = 0;

    std::map<uint64_t, log_entry> log_;
    std::map<utilities::uint128_t, client_table_entry> client_table_;
    std::map<std::string, std::string> state_machine_;
    utilities::uint128_t recovery_nonce_ = {0,0};
    uint32_t last_normal_view_ = 0;
    std::set<int> start_view_change_received_;
    std::vector<vsr_message> do_view_change_received_;


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

    void enter_new_view(uint32_t new_view) {
        if (status_ == ReplicaStatus::NORMAL) {
            last_normal_view_ = view_;
        }
        view_ = new_view;
        status_ = ReplicaStatus::VIEW_CHANGE;
        start_view_change_received_.clear();
        do_view_change_received_.clear();
        start_view_change_received_.insert(id_); // adding self to the start view change received
    }

    int get_primary_id_for_view(uint32_t view_num) const {
        const size_t cluster_size = cluster_config_.size();
        if (cluster_size == 0) {
            return -1; 
        }
        return view_num % cluster_size;
    }
};


#endif // REPLICA_STATE_HPP