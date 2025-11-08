#ifndef REPLICA_HPP
#define REPLICA_HPP

#include "io_uring_loop.hpp"
#include "tcp_server.hpp"
#include "replica_manager.hpp"
#include "protocol_handler.hpp"
#include "replica_state.hpp"
#include <deque>
#include <memory>
#include <utility>


struct inbound_message {
    std::shared_ptr<TcpConnection> connection;
    vsr_message message;
};

enum class StateAction {
    PROCESS_NORMALLY,
    INITIATE_VIEW_CHANGE,
    INITIATE_RECOVERY
};

class Replica {
public:
    Replica(int id, std::vector<peer_config> cluster_config);
    void run();

    void enqueue_message(std::shared_ptr<TcpConnection> connection, vsr_message& message);

private:
    void setup_networking();
    void process_message_queue();

    void handle_ping(const inbound_message& inbound);
    void handle_request(const inbound_message& inbound);
    void handle_prepare(const inbound_message& inbound);
    void handle_prepare_ok(const inbound_message& inbound);
    void handle_commit(const inbound_message& inbound);
    void handle_start_view_change(const inbound_message& inbound);
    void handle_do_view_change(const inbound_message& inbound);
    void handle_start_view(const inbound_message& inbound);
    void handle_recovery_request(const inbound_message& inbound);
    void handle_recovery_response(const inbound_message& inbound);

    bool check_can_process_request(const vsr_message& msg) const;
    bool check_can_process_prepare(const vsr_message& msg) const;
    bool check_can_process_prepare_ok(const vsr_message& msg) const;
    bool check_can_process_commit(const vsr_message& msg) const;
    bool check_can_process_start_view_change(const vsr_message& msg) const;
    bool check_can_process_do_view_change(const vsr_message& msg) const;
    bool check_can_process_start_view(const vsr_message& msg) const;
    bool check_can_process_recovery_request(const vsr_message& msg) const;
    bool check_can_process_recovery_response(const vsr_message& msg) const;

    void execute_commited_ops();
    void advance_primary_commit_number();
    bool can_process(const inbound_message& inbound);

    void schedule_heartbeat();
    void send_commit_message_if_needed();

    void initiate_view_change();
    void initiate_recovery();

    StateAction determine_state_action(const vsr_message& msg) const;

    void schedule_liveness_check();
    void check_primary_liveness();
    std::chrono::steady_clock::time_point last_primary_contact_;

    IoUringLoop loop_;
    ReplicaState state_;
    std::unique_ptr<TcpServer> server_;
    std::unique_ptr<ReplicaManager> manager_;
    std::unique_ptr<ProtocolHandler> protocol_handler_;

    std::map<utilities::uint128_t, std::shared_ptr<TcpConnection>> client_connections_;
    std::deque<inbound_message> message_queue_;
    std::mutex queue_mutex_;

    const std::chrono::milliseconds heartbeat_interval_{200};
    const std::chrono::milliseconds liveness_check_interval_{200};
    const std::chrono::milliseconds primary_timeout_{2000}; 
    const std::chrono::milliseconds recovery_interval_{3000};
    
};

#endif // REPLICA_HPP