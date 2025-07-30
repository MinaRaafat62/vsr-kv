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

    bool check_can_process_request(const vsr_message& msg) const;
    bool check_can_process_prepare(const vsr_message& msg) const;
    bool check_can_process_prepare_ok(const vsr_message& msg) const;
    bool check_can_process_commit(const vsr_message& msg) const;
    // bool check_can_process_start_view_change(const vsr_message& msg) const;
    // bool check_can_process_do_view_change(const vsr_message& msg) const;
    // bool check_can_process_start_view(const vsr_message& msg) const;

    void execute_commited_ops();
    bool can_process(const inbound_message& inbound);

    void schedule_heartbeat();
    void send_commit_message_if_needed();


    IoUringLoop loop_;
    ReplicaState state_;
    std::unique_ptr<TcpServer> server_;
    std::unique_ptr<ReplicaManager> manager_;
    std::unique_ptr<ProtocolHandler> protocol_handler_;

    std::map<utilities::uint128_t, std::shared_ptr<TcpConnection>> client_connections_;
    std::deque<inbound_message> message_queue_;
    std::mutex queue_mutex_;

    const std::chrono::milliseconds heartbeat_interval_{1000};
    uint64_t last_broadcast_op_ = 0;
    
};

#endif // REPLICA_HPP