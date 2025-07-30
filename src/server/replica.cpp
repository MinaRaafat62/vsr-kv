#include "replica.hpp"
#include <iostream>
#include <thread>



Replica::Replica(int id, std::vector<peer_config> cluster_config) 
    : state_(id, cluster_config) {
    setup_networking();
}

void Replica::setup_networking() {
    const auto& self_config = state_.cluster_config_.at(state_.id_);
    server_ = std::make_unique<TcpServer>(loop_, self_config.port);
    std::vector<Peer> peers_to_connect;
    for (const auto& [id, peer_config] : state_.cluster_config_) {
        if (id != state_.id_) {
            peers_to_connect.push_back({id, peer_config.host, peer_config.port});
        }
    }

    manager_ = std::make_unique<ReplicaManager>(loop_, *server_, state_.id_, std::move(peers_to_connect));
    protocol_handler_ = std::make_unique<ProtocolHandler>(*server_);
    protocol_handler_->set_on_message_received([this](auto conn, auto& msg) {
        this->enqueue_message(conn, msg);
    });

    server_->set_replica_manager(std::move(manager_));

}


void Replica::enqueue_message(std::shared_ptr<TcpConnection> connection, vsr_message& message) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        message_queue_.push_back({std::move(connection), message});
    }
    // Post a task to the loop to process the queue. This ensures processing
    // happens on the main loop thread, maintaining single-threaded semantics for state changes.
    loop_.post([this]() {
        this->process_message_queue();
    });
}


void Replica::run() {
    std::cout << "[Replica " << state_.id_ << "] Starting up..." << std::endl;
    server_->start();
    schedule_heartbeat();
    loop_.run();
}

void Replica::schedule_heartbeat() {
    loop_.submit_timeout(heartbeat_interval_, [this](int result) {
        // This callback runs on the loop's thread.
        this->send_commit_message_if_needed();
        // Re-schedule the next heartbeat.
        this->schedule_heartbeat();
    });
}


void Replica::process_message_queue() {
    bool processed_in_pass = false;
    do {
        processed_in_pass = false;
        // We iterate through the queue, removing messages we can process.
        for (auto it = message_queue_.begin(); it != message_queue_.end(); ) {
            if (can_process(*it)) {
                inbound_message message_to_process = *it;
                
                // Remove from queue BEFORE processing
                it = message_queue_.erase(it); 
                
                switch (message_to_process.message.header.command_) {
                    case command::ping:
                        handle_ping(message_to_process);
                        break;
                    case command::request:
                        handle_request(message_to_process);
                        break;
                    case command::prepare:
                        handle_prepare(message_to_process);
                        break;
                    case command::prepare_ok:
                        handle_prepare_ok(message_to_process);
                        break;
                    case command::commit:
                        handle_commit(message_to_process);
                        break;
                    // ... other cases
                    default:
                        std::cout << "Unknown command" << std::endl;
                }
                processed_in_pass = true;
            } else {
                // Can't process this message yet, leave it in the queue.
                ++it;
            }
        }
        // If we processed a message, we loop again, because that processing
        // might have unblocked other messages in the queue.
    } while (processed_in_pass);
}


bool Replica::can_process(const inbound_message& inbound) {
    const auto& msg = inbound.message;
    switch (msg.header.command_) {
        case command::ping:
            return true;
        case command::pong:
            return true;
        case command::request:
            return check_can_process_request(msg);
        case command::prepare:
            return check_can_process_prepare(msg);
        case command::prepare_ok:
            return check_can_process_prepare_ok(msg);
        case command::commit:
            return check_can_process_commit(msg);
        default:
            return false;
    }
}




void Replica::handle_ping(const inbound_message& inbound) {
    const auto& ping_msg = inbound.message;
    std::cout << "[Replica " << state_.id_ << "] Received Ping. Sending Pong." << std::endl;
    vsr_message pong_msg;
    pong_msg.header.command_ = command::pong;
    pong_msg.header.view = state_.view_;
    pong_msg.header.replica = state_.id_;
    pong_msg.header.client = ping_msg.header.client;
    pong_msg.header.request = ping_msg.header.request;

    protocol_handler_->send_message(inbound.connection, pong_msg);
}


bool Replica::check_can_process_request(const vsr_message& msg) const {
    // TODO:: we will need to handle state transfer when the view larger than the current view
    return state_.status_ == ReplicaStatus::NORMAL &&
        state_.is_primary() &&
        msg.header.view == state_.view_;
}


void Replica::handle_request(const inbound_message &inbound) {
    const auto& request_msg = inbound.message;
    auto& client_entry = state_.client_table_[request_msg.header.client];
    if (client_entry.request_number > request_msg.header.request) {
        std::cout << "[Replica " << state_.id_ << "] Ignoring old request from client ."<< std::endl;
        return;
    }
    if (client_entry.request_number == request_msg.header.request) {
        std::cout << "[Replica " << state_.id_ << "] Request already processed. Sending cached result." << std::endl;
        vsr_message reply_msg;
        reply_msg.header.command_ = command::reply;
        reply_msg.header.client = request_msg.header.client;
        reply_msg.header.request = request_msg.header.request;
        reply_msg.payload = client_entry.result;

        protocol_handler_->send_message(inbound.connection, reply_msg);
        return;
    }
    std::cout << "[Replica " << state_.id_ << "] Processing new request from client." << std::endl;
    client_connections_[request_msg.header.client] = inbound.connection;
    state_.op_++;
    state_.log_[state_.op_] = {
        .view = state_.view_,
        .op_type = request_msg.header.operation_,
        .client_id = request_msg.header.client,
        .request_num = request_msg.header.request,
        .payload = request_msg.payload,
        .prepare_ok_acks = {state_.id_} // Primary acks its own op
    };

    client_entry.request_number = request_msg.header.request;
    client_entry.executed = false;
    client_entry.result.clear();

    vsr_message prepare_msg;
    prepare_msg.header.command_ = command::prepare;
    prepare_msg.header.operation_ = request_msg.header.operation_;
    prepare_msg.header.replica = state_.id_;
    prepare_msg.header.client = request_msg.header.client;
    prepare_msg.header.request = request_msg.header.request;
    prepare_msg.header.view = state_.view_;
    prepare_msg.header.op = state_.op_;
    prepare_msg.header.commit = state_.commit_;
    prepare_msg.payload = request_msg.payload;
    protocol_handler_->broadcast_to_peers(prepare_msg);
    std::cout << "[Replica " << state_.id_ << "] Primary prepared Op " << state_.op_ << std::endl;
}


bool Replica::check_can_process_prepare(const vsr_message &msg) const {
    return state_.status_ == ReplicaStatus::NORMAL &&
           !state_.is_primary() &&
           msg.header.view == state_.view_ &&
           msg.header.op == state_.op_ + 1;
}


void Replica::handle_prepare(const inbound_message &inbound) {
    const auto& prepare_msg = inbound.message;
    int primary_id = static_cast<int>(prepare_msg.header.replica);
    std::cout << "[Replica " << state_.id_ << "] Received Prepare for Op " 
              << prepare_msg.header.op << " from Primary " << primary_id << std::endl;

    state_.op_++;
    state_.log_[state_.op_] = {
        .view = state_.view_,
        .op_type = prepare_msg.header.operation_,
        .client_id = prepare_msg.header.client,
        .request_num = prepare_msg.header.request,
        .payload = prepare_msg.payload,
        .prepare_ok_acks = {} // Empty for backups
    };

    auto& client_entry = state_.client_table_[prepare_msg.header.client];
    client_entry.request_number = prepare_msg.header.request;
    client_entry.executed = false;
    client_entry.result.clear();

    vsr_message prepare_ok_msg;
    prepare_ok_msg.header.command_ = command::prepare_ok;
    prepare_ok_msg.header.view = state_.view_;
    prepare_ok_msg.header.op = state_.op_;
    prepare_ok_msg.header.replica = state_.id_;

    protocol_handler_->send_message(inbound.connection, prepare_ok_msg);

    if (prepare_msg.header.commit > state_.commit_){
        state_.commit_ = prepare_msg.header.commit;
        execute_commited_ops();
    }
}

bool Replica::check_can_process_prepare_ok(const vsr_message& msg) const {
    return state_.status_ == ReplicaStatus::NORMAL &&
           state_.is_primary() &&
           msg.header.view == state_.view_;
}


void Replica::handle_prepare_ok(const inbound_message& inbound) {
    const auto& ok_msg = inbound.message;
    uint64_t op_num = ok_msg.header.op;
    int backup_id = ok_msg.header.replica;
    std::cout << "[Replica " << state_.id_ << "] Received PrepareOk for Op " 
              << op_num << " from Replica " << backup_id << std::endl;
    auto it = state_.log_.find(op_num);

    if(it == state_.log_.end()){
        std::cerr << "Received PrepareOk for an unknown op: " << op_num << std::endl;
        return;
    }

    it->second.prepare_ok_acks.insert(backup_id);

    if (it->second.prepare_ok_acks.size() >= state_.fault_tolerance_f_ + 1) {
        std::cout << "[Replica " << state_.id_ << "] Op " << op_num << " is now committable." << std::endl;
        execute_commited_ops();
    }
}


bool Replica::check_can_process_commit(const vsr_message& msg) const {
    return state_.status_ == ReplicaStatus::NORMAL &&
           !state_.is_primary() &&
           msg.header.view == state_.view_;
}

void Replica::handle_commit(const inbound_message& inbound) {
    const auto& commit_msg = inbound.message;
    std::cout << "[Replica " << state_.id_ << "] Received explicit Commit message up to Op " << commit_msg.header.commit << std::endl;
    if (commit_msg.header.commit > state_.commit_) {
        state_.commit_ = commit_msg.header.commit;
        execute_commited_ops();
    }
}



void Replica::execute_commited_ops() {
    while(true) {
        uint64_t next_op_to_commit = state_.commit_ + 1;
        auto log_it = state_.log_.find(next_op_to_commit);
        if (log_it == state_.log_.end()) break;
        if (state_.is_primary() && log_it->second.prepare_ok_acks.size() < state_.fault_tolerance_f_ + 1) break;

        const auto& entry_to_execute = log_it->second;
        auto& client_entry = state_.client_table_[entry_to_execute.client_id];

        std::string result_str = "OK";

        if (!client_entry.executed) {
            std::cout << "[Replica " << state_.id_ << "] Executing Op " << next_op_to_commit << std::endl;
            
            std::string payload_str(entry_to_execute.payload.begin(), entry_to_execute.payload.end());
            std::stringstream ss(payload_str);
            std::string key, value;

            if (entry_to_execute.op_type == operation::set) {
                ss >> key >> value;
                state_.state_machine_[key] = value;
                result_str = "SET " + key + "=" + value;
            } else if (entry_to_execute.op_type == operation::get) {
                ss >> key;
                if (state_.state_machine_.count(key)) {
                    result_str = state_.state_machine_[key];
                } else {
                    result_str = "NOT_FOUND";
                }
            }

            client_entry.executed = true;
            client_entry.result.assign(result_str.begin(), result_str.end());
        }
        
        state_.commit_ = next_op_to_commit;

        if (state_.is_primary()) {
            auto client_conn_it = client_connections_.find(entry_to_execute.client_id);
            if (client_conn_it != client_connections_.end() && client_conn_it->second) {
                std::cout << "[Replica " << state_.id_ << "] Sending Reply for request " << entry_to_execute.request_num << " to client." << std::endl;
            }
            vsr_message reply_msg;
            reply_msg.header.command_ = command::reply;
            reply_msg.header.view = state_.view_;
            reply_msg.header.client = entry_to_execute.client_id;
            reply_msg.header.request = entry_to_execute.request_num;
            reply_msg.payload = client_entry.result;
            protocol_handler_->send_message(client_conn_it->second, reply_msg);
        }
    }
}


void Replica::send_commit_message_if_needed() {
    if (!state_.is_primary() || state_.status_ != ReplicaStatus::NORMAL) {
        return;
    }

    if (state_.commit_ > 0 && last_broadcast_op_ < state_.commit_) {
        std::cout << "[Replica " << state_.id_ << "] Idle. Sending explicit Commit message for commit_ " << state_.commit_ << std::endl;
        
        vsr_message commit_msg;
        commit_msg.header.command_ = command::commit;
        commit_msg.header.view = state_.view_;
        commit_msg.header.commit = state_.commit_;
        
        protocol_handler_->broadcast_to_peers(commit_msg);
        last_broadcast_op_ = state_.commit_; // Update our tracker
    }
}