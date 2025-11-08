#include "replica.hpp"
#include "view_change_messages.hpp"
#include "recovery_messages.hpp"
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
    last_primary_contact_ = std::chrono::steady_clock::now();
    schedule_liveness_check();
    loop_.run();
}

void Replica::schedule_liveness_check() {
    loop_.submit_timeout(liveness_check_interval_, [this](int result) {
        this->check_primary_liveness();
    });
}

void Replica::check_primary_liveness() {
    if (!state_.is_primary() && state_.status_ == ReplicaStatus::NORMAL) {
        auto now = std::chrono::steady_clock::now();
        auto duration_since_contact = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_primary_contact_);

        if (duration_since_contact > primary_timeout_) {
            std::cout << "[Replica " << state_.id_ << "] Primary timeout expired ("
                      << duration_since_contact.count() << "ms). Initiating view change." << std::endl;
            initiate_view_change();
        }
    }
    // Re-schedule the next check to create a continuous polling loop.
    schedule_liveness_check();
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
    bool processed_in_pass;
    do {
        processed_in_pass = false;

        while (!message_queue_.empty()) {
            const auto& msg = message_queue_.front().message;

            if (msg.header.command_ != command::request &&
                msg.header.command_ != command::recovery &&
                msg.header.view < state_.view_)
            {
                std::cout << "[Replica " << state_.id_ << "] Discarding stale server message from past view "
                          << msg.header.view << "." << std::endl;
                message_queue_.pop_front();
                processed_in_pass = true;
                continue;
            }

            if (can_process(message_queue_.front())) {
                inbound_message msg_to_process = message_queue_.front();
                message_queue_.pop_front();
                processed_in_pass = true;

                switch (msg_to_process.message.header.command_) {
                    case command::ping:
                        handle_ping(msg_to_process);
                        break;
                    case command::request:
                        handle_request(msg_to_process);
                        break;
                    case command::prepare:
                        handle_prepare(msg_to_process);
                        break;
                    case command::prepare_ok:
                        handle_prepare_ok(msg_to_process);
                        break;
                    case command::commit:
                        handle_commit(msg_to_process);
                        break;
                    case command::start_view_change:
                        handle_start_view_change(msg_to_process);
                        break;
                    case command::do_view_change:
                        handle_do_view_change(msg_to_process);
                        break;
                    case command::start_view:
                        handle_start_view(msg_to_process);
                        break;
                    case command::recovery:
                        handle_recovery_request(msg_to_process);
                        break;
                    case command::recovery_response:
                        handle_recovery_response(msg_to_process);
                        break;
                    default:
                        std::cerr << "Warning: Unknown command in message queue." << std::endl;
                        break;
                }
            } else {
                break;
            }
        }

        if (!message_queue_.empty() && state_.status_ == ReplicaStatus::NORMAL) {
            const auto& stuck_msg = message_queue_.front().message;
            StateAction action = determine_state_action(stuck_msg);

            switch (action) {
                case StateAction::INITIATE_RECOVERY:
                    initiate_recovery();
                    message_queue_.clear();
                    break;
                case StateAction::INITIATE_VIEW_CHANGE:
                    initiate_view_change();
                    break;
                case StateAction::PROCESS_NORMALLY:
                    break;
            }
        }
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
        case command::start_view_change:
            return check_can_process_start_view_change(msg);
        case command::do_view_change:
            return check_can_process_do_view_change(msg);
        case command::start_view:
            return check_can_process_start_view(msg);
        case command::recovery:
            return check_can_process_recovery_request(msg);
        case command::recovery_response:
            return check_can_process_recovery_response(msg);
        default:
            // If we are in a view change, we might receive messages for the *next* view.
            // We should keep them in the queue.
            if (state_.status_ == ReplicaStatus::VIEW_CHANGE && msg.header.view > state_.view_) {
                return false; // Keep it in the queue, but don't process yet.
            }
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
        state_.is_primary();
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
    last_primary_contact_ = std::chrono::steady_clock::now();
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

    advance_primary_commit_number();
}


bool Replica::check_can_process_commit(const vsr_message& msg) const {
    return state_.status_ == ReplicaStatus::NORMAL &&
           !state_.is_primary() &&
           msg.header.view == state_.view_;
}



void Replica::handle_commit(const inbound_message& inbound) {
    last_primary_contact_ = std::chrono::steady_clock::now();
    const auto& commit_msg = inbound.message;

    if (commit_msg.header.commit > state_.commit_) {
        state_.commit_ = commit_msg.header.commit;
        execute_commited_ops();
    }
}


bool Replica::check_can_process_start_view_change(const vsr_message &msg) const {
    return msg.header.view == state_.view_ && state_.status_ == ReplicaStatus::VIEW_CHANGE;
}

void Replica::handle_start_view_change(const inbound_message &inbound) {
    const auto& svc_msg = inbound.message;
    const auto& header = svc_msg.header;

    std::cout << "[Replica " << state_.id_ << "] Received StartViewChange for view " << header.view
              << " from replica " << static_cast<int>(header.replica) << std::endl;
    
    state_.start_view_change_received_.insert(header.replica);
    if (state_.start_view_change_received_.size() >= state_.fault_tolerance_f_ + 1) {
        std::cout << "[Replica " << state_.id_ << "] Quorum for StartViewChange met. Sending DoViewChange to new primary." << std::endl;
        state_.start_view_change_received_.clear();
        // Prepare the payload with our state.
        do_view_change_payload dvc_payload;
        dvc_payload.last_normal_view = state_.last_normal_view_;
        dvc_payload.log = state_.log_;

        // Wrap it in a VSR message. The header contains our current op/commit numbers.
        vsr_message msg;
        msg.header.command_ = command::do_view_change;
        msg.header.view = state_.view_;
        msg.header.op = state_.op_;
        msg.header.commit = state_.commit_;
        msg.header.replica = state_.id_;
        msg.payload = dvc_payload.serialize();

        // Send the message ONLY to the prospective primary of the new view.
        int new_primary_id = state_.get_primary_id_for_view(state_.view_);
        if (new_primary_id == state_.id_) {
            std::cout << "[Replica " << state_.id_ << "] I am the new primary. Processing my own DoViewChange state." << std::endl;
            inbound_message self_dvc_inbound = { nullptr, msg };
            handle_do_view_change(self_dvc_inbound);
        } else{
            protocol_handler_->send_to_peer(new_primary_id, msg);
        }
    }
    
}


bool Replica::check_can_process_do_view_change(const vsr_message &msg) const {
   if (msg.header.view != state_.view_) return false;
   if (state_.status_ != ReplicaStatus::VIEW_CHANGE) return false;
   if (state_.get_primary_id_for_view(state_.view_) != state_.id_) return false;
   return true;
}

void Replica::handle_do_view_change(const inbound_message &inbound) {
    const auto& dvc_msg = inbound.message;
    const auto& header = dvc_msg.header;
     std::cout << "[Replica " << state_.id_ << "] Processing DoViewChange for view " << header.view
              << " from replica " << static_cast<int>(header.replica) << std::endl;
    state_.do_view_change_received_.emplace_back(dvc_msg);
    
    if (state_.do_view_change_received_.size() >= state_.fault_tolerance_f_ + 1) {
        std::cout << "[Replica " << state_.id_ << "] Quorum for DoViewChange met. Selecting log and becoming primary." << std::endl;

        auto received_messages = state_.do_view_change_received_;
        state_.do_view_change_received_.clear();

        do_view_change_payload best_payload;
        do_view_change_payload::deserialize(received_messages[0].payload, best_payload);
        uint64_t best_op = received_messages[0].header.op;
        uint64_t max_commit = std::max(state_.commit_, received_messages[0].header.commit);

        for (size_t i = 1; i < received_messages.size(); ++i) {
            const auto& current_msg = received_messages[i];
            do_view_change_payload current_payload;

            if (do_view_change_payload::deserialize(current_msg.payload, current_payload)) {
                
                if (current_payload.last_normal_view > best_payload.last_normal_view) {
                    best_payload = current_payload;
                    best_op = current_msg.header.op;
                } else if (current_payload.last_normal_view == best_payload.last_normal_view &&
                           current_msg.header.op > best_op) {
                    best_payload = current_payload;
                    best_op = current_msg.header.op;
                }
                max_commit = std::max(max_commit, current_msg.header.commit);
            }
        }

        state_.log_ = best_payload.log;
        state_.op_ = best_op;
        state_.commit_ = max_commit;
        state_.status_ = ReplicaStatus::NORMAL;
        std::cout << "[Replica " << state_.id_ << "] New state adopted: op=" << state_.op_
                  << ", commit=" << state_.commit_ << ". Broadcasting StartView." << std::endl;
        
        start_view_payload sv_payload;
        sv_payload.log = state_.log_;

        vsr_message msg;
        msg.header.command_ = command::start_view;
        msg.header.view = state_.view_;
        msg.header.op = state_.op_;
        msg.header.commit = state_.commit_;
        msg.header.replica = state_.id_;
        msg.payload = sv_payload.serialize();
        protocol_handler_->broadcast_to_peers(msg);

        execute_commited_ops();
    }
}

bool Replica::check_can_process_start_view(const vsr_message &msg) const {
    if (msg.header.view != state_.view_) return false;
    if (state_.status_ != ReplicaStatus::VIEW_CHANGE) return false;
    if (msg.header.replica != state_.get_primary_id_for_view(state_.view_)) return false;
    return true;
}

bool Replica::check_can_process_recovery_request(const vsr_message &msg) const {
    return state_.status_ == ReplicaStatus::NORMAL;
}

bool Replica::check_can_process_recovery_response(const vsr_message& msg) const {
    return state_.status_ == ReplicaStatus::RECOVERY;
}

void Replica::handle_start_view(const inbound_message &inbound) {
    const auto& sv_msg = inbound.message;
    const auto& header = sv_msg.header;
    std::cout << "[Replica " << state_.id_ << "] Processing StartView for view " << header.view
              << ". Adopting new state from primary " << static_cast<int>(header.replica) << "." << std::endl;

    start_view_payload sv_payload;
    
    if (!start_view_payload::deserialize(inbound.message.payload, sv_payload)) {
        std::cerr << "Critical error: Failed to deserialize StartView payload." << std::endl;
        // In a real system, this might trigger another view change.
        return;
    }

    uint64_t new_primary_commit_number = header.commit;

    state_.view_ = header.view;
    state_.op_ = header.op;
    state_.commit_ = header.commit;
    state_.log_ = sv_payload.log;
    state_.status_ = ReplicaStatus::NORMAL;
    last_primary_contact_ = std::chrono::steady_clock::now();

    for (const auto& [op_num, entry] : state_.log_) {
        auto& client_entry = state_.client_table_[entry.client_id];
        if (entry.request_num > client_entry.request_number) {
            client_entry.request_number = entry.request_num;
            client_entry.executed = false;
        }
    }

    execute_commited_ops();

    int primary_id = state_.get_primary_id();
    for (uint64_t op_num = new_primary_commit_number + 1; op_num <= state_.op_; ++op_num) {
        if (state_.log_.count(op_num)) {
            std::cout << "[Replica " << state_.id_ << "] Sending retroactive PrepareOk for op " << op_num << " to new primary." << std::endl;
            vsr_message prepare_ok_msg;
            prepare_ok_msg.header.command_ = command::prepare_ok;
            prepare_ok_msg.header.view = state_.view_;
            prepare_ok_msg.header.op = op_num;
            prepare_ok_msg.header.replica = state_.id_;
            
            protocol_handler_->send_to_peer(primary_id, prepare_ok_msg);
        }
    }
}

void Replica::handle_recovery_request(const inbound_message &inbound) {
    const auto& recovery_msg = inbound.message;
    int recovering_replica_id = recovery_msg.header.replica;

    recovery_request_payload req_payload;
    if (!recovery_request_payload::deserialize(recovery_msg.payload, req_payload)) {
        std::cerr << "Failed to deserialize recovery request payload from replica " 
                  << recovering_replica_id << std::endl;
        return;
    }

    std::cout << "[Replica " << state_.id_ << "] Received recovery request from Replica " << recovering_replica_id << ". Responding with current state." << std::endl;

    recovery_response_payload res_payload;
    res_payload.nonce = req_payload.nonce;
    res_payload.log = state_.log_;

    vsr_message response_msg;
    response_msg.header.command_ = command::recovery_response;
    response_msg.header.replica = state_.id_;
    response_msg.header.view = state_.view_;
    response_msg.header.op = state_.op_;
    response_msg.header.commit = state_.commit_;
    response_msg.payload = res_payload.serialize();

    protocol_handler_->send_to_peer(recovering_replica_id, response_msg);
}

void Replica::handle_recovery_response(const inbound_message &inbound) {
    const auto& response_msg = inbound.message;
    int responder_id = response_msg.header.replica;

    recovery_response_payload res_payload;
    if (!recovery_response_payload::deserialize(response_msg.payload, res_payload)) {
        std::cerr << "[Replica " << state_.id_ << "] Error: Failed to deserialize recovery response from replica " << responder_id << "." << std::endl;
        return;
    }

    if (!(res_payload.nonce == state_.recovery_nonce_)) {
        std::cout << "[Replica " << state_.id_ << "] Ignoring recovery response from replica " << responder_id << " with old nonce." << std::endl;
        return;
    }

    std::cout << "[Replica " << state_.id_ << "] Received valid recovery response from Replica " << responder_id << "." << std::endl;
    state_.recovery_responses_.push_back(response_msg);

    if (state_.recovery_responses_.size() >= state_.fault_tolerance_f_ + 1) {
        std::cout << "[Replica " << state_.id_ << "] Recovery quorum met. Processing responses to find best state." << std::endl;

        const vsr_message* best_response = &state_.recovery_responses_[0];
        for (size_t i = 1; i < state_.recovery_responses_.size(); ++i) {
            const auto& current_response = state_.recovery_responses_[i];
            if (current_response.header.view > best_response->header.view) {
                best_response = &current_response;
            } else if (current_response.header.view == best_response->header.view &&
                       current_response.header.op > best_response->header.op) {
                best_response = &current_response;
            }
        }

        recovery_response_payload best_payload;
        recovery_response_payload::deserialize(best_response->payload, best_payload);


        state_.view_ = best_response->header.view;
        state_.op_ = best_response->header.op;
        state_.commit_ = best_response->header.commit;
        state_.log_ = best_payload.log;
        state_.last_executed_op_ = 0;

        last_primary_contact_ = std::chrono::steady_clock::now();

        state_.status_ = ReplicaStatus::NORMAL;
        std::cout << "[Replica " << state_.id_ << "] Recovery complete! New state: view=" << state_.view_
                  << ", op=" << state_.op_ << ", commit=" << state_.commit_ << std::endl;


        state_.client_table_.clear();
        for (const auto& [op_num, entry] : state_.log_) {
            auto& client_entry = state_.client_table_[entry.client_id];
            if (entry.request_num > client_entry.request_number) {
                client_entry.request_number = entry.request_num;
                client_entry.executed = false;
            }
        }
        execute_commited_ops();
        
        state_.recovery_nonce_ = {0,0};
        state_.recovery_responses_.clear();
    }
}

void Replica::execute_commited_ops() {
    // This loop ensures the state machine catches up to the committed log state.
    while (state_.last_executed_op_ < state_.commit_) {
        uint64_t op_to_execute = state_.last_executed_op_ + 1;
        auto log_it = state_.log_.find(op_to_execute);

        // If the log entry isn't present, we can't proceed.
        // This might happen during state transfer, so we must wait.
        if (log_it == state_.log_.end()) {
            break;
        }

        const auto& entry = log_it->second;
        auto& client_entry = state_.client_table_[entry.client_id];

        // Only execute if it hasn't been done before for this specific request.
        // This prevents re-executing the same operation if this function is called multiple times.
        if (client_entry.request_number != entry.request_num || !client_entry.executed) {
            std::cout << "[Replica " << state_.id_ << "] Executing Op " << op_to_execute << std::endl;

            std::string result_str;
            std::string payload_str(entry.payload.begin(), entry.payload.end());
            std::stringstream ss(payload_str);
            std::string key, value;

            if (entry.op_type == operation::set) {
                ss >> key >> value;
                state_.state_machine_[key] = value;
                result_str = "SET " + key + "=" + value;
            } else if (entry.op_type == operation::get) {
                ss >> key;
                result_str = state_.state_machine_.count(key) ? state_.state_machine_[key] : "NOT_FOUND";
            }

            // Update the client table with the result and mark as executed.
            client_entry.request_number = entry.request_num;
            client_entry.executed = true;
            client_entry.result.assign(result_str.begin(), result_str.end());

            // If we are the primary that just executed this, we must send the reply.
            if (state_.is_primary()) {
                auto client_conn_it = client_connections_.find(entry.client_id);
                if (client_conn_it != client_connections_.end() && client_conn_it->second) {
                    std::cout << "[Replica " << state_.id_ << "] Sending Reply for request " << entry.request_num << " to client." << std::endl;
                    vsr_message reply_msg;
                    reply_msg.header.command_ = command::reply;
                    reply_msg.header.view = state_.view_;
                    reply_msg.header.client = entry.client_id;
                    reply_msg.header.request = entry.request_num;
                    reply_msg.payload = client_entry.result;
                    protocol_handler_->send_message(client_conn_it->second, reply_msg);
                }
            }
        }

        // Advance our personal execution counter.
        state_.last_executed_op_ = op_to_execute;
    }
}

void Replica::send_commit_message_if_needed() {
    if (!state_.is_primary() || state_.status_ != ReplicaStatus::NORMAL) {
        return;
    }

    vsr_message commit_msg;
    commit_msg.header.command_ = command::commit;
    commit_msg.header.view = state_.view_;
    commit_msg.header.commit = state_.commit_;
    protocol_handler_->broadcast_to_peers(commit_msg);
}

void Replica::initiate_view_change() {
    state_.enter_new_view(state_.view_ + 1);
    std::cout << "[Replica " << state_.id_ << "] Starting View Change for view " << state_.view_ << std::endl;

    vsr_message svc_msg;
    svc_msg.header.command_ = command::start_view_change;
    svc_msg.header.view = state_.view_;
    svc_msg.header.replica = state_.id_;
    protocol_handler_->broadcast_to_peers(svc_msg);
}

void Replica::initiate_recovery() {
    if (state_.status_ == ReplicaStatus::RECOVERY) {
        return;
    }

    state_.status_ = ReplicaStatus::RECOVERY;
    std::cout << "[Replica " << state_.id_ << "] Detected state is out of sync. Entering recovery mode." << std::endl;
    
    state_.recovery_nonce_ = utilities::generate_nonce(state_.id_);
    state_.recovery_responses_.clear();

    recovery_request_payload req_payload;
    req_payload.nonce = state_.recovery_nonce_;

    vsr_message recovery_msg;
    recovery_msg.header.command_ = command::recovery;
    recovery_msg.header.replica = state_.id_;
    recovery_msg.payload = req_payload.serialize();

    protocol_handler_->broadcast_to_peers(recovery_msg);
    std::cout << "[Replica " << state_.id_ << "] Broadcasting recovery request." << std::endl;

}

StateAction Replica::determine_state_action(const vsr_message& stuck_msg) const {
    // This function is only called when we are stuck, so we are guaranteed to be in a NORMAL state.
    
    // --- TRIGGER 1: Stuck on a message from a future view ---
    if (stuck_msg.header.view > state_.view_) {
        // If a view change for the *next* view is in progress, we should join it.
        if (stuck_msg.header.view == state_.view_ + 1 &&
            (stuck_msg.header.command_ == command::start_view_change || stuck_msg.header.command_ == command::do_view_change))
        {
            std::cout << "[Replica " << state_.id_ << "] Trigger: Stuck on view change message for next view " 
                      << stuck_msg.header.view << ". Initiating view change to participate." << std::endl;
            return StateAction::INITIATE_VIEW_CHANGE;
        }
        // Otherwise, the cluster has moved on without us. We are lost and must recover.
        else
        {
            std::cout << "[Replica " << state_.id_ << "] Trigger: Stuck on message from future view " << stuck_msg.header.view
                      << " (current is " << state_.view_ << "). Needs recovery." << std::endl;
            return StateAction::INITIATE_RECOVERY;
        }
    }

    // --- TRIGGER 2: Stuck on a Prepare message in the current view with a log gap ---
    // This is the critical case for a rebooted backup.
    if (!state_.is_primary() &&
        stuck_msg.header.command_ == command::prepare &&
        stuck_msg.header.view == state_.view_ &&
        stuck_msg.header.op > state_.op_ + 1) 
    {
        std::cout << "[Replica " << state_.id_ << "] Trigger: Stuck waiting for op " << state_.op_ + 1 
                  << ", but next in queue is op " << stuck_msg.header.op << ". Needs recovery." << std::endl;
        return StateAction::INITIATE_RECOVERY;
    }

    // If we are stuck for any other reason (e.g., a backup receiving a client request),
    // it's a normal condition, and we just need to wait. No special action is needed.
    return StateAction::PROCESS_NORMALLY;
}

void Replica::advance_primary_commit_number() {
    // This function should only be called on the primary.
    if (!state_.is_primary()) {
        return;
    }

    // Try to advance the commit number as far as possible.
    // This loop handles cases where multiple operations can be committed at once.
    while (true) {
        uint64_t next_op_to_commit = state_.commit_ + 1;
        auto it = state_.log_.find(next_op_to_commit);

        // Stop if the next operation is not in our log.
        if (it == state_.log_.end()) {
            break;
        }

        // Stop if the operation doesn't have enough acknowledgements yet.
        if (it->second.prepare_ok_acks.size() < state_.fault_tolerance_f_ + 1) {
            break;
        }

        // This operation is now officially committed. Advance the counter.
        state_.commit_ = next_op_to_commit;
        std::cout << "[Replica " << state_.id_ << "] Advanced commit number to " << state_.commit_ << std::endl;
    }

    // After advancing the commit number, execute any newly committed operations
    // to apply them to the state machine and reply to clients.
    execute_commited_ops();
}