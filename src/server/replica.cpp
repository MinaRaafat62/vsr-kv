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
    loop_.run();
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
                    // case command::request:
                    //     handle_request(message_to_process);
                    //     break;
                    // case command::prepare:
                    //     handle_prepare(message_to_process);
                    //     break;
                    // case command::prepare_ok:
                    //     handle_prepare_ok(message_to_process);
                    //     break;
                    // case command::commit:
                    //     handle_commit(message_to_process);
                    //     break;
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
        // case command::prepare:
        //     return check_can_process_prepare(msg);
        // case command::prepare_ok:
        //     return check_can_process_prepare_ok(msg);
        // case command::commit:
        //     return check_can_process_commit(msg);
        default:
            return false;
    }
}

bool Replica::check_can_process_request(const vsr_message& msg) const {
    // TODO:: we will need to handle state transfers when the view larger than the current view
    return state_.status_ == ReplicaStatus::NORMAL &&
        state_.is_primary() &&
        msg.header.view == state_.view_;
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