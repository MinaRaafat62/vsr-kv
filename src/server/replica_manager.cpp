#include "replica_manager.hpp"
#include "io_uring_loop.hpp"
#include "tcp_server.hpp"
#include "tcp_connection.hpp"
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

ReplicaManager::ReplicaManager(IoUringLoop& loop, TcpServer& server, int self_id, std::vector<Peer> peers)
    : loop_(loop), server_(server), self_id_(self_id) {
    for (auto& peer : peers) {
        port_to_peer_id_[peer.port] = peer.id;
        peers_by_id_.emplace(peer.id, std::move(peer));
    }
}

void ReplicaManager::connect_to_peers() {
    for (auto& [id, peer] : peers_by_id_) {
        // NEW RULE: Only connect to peers with a lower ID.
        if (self_id_ > peer.id) {
            attempt_connection(peer);
        }
    }
}

void ReplicaManager::attempt_connection(Peer& peer) {
    if (peer.state != Peer::State::DISCONNECTED) return;
    std::cout << "[Replica " << self_id_ << "] Attempting to connect to Peer " << peer.id << std::endl;
    peer.state = Peer::State::CONNECTING;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { peer.state = Peer::State::DISCONNECTED; schedule_reconnect(peer); return; }
    sockaddr_in peer_addr{};
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.host.c_str(), &peer_addr.sin_addr);
    loop_.submit_connect(sock, peer_addr, [this, &peer, sock](int result) {
        if (result >= 0) {
            std::cout << "[Replica " << self_id_ << "] Outgoing connection to Peer " << peer.id << " established." << std::endl;
            peer.state = Peer::State::CONNECTED;
            peer.connection = std::make_shared<TcpConnection>(sock);
            socket_to_peer_id_[sock] = peer.id;
            server_.register_new_connection(peer.connection);
            send_handshake(peer);
        } else {
            peer.state = Peer::State::DISCONNECTED;
            close(sock);
            schedule_reconnect(peer);
        }
    });
}

void ReplicaManager::register_identified_peer(int peer_id, std::shared_ptr<TcpConnection> connection) {
    auto it = peers_by_id_.find(peer_id);
    if (it == peers_by_id_.end()) {
        std::cerr << "Warning: Received handshake from unknown peer ID " << peer_id << std::endl;
        connection->close_socket();
        return;
    }

    Peer& peer = it->second;
    std::cout << "[Replica " << self_id_ << "] Peer " << peer_id << " has been identified via handshake." << std::endl;
    
    peer.state = Peer::State::CONNECTED;
    peer.connection = connection;
    socket_to_peer_id_[connection->get_socket()] = peer.id;
}

void ReplicaManager::on_disconnect(const std::shared_ptr<TcpConnection>& connection) {
    int socket_fd = connection->get_socket();
    if (socket_to_peer_id_.count(socket_fd)) {
        int peer_id = socket_to_peer_id_[socket_fd];
        auto& peer = peers_by_id_.at(peer_id);
        std::cout << "[Replica " << self_id_ << "] Connection to Peer " << peer_id << " was lost." << std::endl;
        peer.state = Peer::State::DISCONNECTED;
        peer.connection = nullptr;
        socket_to_peer_id_.erase(socket_fd);
        
        // Re-apply the rule: only try to reconnect if the peer has a lower ID.
        if (self_id_ > peer.id) {
            schedule_reconnect(peer);
        }
    }
}

void ReplicaManager::schedule_reconnect(Peer& peer) {
    std::cout << "[Replica " << self_id_ << "] Scheduling reconnect to Peer " << peer.id << " in " << reconnect_delay_.count() << " seconds." << std::endl;
    loop_.submit_timeout(reconnect_delay_, [this, &peer](int result) {
        attempt_connection(peer);
    });
}

void ReplicaManager::send_to_peer(int peer_id, const std::vector<char>& data) {
    auto it = peers_by_id_.find(peer_id);
    if (it == peers_by_id_.end()) return;
    Peer& peer = it->second;
    if (peer.state != Peer::State::CONNECTED || !peer.connection) return;
    server_.send(peer.connection, data);
}

void ReplicaManager::add_peer(Peer new_peer) {
    if (peers_by_id_.count(new_peer.id)) return;
    port_to_peer_id_[new_peer.port] = new_peer.id;
    auto& peer_ref = peers_by_id_.emplace(new_peer.id, std::move(new_peer)).first->second;
    if (self_id_ > new_peer.id) {
        attempt_connection(peer_ref);
    }
}

void ReplicaManager::remove_peer(int peer_id) {
    auto it = peers_by_id_.find(peer_id);
    if (it == peers_by_id_.end()) return;
    Peer& peer = it->second;
    if (peer.connection) {
        peer.connection->close_socket();
    }
    port_to_peer_id_.erase(peer.port);
    peers_by_id_.erase(it);
}

void ReplicaManager::broadcast_to_peers(const std::vector<char>& data, int exclude_id) {
    for (const auto& [id, peer] : peers_by_id_) {
        if (id == exclude_id) {
            continue; // Skip the excluded peer
        }
        send_to_peer(id, data);
    }
}

bool ReplicaManager::is_peer_connection(const std::shared_ptr<TcpConnection>& connection) const {
    if (!connection) return false;
    return socket_to_peer_id_.count(connection->get_socket()) > 0;
}

void ReplicaManager::send_handshake(Peer& peer) {
    if (!peer.connection) return;
    std::string handshake_msg = "HANDSHAKE_ID:" + std::to_string(self_id_);
    std::vector<char> data(handshake_msg.begin(), handshake_msg.end());
    server_.send(peer.connection, data);
    std::cout << "[Replica " << self_id_ << "] Sent handshake to Peer " << peer.id << std::endl;
}