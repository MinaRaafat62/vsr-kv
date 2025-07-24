#include "tcp_server.hpp"
#include "io_uring_loop.hpp"
#include "tcp_connection.hpp"
#include "replica_manager.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <unistd.h>

TcpServer::TcpServer(IoUringLoop& loop, int port) : loop_(loop), port_(port), server_socket_(-1) {
    setup_listening_socket();
}

TcpServer::~TcpServer() {
    if (server_socket_ != -1) {
        close(server_socket_);
    }
}

void TcpServer::start() {
    std::cout << "Server starting on port " << port_ << std::endl;
    start_accept();
    if (replica_manager_) {
        replica_manager_->connect_to_peers();
    }
}

void TcpServer::set_on_connect(ConnectionHandler handler) { on_connect_ = std::move(handler); }
void TcpServer::set_on_disconnect(ConnectionHandler handler) { on_disconnect_ = std::move(handler); }
void TcpServer::set_replica_manager(std::unique_ptr<ReplicaManager> manager) { replica_manager_ = std::move(manager); }

void TcpServer::send(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data) {
    loop_.submit_write(connection, data, [this, conn = connection](int result){
        if (result < 0) {
            std::cerr << "Write error on socket " << conn->get_socket() << ": " << strerror(-result) << std::endl;
            remove_connection(conn);
        }
    });
}

void TcpServer::register_new_connection(std::shared_ptr<TcpConnection> connection) {
    int socket_fd = connection->get_socket();
    connections_[socket_fd] = connection;
    on_connect_(connection);
    start_reading(connection);
}

void TcpServer::setup_listening_socket() {
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0) throw std::runtime_error("Failed to create socket");
    const int enable = 1;
    if (setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) throw std::runtime_error("setsockopt(SO_REUSEADDR) failed");
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    server_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(server_socket_, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) throw std::runtime_error("Bind failed");
    if (listen(server_socket_, SOMAXCONN) < 0) throw std::runtime_error("Listen failed");
}

void TcpServer::start_accept() {
    loop_.submit_accept(server_socket_, [this](int client_socket, const sockaddr_in& client_address) {
        if (client_socket >= 0) {
            handle_new_connection(client_socket, client_address);
        } else {
            std::cerr << "Accept error: " << strerror(-client_socket) << std::endl;
        }
        start_accept();
    });
}

void TcpServer::handle_new_connection(int client_socket, const sockaddr_in& client_address) {
    auto connection = std::make_shared<TcpConnection>(client_socket);
    connections_[client_socket] = connection;
    start_reading(connection);
}

void TcpServer::start_reading(std::shared_ptr<TcpConnection> connection) {
    loop_.submit_read(connection, connection->get_buffer().size(), [this, conn = connection](int result) {
        if (result > 0) {
            std::vector<char> received_data(conn->get_buffer().begin(), conn->get_buffer().begin() + result);
            std::string message(received_data.begin(), received_data.end());

            // Check the connection's current state
            switch (conn->get_state()) {
                case TcpConnection::State::UNIDENTIFIED: {
                    // This is the first message. Is it a handshake?
                    if (message.rfind("HANDSHAKE_ID:", 0) == 0) {
                        int peer_id = std::stoi(message.substr(13));
                        conn->set_state(TcpConnection::State::PEER);
                        if (replica_manager_) {
                            replica_manager_->register_identified_peer(peer_id, conn);
                        }
                        on_connect_(conn); // Now we can call the connection handler
                    } else {
                        // Not a handshake, so it must be a client.
                        conn->set_state(TcpConnection::State::CLIENT);
                        on_connect_(conn);
                        on_client_message_(conn, received_data);
                    }
                    break;
                }
                case TcpConnection::State::PEER: {
                    on_peer_message_(conn, received_data);
                    break;
                }
                case TcpConnection::State::CLIENT: {
                    on_client_message_(conn, received_data);
                    break;
                }
            }
            
            // Queue up the next read for this connection
            start_reading(conn);
        } else {
            on_disconnect_(conn);
            remove_connection(conn);
        }
    });
}

void TcpServer::remove_connection(const std::shared_ptr<TcpConnection>& connection) {
    if (connection) {
        if (replica_manager_) {
            replica_manager_->on_disconnect(connection);
        }
        connections_.erase(connection->get_socket());
    }
}


void TcpServer::broadcast_to_peers(const std::vector<char>& data) {
    if (replica_manager_) {
        replica_manager_->broadcast_to_peers(data);
    }
}

void TcpServer::set_on_client_message(ClientMessageHandler handler) { on_client_message_ = std::move(handler); }
void TcpServer::set_on_peer_message(PeerMessageHandler handler) { on_peer_message_ = std::move(handler); }

