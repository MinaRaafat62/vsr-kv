#include "tcp_server.hpp"
#include "io_uring_loop.hpp"
#include "tcp_connection.hpp"

#include <iostream>
#include <stdexcept>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

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
}

void TcpServer::set_on_connect(ConnectionHandler handler) {
    on_connect_ = std::move(handler);
}

void TcpServer::set_on_message(MessageHandler handler) {
    on_message_ = std::move(handler);
}

void TcpServer::set_on_disconnect(ConnectionHandler handler) {
    on_disconnect_ = std::move(handler);
}

void TcpServer::send(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data) {
    loop_.submit_write(connection, data, [this, conn = connection](int result){
        if (result < 0) {
            std::cerr << "Write error on socket " << conn->get_socket() << ": " << strerror(-result) << std::endl;
            remove_connection(conn);
        }
    });
}


void TcpServer::setup_listening_socket() {
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0) {
        throw std::runtime_error("Failed to create socket: " + std::string(strerror(errno)));
    }

    const int enable = 1;
    if (setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        throw std::runtime_error("setsockopt(SO_REUSEADDR) failed: " + std::string(strerror(errno)));
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_socket_, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        throw std::runtime_error("Bind failed: " + std::string(strerror(errno)));
    }

    if (listen(server_socket_, SOMAXCONN) < 0) {
        throw std::runtime_error("Listen failed: " + std::string(strerror(errno)));
    }
}

void TcpServer::start_accept() {
    loop_.submit_accept(server_socket_, [this](int result) {
        if (result >= 0) {
            handle_new_connection(result);
        } else {
            std::cerr << "Accept error: " << strerror(-result) << std::endl;
        }
        // Always queue up the next accept
        start_accept();
    });
}

void TcpServer::handle_new_connection(int client_socket) {
    auto connection = std::make_shared<TcpConnection>(client_socket);
    connections_[client_socket] = connection;
    
    // Call user-defined connection handler
    on_connect_(connection);

    // Start listening for data from the new client
    start_reading(connection);
}

void TcpServer::start_reading(std::shared_ptr<TcpConnection> connection) {
    loop_.submit_read(connection, connection->get_buffer().size(), [this, conn = connection](int result) {
        if (result > 0) {
            // Received data, create a vector with the correct size
            std::vector<char> received_data(conn->get_buffer().begin(), conn->get_buffer().begin() + result);
            on_message_(conn, received_data);
            // Queue up the next read
            start_reading(conn);
        } else if (result == 0) {
            // Client disconnected
            on_disconnect_(conn);
            remove_connection(conn);
        } else {
            // Read error
            std::cerr << "Read error on socket " << conn->get_socket() << ": " << strerror(-result) << std::endl;
            on_disconnect_(conn);
            remove_connection(conn);
        }
    });
}

void TcpServer::remove_connection(const std::shared_ptr<TcpConnection>& connection) {
    if (connection) {
        connections_.erase(connection->get_socket());
    }
}