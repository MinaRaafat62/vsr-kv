#include "tcp_connection.hpp"
#include <unistd.h>
#include <iostream>

TcpConnection::TcpConnection(int socket) : socket_fd_(socket), buffer_(1024) {
    std::cout << "Connection created for socket: " << socket_fd_ << std::endl;
}

TcpConnection::~TcpConnection() {
    std::cout << "Connection for socket " << socket_fd_ << " destroyed." << std::endl;
    close_socket();
}

int TcpConnection::get_socket() const {
    return socket_fd_;
}

std::vector<char>& TcpConnection::get_buffer() {
    return buffer_;
}

void TcpConnection::close_socket() {
    if (socket_fd_ != -1) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
}