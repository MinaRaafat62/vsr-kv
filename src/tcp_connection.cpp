#include "tcp_connection.hpp"
#include <unistd.h>
#include <iostream>

TcpConnection::TcpConnection(int socket) : socket_fd_(socket), buffer_(1024) {}

TcpConnection::~TcpConnection() {
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
        std::cout << "Closing socket " << socket_fd_ << std::endl;
        close(socket_fd_);
        socket_fd_ = -1;
    }
}

TcpConnection::State TcpConnection::get_state() const { return state_; }
void TcpConnection::set_state(State new_state) { state_ = new_state; }