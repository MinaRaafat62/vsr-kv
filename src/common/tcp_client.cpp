#include "tcp_client.hpp"
#include <stdexcept>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <netinet/tcp.h>
#include <iostream>

TcpClient::TcpClient(IoUringLoop& loop, const std::string& host, int port)
    : loop_(loop), host_(host), port_(port) {}

void TcpClient::connect() {
    if (state_ != State::DISCONNECTED) {
        return;
    }
    state_ = State::CONNECTING;

    loop_.post([this]() {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            on_connect_failed_();
            state_ = State::DISCONNECTED;
            schedule_reconnect();
            return;
        }

        const int enable = 1;
        if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(int)) < 0) {
            perror("setsockopt(TCP_NODELAY) failed");
        }

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port_);
        inet_pton(AF_INET, host_.c_str(), &server_addr.sin_addr);

        loop_.submit_connect(sock, server_addr, [this, sock](int result) {
            if (result >= 0) {
                connection_ = std::make_shared<TcpConnection>(sock);
                state_ = State::CONNECTED;
                on_connect_(connection_);
                start_reading();
            } else {
                close(sock);
                on_connect_failed_();
                state_ = State::DISCONNECTED;
                schedule_reconnect();
            }
        });
    });
}

bool TcpClient::is_connected() const {
    return state_ == State::CONNECTED;
}

void TcpClient::send(const std::vector<char>& data) {
    if (!is_connected() || !connection_) return;
    loop_.post([this, data]() {
        // Ensure connection still exists when the posted task runs
        if (!connection_) return;
        loop_.submit_write(connection_, data, [this](int result) {
            if (result < 0) {
                handle_disconnect();
            }
        });
    });
}

void TcpClient::start_reading() {
    loop_.submit_read(connection_, connection_->get_buffer().size(), [this](int result) {
        if (result > 0) {
            std::vector<char> received_data(connection_->get_buffer().begin(), connection_->get_buffer().begin() + result);
            on_message_(received_data);
            start_reading();
        } else {
            handle_disconnect();
        }
    });
}

void TcpClient::handle_disconnect() {
    State expected = State::CONNECTED;
    if (!state_.compare_exchange_strong(expected, State::DISCONNECTED)) {
        return; 
    }

    on_disconnect_(connection_);
    connection_.reset();
    
    schedule_reconnect();
}


void TcpClient::schedule_reconnect() {
    std::cout << "[SYSTEM] Scheduling reconnect to " << host_ << ":" << port_ << " in " << reconnect_delay_.count() << " seconds." << std::endl;

    loop_.post([this]() {
        loop_.submit_timeout(reconnect_delay_, [this](int result) {
            std::cout << "[SYSTEM] Attempting to reconnect to " << host_ << ":" << port_ << "..." << std::endl;
            this->connect();
        });
    });
}


void TcpClient::set_on_connect(ConnectionCallback handler) { on_connect_ = std::move(handler); }
void TcpClient::set_on_disconnect(ConnectionCallback handler) { on_disconnect_ = std::move(handler); }
void TcpClient::set_on_message(MessageCallback handler) { on_message_ = std::move(handler); }
void TcpClient::set_on_connect_failed(ConnectionFailedCallback handler) { on_connect_failed_ = std::move(handler); }