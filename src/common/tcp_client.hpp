#ifndef TCP_CLIENT_HPP
#define TCP_CLIENT_HPP

#include "io_uring_loop.hpp"
#include "tcp_connection.hpp"
#include <functional>
#include <string>
#include <memory>
#include <atomic> // Required for std::atomic

using ConnectionCallback = std::function<void(std::shared_ptr<TcpConnection>)>;
using MessageCallback = std::function<void(const std::vector<char>&)>;
using ConnectionFailedCallback = std::function<void()>;

class TcpClient {
public:
    TcpClient(IoUringLoop& loop, const std::string& host, int port);

    void connect();
    void send(const std::vector<char>& data);

    // A public method to check the connection state.
    bool is_connected() const;

    void set_on_connect(ConnectionCallback handler);
    void set_on_disconnect(ConnectionCallback handler);
    void set_on_message(MessageCallback handler);
    void set_on_connect_failed(ConnectionFailedCallback handler);

private:
    void start_reading();
    void handle_disconnect();

    IoUringLoop& loop_;
    std::string host_;
    int port_;
    std::shared_ptr<TcpConnection> connection_;

    // An atomic bool for thread-safe status checks.
    std::atomic<bool> is_connected_ = false;

    ConnectionCallback on_connect_ = [](auto){};
    ConnectionCallback on_disconnect_ = [](auto){};
    MessageCallback on_message_ = [](const auto&){};
    ConnectionFailedCallback on_connect_failed_ = []{};
};

#endif // TCP_CLIENT_HPP