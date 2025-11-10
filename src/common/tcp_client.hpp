#ifndef TCP_CLIENT_HPP
#define TCP_CLIENT_HPP

#include "io_uring_loop.hpp"
#include "tcp_connection.hpp"
#include <functional>
#include <string>
#include <memory>
#include <atomic>
#include <chrono>

using ConnectionCallback = std::function<void(std::shared_ptr<TcpConnection>)>;
using MessageCallback = std::function<void(const std::vector<char>&)>;
using ConnectionFailedCallback = std::function<void()>;

class TcpClient {
public:
    TcpClient(IoUringLoop& loop, const std::string& host, int port);

    void connect();
    void send(const std::vector<char>& data);
    bool is_connected() const;

    void set_on_connect(ConnectionCallback handler);
    void set_on_disconnect(ConnectionCallback handler);
    void set_on_message(MessageCallback handler);
    void set_on_connect_failed(ConnectionFailedCallback handler);

private:
    void start_reading();
    void handle_disconnect();
    void schedule_reconnect();

    enum class State { DISCONNECTED, CONNECTING, CONNECTED };

    IoUringLoop& loop_;
    std::string host_;
    int port_;
    std::shared_ptr<TcpConnection> connection_;

    std::atomic<State> state_ = State::DISCONNECTED;

    ConnectionCallback on_connect_ = [](auto){};
    ConnectionCallback on_disconnect_ = [](auto){};
    MessageCallback on_message_ = [](const auto&){};
    ConnectionFailedCallback on_connect_failed_ = []{};

    const std::chrono::seconds reconnect_delay_{5};
};

#endif // TCP_CLIENT_HPP