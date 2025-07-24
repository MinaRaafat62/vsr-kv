#ifndef IO_URING_LOOP_HPP
#define IO_URING_LOOP_HPP

#include <liburing.h>
#include <functional>
#include <memory>
#include <vector>
#include <chrono>
#include <variant>
#include <netinet/in.h>

class TcpConnection;

using IoCallback = std::function<void(int)>;
using AcceptCallback = std::function<void(int, const sockaddr_in&)>;

class IoUringLoop {
public:
    explicit IoUringLoop(unsigned int queue_depth = 256);
    ~IoUringLoop();

    IoUringLoop(const IoUringLoop&) = delete;
    IoUringLoop& operator=(const IoUringLoop&) = delete;

    void run();

    void submit_accept(int server_socket, AcceptCallback callback);
    void submit_read(std::shared_ptr<TcpConnection> connection, size_t size, IoCallback callback);
    void submit_write(std::shared_ptr<TcpConnection> connection, const std::vector<char>& buffer, IoCallback callback);
    void submit_connect(int socket, const sockaddr_in& address, IoCallback callback);
    void submit_timeout(std::chrono::nanoseconds duration, IoCallback callback);

private:
    struct IORequest {
        std::variant<IoCallback, AcceptCallback> callback;

        std::shared_ptr<TcpConnection> connection;
        std::vector<char> write_buffer;
        sockaddr_in remote_address;
        __kernel_timespec timeout_spec;
    };

    io_uring ring_;
    bool is_running_ = true;
};

#endif // IO_URING_LOOP_HPP