#ifndef IO_URING_LOOP_HPP
#define IO_URING_LOOP_HPP

#include <liburing.h>
#include <functional>
#include <memory>
#include <vector>
#include <chrono>
#include <variant>
#include <netinet/in.h>
#include <mutex>
#include <deque>

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

    // NEW: A thread-safe way to post work to the loop.
    void post(std::function<void()> work);

    // These are now intended to be called ONLY from the loop's own thread.
    void submit_accept(int server_socket, AcceptCallback callback);
    void submit_read(std::shared_ptr<TcpConnection> connection, size_t size, IoCallback callback);
    void submit_write(std::shared_ptr<TcpConnection> connection, const std::vector<char>& buffer, IoCallback callback);
    void submit_connect(int socket, const sockaddr_in& address, IoCallback callback);
    void submit_timeout(std::chrono::nanoseconds duration, IoCallback callback);

private:
    enum class RequestType {
        ACCEPT, READ, WRITE, CONNECT, TIMEOUT, EVENTFD
    };

    struct IORequest {
        RequestType type;
        std::variant<IoCallback, AcceptCallback> callback;
        std::shared_ptr<TcpConnection> connection;
        std::vector<char> write_buffer;
        sockaddr_in remote_address;
        __kernel_timespec timeout_spec;
    };

    void process_work_queue();
    void post_eventfd_read();

    io_uring ring_;
    bool is_running_ = true;

    // --- Thread-safety additions ---
    int event_fd_;
    uint64_t event_fd_buffer_ = 0;
    std::mutex work_queue_mutex_;
    std::deque<std::function<void()>> work_queue_;
};

#endif // IO_URING_LOOP_HPP