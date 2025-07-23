#ifndef IO_URING_LOOP_HPP
#define IO_URING_LOOP_HPP

#include <liburing.h>
#include <functional>
#include <memory>
#include <vector>

class TcpConnection;

using IoCallback = std::function<void(int)>;

class IoUringLoop {
public:
    explicit IoUringLoop(unsigned int queue_depth = 256);
    ~IoUringLoop();

    // Disable copy and move semantics
    IoUringLoop(const IoUringLoop&) = delete;
    IoUringLoop& operator=(const IoUringLoop&) = delete;

    // The main event loop. This function blocks and processes I/O events.
    void run();

    // Submit an asynchronous request to accept a new connection.
    void submit_accept(int server_socket, IoCallback callback);

    // Submit an asynchronous request to read from a socket.
    void submit_read(std::shared_ptr<TcpConnection> connection, size_t size, IoCallback callback);

    // Submit an asynchronous request to write to a socket.
    void submit_write(std::shared_ptr<TcpConnection> connection, const std::vector<char>& buffer, IoCallback callback);

private:
    struct IORequest {
        IoCallback on_complete;
        std::shared_ptr<TcpConnection> connection; // Keep connection alive
        std::vector<char> write_buffer; // Used only for write operations
    };

    io_uring ring_;
    bool is_running_ = true;
};



#endif