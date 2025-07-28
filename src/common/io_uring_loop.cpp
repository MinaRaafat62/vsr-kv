#include "io_uring_loop.hpp"
#include "tcp_connection.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <sys/eventfd.h>
#include <unistd.h>

IoUringLoop::IoUringLoop(unsigned int queue_depth) {
    if (io_uring_queue_init(queue_depth, &ring_, 0) < 0) {
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(strerror(errno)));
    }
    // Create an eventfd to wake up the loop from other threads.
    event_fd_ = eventfd(0, EFD_NONBLOCK);
    if (event_fd_ == -1) {
        throw std::runtime_error("Failed to create eventfd: " + std::string(strerror(errno)));
    }
}

IoUringLoop::~IoUringLoop() {
    io_uring_queue_exit(&ring_);
    close(event_fd_);
}

void IoUringLoop::post(std::function<void()> work) {
    {
        std::lock_guard<std::mutex> lock(work_queue_mutex_);
        work_queue_.push_back(std::move(work));
    }
    // Write to the eventfd to wake up the loop.
    uint64_t val = 1;
    ssize_t res = write(event_fd_, &val, sizeof(val));
    if (res < 0) {
        std::cerr << "Failed to write to eventfd" << std::endl;
    }
}

void IoUringLoop::process_work_queue() {
    std::deque<std::function<void()>> current_work;
    {
        std::lock_guard<std::mutex> lock(work_queue_mutex_);
        current_work.swap(work_queue_);
    }
    for (auto& work_item : current_work) {
        work_item();
    }
}

void IoUringLoop::post_eventfd_read() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::EVENTFD;
    io_uring_prep_read(sqe, event_fd_, &event_fd_buffer_, sizeof(event_fd_buffer_), 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::run() {
    post_eventfd_read(); // Post the first read on our eventfd.

    while (is_running_) {
        io_uring_submit_and_wait(&ring_, 1);

        process_work_queue();

        io_uring_cqe* cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&ring_, head, cqe) {
            count++;
            auto request = std::unique_ptr<IORequest>(static_cast<IORequest*>(io_uring_cqe_get_data(cqe)));
            
            if (!request) continue;

            if (request->type == RequestType::EVENTFD) {
                post_eventfd_read(); // Re-arm the eventfd read for the next wakeup.
            } else {
                std::visit([&](auto&& cb) {
                    using T = std::decay_t<decltype(cb)>;
                    if constexpr (std::is_same_v<T, AcceptCallback>) {
                        cb(cqe->res, request->remote_address);
                    } else if constexpr (std::is_same_v<T, IoCallback>) {
                        cb(cqe->res);
                    }
                }, request->callback);
            }
        }
        if (count > 0) {
            io_uring_cq_advance(&ring_, count);
        }
    }
}

// --- Submit methods ---
// These are now private helpers, called via post() from other threads.

void IoUringLoop::submit_accept(int server_socket, AcceptCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::ACCEPT;
    request->callback = std::move(callback);
    socklen_t client_len = sizeof(request->remote_address);
    io_uring_prep_accept(sqe, server_socket, reinterpret_cast<sockaddr*>(&request->remote_address), &client_len, 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_read(std::shared_ptr<TcpConnection> connection, size_t size, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::READ;
    request->callback = std::move(callback);
    request->connection = connection;
    connection->get_buffer().resize(size);
    io_uring_prep_read(sqe, connection->get_socket(), connection->get_buffer().data(), size, 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_write(std::shared_ptr<TcpConnection> connection, const std::vector<char>& buffer, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::WRITE;
    request->callback = std::move(callback);
    request->connection = connection;
    request->write_buffer = buffer;
    io_uring_prep_write(sqe, connection->get_socket(), request->write_buffer.data(), request->write_buffer.size(), 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_connect(int socket, const sockaddr_in& address, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::CONNECT;
    request->callback = std::move(callback);
    request->remote_address = address;
    io_uring_prep_connect(sqe, socket, reinterpret_cast<const sockaddr*>(&request->remote_address), sizeof(request->remote_address));
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_timeout(std::chrono::nanoseconds duration, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;
    auto request = std::make_unique<IORequest>();
    request->type = RequestType::TIMEOUT;
    request->callback = std::move(callback);
    request->timeout_spec.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    request->timeout_spec.tv_nsec = (duration % std::chrono::seconds(1)).count();
    io_uring_prep_timeout(sqe, &request->timeout_spec, 0, 0);
    io_uring_sqe_set_data(sqe, request.release());
}