#include "io_uring_loop.hpp"
#include "tcp_connection.hpp" // Now we need the full definition
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <netinet/in.h>

IoUringLoop::IoUringLoop(unsigned int queue_depth) {
    if (io_uring_queue_init(queue_depth, &ring_, 0) < 0) {
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(strerror(errno)));
    }
}

IoUringLoop::~IoUringLoop() {
    io_uring_queue_exit(&ring_);
}

void IoUringLoop::run() {
    while (is_running_) {
        io_uring_submit_and_wait(&ring_, 1);

        io_uring_cqe* cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&ring_, head, cqe) {
            count++;
            // Reconstitute the unique_ptr from the raw pointer to manage its lifetime.
            // When this unique_ptr goes out of scope, the IORequest is automatically deleted.
            std::unique_ptr<IORequest> request(static_cast<IORequest*>(io_uring_cqe_get_data(cqe)));
            
            if (request && request->on_complete) {
                // Execute the callback with the result of the operation.
                request->on_complete(cqe->res);
            }
        }

        if (count > 0) {
            io_uring_cq_advance(&ring_, count);
        }
    }
}

void IoUringLoop::submit_accept(int server_socket, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return; // Queue is full

    // Ownership of the request is passed to io_uring via the raw pointer.
    // We get it back in the run loop to manage its destruction.
    auto request = std::make_unique<IORequest>();
    request->on_complete = std::move(callback);

    io_uring_prep_accept(sqe, server_socket, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_read(std::shared_ptr<TcpConnection> connection, size_t size, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;

    auto request = std::make_unique<IORequest>();
    request->on_complete = std::move(callback);
    request->connection = connection; // Hold a shared_ptr to keep the connection alive
    
    // Ensure buffer in connection is large enough
    connection->get_buffer().resize(size);

    io_uring_prep_read(sqe, connection->get_socket(), connection->get_buffer().data(), size, 0);
    io_uring_sqe_set_data(sqe, request.release());
}

void IoUringLoop::submit_write(std::shared_ptr<TcpConnection> connection, const std::vector<char>& buffer, IoCallback callback) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return;

    auto request = std::make_unique<IORequest>();
    request->on_complete = std::move(callback);
    request->connection = connection;
    request->write_buffer = buffer; // Copy the data to be written

    io_uring_prep_write(sqe, connection->get_socket(), request->write_buffer.data(), request->write_buffer.size(), 0);
    io_uring_sqe_set_data(sqe, request.release());
}