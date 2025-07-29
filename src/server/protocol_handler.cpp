#include "protocol_handler.hpp"
#include "tcp_connection.hpp"
#include "tcp_server.hpp"
#include <iostream>
#include <span>
#include <cstring>
using byte = uint8_t;


ProtocolHandler::ProtocolHandler(TcpServer& server) : server_(server) {
    // Hook into the networking layer. The server will call handle_raw_data whenever it gets bytes.
    server_.set_on_message([this](std::shared_ptr<TcpConnection> conn, const std::vector<char>& data){
        this->handle_raw_data(conn, data);
    });

    server_.set_on_disconnect([this](auto conn){
        this->on_disconnect(conn);
    });
}


void ProtocolHandler::set_on_message_received(VsrMessageHandler handler) {
    on_message_received_ = std::move(handler);
}

void ProtocolHandler::on_disconnect(std::shared_ptr<TcpConnection> connection) {
    if (connection) {
        connection_buffers_.erase(connection->get_socket());
    }
}

void ProtocolHandler::handle_raw_data(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data) {
    int socket_fd = connection->get_socket();
    if (socket_fd == -1) return;

    // Append new data to this connection's buffer.
    auto& buffer = connection_buffers_[socket_fd];
    buffer.insert(buffer.end(), data.begin(), data.end());

    // Loop to process as many complete messages as we have in the buffer.
    while (true) {
        //  Do we have enough data for at least a header?
        if (buffer.size() < sizeof(vsr_header)) {
            break; // Not enough data, wait for the next read.
        }

        // Peek at the header to determine the full message size.
        vsr_header header;
        std::memcpy(&header, buffer.data(), sizeof(vsr_header));
        uint32_t expected_size = header.size;

        // Sanity check the size.
        if (expected_size == 0 || expected_size > 65536) { // Max 64KB msg
             std::cerr << "Protocol error: Invalid message size " << expected_size 
                       << " on socket " << socket_fd << ". Closing." << std::endl;
             connection_buffers_.erase(socket_fd);
             connection->close_socket();
             return;
        }

        // Do we have the complete message in our buffer?
        if (buffer.size() < expected_size) {
            break; // Not yet, wait for the next read.
        }

        //  Extract the full message data.
        std::vector<char> full_message_data(buffer.begin(), buffer.begin() + expected_size);
        
        // Try to deserialize it.
        vsr_message msg;
        if (vsr_message::deserialize(full_message_data, msg)) {
            on_message_received_(connection, msg);
        } else {
            std::cerr << "Protocol handler failed to deserialize message on socket " 
                      << socket_fd << ". Closing connection." << std::endl;
            connection_buffers_.erase(socket_fd);
            connection->close_socket();
            return;
        }

        // 2g. Remove the processed message from the front of the buffer.
        buffer.erase(buffer.begin(), buffer.begin() + expected_size);
    }
}

void ProtocolHandler::send_message(std::shared_ptr<TcpConnection> connection, const vsr_message& msg) {
    // The protocol layer's job is to serialize.
    std::vector<byte> serialized_msg_byte = msg.serialize();
    std::vector<char> serialized_msg_char(
        reinterpret_cast<char*>(serialized_msg_byte.data()), 
        reinterpret_cast<char*>(serialized_msg_byte.data() + serialized_msg_byte.size())
    );

    // Pass the raw bytes down to the networking layer.
    server_.send(connection, serialized_msg_char);
}

void ProtocolHandler::broadcast_to_peers(const vsr_message& msg) {
    std::vector<byte> serialized_msg_byte = msg.serialize();
    std::vector<char> serialized_msg_char(
        reinterpret_cast<char*>(serialized_msg_byte.data()), 
        reinterpret_cast<char*>(serialized_msg_byte.data() + serialized_msg_byte.size())
    );
    server_.broadcast_to_peers(serialized_msg_char);
}