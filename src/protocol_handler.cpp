#include "protocol_handler.hpp"
#include "tcp_connection.hpp"
#include "tcp_server.hpp"
#include <iostream>

protocol_handler::protocol_handler(TcpServer& server) : server_(server) {
    // Hook into the networking layer. The server will call handle_raw_data whenever it gets bytes.
    server_.set_on_message([this](std::shared_ptr<TcpConnection> conn, const std::vector<char>& data){
        this->handle_raw_data(conn, data);
    });
}

void protocol_handler::set_on_message_received(VsrMessageHandler handler) {
    on_message_received_ = std::move(handler);
}

void protocol_handler::handle_raw_data(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data) {
    vsr_message msg;
    // The protocol layer's job is to deserialize.
    if (vsr_message::deserialize(data, msg)) {
        // If successful, pass the structured message up to the application layer.
        on_message_received_(connection, msg);
    } else {
        std::cerr << "Protocol handler failed to deserialize message on socket " 
                  << connection->get_socket() << ". Closing connection." << std::endl;
        connection->close_socket(); // Invalid data might indicate a protocol error or bad actor.
    }
}

void protocol_handler::send_message(std::shared_ptr<TcpConnection> connection, const vsr_message& msg) {
    // The protocol layer's job is to serialize.
    std::vector<byte> serialized_msg_byte = msg.serialize();
    std::vector<char> serialized_msg_char(
        reinterpret_cast<char*>(serialized_msg_byte.data()), 
        reinterpret_cast<char*>(serialized_msg_byte.data() + serialized_msg_byte.size())
    );

    // Pass the raw bytes down to the networking layer.
    server_.send(connection, serialized_msg_char);
}

void protocol_handler::broadcast_to_peers(const vsr_message& msg) {
    std::vector<byte> serialized_msg_byte = msg.serialize();
    std::vector<char> serialized_msg_char(
        reinterpret_cast<char*>(serialized_msg_byte.data()), 
        reinterpret_cast<char*>(serialized_msg_byte.data() + serialized_msg_byte.size())
    );
    server_.broadcast_to_peers(serialized_msg_char);
}