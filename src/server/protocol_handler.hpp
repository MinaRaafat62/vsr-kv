#ifndef PROTOCOL_HANDLER_HPP
#define PROTOCOL_HANDLER_HPP

#include "vsr_message.hpp"
#include <functional>
#include <memory>
#include <map>

class TcpConnection;
class TcpServer;

// Callback for the application layer, providing a fully parsed message.
using VsrMessageHandler = std::function<void(std::shared_ptr<TcpConnection>, vsr_message&)>;

class ProtocolHandler {
public:
    // The handler hooks into the server upon construction.
    explicit ProtocolHandler(TcpServer& server);

    // The application layer sets this callback to process incoming messages.
    void set_on_message_received(VsrMessageHandler handler);

    // The application layer uses this to send messages without worrying about serialization.
    void send_message(std::shared_ptr<TcpConnection> connection, const vsr_message& msg);
    
    // Convenience method for broadcasting.
    void broadcast_to_peers(const vsr_message& msg);

private:
    // This is the function that gets registered with the TcpServer.
    void handle_raw_data(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data);
    void on_disconnect(std::shared_ptr<TcpConnection> connection);

    TcpServer& server_;
    VsrMessageHandler on_message_received_ = [](auto, auto&){};
    std::map<int, std::vector<char>> connection_buffers_;
};

#endif // PROTOCOL_HANDLER_HPP