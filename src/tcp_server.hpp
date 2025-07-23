#ifndef TCP_SERVER_HPP
#define TCP_SERVER_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <map>

// Forward declarations
class IoUringLoop;
class TcpConnection;

// User-defined handlers for server events
using ConnectionHandler = std::function<void(std::shared_ptr<TcpConnection>)>;
using MessageHandler = std::function<void(std::shared_ptr<TcpConnection>, const std::vector<char>& data)>;

class TcpServer {
public:
    TcpServer(IoUringLoop& loop, int port);
    ~TcpServer();

    // Disable copy and move
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Start the server (begins listening and accepting)
    void start();
    
    // Set the user-defined handlers
    void set_on_connect(ConnectionHandler handler);
    void set_on_message(MessageHandler handler);
    void set_on_disconnect(ConnectionHandler handler);

    // Send data to a specific connection
    void send(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data);

private:
    void setup_listening_socket();
    void start_accept();
    void handle_new_connection(int client_socket);
    void start_reading(std::shared_ptr<TcpConnection> connection);
    void remove_connection(const std::shared_ptr<TcpConnection>& connection);

    IoUringLoop& loop_;
    int port_;
    int server_socket_;

    // Default handlers
    ConnectionHandler on_connect_ = [](auto){};
    MessageHandler on_message_ = [](auto, const auto&){};
    ConnectionHandler on_disconnect_ = [](auto){};

    // Keep track of active connections
    std::map<int, std::shared_ptr<TcpConnection>> connections_;
};

#endif // TCP_SERVER_HPP