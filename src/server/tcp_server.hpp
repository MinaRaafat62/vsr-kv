#ifndef TCP_SERVER_HPP
#define TCP_SERVER_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <netinet/in.h>

class IoUringLoop;
class TcpConnection;
class ReplicaManager;

using ConnectionHandler = std::function<void(std::shared_ptr<TcpConnection>)>;
using MessageHandler = std::function<void(std::shared_ptr<TcpConnection>, const std::vector<char>& data)>;

class TcpServer {
public:
    TcpServer(IoUringLoop& loop, int port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void set_on_connect(ConnectionHandler handler);
    void set_on_disconnect(ConnectionHandler handler);
    void send(std::shared_ptr<TcpConnection> connection, const std::vector<char>& data);
    void set_replica_manager(std::unique_ptr<ReplicaManager> manager);
    void register_new_connection(std::shared_ptr<TcpConnection> connection);
    void broadcast_to_peers(const std::vector<char>& data);
    void set_on_message(MessageHandler handler);

private:
    void setup_listening_socket();
    void start_accept();
    void handle_new_connection(int client_socket, const sockaddr_in& client_address);
    void start_reading(std::shared_ptr<TcpConnection> connection);
    void remove_connection(const std::shared_ptr<TcpConnection>& connection);

    IoUringLoop& loop_;
    int port_;
    int server_socket_;
    ConnectionHandler on_connect_ = [](auto){};
    ConnectionHandler on_disconnect_ = [](auto){};
    std::map<int, std::shared_ptr<TcpConnection>> connections_;
    std::unique_ptr<ReplicaManager> replica_manager_;
    MessageHandler on_message_ = [](auto, const auto&){};
};

#endif // TCP_SERVER_HPP