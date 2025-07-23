#ifndef TCP_CONNECTION_HPP
#define TCP_CONNECTION_HPP

#include <vector>
#include <memory>
#include <netinet/in.h>

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(int socket);
    ~TcpConnection();

    int get_socket() const;
    std::vector<char>& get_buffer();
    void close_socket();

private:
    int socket_fd_;
    std::vector<char> buffer_;
};

#endif // TCP_CONNECTION_HPP