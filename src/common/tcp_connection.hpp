#ifndef TCP_CONNECTION_HPP
#define TCP_CONNECTION_HPP

#include <vector>
#include <memory>
#include <netinet/in.h>

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    explicit TcpConnection(int socket);
    ~TcpConnection();

    int get_socket() const;
    std::vector<char>& get_buffer();
    void close_socket();
    enum class State { UNIDENTIFIED, PEER, CLIENT };
    State get_state() const;
    void set_state(State new_state);

private:
    int socket_fd_;
    std::vector<char> buffer_;
    State state_ = State::UNIDENTIFIED; // Default to unidentified
};

#endif // TCP_CONNECTION_HPP