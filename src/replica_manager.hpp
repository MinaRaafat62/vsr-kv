#ifndef REPLICA_MANAGER_HPP
#define REPLICA_MANAGER_HPP

#include <vector>
#include <string>
#include <memory>
#include <map>
#include <chrono>
#include <netinet/in.h>

class IoUringLoop;
class TcpServer;
class TcpConnection;

struct Peer {
    int id;
    std::string host;
    int port;
    enum class State { DISCONNECTED, CONNECTING, CONNECTED } state = State::DISCONNECTED;
    std::shared_ptr<TcpConnection> connection = nullptr;
};

class ReplicaManager {
public:
    ReplicaManager(IoUringLoop& loop, TcpServer& server, int self_id, std::vector<Peer> peers);
    void connect_to_peers();
    void on_disconnect(const std::shared_ptr<TcpConnection>& connection);
    void send_to_peer(int peer_id, const std::vector<char>& data);
    void add_peer(Peer new_peer);
    void remove_peer(int peer_id);
    void broadcast_to_peers(const std::vector<char>& data, int exclude_id = -1);
    bool is_peer_connection(const std::shared_ptr<TcpConnection>& connection) const;
    void register_identified_peer(int peer_id, std::shared_ptr<TcpConnection> connection);


private:
    void attempt_connection(Peer& peer);
    void schedule_reconnect(Peer& peer);
    void send_handshake(Peer& peer);

    IoUringLoop& loop_;
    TcpServer& server_;
    int self_id_;
    std::map<int, Peer> peers_by_id_;
    std::map<int, int> socket_to_peer_id_;
    std::map<int, int> port_to_peer_id_;
    const std::chrono::seconds reconnect_delay_{5};
};

#endif // REPLICA_MANAGER_HPP