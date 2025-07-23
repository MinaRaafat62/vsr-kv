#include "io_uring_loop.hpp"
#include "tcp_server.hpp"
#include "tcp_connection.hpp"
#include <iostream>
#include <vector>
#include <string>

int main() {
    constexpr int PORT = 8080;

    try {
        IoUringLoop loop;
        TcpServer server(loop, PORT);

        server.set_on_connect([](std::shared_ptr<TcpConnection> conn) {
            std::cout << "New client connected, socket: " << conn->get_socket() << std::endl;
        });

        server.set_on_disconnect([](std::shared_ptr<TcpConnection> conn) {
            std::cout << "Client disconnected, socket: " << conn->get_socket() << std::endl;
        });

        server.set_on_message([&server](std::shared_ptr<TcpConnection> conn, const std::vector<char>& data) {
            std::string message(data.begin(), data.end());
            std::cout << "Received from socket " << conn->get_socket() << ": " << message;

            // Echo the data back to the client
            server.send(conn, data);
        });

        server.start();
        loop.run();

    } catch (const std::exception& e) {
        std::cerr << "Critical error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}