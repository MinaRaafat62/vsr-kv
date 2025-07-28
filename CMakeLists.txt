cmake_minimum_required(VERSION 3.22)
project(ModernTCPServer CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


#=============================================================================
# 1. DEFINE A STATIC LIBRARY FOR SHARED CODE
# This library will contain all the common networking and protocol logic.
# It gets compiled once and then linked into both the server and the client.
#=============================================================================
add_library(vsr_common STATIC
    src/common/io_uring_loop.cpp
    src/common/tcp_client.cpp
    src/common/tcp_connection.cpp
    src/common/vsr_message.cpp
)


# Make the headers in src/common available to any target that links vsr_common.
# The 'PUBLIC' keyword is crucial here.
target_include_directories(vsr_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/common
)

# The common library itself needs the uring library.
target_link_libraries(vsr_common PUBLIC uring)


#=============================================================================
# 2. DEFINE THE SERVER EXECUTABLE
#=============================================================================
add_executable(server
    src/server/server_main.cpp
    src/server/replica_manager.cpp
    src/server/tcp_server.cpp
    src/server/protocol_handler.cpp
)

# Make the headers in src/server available to the server executable.
target_include_directories(server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/server
)

# Link the server against our common library. This automatically gives it
# access to all the public headers and libraries from vsr_common.
target_link_libraries(server PRIVATE vsr_common)



#=============================================================================
# 3. DEFINE THE CLIENT EXECUTABLE
#=============================================================================
add_executable(client
    src/client/client_main.cpp
)

# Link the client against our common library.
target_link_libraries(client PRIVATE vsr_common)