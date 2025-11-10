cmake_minimum_required(VERSION 3.22)
project(VsrProject CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
FetchContent_Declare(
    blake3_proj
    GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
    GIT_TAG        1.8.2
)

FetchContent_GetProperties(blake3_proj)
if(NOT blake3_proj_POPULATED)
    FetchContent_Populate(blake3_proj)
    add_subdirectory(${blake3_proj_SOURCE_DIR}/c ${blake3_proj_BINARY_DIR})
endif()


add_library(B3::blake3 ALIAS blake3)


#=============================================================================
# 1. DEFINE A STATIC LIBRARY FOR SHARED CODE
#=============================================================================
add_library(vsr_common STATIC
    src/common/io_uring_loop.cpp
    src/common/tcp_client.cpp
    src/common/tcp_connection.cpp
    src/common/vsr_message.cpp
    src/common/checksum.cpp
    src/common/utilities.cpp
)

target_include_directories(vsr_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/common
)

target_link_libraries(vsr_common PUBLIC uring B3::blake3)


#=============================================================================
# 2. DEFINE THE SERVER EXECUTABLE
#=============================================================================
add_executable(server
    src/server/server_main.cpp
    src/server/replica_manager.cpp
    src/server/tcp_server.cpp
    src/server/protocol_handler.cpp
    src/server/replica.cpp
    src/server/view_change_messages.cpp
    src/server/serialization.cpp
    src/server/recovery_messages.cpp
)

target_include_directories(server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/server
)

target_link_libraries(server PRIVATE vsr_common)


#=============================================================================
# 3. DEFINE THE CLIENT EXECUTABLE
#=============================================================================
add_executable(client
    src/client/client_main.cpp
)

target_link_libraries(client PRIVATE vsr_common)