#ifndef VSR_MESSAGE_HPP
#define VSR_MESSAGE_HPP
#include "utilities.hpp"
#include <cstdint>
#include <string>
#include <vector>


using byte = uint8_t;

enum class command : byte {
    reserved,
    ping,
    pong,
    request,
    prepare,
    prepare_ok,
    reply,
    commit,
    start_view_change,
    do_view_change,
    start_view,
    recovery,
    recovery_response,
};

enum class operation : byte {
    reserved,
    set,
    update,
    get,
};

struct [[gnu::packed]] vsr_header {
    utilities::uint128_t checksum;
    utilities::uint128_t checksum_body;
    utilities::uint128_t parent;
    utilities::uint128_t client;
    utilities::uint128_t context;
    uint32_t size;
    uint32_t request;
    uint32_t cluster;
    uint32_t epoch;
    uint32_t view;
    uint64_t op;
    uint64_t commit;
    uint64_t offset;
    byte replica;
    command command_;
    operation operation_;
    byte version;
};


class vsr_message {
public:
    vsr_message() : header{} {}
    vsr_header header;
    std::vector<byte> payload;

    std::vector<byte> serialize() const;

    static bool deserialize(const std::vector<byte>& data, vsr_message& msg);
    static bool deserialize(const std::vector<char>& data, vsr_message& msg);

};

#endif