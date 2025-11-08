#ifndef RECOVERY_MESSAGES_HPP
#define RECOVERY_MESSAGES_HPP

#include "replica_types.hpp"
#include <vector>
#include <cstdint>
#include <map>

struct recovery_request_payload
{
    utilities::uint128_t nonce;
    std::vector<byte> serialize() const;
    static bool deserialize(const std::vector<byte>& data, recovery_request_payload& msg);
};


struct recovery_response_payload {
    utilities::uint128_t nonce;
    std::map<uint64_t, log_entry> log;

    std::vector<byte> serialize() const;
    static bool deserialize(const std::vector<byte>& data, recovery_response_payload& msg);
};

#endif // RECOVERY_MESSAGES_HPP