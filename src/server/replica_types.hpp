#ifndef REPLICA_TYPES_HPP
#define REPLICA_TYPES_HPP

#include "utilities.hpp"
#include "vsr_message.hpp"
#include <cstdint>
#include <vector>
#include <set>
#include <string>
#include <map>

using byte = uint8_t;

struct log_entry {
    uint32_t view;
    operation op_type;
    utilities::uint128_t client_id;
    uint32_t request_num;
    std::vector<byte> payload;
    std::set<int> prepare_ok_acks;
};

struct client_table_entry {
    uint32_t request_number = 0;
    bool executed = false;
    std::vector<byte> result;
};

#endif // REPLICA_TYPES_HPP