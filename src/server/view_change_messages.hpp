#ifndef VIEW_CHANGE_MESSAGES_HPP
#define VIEW_CHANGE_MESSAGES_HPP

#include "replica_types.hpp"
#include <vector>
#include <cstdint>
#include <map>

// Sent by a replica after receiving f+1 StartViewChange messages.
// Payload for a vsr_message with command::do_view_change.
// The header contains the new view, the sender's op/commit numbers, and replica ID.
struct do_view_change_payload {
    uint32_t last_normal_view;
    std::map<uint64_t, log_entry> log;

    std::vector<byte> serialize() const;
    static bool deserialize(const std::vector<byte>& data, do_view_change_payload& msg);
};


// Sent by the new primary to signal the start of the new view.
// Payload for a vsr_message with command::start_view.
// The header contains the new view, op, and commit numbers.
struct start_view_payload {
    std::map<uint64_t, log_entry> log;

    std::vector<byte> serialize() const;
    static bool deserialize(const std::vector<byte>& data, start_view_payload& msg);
};

#endif // VIEW_CHANGE_MESSAGES_HPP