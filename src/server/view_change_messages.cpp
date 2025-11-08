
#include "view_change_messages.hpp"
#include "serialization.hpp"
#include <cstring>


std::vector<byte> do_view_change_payload::serialize() const {
    std::vector<byte> buffer;
    write_pod(buffer, last_normal_view);
    write_log_to_buffer(buffer, log);
    return buffer;
}


bool do_view_change_payload::deserialize(const std::vector<byte>& data, do_view_change_payload& msg) {
    const byte* ptr = data.data();
    const byte* end = ptr + data.size();
    
    if (!read_pod(ptr, end, msg.last_normal_view)) return false;
    return read_log_from_buffer(ptr, end, msg.log);
}

std::vector<byte> start_view_payload::serialize() const {
    std::vector<byte> buffer;
    write_log_to_buffer(buffer, log);
    return buffer;
}


bool start_view_payload::deserialize(const std::vector<byte>& data, start_view_payload& msg) {
    const byte* ptr = data.data();
    const byte* end = ptr + data.size();
    return read_log_from_buffer(ptr, end, msg.log);
}