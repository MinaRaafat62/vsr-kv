#include "recovery_messages.hpp"
#include "serialization.hpp"

std::vector<byte> recovery_request_payload::serialize() const {
    std::vector<byte> buffer;
    write_pod(buffer, nonce);
    return buffer;
}


bool recovery_request_payload::deserialize(const std::vector<byte> &data, recovery_request_payload &msg) {
    const byte* ptr = data.data();
    const byte* end = ptr + data.size();
    return read_pod(ptr, end, msg.nonce);
}

std::vector<byte> recovery_response_payload::serialize() const {
    std::vector<byte> buffer;
    write_pod(buffer, nonce);
    write_log_to_buffer(buffer, log);
    return buffer;
}

bool recovery_response_payload::deserialize(const std::vector<byte>& data, recovery_response_payload& msg) {
    const byte* ptr = data.data();
    const byte* end = ptr + data.size();
    if (!read_pod(ptr, end, msg.nonce)) return false;
    return read_log_from_buffer(ptr, end, msg.log);
}