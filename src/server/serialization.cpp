#include "serialization.hpp"

void write_log_to_buffer(std::vector<byte>& buffer, const std::map<uint64_t, log_entry>& log) {
    // Write the number of key-value pairs.
    write_pod<uint64_t>(buffer, log.size());

    // Iterate and write each pair.
    for (const auto& [op_num, entry] : log) {
        // Write the key.
        write_pod<uint64_t>(buffer, op_num);

        // Write the value (the log_entry).
        write_pod<uint32_t>(buffer, entry.view);
        write_pod<operation>(buffer, entry.op_type);
        write_pod<utilities::uint128_t>(buffer, entry.client_id);
        write_pod<uint32_t>(buffer, entry.request_num);
        
        // For the dynamic vector inside the log_entry, first write its size...
        write_pod<uint64_t>(buffer, entry.payload.size());
        // then write its content.
        buffer.insert(buffer.end(), entry.payload.begin(), entry.payload.end());
    }
}

bool read_log_from_buffer(const byte*& ptr, const byte* end, std::map<uint64_t, log_entry>& log) {
    // Read the number of key-value pairs.
    uint64_t log_size;
    if (!read_pod(ptr, end, log_size)) return false;

    // Loop that many times to read each pair.
    for (uint64_t i = 0; i < log_size; ++i) {
        // Read the key.
        uint64_t op_num;
        if (!read_pod(ptr, end, op_num)) return false;

        // Read the value (the log_entry).
        log_entry entry;
        if (!read_pod(ptr, end, entry.view)) return false;
        if (!read_pod(ptr, end, entry.op_type)) return false;
        if (!read_pod(ptr, end, entry.client_id)) return false;
        if (!read_pod(ptr, end, entry.request_num)) return false;

        // For the dynamic vector, first read its size...
        uint64_t payload_size;
        if (!read_pod(ptr, end, payload_size)) return false;

        // then read its content.
        if (ptr + payload_size > end) return false;
        entry.payload.assign(ptr, ptr + payload_size);
        ptr += payload_size;

        // Insert the reconstructed pair into the map.
        log[op_num] = entry;
    }
    return true;
}