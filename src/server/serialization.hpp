#ifndef SERIALIZATION_HPP
#define SERIALIZATION_HPP

#include "replica_types.hpp" 
#include <vector>
#include <cstdint>
#include <map>
#include <cstring>


template<typename T>
void write_pod(std::vector<byte>& buffer, const T& value) {
    const byte* bytes = reinterpret_cast<const byte*>(&value);
    buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
}

template<typename T>
bool read_pod(const byte*& ptr, const byte* end, T& value) {
    if (ptr + sizeof(T) > end) return false;
    std::memcpy(&value, ptr, sizeof(T));
    ptr += sizeof(T);
    return true;
}


void write_log_to_buffer(std::vector<byte>& buffer, const std::map<uint64_t, log_entry>& log);
bool read_log_from_buffer(const byte*& ptr, const byte* end, std::map<uint64_t, log_entry>& log);

#endif // SERIALIZATION_HPP