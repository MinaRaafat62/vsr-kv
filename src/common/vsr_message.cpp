#include "vsr_message.hpp"
#include <cstring> 
#include <vector>
#include <iostream>

// This is the implementation for the serialize function. It remains unchanged.
std::vector<byte> vsr_message::serialize() const {
    vsr_header header_to_serialize = header;
    header_to_serialize.size = sizeof(vsr_header) + payload.size();
    std::vector<byte> buffer(header_to_serialize.size);
    std::memcpy(buffer.data(), &header_to_serialize, sizeof(vsr_header));
    if (!payload.empty()) {
        std::memcpy(buffer.data() + sizeof(vsr_header), payload.data(), payload.size());
    }
    return buffer;
}

bool vsr_message::deserialize(const std::vector<char>& data, vsr_message& msg) {
    // Create a vector of bytes from the vector of chars.
    const std::vector<byte> byte_data(
        reinterpret_cast<const byte*>(data.data()), 
        reinterpret_cast<const byte*>(data.data() + data.size())
    );
    // Call the other overload that does the actual work.
    return deserialize(byte_data, msg);
}

bool vsr_message::deserialize(const std::vector<byte>& data, vsr_message& msg) {
    // A valid message must at least contain a full header.
    if (data.size() < sizeof(vsr_header)) {
        std::cerr << "Deserialization error: data size (" << data.size() 
                  << ") is smaller than header size (" << sizeof(vsr_header) << ")." << std::endl;
        return false;
    }

    // Copy the header data from the buffer into the vsr_header struct.
    std::memcpy(&msg.header, data.data(), sizeof(vsr_header));

    // Validate the total size specified in the header.
    if (data.size() != msg.header.size) {
        std::cerr << "Deserialization error: data size (" << data.size() 
                  << ") does not match size in header (" << msg.header.size << ")." << std::endl;
        return false;
    }

    // The size in the header cannot be smaller than the header itself.
    if (msg.header.size < sizeof(vsr_header)) {
        std::cerr << "Deserialization error: size in header (" << msg.header.size
                  << ") is smaller than the header itself (" << sizeof(vsr_header) << ")." << std::endl;
        return false;
    }
    
    // Calculate the payload size.
    size_t payload_size = msg.header.size - sizeof(vsr_header);

    if (payload_size > 0) {
        // Copy the payload from the buffer.
        msg.payload.assign(data.begin() + sizeof(vsr_header), data.end());
    } else {
        msg.payload.clear();
    }

    return true;
}