#include "vsr_message.hpp"
#include "checksum.hpp"
#include <cstring> 
#include <vector>
#include <iostream>

void vsr_message::calculate_and_set_body_checksum()
{
    if (payload.empty()){
        header.checksum_body = {0,0};
    } else {
        header.checksum_body = checksum::calculate_checksum_128(payload);
    }
}

void vsr_message::calculate_and_set_header_checksum(){
    header.checksum = {0,0};
    std::span<const byte> header_bytes(
        reinterpret_cast<const byte*>(&header), 
        sizeof(vsr_header)
    );
    header.checksum  = checksum::calculate_checksum_128(header_bytes);
}

bool vsr_message::verify_body_checksum() const {
    if (payload.empty()) {
        return header.checksum_body.high == 0 && header.checksum_body.low == 0;
    }
    auto calculated_checksum = checksum::calculate_checksum_128(payload);
    return calculated_checksum == header.checksum_body;
}


bool vsr_message::verify_header_checksum() const {
    vsr_header header_copy = header;
    header_copy.checksum = {0, 0};

    std::span<const byte> header_bytes(
        reinterpret_cast<const byte*>(&header_copy), 
        sizeof(vsr_header)
    );

    auto calculated_checksum = checksum::calculate_checksum_128(header_bytes);
    return calculated_checksum == header.checksum;
}


std::vector<byte> vsr_message::serialize() const {
    // Create a mutable copy to work with.
    vsr_message msg_to_serialize = *this;

    //  Set all payload-dependent fields in the header first, including the size.
    msg_to_serialize.calculate_and_set_body_checksum();
    msg_to_serialize.header.size = sizeof(vsr_header) + msg_to_serialize.payload.size();

    // Now that all other header fields are final, calculate the header checksum.
    msg_to_serialize.calculate_and_set_header_checksum();

    // Serialize the prepared message.
    std::vector<byte> buffer(msg_to_serialize.header.size);
    std::memcpy(buffer.data(), &msg_to_serialize.header, sizeof(vsr_header));
    if (!msg_to_serialize.payload.empty()) {
        std::memcpy(buffer.data() + sizeof(vsr_header), msg_to_serialize.payload.data(), msg_to_serialize.payload.size());
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
    // Basic size validation
    if (data.size() < sizeof(vsr_header)) {
        std::cerr << "Deserialization error: data size is smaller than header." << std::endl;
        return false;
    }

    // Copy data into the message struct
    std::memcpy(&msg.header, data.data(), sizeof(vsr_header));
    if (msg.header.size != data.size()) {
        std::cerr << "Deserialization error: size in header does not match data size." << std::endl;
        return false;
    }
    size_t payload_size = msg.header.size - sizeof(vsr_header);
    if (payload_size > 0) {
        msg.payload.assign(data.begin() + sizeof(vsr_header), data.end());
    }

    //  Automatically verify checksums. If they fail, deserialization fails.
    if (!msg.verify_header_checksum()) {
        std::cerr << "Deserialization error: Header checksum verification failed." << std::endl;
        return false;
    }
    if (!msg.verify_body_checksum()) {
        std::cerr << "Deserialization error: Body checksum verification failed." << std::endl;
        return false;
    }

    // Only if all checks pass, we return true.
    return true;
}