#include "checksum.hpp"
#include <blake3.h>
#include <cstring>


namespace checksum {
utilities::uint128_t calculate_checksum_128(std::span<const byte> data) {
    // The Blake3 hash output is 32 bytes long.
    uint8_t hash_output[BLAKE3_OUT_LEN];

    // Initialize the hasher, hash the data, and finalize to get the output.
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data.data(), data.size());
    blake3_hasher_finalize(&hasher, hash_output, BLAKE3_OUT_LEN);

    // The protocol uses a 128-bit (16-byte) checksum. We will take the first 16 bytes
    utilities::uint128_t result;
    std::memcpy(&result, hash_output, sizeof(result));
    
    return result;
}
}