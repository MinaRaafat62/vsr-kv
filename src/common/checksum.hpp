#ifndef CHECKSUM_HPP
#define CHECKSUM_HPP

#include "utilities.hpp"
#include <vector>
#include <span>

using byte = uint8_t;

namespace checksum {
utilities::uint128_t calculate_checksum_128(std::span<const byte> data);
}
#endif // CHECKSUM_HPP