#ifndef UTILITIES_HPP
#define UTILITIES_HPP

#include <cstdint>

namespace utilities {

struct uint128_t {
    uint64_t high;
    uint64_t low;

    bool operator==(const uint128_t& other) const {
        return high == other.high && low == other.low;
    }
};

}

#endif