#ifndef UTILITIES_HPP
#define UTILITIES_HPP

#include <cstdint>
#include <atomic>
#include <ctime>

namespace utilities {

struct uint128_t {
    uint64_t high;
    uint64_t low;

    bool operator==(const uint128_t& other) const {
        return high == other.high && low == other.low;
    }

    bool operator<(const uint128_t& other) const {
        if (high < other.high) {
            return true;
        }
        if (high > other.high) {
            return false;
        }
        // If high parts are equal, compare the low parts
        return low < other.low;
    }
};


uint128_t generate_nonce(int replica_id);

}

#endif