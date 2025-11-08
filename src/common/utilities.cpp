#include "utilities.hpp"

utilities::uint128_t utilities::generate_nonce(int replica_id)
{
    static std::atomic<uint32_t> counter = 0;
    
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);

    uint64_t high = ts.tv_sec * 1000000000ULL + ts.tv_nsec;
    
    // The low part combines the unique replica ID and the process-local counterPP
    // to prevent collisions between replicas or rapid calls within the same replica.
    uint64_t low = (static_cast<uint64_t>(replica_id) << 32) | counter++;

    return {high, low};
}