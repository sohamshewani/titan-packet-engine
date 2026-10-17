#ifndef TITAN_FILTER_ENGINE_HPP
#define TITAN_FILTER_ENGINE_HPP

#include <cstdint>
#include <cstddef>
#include <cassert>
#include <new>
#include <atomic>
#include <array>
#include <memory>
#include <system_error>
#include <immintrin.h>

namespace titan {

// ============================================================================
// System Constants & Error Handling
// ============================================================================

constexpr std::size_t kCacheLineSize = 64;

enum class FilterError : uint32_t {
    Success = 0,
    ArenaExhausted,
    RingBufferFull,
    RingBufferEmpty,
    MalformedHeader,
    FlowWindowViolation
};

class FilterException : public std::system_error {
public:
    explicit FilterException(FilterError err, const char* msg)
        : std::system_error(static_warn_cast(err), std::generic_category(), msg),
          error_code_(err) {}

    [[nodiscard]] FilterError get_error() const noexcept { return error_code_; }

private:
    static int static_warn_cast(FilterError err) noexcept {
        return static_cast<int>(err);
    }
    FilterError error_code_;
};

// ============================================================================
// 1. Arena Allocator (Cache-line aligned, bump-pointer, zero-fragmentation)
// ============================================================================

template <std::size_t CapacityBytes>
class ArenaAllocator {
public:
    static_assert(CapacityBytes >= kCacheLineSize, "Arena capacity must be at least one cache line.");

    ArenaAllocator() noexcept 
        : offset_(0), epoch_(0) {
        // Ensure explicit 64-byte alignment of the backing buffer
        uintptr_t raw_addr = reinterpret_cast<uintptr_t>(storage_);
        uintptr_t aligned_addr = (raw_addr + (kCacheLineSize - 1)) & ~(kCacheLineSize - 1);
        // Adjust tracking if needed, but storage_ alignment is handled via alignas
    }

    ~ArenaAllocator() noexcept = default;

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        static_assert(alignof(T) <= kCacheLineSize, "Alignment exceeds cache line size bound.");
        constexpr std::size_t size = sizeof(T);
        constexpr std::size_t alignment = alignof(T) > alignof(std::max_align_t) ? alignof(T) : alignof(std::max_align_t);

        std::size_t current_offset = offset_.load(std::memory_order_relaxed);
        
        while (true) {
            uintptr_t current_address = reinterpret_cast<uintptr_t>(&storage_[current_offset]);
            std::size_t padding = (alignment - (current_address % alignment)) % alignment;
            std::size_t total_allocation = size + padding;

            if (current_offset + total_allocation > CapacityBytes) {
                // Arena Exhaustion: Trigger fallback or throw according to runtime policy
                throw FilterException(FilterError::ArenaExhausted, "ArenaAllocator capacity exhausted in hot-path.");
            }

            if (offset_.compare_exchange_weak(current_offset, current_offset + total_allocation,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                void* ptr = &storage_[current_offset + padding];
                return ::new (ptr) T(std::forward<Args>(args)...);
            }
        }
    }

    void* allocate_raw(std::size_t size, std::size_t alignment = kCacheLineSize) noexcept {
        std::size_t current_offset = offset_.load(std::memory_order_relaxed);

        while (true) {
            uintptr_t current_address = reinterpret_cast<uintptr_t>(&storage_[current_offset]);
            std::size_t padding = (alignment - (current_address % alignment)) % alignment;
            std::size_t total_allocation = size + padding;

            if (current_offset + total_allocation > CapacityBytes) {
                return nullptr;
            }

            if (offset_.compare_exchange_weak(current_offset, current_offset + total_allocation,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return &storage_[current_offset + padding];
            }
        }
    }

    void reset_epoch() noexcept {
        offset_.store(0, std::memory_order_release);
        epoch_.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t bytes_allocated() const noexcept {
        return offset_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t current_epoch() const noexcept {
        return epoch_.load(std::memory_order_relaxed);
    }

private:
    alignas(kCacheLineSize) uint8_t storage_[CapacityBytes + kCacheLineSize];
    std::atomic<std::size_t> offset_;
    std::atomic<uint64_t> epoch_;
};

// ============================================================================
// 2. Lock-Free Circular Ring Buffer (SpscRingBuffer)
// ============================================================================

template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Ring buffer capacity must be a power of two.");

public:
    SpscRingBuffer() noexcept : head_(0), tail_(0) {}

    ~SpscRingBuffer() noexcept {
        T dummy;
        while (pop(dummy)) {
            // Drain elements safely to invoke destructors if applicable
        }
    }

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;
    SpscRingBuffer(SpscRingBuffer&&) = delete;
    SpscRingBuffer& operator=(SpscRingBuffer&&) = delete;

    bool push(const T& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Ring Buffer Full (Drop-Tail or backpressure boundary)
        }

        buffer_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false;
        }

        buffer_[current_tail & kMask] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Ring Buffer Empty
        }

        item = std::move(buffer_[current_head & kMask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_acquire);
        std::size_t tail = tail_.load(std::memory_order_acquire);
        return (tail >= head) ? (tail - head) : 0;
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // Cache-line separation to eliminate false sharing between producer and consumer threads
    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    alignas(kCacheLineSize) std::array<T, Capacity> buffer_;
};

// ============================================================================
// 3. Sliding-Window Flow Control (FlowWindow)
// ============================================================================

class FlowWindow {
public:
    static constexpr std::size_t kWindowBits = 256;
    static constexpr std::size_t kWordSize = sizeof(uint64_t) * 8;
    static constexpr std::size_t kNumWords = kWindowBits / kWordSize;

    FlowWindow() noexcept : base_sequence_(0) {
        bits_.fill(0);
    }

    ~FlowWindow() noexcept = default;

    FlowWindow(const FlowWindow&) noexcept = default;
    FlowWindow& operator=(const FlowWindow&) noexcept = default;
    FlowWindow(FlowWindow&&) noexcept = default;
    FlowWindow& operator=(FlowWindow&&) noexcept = default;

    [[nodiscard]] bool check_and_update(uint64_t sequence_number) noexcept {
        if (sequence_number < base_sequence_) {
            int64_t diff = static_cast<int64_t>(base_sequence_) - static_cast<int64_t>(sequence_number);
            if (static_cast<std::size_t>(diff) < kWindowBits) {
                std::size_t bit_index = static_cast<std::size_t>(sequence_number % kWindowBits);
                std::size_t word_idx = bit_index / kWordSize;
                std::size_t bit_offset = bit_index % kWordSize;
                
                uint64_t mask = (1ULL << bit_offset);
                if ((bits_[word_idx] & mask) != 0) {
                    return false; // Duplicate packet dropped
                }
                bits_[word_idx] |= mask;
                return true;
            }
            return false; // Too old, outside tracking window
        }

        uint64_t diff = sequence_number - base_sequence_;
        if (diff >= kWindowBits) {
            std::size_t shift_words = diff / kWordSize;
            if (shift_words >= kNumWords) {
                bits_.fill(0);
                base_sequence_ = sequence_number - (sequence_number % kWordSize);
            } else {
                advance_window(shift_words);
                base_sequence_ += (shift_words * kWordSize);
            }
            diff = sequence_number - base_sequence_;
        }

        std::size_t bit_index = static_cast<std::size_t>(sequence_number % kWindowBits);
        std::size_t word_idx = bit_index / kWordSize;
        std::size_t bit_offset = bit_index % kWordSize;

        uint64_t mask = (1ULL << bit_offset);
        if ((bits_[word_idx] & mask) != 0) {
            return false; // Duplicate packet dropped
        }
        bits_[word_idx] |= mask;
        return true;
    }

    void reset() noexcept {
        base_sequence_ = 0;
        bits_.fill(0);
    }

private:
    void advance_window(std::size_t word_shifts) noexcept {
        if (word_shifts >= kNumWords) {
            bits_.fill(0);
            return;
        }
        for (std::size_t i = 0; i < kNumWords - word_shifts; ++i) {
            bits_[i] = bits_[i + word_shifts];
        }
        for (std::size_t i = kNumWords - word_shifts; i < kNumWords; ++i) {
            bits_[i] = 0;
        }
    }

    uint64_t base_sequence_;
    std::array<uint64_t, kNumWords> bits_;
};

// ============================================================================
// 4. Packet Structures & Core Filter Engine
// ============================================================================

struct alignas(kCacheLineSize) PacketHeader {
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    uint8_t  protocol;
    uint8_t  flags;
    uint16_t payload_len;
    uint64_t sequence_number;
};

struct PacketBuffer {
    PacketHeader header;
    const uint8_t* payload;
    std::size_t capacity_allocated;
};

class FilterEngine {
public:
    explicit FilterEngine(std::size_t max_rules = 1024) 
        : rule_count_(0) {
        rule_table_.fill(0);
    }

    ~FilterEngine() noexcept = default;

    FilterEngine(const FilterEngine&) = delete;
    FilterEngine& operator=(const FilterEngine&) = delete;
    FilterEngine(FilterEngine&&) = delete;
    FilterEngine& operator=(FilterEngine&&) = delete;

    void add_rule(uint32_t subnet_mask, uint32_t target_ip) noexcept {
        if (rule_count_ < kMaxRules) {
            rule_table_[rule_count_++] = Rule{subnet_mask, target_ip};
        }
    }

    [[nodiscard]] FilterError evaluate(const PacketBuffer& pkt, FlowWindow& flow_window) noexcept {
        // 1. Boundary & Malformed Check invariant
        if (pkt.payload == nullptr && pkt.header.payload_len > 0) {
            return FilterError::MalformedHeader;
        }

        // 2. Sliding-Window Flow Control Verification
        if (!flow_window.check_and_update(pkt.header.sequence_number)) {
            return FilterError::FlowWindowViolation;
        }

        // 3. High-Performance Packet Inspection
        for (std::size_t i = 0; i < rule_count_; ++i) {
            if ((pkt.header.dst_ip & rule_table_[i].mask) == (rule_table_[i].ip & rule_table_[i].mask)) {
                return FilterError::Success;
            }
        }

        return FilterError::Success;
    }

    void spin_backpressure_pause() noexcept {
        _mm_pause();
    }

private:
    struct Rule {
        uint32_t mask;
        uint32_t ip;
    };

    static constexpr std::size_t kMaxRules = 1024;
    std::array<Rule, kMaxRules> rule_table_;
    std::size_t rule_count_;
};

} // namespace titan

#endif // TITAN_FILTER_ENGINE_HPP