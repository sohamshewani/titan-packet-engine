#include <atomic>
#include <cstdint>
#include <cstddef>
#include <new>
#include <immintrin.h>
#include <chrono>
#include <cstring>
#include <array>

namespace titan {

constexpr size_t CACHE_LINE_SIZE = 64;

// 1. Arena Allocator
class ArenaAllocator {
private:
    uint8_t* buffer_;
    size_t capacity_;
    std::atomic<size_t> offset_;

public:
    explicit ArenaAllocator(size_t capacity) 
        : capacity_(capacity), offset_(0) {
        // Enforce 64-byte alignment for the backing store
        int ret = posix_memalign(reinterpret_cast<void**>(&buffer_), CACHE_LINE_SIZE, capacity_);
        if (ret != 0 || buffer_ == nullptr) {
            throw std::bad_alloc();
        }
        std::memset(buffer_, 0, capacity_);
    }

    ~ArenaAllocator() {
        free(buffer_);
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    template <typename T>
    T* allocate(size_t count = 1) {
        size_t size = sizeof(T) * count;
        // Align request size to 64 bytes
        size_t aligned_size = (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
        
        size_t current_offset = offset_.load(std::memory_order_relaxed);
        while (true) {
            if (current_offset + aligned_size > capacity_) {
                // Arena Exhaustion: Trigger epoch reset / fallback
                return nullptr;
            }
            if (offset_.compare_exchange_weak(current_offset, current_offset + aligned_size,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return reinterpret_cast<T*>(buffer_ + current_offset);
            }
        }
    }

    void reset() {
        offset_.store(0, std::memory_order_release);
    }

    size_t available() const {
        size_t current = offset_.load(std::memory_order_relaxed);
        return current <= capacity_ ? capacity_ - current : 0;
    }
};

// 2. Lock-Free Circular Ring Buffer
struct PacketDescriptor {
    uint8_t* payload;
    uint32_t length;
    uint32_t flow_id;
    uint64_t timestamp;
};

template <size_t Capacity>
class SpscRingBuffer {
    static_uff:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

private:
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_{0};
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_{0};
    alignas(CACHE_LINE_SIZE) std::array<PacketDescriptor, Capacity> buffer_;

public:
    SpscRingBuffer() = default;
    ~SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    bool push(const PacketDescriptor& pkt) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t current_head = head_.load(std::memory_order_acquire);

        if (current_tail - current_head >= Capacity) {
            // Ring Buffer Overflow: Drop-tail policy applied
            return false;
        }

        buffer_[current_tail & (Capacity - 1)] = pkt;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(PacketDescriptor& pkt) {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false;
        }

        pkt = buffer_[current_head & (Capacity - 1)];
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    size_t size() const {
        size_t h = head_.load(std::memory_order_acquire);
        size_t t = tail_.load(std::memory_order_acquire);
        return t >= h ? t - h : 0;
    }
};

// 3. Sliding-Window Flow Control
class FlowWindow {
private:
    static constexpr size_t WINDOW_SIZE = 64;
    uint64_t bitset_;
    uint64_t last_window_marker_;

public:
    FlowWindow() : bitset_(0), last_window_marker_(0) {}

    bool validate_and_update(uint64_t sequence_number) {
        if (sequence_number <= last_window_marker_) {
            int64_t diff = static_cast<int64_t>(last_window_marker_) - static_cast<int64_t>(sequence_number);
            if (diff >= static_cast<int64_t>(WINDOW_SIZE)) {
                return false; // Too old, drop
            }
            uint32_t bit_pos = static_cast<uint32_t>(diff);
            if ((bitset_ & (1ULL << bit_pos)) != 0ULL) {
                return false; // Duplicate packet
            }
            bitset_ |= (1ULL << bit_pos);
            return true;
        } else {
            uint64_t diff = sequence_number - last_window_marker_;
            if (diff >= WINDOW_SIZE) {
                bitset_ = 1ULL; // Reset window, current is newest
            } else {
                bitset_ <<= diff;
                bitset_ |= 1ULL;
            }
            last_window_marker_ = sequence_number;
            return true;
        }
    }
};

// 4. Poll Loop Implementation
class PollLoop {
private:
    ArenaAllocator arena_;
    SpscRingBuffer<1024> rx_ring_;
    SpscRingBuffer<1024> dead_letter_ring_;
    FlowWindow flow_window_;
    std::atomic<bool> running_;
    uint64_t dropped_packets_counter_;
    uint64_t malformed_packets_counter_;

    bool process_packet(const PacketDescriptor& pkt) {
        // Poisoned Packet / Malformed Header Verification
        if (pkt.payload == nullptr || pkt.length < sizeof(uint32_t) || pkt.length > 9000) {
            malformed_packets_counter_++;
            dead_letter_ring_.push(pkt);
            return false;
        }

        // Sliding-Window Flow Control evaluation
        uint64_t seq = *reinterpret_cast<const uint64_t*>(pkt.payload);
        if (!flow_window_.validate_and_update(seq)) {
            dropped_packets_counter_++;
            return false;
        }

        // Zero-copy invariant maintained: Payload pointer untouched.
        return true;
    }

public:
    explicit PollLoop(size_t arena_size)
        : arena_(arena_size),
          running_(false),
          dropped_packets_counter_(0),
          malformed_packets_counter_(0) {}

    ~PollLoop() {
        stop();
    }

    void start() {
        running_.store(true, std::memory_order_release);
        
        // Main poll loop execution context
        while (running_.load(std::memory_order_acquire)) {
            PacketDescriptor pkt;
            if (rx_ring_.pop(pkt)) {
                process_packet(pkt);
            } else {
                // Hardware-friendly backpressure spin
                _mm_pause();
            }

            // Periodically check arena capacity and handle epoch resets
            if (arena_.available() < 256) {
                arena_.reset();
            }
        }
    }

    void stop() {
        running_.store(false, std::memory_order_release);
    }

    bool enqueue_ingress(const uint8_t* raw_payload, uint32_t length, uint32_t flow_id) {
        // Allocate packet tracker structure within the arena
        PacketDescriptor* pkt = arena_.allocate<PacketDescriptor>();
        if (pkt == nullptr) {
            // Arena Exhaustion fallback: Drop or route safely
            return false;
        }

        pkt->payload = const_cast<uint8_t*>(raw_payload);
        pkt->length = length;
        pkt->flow_id = flow_id;
        pkt->timestamp = static_cast<uint64_t>(
            std::chrono::high_resolution_clock::now().time_since_epoch().count()
        );

        if (!rx_ring_.push(*pkt)) {
            dropped_packets_counter_++;
            return false;
        }

        return true;
    }

    uint64_t get_dropped_count() const {
        return dropped_packets_counter_;
    }

    uint64_t get_malformed_count() const {
        return malformed_packets_counter_;
    }
};

} // namespace titan