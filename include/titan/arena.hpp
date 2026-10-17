#ifndef TITAN_ARENA_HPP
#define TITAN_ARENA_HPP

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <atomic>
#include <array>
#include <bitset>
#include <system_error>
#include <new>

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif

namespace titan {

enum class ErrorCode : uint32_t {
    Success = 0,
    ArenaExhausted,
    RingBufferFull,
    RingBufferEmpty,
    MalformedHeader,
    FlowWindowViolation
};

class TitanErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan-packet-engine";
    }

    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            aligncase ErrorCode::Success: return "Success";
            case ErrorCode::ArenaExhausted: return "Arena memory exhausted";
            case ErrorCode::RingBufferFull: return "SPSC ring buffer full (drop-tail triggered)";
            case ErrorCode::RingBufferEmpty: return "SPSC ring buffer empty";
            case ErrorCode::MalformedHeader: return "Poisoned packet or malformed header detected";
            case ErrorCode::FlowWindowViolation: return "Sliding window rate limit or duplicate violation";
            default: return "Unknown error";
        }
    }
};

inline const std::error_category& get_titan_error_category() noexcept {
    static TitanErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) noexcept {
    return {static_cast<int>(e), get_titan_error_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::ErrorCode> : true_type {};
}

namespace titan {

class alignas(64) ArenaAllocator {
public:
    explicit ArenaAllocator(std::size_t capacity_bytes = 1024 * 1024 * 64)
        : capacity_(align_up(capacity_bytes, 64)),
          offset_(0) {
        #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
        void* ptr = nullptr;
        if (posix_memalign(&ptr, 64, capacity_) != 0 || ptr == nullptr) {
            throw std::bad_alloc();
        }
        memory_ = static_cast<std::byte*>(ptr);
        #else
        void* ptr = std::malloc(capacity_ + 64);
        if (ptr == nullptr) {
            throw std::bad_alloc();
        }
        std::uintptr_t raw = reinterpret_cast<std::uintptr_t>(ptr);
        std::uintptr_t aligned = (raw + 63) & ~static_cast<std::uintptr_t>(63);
        memory_ = reinterpret_cast<std::byte*>(aligned);
        base_allocation_ = ptr;
        #endif
    }

    ~ArenaAllocator() noexcept {
        #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
        std::free(memory_);
        #else
        std::free(base_allocation_);
        #endif
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    ArenaAllocator(ArenaAllocator&& other) noexcept
        : memory_(other.memory_),
          base_allocation_(other.base_allocation_),
          capacity_(other.capacity_),
          offset_(other.offset_.load(std::memory_order_relaxed)) {
        other.memory_ = nullptr;
        other.base_allocation_ = nullptr;
        other.capacity_ = 0;
        other.offset_.store(0, std::memory_order_relaxed);
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
            std::free(memory_);
            #else
            std::free(base_allocation_);
            #endif
            memory_ = other.memory_;
            base_allocation_ = other.base_allocation_;
            capacity_ = other.capacity_;
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            
            other.memory_ = nullptr;
            other.base_allocation_ = nullptr;
            other.capacity_ = 0;
            other.offset_.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        std::size_t size = sizeof(T);
        std::size_t align = alignof(T);
        if (align < 64) {
            align = 64;
        }

        void* mem = allocate_bytes(size, align);
        if (mem == nullptr) {
            return nullptr;
        }
        return ::new (mem) T(std::forward<Args>(args)...);
    }

    void* allocate_bytes(std::size_t size, std::size_t alignment = 64) noexcept {
        if (alignment < 64) {
            alignment = 64;
        }
        std::size_t current_offset = offset_.load(std::memory_order_relaxed);
        while (true) {
            std::uintptr_t current_address = reinterpret_cast<std::uintptr_t>(memory_ + current_offset);
            std::size_t padding = (alignment - (current_address % alignment)) % alignment;
            std::size_t total_needed = size + padding;

            if (current_offset + total_needed > capacity_) {
                return nullptr;
            }

            if (offset_.compare_exchange_weak(current_offset, current_offset + total_needed,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return static_cast<void*>(memory_ + current_offset + padding);
            }
        }
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] std::size_t allocated_bytes() const noexcept { return offset_.load(std::memory_order_relaxed); }

private:
    [[nodiscard]] static constexpr std::size_t align_up(std::size_t val, std::size_t alignment) noexcept {
        return (val + alignment - 1) & ~(alignment - 1);
    }

    std::byte* memory_{nullptr};
    void* base_allocation_{nullptr};
    std::size_t capacity_{0};
    alignas(64) std::atomic<std::size_t> offset_{0};
};

template <typename T, std::size_t Capacity>
class alignas(64) SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "SpscRingBuffer capacity must be a power of two.");

public:
    SpscRingBuffer() : head_(0), tail_(0) {}

    ~SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    bool push(const T& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false;
        }

        buffer_[current_tail & Mask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false;
        }

        buffer_[current_tail & Mask] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false;
        }

        item = std::move(buffer_[current_head & Mask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool full() const noexcept {
        return (tail_.load(std::memory_order_acquire) - head_.load(std::memory_order_acquire)) >= Capacity;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t tail = tail_.load(std::memory_order_acquire);
        std::size_t head = head_.load(std::memory_order_acquire);
        return (tail >= head) ? (tail - head) : 0;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

private:
    static constexpr std::size_t Mask = Capacity - 1;

    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    alignas(64) std::array<T, Capacity> buffer_;
};

class alignas(64) FlowWindow {
public:
    explicit FlowWindow(std::size_t window_size = 1024)
        : window_size_(window_size),
          bitset_storage_(window_size_, false) {}

    ~FlowWindow() = default;

    FlowWindow(const FlowWindow&) = delete;
    FlowWindow& operator=(const FlowWindow&) = delete;

    FlowWindow(FlowWindow&&) noexcept = default;
    FlowWindow& operator=(FlowWindow&&) noexcept = default;

    [[nodiscard]] std::error_code evaluate(uint64_t flow_id) noexcept {
        std::size_t index = static_cast<std::size_t>(flow_id) % window_size_;
        
        if (bitset_storage_[index]) {
            return make_error_code(ErrorCode::FlowWindowViolation);
        }

        bitset_storage_.set(index, true);
        return make_error_code(ErrorCode::Success);
    }

    void reset() noexcept {
        bitset_storage_.reset();
    }

    [[nodiscard]] std::size_t window_size() const noexcept {
        return window_size_;
    }

private:
    std::size_t window_size_;
    std::bitset<65536> bitset_storage_;
};

struct PacketContext {
    const std::byte* payload;
    std::size_t length;
    uint64_t flow_tuple_hash;
    uint64_t timestamp;

    [[nodiscard]] bool validate() const noexcept {
        if (payload == nullptr || length == 0 || length > 65535) {
            return false;
        }
        return true;
    }
};

class alignas(64) PacketEnginePipeline {
public:
    PacketEnginePipeline(std::size_t arena_size, std::size_t ring_capacity)
        : arena_(arena_size),
          ingest_ring_(),
          dead_letter_ring_(),
          flow_window_() {}

    ~PacketEnginePipeline() = default;

    PacketEnginePipeline(const PacketEnginePipeline&) = delete;
    PacketEnginePipeline& operator=(const PacketEnginePipeline&) = delete;

    std::error_code ingest_packet(const std::byte* raw_data, std::size_t len, uint64_t flow_hash, uint64_t ts) noexcept {
        PacketContext ctx{raw_data, len, flow_hash, ts};
        if (!ctx.validate()) {
            dead_letter_ring_.push(ctx);
            return make_error_code(ErrorCode::MalformedHeader);
        }

        std::error_code ec = flow_window_.evaluate(flow_hash);
        if (ec) {
            return ec;
        }

        if (!ingest_ring_.push(ctx)) {
            return make_error_code(ErrorCode::RingBufferFull);
        }

        return make_error_code(ErrorCode::Success);
    }

    bool poll_packet(PacketContext& out_ctx) noexcept {
        return ingest_ring_.pop(out_ctx);
    }

    bool poll_dead_letter(PacketContext& out_ctx) noexcept {
        return dead_letter_ring_.pop(out_ctx);
    }

    void perform_epoch_recycling() noexcept {
        arena_.reset();
        flow_window_.reset();
    }

    [[nodiscard]] ArenaAllocator& arena() noexcept { return arena_; }
    [[nodiscard]] FlowWindow& flow_window() noexcept { return flow_window_; }

private:
    ArenaAllocator arena_;
    SpscRingBuffer<PacketContext, 4096> ingest_ring_;
    SpscRingBuffer<PacketContext, 1024> dead_letter_ring_;
    FlowWindow flow_window_;
};

} // namespace titan

#endif // TITAN_ARENA_HPP