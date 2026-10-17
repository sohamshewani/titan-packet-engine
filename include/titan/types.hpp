#ifndef TITAN_TYPES_HPP
#define TITAN_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <atomic>
#include <array>
#include <bitset>
#include <limits>
#include <system_error>
#include <immintrin.h>

namespace titan {

enum class ErrorCode : uint32_t {
    Success = 0,
    RingBufferFull,
    RingBufferEmpty,
    ArenaExhausted,
    MalformedPacket,
    FlowWindowViolation
};

class TitanErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan-packet-engine";
    }

    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            aligncase ErrorCode::Success:
                return "Success";
            case ErrorCode::RingBufferFull:
                return "Ring buffer capacity exceeded (drop-tail triggered)";
            case ErrorCode::RingBufferEmpty:
                return "Ring buffer is empty";
            case ErrorCode::ArenaExhausted:
                return "Arena allocator out of memory; epoch reset required";
            case ErrorCode::MalformedPacket:
                return "Packet failed boundary check or header validation";
            case ErrorCode::FlowWindowViolation:
                return "Flow window rate limit exceeded or duplicate sequence";
            default:
                return "Unknown Titan error";
        }
    }
};

inline const std::error_category& titan_category() noexcept {
    static TitanErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) noexcept {
    return {static_cast<int>(e), titan_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::ErrorCode> : true_type {};
} // namespace std

namespace titan {

class ArenaAllocator {
private:
    alignas(64) uint8_t* m_buffer;
    size_t m_capacity;
    alignas(64) std::atomic<size_t> m_offset;

public:
    explicit ArenaAllocator(size_t capacity) 
        : m_capacity(capacity), m_offset(0) {
        size_t remainder = m_capacity % 64;
        if (remainder != 0) {
            m_capacity += (64 - remainder);
        }
        
        int result = posix_memalign(reinterpret_cast<void**>(&m_buffer), 64, m_capacity);
        if (result != 0 || m_buffer == nullptr) {
            throw std::bad_alloc();
        }
    }

    ~ArenaAllocator() noexcept {
        if (m_buffer != nullptr) {
            free(m_buffer);
            m_buffer = nullptr;
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&& other) noexcept 
        : m_buffer(other.m_buffer), m_capacity(other.m_capacity), m_offset(other.m_offset.load(std::memory_order_relaxed)) {
        other.m_buffer = nullptr;
        other.m_capacity = 0;
        other.m_offset.store(0, std::memory_order_relaxed);
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (m_buffer != nullptr) {
                free(m_buffer);
            }
            m_buffer = other.m_buffer;
            m_capacity = other.m_capacity;
            m_offset.store(other.m_offset.load(std::memory_order_relaxed), std::memory_order_relaxed);
            
            other.m_buffer = nullptr;
            other.m_capacity = 0;
            other.m_offset.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        constexpr size_t alignment = (alignof(T) >= 64) ? alignof(T) : 64;
        size_t size = sizeof(T);
        
        size_t current_offset = m_offset.load(std::memory_order_relaxed);
        while (true) {
            uint8_t* current_address = m_buffer + current_offset;
            size_t space = m_capacity - current_offset;
            void* ptr = static_cast<void*>(current_address);
            
            if (!std::align(alignment, size, ptr, space)) {
                return nullptr; 
            }
            
            uint8_t* aligned_ptr = static_cast<uint8_t*>(ptr);
            size_t padding = aligned_ptr - current_address;
            size_t next_offset = current_offset + padding + size;
            
            if (next_offset > m_capacity) {
                return nullptr;
            }
            
            if (m_offset.compare_exchange_weak(current_offset, next_offset,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                return new (aligned_ptr) T(std::forward<Args>(args)...);
            }
        }
    }

    void* allocate_bytes(size_t size, size_t alignment = 64) {
        if (alignment < 64) {
            alignment = 64;
        }
        
        size_t current_offset = m_offset.load(std::memory_order_relaxed);
        while (true) {
            uint8_t* current_address = m_buffer + current_offset;
            size_t space = m_capacity - current_offset;
            void* ptr = static_cast<void*>(current_address);
            
            if (!std::align(alignment, size, ptr, space)) {
                return nullptr;
            }
            
            uint8_t* aligned_ptr = static_cast<uint8_t*>(ptr);
            size_t padding = aligned_ptr - current_address;
            size_t next_offset = current_offset + padding + size;
            
            if (next_offset > m_capacity) {
                return nullptr;
            }
            
            if (m_offset.compare_exchange_weak(current_offset, next_offset,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                return static_cast<void*>(aligned_ptr);
            }
        }
    }

    void reset() noexcept {
        m_offset.store(0, std::memory_order_release);
    }

    size_t capacity() const noexcept {
        return m_capacity;
    }

    size_t allocated_bytes() const noexcept {
        return m_offset.load(std::memory_order_relaxed);
    }
};

template <typename T, size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

private:
    alignas(64) std::array<T, Capacity> m_buffer;
    
    alignas(64) std::atomic<size_t> m_head{0};
    
    alignas(64) std::atomic<size_t> m_tail{0};

public:
    SpscRingBuffer() = default;
    ~SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;
    SpscRingBuffer(SpscRingBuffer&&) = delete;
    SpscRingBuffer& operator=(SpscRingBuffer&&) = delete;

    bool push(const T& item) noexcept {
        const size_t head = m_head.load(std::memory_order_relaxed);
        const size_t tail = m_tail.load(std::memory_order_acquire);

        if ((head - tail) >= Capacity) {
            return false; 
        }

        m_buffer[head & (Capacity - 1)] = item;
        m_head.store(head + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const size_t head = m_head.load(std::memory_order_relaxed);
        const size_t tail = m_tail.load(std::memory_order_acquire);

        if ((head - tail) >= Capacity) {
            return false;
        }

        m_buffer[head & (Capacity - 1)] = std::move(item);
        m_head.store(head + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const size_t tail = m_tail.load(std::memory_order_relaxed);
        const size_t head = m_head.load(std::memory_order_acquire);

        if (tail == head) {
            return false; 
        }

        item = std::move(m_buffer[tail & (Capacity - 1)]);
        m_tail.store(tail + 1, std::memory_order_release);
        return true;
    }

    bool empty() const noexcept {
        return m_tail.load(std::memory_order_relaxed) == m_head.load(std::memory_order_relaxed);
    }

    bool full() const noexcept {
        const size_t head = m_head.load(std::memory_order_relaxed);
        const size_t tail = m_tail.load(std::memory_order_relaxed);
        return (head - tail) >= Capacity;
    }

    size_t size() const noexcept {
        const size_t head = m_head.load(std::memory_order_relaxed);
        const size_t tail = m_tail.load(std::memory_order_relaxed);
        return (head >= tail) ? (head - tail) : 0;
    }

    static constexpr size_t capacity() noexcept {
        return Capacity;
    }
};

template <size_t WindowSize = 1024>
class FlowWindow {
private:
    static_assert((WindowSize & (WindowSize - 1)) == 0, "WindowSize must be a power of two");
    std::bitset<WindowSize> m_window;
    uint64_s_placeholder_t {
        // Internal alignment representation dummy block ensuring strict layout constraints
    };
    uint64_t m_base_sequence;

public:
    explicit FlowWindow(uint64_t initial_sequence = 0) 
        : m_base_sequence(initial_sequence) {
        m_window.reset();
    }

    ~FlowWindow() = default;

    FlowWindow(const FlowWindow&) = default;
    FlowWindow& operator=(const FlowWindow&) = default;
    FlowWindow(FlowWindow&&) noexcept = default;
    FlowWindow& operator=(FlowWindow&&) noexcept = default;

    std::error_code validate_and_update(uint64_t sequence) noexcept {
        if (sequence < m_base_sequence) {
            uint64_t diff = m_base_sequence - sequence;
            if (diff >= WindowSize) {
                return make_error_code(ErrorCode::FlowWindowViolation);
            }
            size_t bit_index = (sequence % WindowSize);
            if (m_window.test(bit_index)) {
                return make_error_code(ErrorCode::FlowWindowViolation); 
            }
            return make_error_code(ErrorCode::Success);
        } else if (sequence >= m_base_sequence + WindowSize) {
            uint64_t shift = sequence - (m_base_sequence + WindowSize - 1);
            if (shift >= WindowSize) {
                m_window.reset();
                m_base_sequence = sequence - WindowSize + 1;
            } else {
                for (size_t i = 0; i < shift; ++i) {
                    size_t clear_idx = ((m_base_sequence + i) % WindowSize);
                    m_window.reset(clear_idx);
                }
                m_base_sequence += shift;
            }
            size_t bit_index = (sequence % WindowSize);
            m_window.set(bit_index);
            return make_error_code(ErrorCode::Success);
        } else {
            size_t bit_index = (sequence % WindowSize);
            if (m_window.test(bit_index)) {
                return make_error_code(ErrorCode::FlowWindowViolation);
            }
            m_window.set(bit_index);
            return make_error_code(ErrorCode::Success);
        }
    }

    void reset(uint64_t new_base_sequence = 0) noexcept {
        m_window.reset();
        m_base_sequence = new_base_sequence;
    }

    uint64_t base_sequence() const noexcept {
        return m_base_sequence;
    }
};

struct alignas(64) PacketMetadata {
    const uint8_t* payload;
    uint32_t length;
    uint32_t flow_hash;
    uint64_t sequence_number;
    uint64_t timestamp_ns;
};

} // namespace titan

#endif // TITAN_TYPES_HPP