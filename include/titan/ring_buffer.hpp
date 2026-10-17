#ifndef TITAN_RING_BUFFER_HPP
#define TITAN_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <utility>
#include <immintrin.h>

namespace titan {

template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");

public:
    enum class OverflowPolicy {
        DropTail,
        SpinWait
    };

    explicit SpscRingBuffer(OverflowPolicy policy = OverflowPolicy::DropTail)
        : head_(0), tail_(0), drop_count_(0), policy_(policy) {
        // Zero-initialize storage block
        for (std::size_t i = 0; i < Capacity; ++i) {
            states_[i].store(0, std::memory_order_relaxed);
        }
    }

    ~SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;
    SpscRingBuffer(SpscRingBuffer&&) = delete;
    SpscRingBuffer& operator=(SpscRingBuffer&&) = delete;

    template <typename... Args>
    bool emplace(Args&&... args) {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            if (policy_ == OverflowPolicy::DropTail) {
                drop_count_.fetch_add(1, std::memory_order_relaxed);
                return false;
            } else {
                while ((tail_.load(std::memory_order_relaxed) - head_.load(std::memory_order_acquire)) >= Capacity) {
                    _mm_pause();
                }
            }
        }

        const std::size_t idx = current_tail & (Capacity - 1);
        
        // Construct item in-place
        ::new (static_cast<void*>(&buffer_[idx])) T(std::forward<Args>(args)...);
        
        // Ensure construction completes before making visible via tail increment
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(const T& item) {
        return emplace(item);
    }

    bool push(T&& item) {
        return emplace(std::move(item));
    }

    bool pop(T& item) {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false;
        }

        const std::size_t idx = current_head & (Capacity - 1);
        
        // Move or copy item out
        item = std::move(buffer_[idx].value);
        
        // Explicitly destroy instance in storage buffer
        buffer_[idx].value.~T();

        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_acquire);
        std::size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) {
            return tail - head;
        }
        return 0;
    }

    [[nodiscard]] std::size_t drop_count() const noexcept {
        return drop_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

private:
    union Storage {
        Storage() {}
        ~Storage() {}
        T value;
    };

    // Cache-line isolation to prevent false sharing between producer and consumer threads
    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    alignas(64) std::atomic<std::size_t> drop_count_;
    
    OverflowPolicy policy_;
    
    alignas(64) Storage buffer_[Capacity];
    std::atomic<uint8_t> states_[Capacity];
};

} // namespace titan

#endif // TITAN_RING_BUFFER_HPP