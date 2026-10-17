#ifndef TITAN_FLOW_WINDOW_HPP
#define TITAN_FLOW_WINDOW_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <array>
#include <new>
#include <immintrin.h>

namespace titan {

template <size_t WindowSize = 1024>
class FlowWindow {
public:
    static_assert((WindowSize & (WindowSize - 1)) == 0, "WindowSize must be a power of two");
    static_assert(WindowSize >= 64, "WindowSize must be at least 64 to fit within bitset word boundaries");

    constexpr FlowWindow() noexcept 
        : base_sequence_(0),
          bitset_{},
          packet_count_(0),
          duplicate_count_(0) {}

    ~FlowWindow() noexcept = default;

    FlowWindow(const FlowWindow&) = delete;
    FlowWindow& operator=(const FlowWindow&) = delete;
    FlowWindow(FlowWindow&&) noexcept = delete;
    FlowWindow& operator=(FlowWindow&&) noexcept = delete;

    enum class Result : uint8_t {
        Accepted = 0,
        Duplicate = 1,
        Stale = 2,
        Advanced = 3
    };

    [[nodiscard]] Result evaluate(uint64_t sequence_number) noexcept {
        uint64_t current_base = base_sequence_.load(std::memory_order_relaxed);

        if (sequence_number < current_base) {
            if (current_base - sequence_number > WindowSize) {
                return Result::Stale;
            }
            // Within trailing window boundary but behind base
            return Result::Duplicate;
        }

        uint64_t diff = sequence_number - current_base;

        if (diff >= WindowSize) {
            // Sequence is far ahead; slide window forward
            uint64_t shift = diff - WindowSize + 1;
            slide_window(current_base, current_base + shift);
            current_base = base_sequence_.load(std::memory_order_relaxed);
            diff = sequence_number - current_base;
        }

        size_t word_idx = (diff / 64);
        size_t bit_idx = (diff % 64);

        if (word_idx >= c_num_words) {
            return Result::Stale;
        }

        uint64_t mask = (1ULL << bit_idx);
        uint64_t current_word = bitset_[word_idx].load(std::memory_order_relaxed);

        if ((current_word & mask) != 0) {
            packet_count_.fetch_add(1, std::memory_order_relaxed);
            duplicate_count_.fetch_add(1, std::memory_order_relaxed);
            return Result::Duplicate;
        }

        // Try to set the bit atomically or via relaxed sequential consistency for single-writer per flow
        bitset_[word_idx].store(current_word | mask, std::memory_order_relaxed);
        packet_count_.fetch_add(1, std::memory_order_relaxed);
        return Result::Accepted;
    }

    void reset(uint64_t new_base) noexcept {
        base_sequence_.store(new_base, std::memory_order_relaxed);
        for (size_t i = 0; i < c_num_words; ++i) {
            bitset_[i].store(0, std::memory_order_relaxed);
        }
        packet_count_.store(0, std::memory_order_relaxed);
        duplicate_count_.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t base_sequence() const noexcept {
        return base_sequence_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t packet_count() const noexcept {
        return packet_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t duplicate_count() const noexcept {
        return duplicate_count_.load(std::memory_order_relaxed);
    }

private:
    static constexpr size_t c_num_words = WindowSize / 64;

    alignas(64) std::atomic<uint64_t> base_sequence_;
    alignas(64) std::array<std::atomic<uint64_t>, c_num_words> bitset_;
    alignas(64) std::atomic<uint64_t> packet_count_;
    alignas(64) std::atomic<uint64_t> duplicate_count_;

    void slide_window(uint64_t old_base, uint64_t new_base) noexcept {
        uint64_t shift_distance = new_base - old_base;
        if (shift_distance >= WindowSize) {
            reset(new_base);
            return;
        }

        base_sequence_.store(new_base, std::memory_order_relaxed);

        size_t word_shift = shift_distance / 64;
        size_t bit_shift = shift_distance % 64;

        if (word_shift >= c_num_words) {
            reset(new_base);
            return;
        }

        for (size_t i = 0; i < c_num_words; ++i) {
            size_t src_idx = i + word_shift;
            uint64_t val = 0;

            if (src_idx < c_num_words) {
                val = bitset_[src_idx].load(std::memory_order_relaxed);
            }

            if (bit_shift > 0 && src_idx + 1 < c_num_words) {
                uint64_t next_val = bitset_[src_idx + 1].load(std::memory_order_relaxed);
                val = (val >> bit_shift) | (next_val << (64 - bit_shift));
            } else if (bit_shift > 0) {
                val = (val >> bit_shift);
            }

            bitset_[i].store(val, std::memory_order_relaxed);
        }

        // Clear out the newly exposed bits at the high end of the window
        size_t cleared_words_start = (WindowSize - shift_distance) / 64;
        for (size_t i = cleared_words_start; i < c_num_words; ++i) {
            // Mask out bits that fall outside the active window range
            uint64_t current = bitset_[i].load(std::memory_order_relaxed);
            size_t valid_bits_remainder = (WindowSize - shift_distance) % 64;
            if (i == cleared_words_start && valid_bits_remainder > 0) {
                uint64_t mask = (1ULL << valid_bits_remainder) - 1;
                bitset_[i].store(current & mask, std::memory_order_relaxed);
            } else if (i > cleared_words_start) {
                bitset_[i].store(0, std::memory_order_relaxed);
            }
        }
    }
};

} // namespace titan

#endif // TITAN_FLOW_WINDOW_HPP