#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <cstring>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <algorithm>
#include <random>

// ============================================================================
// SIMULATED 'titan-packet-engine' INTERNAL IMPLEMENTATION
// ============================================================================

namespace Titan {

constexpr size_t MAX_PACKET_SIZE = 65535;
constexpr size_t MIN_PACKET_SIZE = 64;
constexpr size_t RING_BUFFER_SIZE = 4096;

enum class EngineState {
    UNINITIALIZED,
    READY,
    RUNNING,
    ERROR_STATE,
    RECOVERING,
    SHUTDOWN
};

enum class PacketStatus {
    SUCCESS,
    BUFFER_OVERFLOW,
    BUFFER_UNDERFLOW,
    INVALID_CHECKSUM,
    ENGINE_BUSY,
    CORRUPTED_STATE
};

struct Packet {
    uint32_t id;
    uint16_t length;
    uint32_t checksum;
    uint8_t data[MAX_PACKET_SIZE];
};

class PacketEngine {
private:
    std::atomic<EngineState> state_;
    std::atomic<uint64_t> packets_processed_;
    std::atomic<uint64_t> packets_dropped_;
    std::atomic<uint64_t> error_count_;
    
    // Simulated ring buffer
    std::unique_ptr<Packet[]> ring_buffer_;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
    
    mutable std::mutex engine_mutex_;
    bool fault_injected_;

    uint32_t calculate_checksum(const uint8_t* data, uint16_t len) const {
        uint32_t sum = 0;
        for (uint16_t i = 0; i < len; ++i) {
            sum += data[i];
        }
        return sum;
    }

public:
    PacketEngine() 
        : state_(EngineState::UNINITIALIZED),
          packets_processed_(0),
          packets_dropped_(0),
          error_count_(0),
          ring_buffer_(std::make_unique<Packet[]>(RING_BUFFER_SIZE)),
          head_(0),
          tail_(0),
          fault_injected_(false) {}

    ~PacketEngine() {
        shutdown();
    }

    bool initialize() {
        std::lock_guard<std::mutex> lock(engine_mutex_);
        Engine expected = EngineState::UNINITIALIZED;
        if (state_.compare_exchange_strong(expected, EngineState::READY)) {
            head_ = 0;
            tail_ = 0;
            packets_processed_ = 0;
            packets_dropped_ = 0;
            error_count_ = 0;
            fault_injected_ = false;
            return true;
        }
        return false;
    }

    void start() {
        EngineState expected = EngineState::READY;
        if (!state_.compare_exchange_strong(expected, EngineState::RUNNING)) {
            // Can also start from recovering
            EngineState expected_rec = EngineState::RECOVERING;
            state_.compare_exchange_strong(expected_rec, EngineState::RUNNING);
        }
    }

    void shutdown() {
        state_.store(EngineState::SHUTDOWN);
    }

    EngineState get_state() const {
        return state_.load();
    }

    PacketStatus inject_packet(const uint8_t* data, uint16_t length, uint32_t id) {
        EngineState current_state = state_.load();
        if (current_state != EngineState::RUNNING) {
            return PacketStatus::ENGINE_BUSY;
        }

        if (length < MIN_PACKET_SIZE) {
            error_count_++;
            return PacketStatus::BUFFER_UNDERFLOW;
        }
        if (length > MAX_PACKET_SIZE) {
            error_count_++;
            return PacketStatus::BUFFER_OVERFLOW;
        }

        if (fault_injected_) {
            error_count_++;
            return PacketStatus::CORRUPTED_STATE;
        }

        size_t current_head = head_.load();
        size_t next_head = (current_head + 1) % RING_BUFFER_SIZE;
        
        if (next_head == tail_.load()) {
            packets_dropped_++;
            return PacketStatus::ENGINE_BUSY; // Ring buffer full
        }

        // Copy and enqueue
        ring_buffer_[current_head].id = id;
        ring_buffer_[current_head].length = length;
        std::memcpy(ring_buffer_[current_head].data, data, length);
        ring_buffer_[current_head].checksum = calculate_checksum(data, length);

        head_.store(next_head);
        packets_processed_++;
        return PacketStatus::SUCCESS;
    }

    bool poll_packet(Packet& pkt) {
        EngineState current_state = state_.load();
        if (current_state != EngineState::RUNNING && current_state != EngineState::RECOVERING) {
            return false;
        }

        size_t current_tail = tail_.load();
        if (current_tail == head_.load()) {
            return false; // Empty
        }

        pkt = ring_buffer_[current_tail];
        
        // Verify invariant: checksum must match data
        uint32_t expected_checksum = calculate_checksum(pkt.data, pkt.length);
        if (pkt.checksum != expected_checksum) {
            error_count_++;
            state_.store(EngineState::ERROR_STATE);
            return false;
        }

        tail_.store((current_tail + 1) % RING_BUFFER_SIZE);
        return true;
    }

    void simulate_fault() {
        std::lock_guard<std::mutex> lock(engine_mutex_);
        fault_injected_ = true;
        state_.store(EngineState::ERROR_STATE);
    }

    bool recover() {
        std::lock_guard<std::mutex> lock(engine_mutex_);
        if (state_.load() != EngineState::ERROR_STATE) {
            return false;
        }
        
        // Clear buffers and reset flags
        head_ = 0;
        tail_ = 0;
        fault_injected_ = false;
        state_.store(EngineState::RECOVERING);
        return true;
    }

    uint64_t get_processed_count() const { return packets_processed_.load(); }
    uint64_t get_dropped_count() const { return packets_dropped_.load(); }
    uint64_t get_error_count() const { return error_count_.load(); }
};

} // namespace Titan

// ============================================================================
// EXHAUSTIVE TEST SUITE
// ============================================================================

class TestSuite {
private:
    int tests_run_ = 0;
    int tests_failed_ = 0;

    void assert_true(bool condition, const std::string& msg) {
        tests_run_++;
        if (!condition) {
            tests_failed_++;
            std::cerr << "  [FAIL] " << msg << std::endl;
            assert(false);
        } else {
            std::cout << "  [PASS] " << msg << std::endl;
        }
    }

public:
    void run_all_tests() {
        std::cout << "============================================================" << std::endl;
        std::cout << "Starting Titan Packet Engine Exhaustive Test Suite" << std::endl;
        std::cout << "============================================================" << std::endl;

        run_unit_tests();
        run_boundary_tests();
        run_invariant_tests();
        run_concurrent_stress_tests();
        run_failure_recovery_tests();

        std::cout << "============================================================" << std::endl;
        std::cout << "Test Execution Complete." << std::endl;
        std::cout << "Total Tests: " << tests_run_ << std::endl;
        std::cout << "Failed:      " << tests_failed_ << std::endl;
        std::cout << "============================================================" << std::endl;
    }

private:
    // 1. UNIT TESTS
    void run_unit_tests() {
        std::cout << "\n--- Running Unit Tests ---" << std::endl;
        Titan::PacketEngine engine;

        assert_true(engine.get_state() == Titan::EngineState::UNINITIALIZED, "Initial state must be UNINITIALIZED");
        assert_true(engine.initialize() == true, "Engine initialization should succeed");
        assert_true(engine.get_state() == Titan::EngineState::READY, "State after init must be READY");

        engine.start();
        assert_true(engine.get_state() == Titan::EngineState::RUNNING, "State after start must be RUNNING");

        uint8_t buffer[Titan::MIN_PACKET_SIZE];
        std::memset(buffer, 0xAB, sizeof(buffer));

        Titan::PacketStatus status = engine.inject_packet(buffer, sizeof(buffer), 1);
        assert_true(status == Titan::PacketStatus::SUCCESS, "Valid packet injection must succeed");

        Titan::Packet out_pkt;
        bool polled = engine.poll_packet(out_pkt);
        assert_true(polled == true, "Polling injected packet must succeed");
        assert_true(out_pkt.id == 1, "Polled packet ID must match injected ID");
        assert_true(out_pkt.length == sizeof(buffer), "Polled packet length must match");
        assert_true(std::memcmp(out_pkt.data, buffer, sizeof(buffer)) == 0, "Packet payload must match");

        engine.shutdown();
        assert_true(engine.get_state() == Titan::EngineState::SHUTDOWN, "Engine state must be SHUTDOWN");
    }

    // 2. BOUNDARY & EDGE-CASE TESTS
    void run_boundary_tests() {
        std::cout << "\n--- Running Boundary & Edge-Case Tests ---" << std::endl;
        Titan::PacketEngine engine;
        engine.initialize();
        engine.start();

        // Edge case A: Packet size below minimum (MIN_PACKET_SIZE - 1)
        {
            uint8_t small_buf[Titan::MIN_PACKET_SIZE - 1];
            std::memset(small_buf, 0, sizeof(small_buf));
            auto status = engine.inject_packet(small_buf, sizeof(small_buf), 99);
            assert_true(status == Titan::PacketStatus::BUFFER_UNDERFLOW, "Packets below MIN_PACKET_SIZE must trigger BUFFER_UNDERFLOW");
        }

        // Edge case B: Exact minimum packet size
        {
            uint8_t min_buf[Titan::MIN_PACKET_SIZE];
            std::memset(min_buf, 0x55, sizeof(min_buf));
            auto status = engine.inject_packet(min_buf, sizeof(min_buf), 100);
            assert_true(status == Titan::PacketStatus::SUCCESS, "Packets at exact MIN_PACKET_SIZE must succeed");
            Titan::Packet p;
            engine.poll_packet(p);
        }

        // Edge case C: Exact maximum packet size
        {
            auto max_buf = std::make_unique<uint8_t[]>(Titan::MAX_PACKET_SIZE);
            std::memset(max_buf.get(), 0xAA, Titan::MAX_PACKET_SIZE);
            auto status = engine.inject_packet(max_buf.get(), Titan::MAX_PACKET_SIZE, 101);
            assert_true(status == Titan::PacketStatus::SUCCESS, "Packets at exact MAX_PACKET_SIZE must succeed");
            Titan::Packet p;
            engine.poll_packet(p);
        }

        // Edge case D: Packet size above maximum (MAX_PACKET_SIZE + 1)
        // Note: we can't easily allocate MAX+1 on stack, use vector
        {
            std::vector<uint8_t> large_buf(Titan::MAX_PACKET_SIZE + 1, 0xFF);
            auto status = engine.inject_packet(large_buf.