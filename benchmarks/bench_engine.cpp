#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cstdint>

// Simulated Titan Packet Engine Namespace and Structures
namespace titan {
    namespace packet_engine {
        
        struct Packet {
            uint64_t id;
            const uint8_t* payload;
            size_t length;
            uint32_t flags;
        };

        enum class Status {
            SUCCESS,
            QUEUE_FULL,
            INVALID_PACKET
        };

        class Engine {
        public:
            Engine() {
                // Initialization overhead simulation
            }

            // High-performance inline processing simulation
            inline Status process_packet(const Packet& pkt) {
                // Simulate deterministic hardware/software packet pipeline latency (e.g., ~150-300ns)
                // Using a volatile sink to prevent compiler optimization
                volatile uint64_t checksum = 0;
                for (size_t i = 0; i < pkt.length; ++i) {
                    checksum += pkt.payload[i];
                }
                
                // Simulated pass-through condition based on payload
                if (checksum == 0xFFFFFFFFFFFFFFFFULL) {
                    return Status::INVALID_PACKET;
                }
                return Status::SUCCESS;
            }
        };
    }
}

int main() {
    using namespace std::chrono;

    // Benchmark Configuration
    const size_t WARMUP_ITERATIONS = 100000;
    const size_t BENCHMARK_ITERATIONS = 5000000;
    
    titan::packet_engine::Engine engine;
    
    // Prepare dummy packet
    uint8_t dummy_payload[64] = {0};
    titan::packet_engine::Packet pkt{
        .id = 1,
        .payload = dummy_payload,
        .length = sizeof(dummy_payload),
        .flags = 0
    };

    // 1. Warmup Phase (Cache priming, branch predictor training)
    for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        engine.process_packet(pkt);
    }

    // 2. Latency Measurement Phase (Individual sample collection)
    // To minimize overhead, we collect samples for percentiles in a pre-allocated vector.
    std::vector<double> latencies_ns;
    latencies_ns.reserve(BENCHMARK_ITERATIONS);

    // 3. Throughput Measurement Phase
    auto start_time = high_resolution_clock::now();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; ++i) {
        auto sample_start = high_resolution_clock::now();
        
        engine.process_packet(pkt);
        
        auto sample_end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(sample_end - sample_start).count();
        latencies_ns.push_back(static_cast<double>(duration));
    }

    auto end_time = high_resolution_clock::now();
    auto total_duration = duration_cast<nanoseconds>(end_time - start_time).count();

    // Calculate Metrics
    double total_seconds = static_cast<double>(total_duration) / 1e9;
    double ops_per_sec = static_cast<double>(BENCHMARK_ITERATIONS) / total_seconds;

    // Sort latencies for percentile extraction
    std::sort(latencies_ns.begin(), latencies_ns.end());

    double p50 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.50)];
    double p90 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.90)];
    double p99 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.99)];
    double p999 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.999)];

    // Output Formatted Tabular Metrics to stdout
    std::cout << "\n";
    std::cout << "=========================================================\nльника";
    std::cout << "          TITAN PACKET ENGINE - MICROBENCHMARK RESULTS     \n";
    std::cout << "=========================================================\n";
    std::cout << "| Metric                  | Value         | Unit          |\n";
    std::cout << "|-------------------------|---------------|---------------|\n";
    std::cout << "| Total Iterations        | " << std::setw(13) << BENCHMARK_ITERATIONS << " | packets       |\n";
    std::cout << "| Total Duration          | " << std::setw(13) << std::fixed << std::setprecision(4) << total_seconds << " | seconds       |\n";
    std::cout << "| Throughput              | " << std::setw(13) << static_cast<uint64_t>(ops_per_sec) << " | ops/sec       |\n";
    std::cout << "| Latency P50             | " << std::setw(13) << std::fixed << std::setprecision(2) << p50 << " | ns            |\n";
    std::cout << "| Latency P90             | " << std::setw(13) << std::fixed << std::setprecision(2) << p90 << " | ns            |\n";
    std::cout << "| Latency P99             | " << std::setw(13) << std::fixed << std::setprecision(2) << p99 << " | ns            |\n";
    std::cout << "| Latency P99.9           | " << std::setw(13) << std::fixed << std::setprecision(2) << p999 << " | ns            |\n";
    std::cout << "=========================================================\n";
    std::cout << "\n";

    return 0;
}