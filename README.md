# titan-packet-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/titan-packet-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/titan-packet-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/titan-packet-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/titan-packet-engine?style=flat)](https://github.com/sohamshewani/titan-packet-engine)

## 1. Executive Summary & Problem Space

Modern enterprise data centers, software-defined networking (SDN) appliances, and high-frequency edge routers require packet processing pipelines that sustain millions of packets per second (Mpps) with sub-microsecond determinism. Traditional kernel-bypass and user-space networking engines frequently stumble over hidden performance cliffs: fragmented memory allocations inducing Translation Lookaside Buffer (TLB) thrashing, lock contention on shared descriptor rings, and unpredictable tail latency ($\text{p}_{99.99}$) caused by dynamic garbage collection or cache-line bouncing.

`titan-packet-engine` is an enterprise-grade, ultra-low latency packet processing engine engineered from the bare metal up in Modern C++ (C++20). By eliminating heap allocations on the hot path, leveraging strict cache-conscious arena layouts, enforcing zero-copy single-writer ring buffers, and integrating a SIMD-accelerated filter engine (`include/titan/filter_engine.hpp`), Titan delivers deterministic wire-speed packet intake, validation, flow tracking, and routing. 

---

## 2. Structural ASCII Architecture / Dataflow Diagram

The pipeline architecture enforces a strict separation of concerns, routing raw ingress frames through an uninterrupted zero-copy pipeline directly to egress or drop targets.

```
 +-----------------------------------------------------------------------+
 |                     NIC / HARDWARE INGRESS LAYER                      |
 +-----------------------------------------------------------------------+
                                     |
                                     v (DMA / Raw Frame Ingress)
 +-----------------------------------------------------------------------+
 |                         POLL LOOP (`src/poll_loop.cpp`)               |
 |  - Polling Event Multiplexing (epoll / io_uring / busy-poll)          |
 +-----------------------------------------------------------------------+
                                     |
                                     v (Zero-Copy Buffer Assignment)
 +-----------------------------------------------------------------------+
 |                    ARENA ALLOCATOR (`include/titan/arena.hpp`)        |
 |  - Pre-allocated Monotonic Blocks                                     |
 |  - O(1) Lifetime Management & Cache-Line Alignment                    |
 +-----------------------------------------------------------------------+
                                     |
                                     v (Packet Descriptor & Payload)
 +-----------------------------------------------------------------------+
 |                RING BUFFER (`include/titan/ring_buffer.hpp`)          |
 |  - Lock-Free Single-Producer / Single-Consumer (SPSC) Queue           |
 |  - Atomic Head/Tail pointers with Cache-Line Padding                  |
 +-----------------------------------------------------------------------+
                                     |
                                     v (Batch Dispatch)
 +-----------------------------------------------------------------------+
 |               FILTER ENGINE (`include/titan/filter_engine.hpp`)       |
 |  - Byte-Code / Vectorized Predicate Matching                          |
 |  - Early-Drop & Header Validation                                     |
 +-----------------------------------------------------------------------+
                         |                           |
            (Match)      v                           v      (Drop)
        +-------------------------------+   +-----------------------+
        | FLOW WINDOW (`include/titan/flow_window.hpp`)             |
        |  - Stateful Session Tracking  |   | DISCARD / METRICS LOG |
        |  - Sliding Window Validation  |   +-----------------------+
        +-------------------------------+
                         |
                         v (Processed Packet Dispatch)
 +-----------------------------------------------------------------------+
 |                       EGRESS / ROUTING INTERFACE                      |
 +-----------------------------------------------------------------------+
```

---

## 3. Concurrency Model & Memory Invariants

Titan implements a strict **Thread-Per-Core (Shared-Nothing)** concurrency model supplemented by lock-free synchronization primitives where cross-thread communication is unavoidable.

*   **Single-Producer Single-Consumer (SPSC) Ring Buffers (`include/titan/ring_buffer.hpp`):**
    *   To prevent false sharing, producer indices (`head_`) and consumer indices (`tail_`) are strictly separated into distinct 64-byte cache lines utilizing `alignas(64)`.
    *   Memory ordering relies on `std::memory_order_acquire` and `std::memory_order_release` semantics, guaranteeing sequential consistency for payload indices without expensive full memory barriers (`std::memory_order_seq_cst`).
*   **Monotonic Arena Allocation (`include/titan/arena.hpp`):**
    *   All packet headers, states, and temporary descriptors are allocated out of thread-local memory arenas. 
    *   Deallocation is non-existent on the critical path; memory is reclaimed in bulk by resetting the arena cursor per batch epoch, ensuring deterministic $O(1)$ memory operations and zero internal fragmentation.
*   **Flow Window Management (`include/titan/flow_window.hpp`):**
    *   Flow states are indexed via open-addressing hash maps with power-of-two sizing, avoiding rehashing stalls during peak traffic surges. Mutex-free updates are enforced by binding each flow partition to an exclusive processing core.

---

## 4. Performance Characteristics & Algorithmic Complexity Table

| Component / Subsystem | Operation | Time Complexity ($\text{Big-O}$) | Space Complexity | $\text{p}_{99}$ Latency Target | Bottleneck Profile |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Arena Allocator** | Allocation (`alloc`) | $O(1)$ | $O(N)$ fixed pool | $< 5\text{ns}$ | Memory bus bandwidth |
| **Ring Buffer** | Push / Pop | $O(1)$ lock-free | $O(N)$ capacity | $< 12\text{ns}$ | Cache-line bouncing / L3 hit rate |
| **Filter Engine** | Match Evaluation | $O(K)$ ($K$ = rules) | $O(K)$ rule table | $< 40\text{ns}$ | Instruction cache / Vector width |
| **Flow Window** | State Lookup / Insert | $O(1)$ expected | $O(F)$ active flows | $< 25\text{ns}$ | Hash collision rate / TLB misses |
| **Poll Loop** | Event Polling | $O(E)$ ($E$ = events) | $O(1)$ | $< 200\text{ns}$ | System call overhead / Interrupt throttling |

---

## 5. Security Architecture & Threat Model

Enterprise edge engines operate in hostile network environments. Titan is designed defensively against malicious inputs and adversarial packet injection:

*   **Input Boundary Hardening:** Every incoming byte stream from raw sockets undergoes rigorous bounds checking within `include/titan/types.hpp`. Malformed headers, truncated Ethernet frames, and invalid IP/TCP option lengths are intercepted and dropped prior to struct casting.
*   **Memory Corruption Protections:**
    *   **Zero Heap Usage on Hot Path:** Eliminates classes of vulnerabilities associated with dynamic memory management (use-after-free, double-free, heap overflows).
    *   **Strict Type Safety:** Uses strongly-typed enums and explicit width primitives (`uint32_t`, `uintptr_t`) instead of raw pointer arithmetic.
*   **Sanitization and Hardening Flags:**
    *   The build system (`Makefile`) forces `-D_FORTIFY_SOURCE=2`, `-fstack-protector-strong`, `-Werror`, and AddressSanitizer/UndefinedBehaviorSanitizer instrumentation profiles for debug targets.
*   **Denial-of-Service (DoS) Mitigation:**
    *   The `flow_window` implements bounded memory tracking with aggressive eviction strategies (LRU/TTL) to mitigate state-exhaustion attacks (SYN flood vectors).

---

## 6. Build, Verification & Benchmarking Recipes

### Prerequisites
*   GCC 11+ or Clang 13+ supporting C++20.
*   GNU Make 4.0+.
*   POSIX-compliant OS (Linux recommended for optimal epoll/poll performance).

### 1. Build Production Binaries
Compile the engine binaries with maximum optimizations (`-O3 -march=native -flto`):
```bash
make clean
make release
```

### 2. Run Invariant Verification & Unit Tests
Execute the comprehensive test suite (`tests/test_invariants.cpp`) to validate lock-free ring buffer invariants, arena bounds, and filter matching correctness:
```bash
make test
./bin/test_invariants
```

### 3. Execute Micro-Benchmarks
Run the performance benchmarking harness (`benchmarks/bench_engine.cpp`) to measure throughput (Mpps), latency distributions ($\text{p}_{50}, \text{p}_{99}, \text{p}_{99.99}$), and instruction cycles per packet:
```bash
make bench
./bin/bench_engine --iterations=10000000 --batch-size=64