CXX ?= g++
CXXFLAGS = -O3 -Wall -Wextra -std=c++17 -MMD -MP
LDFLAGS = -lpthread

TARGET = titan-packet-engine
SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
DEPS = $(OBJS:.o=.d)

all: build

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

build: $(BUILD_DIR) $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(INC_DIR) -c $< -o $@

-include $(DEPS)

test: build
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$(ls -A $(TEST_DIR) 2>/dev/null)" ]; then \
		echo "Running tests..."; \
		$(CXX) $(CXXFLAGS) -I$(INC_DIR) $(TEST_DIR)/*.cpp -o $(BUILD_DIR)/runner $(LDFLAGS) && $(BUILD_DIR)/runner; \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: build
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$(ls -A $(BENCH_DIR) 2>/dev/null)" ]; then \
		echo "Running benchmarks..."; \
		$(CXX) $(CXXFLAGS) -O3 -I$(INC_DIR) $(BENCH_DIR)/*.cpp -o $(BUILD_DIR)/bench $(LDFLAGS) && $(BUILD_DIR)/bench; \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)

.PHONY: all build test benchmark clean