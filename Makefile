CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -pthread
LDFLAGS := -pthread -lrt

SRC_DIR := src
BIN := dispatcher ingester processor reporter

all: $(BIN)

dispatcher: $(SRC_DIR)/dispatcher.cpp $(SRC_DIR)/common/common.hpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC_DIR)/dispatcher.cpp $(LDFLAGS)

ingester: $(SRC_DIR)/ingester.cpp $(SRC_DIR)/common/common.hpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC_DIR)/ingester.cpp $(LDFLAGS)

processor: $(SRC_DIR)/processor.cpp $(SRC_DIR)/common/common.hpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC_DIR)/processor.cpp $(LDFLAGS)

reporter: $(SRC_DIR)/reporter.cpp $(SRC_DIR)/common/common.hpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC_DIR)/reporter.cpp $(LDFLAGS)

clean:
	rm -f $(BIN)
	rm -f .dispatcher.pid
	rm -f output/report.txt output/report.csv
	rm -f logs/*.log

.PHONY: all clean
