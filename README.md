# Parallel CSV Data Processing Pipeline (Financial Tick Data)

This project is a Linux-oriented CSV processing pipeline written in C++ that shows practical use of operating systems concepts and systems-level programming. It combines multi-process orchestration, inter-process communication, synchronization, and multi-threaded work distribution to transform raw financial tick data into structured reports.

The implementation reflects C++ proficiency through RAII-friendly resource handling, STL-based aggregation, thread-safe coordination, and clear separation of responsibilities across the pipeline stages.

## What This Project Demonstrates

- Process management through `fork()` / `exec()` coordination and child reaping.
- IPC using FIFOs, shared memory, and named semaphores.
- Concurrency with a worker thread pool and bounded buffering.
- Careful cleanup of OS resources on normal exit and interruption.
- Practical C++ design for parsing, aggregation, and reporting.

## Components

- `dispatcher`: creates FIFO/shared memory/semaphore, forks+execs children, handles signals, reaps children, and performs IPC cleanup.
- `ingester`: scans input directory for CSV files and sends chunked records through FIFO.
- `processor`: reads chunks from FIFO, runs a worker thread pool, aggregates financial tick metrics, writes result to shared memory.
- `reporter`: waits on named semaphore, reads shared memory, generates `report.txt` and `report.csv`.

## Repository Layout

- `src/`: C++ sources for the pipeline stages and shared definitions.
- `Makefile`: builds the four executables.
- `run.sh`: wrapper script that builds the project, launches the dispatcher, and summarizes the run.
- `data/sample.csv`: sample input file for local runs.
- `DECLARATION.txt`: project declaration file.
- `logs/`, `output/`, and `report/`: runtime or submission artifacts that are not meant to be committed.

## Financial Tick Data Variant

Each CSV row is expected as:

```text
symbol,price,volume
```

Aggregation per symbol:
- VWAP = sum(price * volume) / sum(volume)
- High
- Low
- Total Volume
- Total Records

## Build

```bash
make
```

Compiler flags:

```bash
g++ -std=c++17 -Wall -Wextra -pthread
```

The `run.sh` wrapper checks for `g++` and `make` before launching the pipeline.

## Run

```bash
chmod +x run.sh
./run.sh -i input -o output -n 4 -q 8
```

Options:
- `-i` input directory with `.csv` files
- `-o` output directory
- `-n` number of worker threads
- `-q` queue size for bounded buffer
- `-c` clean build/output artifacts
- `-h` help

## Output

- `output/report.txt`: human-readable report written via `dup()/dup2()` stdout redirection.
- `output/report.csv`: machine-readable summary.
- `logs/*.log`: per-process logs from child stdout/stderr redirection.

These directories are generated at runtime and are ignored by Git.

## Cleanup Behavior

On normal exit or interruption, the pipeline cleans:
- FIFO path in `/tmp`
- POSIX shared memory object
- POSIX named semaphore

No zombies are left because dispatcher reaps all children with `waitpid()`.
