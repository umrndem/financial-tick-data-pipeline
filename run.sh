#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR=""
OUTPUT_DIR=""
THREADS=4
QUEUE=8
CLEAN=0
PID_FILE=".dispatcher.pid"

print_help() {
    cat <<EOF
Usage: $0 -i <input_dir> -o <output_dir> -n <threads> [-q <queue>] [-c] [-h]
  -i   Input directory containing .csv files
  -o   Output directory for report files
  -n   Number of worker threads
  -q   Queue size for bounded buffer (default: 8)
  -c   Clean and exit
  -h   Show help
EOF
}

verify_tools() {
    local tools
    tools=(g++ make)
    local t
    for t in "${tools[@]}"; do
        if ! command -v "$t" >/dev/null 2>&1; then
            echo "Error: required tool '$t' is not installed." >&2
            exit 1
        fi
    done
}

# shellcheck disable=SC2317
cleanup() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid="$(cat "$PID_FILE")"
        if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
        fi
        rm -f "$PID_FILE"
    fi
}

validate_input_dir() {
    if [[ ! -d "$INPUT_DIR" ]]; then
        echo "Error: input directory '$INPUT_DIR' does not exist." >&2
        exit 1
    fi

    local found_csv=0
    local f
    for f in "$INPUT_DIR"/*.csv; do
        case "$f" in
            *.csv)
                if [[ -f "$f" ]]; then
                    found_csv=1
                    break
                fi
                ;;
        esac
    done

    if [[ "$found_csv" -ne 1 ]]; then
        echo "Error: no .csv files found in '$INPUT_DIR'." >&2
        exit 1
    fi
}

summarize_run() {
    local start_ts="$1"
    local end_ts="$2"
    local status="$3"
    local report_csv="$OUTPUT_DIR/report.csv"
    local records=0

    if [[ -f "$report_csv" ]]; then
        records="$(awk -F',' 'NR>1 {sum += $6} END {print sum + 0}' "$report_csv")"
    fi

    local runtime
    runtime=$((end_ts - start_ts))

    echo "Run Summary"
    echo "  runtime_seconds: $runtime"
    echo "  records_processed: $records"
    echo "  dispatcher_exit_status: $status"
}

while getopts ":i:o:n:q:ch" opt; do
    case "$opt" in
        i) INPUT_DIR="$OPTARG" ;;
        o) OUTPUT_DIR="$OPTARG" ;;
        n) THREADS="$OPTARG" ;;
        q) QUEUE="$OPTARG" ;;
        c) CLEAN=1 ;;
        h)
            print_help
            exit 0
            ;;
        :)
            echo "Error: option -$OPTARG requires an argument." >&2
            print_help
            exit 1
            ;;
        \?)
            echo "Error: invalid option -$OPTARG" >&2
            print_help
            exit 1
            ;;
    esac
done

if [[ "$CLEAN" -eq 1 ]]; then
    make clean
    exit 0
fi

if [[ -z "$INPUT_DIR" || -z "$OUTPUT_DIR" || -z "$THREADS" ]]; then
    print_help
    exit 1
fi

mkdir -p "$OUTPUT_DIR" logs

verify_tools
validate_input_dir

if ! make; then
    echo "Error: make failed, build did not complete." >&2
    exit 1
fi

trap cleanup EXIT INT TERM

FIFO_PATH="/tmp/osproj_fifo_$$"
SHM_NAME="/osproj_shm_$$"
SEM_NAME="/osproj_sem_$$"

start_ts="$(date +%s)"

./dispatcher -i "$INPUT_DIR" -o "$OUTPUT_DIR" -n "$THREADS" -q "$QUEUE" \
    --fifo "$FIFO_PATH" --shm "$SHM_NAME" --sem "$SEM_NAME" &

dispatcher_pid=$!
echo "$dispatcher_pid" > "$PID_FILE"

set +e
wait "$dispatcher_pid"
status=$?
set -e

end_ts="$(date +%s)"
summarize_run "$start_ts" "$end_ts" "$status"

exit "$status"
