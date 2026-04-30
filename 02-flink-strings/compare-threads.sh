#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/target/thread-demo-logs"
INSPECT_DELAY_SECONDS="${INSPECT_DELAY_SECONDS:-2}"
STOP_AFTER_INSPECTION_SECONDS="${STOP_AFTER_INSPECTION_SECONDS:-1}"

ACTIVE_APP_PID=""

cleanup() {
    if [[ -n "$ACTIVE_APP_PID" ]] && kill -0 "$ACTIVE_APP_PID" 2>/dev/null; then
        kill "$ACTIVE_APP_PID" 2>/dev/null || true
    fi
}

require_command() {
    local command_name="$1"
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Missing required command: $command_name"
        echo "This script needs Java and Maven so it can compile the examples and inspect the running JVMs."
        exit 1
    fi
}

overall_thread_count() {
    local pid="$1"
    local count

    if [[ -r "/proc/$pid/status" ]]; then
        count="$(awk '/^Threads:/ { print $2 }' "/proc/$pid/status")"
        if [[ -n "$count" ]]; then
            echo "$count"
            return 0
        fi
    fi

    if command -v ps >/dev/null 2>&1; then
        count="$(ps -o nlwp= -p "$pid" 2>/dev/null | tr -d ' ')"
        if [[ -n "$count" ]]; then
            echo "$count"
            return 0
        fi

        count="$(ps -M -p "$pid" 2>/dev/null | awk 'NR > 1 { count++ } END { if (count > 0) print count }')"
        if [[ -n "$count" ]]; then
            echo "$count"
            return 0
        fi
    fi

    echo "unavailable"
}

print_thread_summary() {
    local label="$1"
    local pid="$2"
    local app_log="$3"
    local safe_label="${label// /-}"
    local dump_file="$LOG_DIR/${safe_label}-thread-dump.txt"
    local names_file="$LOG_DIR/${safe_label}-thread-names.txt"
    local process_thread_count
    local start_line

    start_line="$(wc -l < "$app_log" | tr -d ' ')"
    start_line=$((start_line + 1))
    process_thread_count="$(overall_thread_count "$pid")"

    # SIGQUIT asks HotSpot to print a thread dump without stopping the program.
    kill -QUIT "$pid"

    for _ in {1..40}; do
        tail -n +"$start_line" "$app_log" > "$dump_file"
        if grep -q '^"' "$dump_file"; then
            break
        fi
        sleep 0.25
    done

    awk '/^"/ {
        name = $0
        sub(/^"/, "", name)
        sub(/".*/, "", name)
        print name
    }' "$dump_file" > "$names_file"

    local thread_count
    thread_count="$(wc -l < "$names_file" | tr -d ' ')"

    if [[ "$thread_count" == "0" ]]; then
        echo
        echo "Could not read a thread dump for $label."
        echo "Last lines of the application log:"
        tail -n 40 "$app_log" || true
        return 1
    fi

    echo
    echo "Threads while $label is running"
    echo "JVM pid: $pid"
    echo "Overall process threads: $process_thread_count"
    echo "Threads listed in Java dump: $thread_count"
    sed 's/^/ - /' "$names_file"
    echo "Full thread dump: $dump_file"
}

build_classpath() {
    local label="$1"
    local module="$2"
    local safe_label="${label// /-}"
    local classpath_file="$LOG_DIR/${safe_label}-classpath.txt"
    local build_log="$LOG_DIR/${safe_label}-maven.log"
    local classes_dir="$ROOT_DIR/$module/target/classes"

    : > "$classpath_file"

    echo "Preparing compiled classes and dependencies..." >&2
    (
        cd "$ROOT_DIR"
        mvn -q -pl "$module" compile dependency:build-classpath -Dmdep.outputFile="$classpath_file"
    ) > "$build_log" 2>&1

    if [[ -s "$classpath_file" ]]; then
        echo "$classes_dir:$(cat "$classpath_file")"
    else
        echo "$classes_dir"
    fi
}

stop_app_after_inspection() {
    local label="$1"
    local pid="$2"

    sleep "$STOP_AFTER_INSPECTION_SECONDS"

    if kill -0 "$pid" 2>/dev/null; then
        echo
        echo "Stopping $label after the thread inspection."
        kill "$pid" 2>/dev/null || true
    fi

    wait "$pid" 2>/dev/null || true
    ACTIVE_APP_PID=""
}

run_and_inspect() {
    local label="$1"
    local module="$2"
    local main_class="$3"
    shift 3
    local app_log="$LOG_DIR/${label// /-}.log"
    local classpath

    echo
    echo "============================================================"
    echo "Starting $label"
    classpath="$(build_classpath "$label" "$module")"
    echo "Command: java -classpath <compiled classes and dependencies> $main_class"
    echo "Application output: $app_log"

    : > "$app_log"

    if (($# > 0)); then
        java "$@" -classpath "$classpath" "$main_class" > "$app_log" 2>&1 &
    else
        java -classpath "$classpath" "$main_class" > "$app_log" 2>&1 &
    fi

    ACTIVE_APP_PID="$!"

    sleep "$INSPECT_DELAY_SECONDS"

    if ! kill -0 "$ACTIVE_APP_PID" 2>/dev/null; then
        echo "$label finished or failed before the thread inspection."
        echo "Last lines of the application log:"
        tail -n 40 "$app_log" || true
        wait "$ACTIVE_APP_PID" 2>/dev/null || true
        ACTIVE_APP_PID=""
        return 1
    fi

    print_thread_summary "$label" "$ACTIVE_APP_PID" "$app_log"
    stop_app_after_inspection "$label" "$ACTIVE_APP_PID"
}

main() {
    require_command java
    require_command mvn
    mkdir -p "$LOG_DIR"

    run_and_inspect \
        "01 plain Java threads" \
        "01-plain-java-threads" \
        "edu.streaming.step01.ManualThreadPipeline"

    run_and_inspect \
        "02 Flink strings" \
        "02-flink-strings" \
        "edu.streaming.step02.FlinkStringPipeline" \
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=error"

    echo
    echo "Compare the two lists: step 01 has the explicit classroom threads, while step 02 includes Flink runtime threads."
}

trap cleanup EXIT INT TERM
main "$@"
