#!/usr/bin/env bash
#
# Copyright 2026 fbtBerger Technology
# SPDX-License-Identifier: Apache-2.0
#
# A three-node Raft cluster on one machine, for demonstrating the election
# defects that ElectionSwitches can arm (see README, "Election switches").
# No Docker, no database: the state machine is the built-in key-value store,
# and neither defect has anything to do with what is being replicated.
#
#   scripts/demo-cluster.sh start                  # all fixes on -- the control run
#   scripts/demo-cluster.sh start --defect prevote # issue #3: two elections, one round
#   scripts/demo-cluster.sh start --defect restart # issue #2: arm, then restart a node
#   scripts/demo-cluster.sh restart-node 2         # the move that shows issue #2
#   scripts/demo-cluster.sh elections              # every election line, all nodes, in order
#   scripts/demo-cluster.sh status | logs [n] | stop
#
# Each node keeps an interactive CLI on stdin (SET/GET/STATUS/...). The script
# holds that open through a per-node FIFO, so `send 1 STATUS` reaches node 1
# without a terminal attached to it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# NOT under build/: `./gradlew clean` deletes that tree, and it takes the pid files
# with it. The running JVMs survive, `stop` can no longer find them, and the next
# `start` dies on "Address already in use" -- with the orphans still holding the
# ports and still writing into logs nobody is reading. Cost one confused debugging
# round on the first day this script existed.
RUN_DIR="$REPO_ROOT/.demo-cluster"
LOG_DIR="$RUN_DIR/logs"
JAR="$REPO_ROOT/build/libs/fbtberger-raft-1.0.0-all.jar"
NODES=(1 2 3)

# The switch defaults live in ElectionSwitches; repeating them here would be a
# second source of truth. An empty SWITCHES means "whatever the library ships".
SWITCHES=()

usage() {
    sed -n '7,25p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
    exit "${1:-0}"
}

require_jar() {
    if [[ ! -f "$JAR" ]]; then
        echo "building $(basename "$JAR") ..."
        (cd "$REPO_ROOT" && ./gradlew --quiet shadowJar)
    fi
}

pid_of() {
    local n="$1" pidfile="$RUN_DIR/node$n.pid"
    [[ -f "$pidfile" ]] || return 1
    local pid
    pid="$(cat "$pidfile")"
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

start_node() {
    local n="$1" fifo="$RUN_DIR/node$n.stdin"
    if pid_of "$n" >/dev/null; then
        echo "node$n already running (pid $(pid_of "$n"))"
        return 0
    fi
    rm -f "$fifo"
    mkfifo "$fifo"
    # 0<> opens the FIFO read-write, so the descriptor is its own writer and the
    # CLI never sees EOF. Redirecting from /dev/null instead would end
    # RaftServer's readLine loop immediately and shut the node down again.
    (cd "$REPO_ROOT" && exec java "${SWITCHES[@]}" -jar "$JAR" \
        "config/local/node$n.properties" \
        > "$LOG_DIR/node$n.log" 2>&1 0<> "$fifo") &
    echo $! > "$RUN_DIR/node$n.pid"
    echo "node$n started (pid $(cat "$RUN_DIR/node$n.pid")), log $LOG_DIR/node$n.log"
}

stop_node() {
    local n="$1" pid
    if ! pid="$(pid_of "$n")"; then
        # No pid file, but the node may still be up -- someone deleted the run
        # directory, or the shell that started it is gone. Match on the config path,
        # which is unique per node and cannot hit anything else on this machine.
        local orphan
        orphan="$(pgrep -f "[-]jar .*config/local/node$n\.properties" || true)"
        if [[ -n "$orphan" ]]; then
            echo "node$n running without a pid file (pid $orphan), killing it"
            kill $orphan 2>/dev/null || true
        else
            echo "node$n not running"
        fi
        rm -f "$RUN_DIR/node$n.pid"
        return 0
    fi
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$RUN_DIR/node$n.pid" "$RUN_DIR/node$n.stdin"
    echo "node$n stopped"
}

parse_switches() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --defect)
                case "${2:-}" in
                    none)    ;;
                    prevote) SWITCHES+=(-Draft.prevote.quorum-latch=false) ;;
                    # Issue #2 needs both halves: the boot delay decides whether the
                    # restarting node campaigns, stickiness whether it wins. Arming
                    # only one produces a node that campaigns and is refused.
                    restart) SWITCHES+=(-Draft.election.boot-delay-factor=1
                                        -Draft.prevote.leader-stickiness=false) ;;
                    *) echo "unknown defect '${2:-}' (none|prevote|restart)" >&2; exit 2 ;;
                esac
                shift 2 ;;
            -D*) SWITCHES+=("$1"); shift ;;
            *) echo "unknown option '$1'" >&2; usage 2 ;;
        esac
    done
}

cmd_start() {
    parse_switches "$@"
    require_jar
    mkdir -p "$LOG_DIR" "$RUN_DIR/data"
    if [[ ${#SWITCHES[@]} -gt 0 ]]; then
        echo "election switches: ${SWITCHES[*]}"
    else
        echo "election switches: library defaults (every fix on)"
    fi
    # Remembered for restart-node: a node that comes back with the library defaults
    # while the rest of the cluster runs an armed defect is a different experiment
    # than the one being demonstrated, and nothing in the log would say so.
    if [[ ${#SWITCHES[@]} -gt 0 ]]; then
        printf '%s\n' "${SWITCHES[@]}" > "$RUN_DIR/switches"
    else
        : > "$RUN_DIR/switches"   # printf of an empty array would leave a blank line behind
    fi
    for n in "${NODES[@]}"; do start_node "$n"; done
    echo
    echo "watch: scripts/demo-cluster.sh elections"
}

cmd_stop() {
    for n in "${NODES[@]}"; do stop_node "$n"; done
}

cmd_status() {
    for n in "${NODES[@]}"; do
        if pid="$(pid_of "$n")"; then
            printf 'node%s  pid %-8s %s\n' "$n" "$pid" \
                "$(grep -oE 'elected LEADER for term [0-9]+|stepping down[^)]*' \
                    "$LOG_DIR/node$n.log" 2>/dev/null | tail -1)"
        else
            printf 'node%s  stopped\n' "$n"
        fi
    done
}

# The whole point of the exercise: every election-path line from all three nodes,
# merged in timestamp order. Two "elected LEADER" lines for one node with no
# step-down between them is issue #3.
cmd_elections() {
    grep -hE 'PreVote succeeded|elected LEADER|stepping down|starting as FOLLOWER|boot grace|election switches' \
        "$LOG_DIR"/node*.log 2>/dev/null | sort -k1,2 || echo "no logs yet"
}

cmd_logs() {
    local n="${1:-}"
    if [[ -n "$n" ]]; then tail -f "$LOG_DIR/node$n.log"; else tail -f "$LOG_DIR"/node*.log; fi
}

cmd_send() {
    local n="$1"; shift
    pid_of "$n" >/dev/null || { echo "node$n not running" >&2; exit 1; }
    echo "$*" > "$RUN_DIR/node$n.stdin"
}

# Issue #2 is a restart, not a start: the cluster has to be healthy and the
# incumbent heartbeating before the node comes back, or there is nothing to unseat.
cmd_restart_node() {
    local n="$1"
    if [[ ${#SWITCHES[@]} -eq 0 && -s "$RUN_DIR/switches" ]]; then
        mapfile -t SWITCHES < "$RUN_DIR/switches"
    fi
    [[ ${#SWITCHES[@]} -gt 0 ]] && echo "election switches: ${SWITCHES[*]}"
    stop_node "$n"
    sleep 2
    start_node "$n"
}

case "${1:-}" in
    start)        shift; cmd_start "$@" ;;
    stop)         cmd_stop ;;
    status)       cmd_status ;;
    elections)    cmd_elections ;;
    logs)         shift; cmd_logs "$@" ;;
    send)         shift; cmd_send "$@" ;;
    restart-node) shift; parse_switches "${@:2}"; cmd_restart_node "$1" ;;
    -h|--help|"") usage ;;
    *)            echo "unknown command '$1'" >&2; usage 2 ;;
esac
