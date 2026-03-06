#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs/dev"
BIN_DIR="$RUN_DIR/bin"

SERVICES=(signer api broadcaster depositor)

mkdir -p "$RUN_DIR" "$LOG_DIR" "$BIN_DIR"

load_env() {
  if [[ -f "$ROOT_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
    set +a
  fi
}

pid_file() {
  local svc="$1"
  echo "$RUN_DIR/${svc}.pid"
}

log_file() {
  local svc="$1"
  echo "$LOG_DIR/${svc}.log"
}

is_running() {
  local svc="$1"
  local pf
  pf="$(pid_file "$svc")"
  [[ -f "$pf" ]] || return 1
  local pid
  pid="$(cat "$pf")"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

command_for_service() {
  local svc="$1"
  case "$svc" in
    signer) echo "$BIN_DIR/signer" ;;
    api) echo "$BIN_DIR/api" ;;
    broadcaster) echo "$BIN_DIR/broadcaster" ;;
    depositor) echo "$BIN_DIR/depositor" ;;
    *) echo "" ;;
  esac
}

pattern_for_service() {
  local svc="$1"
  case "$svc" in
    signer) echo "$BIN_DIR/signer|wallet-system/cmd/signer|go-build" ;;
    api) echo "$BIN_DIR/api|wallet-system/cmd/api|go-build" ;;
    broadcaster) echo "$BIN_DIR/broadcaster|wallet-system/cmd/broadcaster|go-build" ;;
    depositor) echo "$BIN_DIR/depositor|wallet-system/cmd/depositor|go-build" ;;
    *) echo "" ;;
  esac
}

build_one() {
  local svc="$1"
  local out
  out="$(command_for_service "$svc")"
  if [[ -z "$out" ]]; then
    echo "[error] unknown service: $svc" >&2
    exit 1
  fi
  echo "[build] $svc"
  (
    cd "$ROOT_DIR"
    go build -o "$out" "./cmd/$svc"
  )
}

start_one() {
  local svc="$1"
  if is_running "$svc"; then
    echo "[skip] $svc already running (pid $(cat "$(pid_file "$svc")"))"
    return
  fi

  local cmd
  cmd="$(command_for_service "$svc")"
  if [[ -z "$cmd" ]]; then
    echo "[error] unknown service: $svc" >&2
    exit 1
  fi
  build_one "$svc"

  echo "[start] $svc"
  (
    cd "$ROOT_DIR"
    nohup "$cmd" >/dev/null 2>&1 &
    echo $! >"$(pid_file "$svc")"
  )

  sleep 0.4
  if is_running "$svc"; then
    echo "[ok] $svc pid $(cat "$(pid_file "$svc")") log $(log_file "$svc")"
  else
    echo "[fail] $svc failed to start, check $(log_file "$svc")" >&2
    exit 1
  fi
}

stop_one() {
  local svc="$1"
  local pf
  pf="$(pid_file "$svc")"
  if ! is_running "$svc"; then
    rm -f "$pf"
    local pattern
    pattern="$(pattern_for_service "$svc")"
    if [[ -n "$pattern" ]]; then
      pkill -f "$pattern" 2>/dev/null || true
    fi
    echo "[skip] $svc pid file not running, fallback cleanup attempted"
    return
  fi

  local pid
  pid="$(cat "$pf")"
  echo "[stop] $svc pid $pid"
  kill "$pid" 2>/dev/null || true

  for _ in {1..20}; do
    if kill -0 "$pid" 2>/dev/null; then
      sleep 0.2
    else
      break
    fi
  done

  if kill -0 "$pid" 2>/dev/null; then
    echo "[kill] $svc pid $pid"
    kill -9 "$pid" 2>/dev/null || true
  fi

  rm -f "$pf"
  local pattern
  pattern="$(pattern_for_service "$svc")"
  if [[ -n "$pattern" ]]; then
    pkill -f "$pattern" 2>/dev/null || true
  fi
  echo "[ok] $svc stopped"
}

status_one() {
  local svc="$1"
  if is_running "$svc"; then
    echo "$svc: running (pid $(cat "$(pid_file "$svc")"))"
  else
    echo "$svc: stopped"
  fi
}

logs_one() {
  local svc="$1"
  local lf
  lf="$(log_file "$svc")"
  if [[ ! -f "$lf" ]]; then
    echo "[info] no log file for $svc"
    return
  fi
  tail -n 80 "$lf"
}

print_usage() {
  cat <<USAGE
Usage:
  scripts/dev_services.sh start [service...]
  scripts/dev_services.sh stop [service...]
  scripts/dev_services.sh restart [service...]
  scripts/dev_services.sh status [service...]
  scripts/dev_services.sh logs <service>

Services: ${SERVICES[*]}
Examples:
  scripts/dev_services.sh start
  scripts/dev_services.sh restart api broadcaster
  scripts/dev_services.sh logs depositor
USAGE
}

pick_services() {
  if [[ "$#" -eq 0 ]]; then
    printf '%s\n' "${SERVICES[@]}"
  else
    printf '%s\n' "$@"
  fi
}

main() {
  load_env

  local action="${1:-}"
  if [[ -z "$action" ]]; then
    print_usage
    exit 1
  fi
  shift || true

  case "$action" in
    start)
      while IFS= read -r svc; do start_one "$svc"; done < <(pick_services "$@")
      ;;
    stop)
      while IFS= read -r svc; do stop_one "$svc"; done < <(pick_services "$@")
      ;;
    restart)
      while IFS= read -r svc; do stop_one "$svc"; done < <(pick_services "$@")
      while IFS= read -r svc; do start_one "$svc"; done < <(pick_services "$@")
      ;;
    status)
      while IFS= read -r svc; do status_one "$svc"; done < <(pick_services "$@")
      ;;
    logs)
      if [[ "$#" -ne 1 ]]; then
        echo "[error] logs requires exactly one service name" >&2
        exit 1
      fi
      logs_one "$1"
      ;;
    *)
      print_usage
      exit 1
      ;;
  esac
}

main "$@"
