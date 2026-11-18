#!/bin/bash
# Print the Python stacks of every Python process below PID $1 into the shard
# log: each gets SIGUSR1, which sitecustomize.py next to this script turns into
# a faulthandler dump of all its threads. Only Python processes are signalled;
# SIGUSR1 would kill anything else (/usr/bin/time, bash).
root="$1"
pids=$(ps -e -o pid=,ppid= | awk -v root="$root" '
  { parent[$1] = $2 }
  END {
    for (p in parent) {
      q = p
      while (q in parent && q != root && q > 1) q = parent[q]
      if (q == root && p != root) print p
    }
  }')
for pid in $pids; do
  comm=$(cat "/proc/$pid/comm" 2>/dev/null) || continue
  case "$comm" in
    python*|pycmor)
      args=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | cut -c1-160)
      echo "[WATCHDOG] stacks of pid $pid ($args):" >&2
      kill -USR1 "$pid" 2>/dev/null || true
      ;;
  esac
done
