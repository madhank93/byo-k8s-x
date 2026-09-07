#!/usr/bin/env bash
# sweep.sh — fail if a stage's snapshot is byte-identical to the one before it.
#
# In byox this check did not exist and snapshots were captured once per feature
# group: 115 redis stages shared 11 snapshots, so stage 1 displayed 181 lines
# of work belonging to stages 2-7. Undoing that took days. Here it is a gate
# from the first stage, for every course.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

dups=0
for slug in $(go run ./cmd/byok8s courses --shipped); do
  stages="$root/courses/$slug/reference/stages"
  [ -d "$stages" ] || { echo "$slug: no snapshots yet"; continue; }
  entry=$(awk '/^entrypoint: /{print $2; exit}' "$root/courses/$slug/course.yml")
  entry="${entry:-main.go}"

  prev=""; prevname=""
  for d in $(ls -d "$stages"/*/ 2>/dev/null | sort); do
    [ -f "$d/$entry" ] || continue
    sum=$(shasum "$d/$entry" | cut -d' ' -f1)
    if [ "$sum" = "$prev" ]; then
      echo "DUPLICATE: $slug $(basename "$prevname") and $(basename "$d") are identical"
      dups=$((dups+1))
    fi
    prev="$sum"; prevname="$d"
  done
done
[ "$dups" -eq 0 ] || { echo "$dups duplicate snapshot(s): each stage must show its own step"; exit 1; }
echo "sweep clean: every snapshot differs from the one before it"
