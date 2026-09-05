#!/usr/bin/env bash
# sweep.sh — fail if a stage's snapshot is byte-identical to the one before it.
#
# In byox this check did not exist and snapshots were captured once per feature
# group: 115 redis stages shared 11 snapshots, so stage 1 displayed 181 lines
# of work belonging to stages 2-7. Undoing that took days. Here it is a gate
# from the first stage.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stages="$root/courses/kubectl/reference/stages"
[ -d "$stages" ] || { echo "no snapshots yet"; exit 0; }

prev=""; prevname=""; dups=0
for d in $(ls -d "$stages"/*/ 2>/dev/null | sort); do
  [ -f "$d/main.go" ] || continue
  sum=$(shasum "$d/main.go" | cut -d' ' -f1)
  if [ "$sum" = "$prev" ]; then
    echo "DUPLICATE: $(basename "$prevname") and $(basename "$d") are identical"
    dups=$((dups+1))
  fi
  prev="$sum"; prevname="$d"
done
[ "$dups" -eq 0 ] || { echo "$dups duplicate snapshot(s): each stage must show its own step"; exit 1; }
echo "sweep clean: every snapshot differs from the one before it"
