#!/usr/bin/env bash
# vcheck.sh <course> <stage-N> [file]
#
# Grades a program against stages 1..N and stores nothing. vsnap.sh is the same
# check with a snapshot on the end, so it is the wrong tool for "did I break an
# earlier stage": it overwrites stage N's snapshot with whatever is in the
# authoring copy now, which for work in progress is the next stage's code.
set -euo pipefail

slug_arg="${1:?usage: vcheck.sh <course> <stage-N> [file]}"
n="${2:?usage: vcheck.sh <course> <stage-N> [file]}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
course="$root/courses/$slug_arg"
[ -d "$course" ] || { echo "no course $slug_arg"; exit 1; }

entry=$(awk '/^entrypoint: /{print $2; exit}' "$course/course.yml")
entry="${entry:-main.go}"
program="${3:-$course/reference/work/$entry}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/app"
cp "$course"/reference/work/* "$tmp/app/" 2>/dev/null || true
cp "$program" "$tmp/app/$entry"

cases=$(awk '/^  - slug: /{c++; if (c<='"$n"') printf "%s{\"slug\":\"%s\",\"title\":\"Stage #%d\"}", (c>1?",":""), $3, c} END{}' "$course/course.yml")
BYOK8S_COURSE="$slug_arg" BYOK8S_SUBMISSION_DIR="$tmp" BYOK8S_TEST_CASES_JSON="[$cases]" go run "$root/cmd/tester"
