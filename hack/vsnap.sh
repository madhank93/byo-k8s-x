#!/usr/bin/env bash
# vsnap.sh <course> <stage-N>
#
# Verifies the authoring copy (courses/<course>/reference/work) against stages
# 1..N and snapshots it as stage N *only* on a full pass. Nothing unverified
# is ever stored, which is what stops a broken snapshot becoming the starting
# point of the next stage.
set -euo pipefail

slug_arg="${1:?usage: vsnap.sh <course> <stage-N>}"
n="${2:?usage: vsnap.sh <course> <stage-N>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
course="$root/courses/$slug_arg"
[ -d "$course" ] || { echo "no course $slug_arg (byok8s courses)"; exit 1; }

# The one file a snapshot holds. Every course is Go today; course.yml carries
# the field so the scripts do not have to assume it.
entry=$(awk '/^entrypoint: /{print $2; exit}' "$course/course.yml")
entry="${entry:-main.go}"

slug=$(awk '/^  - slug: /{c++; if (c=='"$n"') {print $3; exit}}' "$course/course.yml")
[ -n "$slug" ] || { echo "no stage $n in $slug_arg/course.yml"; exit 1; }
dir=$(printf "%02d-%s" "$n" "$slug")

# Grade a scratch copy so the learner's own app/ is never touched.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/app"
cp "$course"/reference/work/* "$tmp/app/"

cases=$(awk '/^  - slug: /{c++; if (c<='"$n"') printf "%s{\"slug\":\"%s\",\"title\":\"Stage #%d\"}", (c>1?",":""), $3, c} END{}' "$course/course.yml")
BYOK8S_COURSE="$slug_arg" BYOK8S_SUBMISSION_DIR="$tmp" BYOK8S_TEST_CASES_JSON="[$cases]" go run "$root/cmd/tester"

mkdir -p "$course/reference/stages/$dir"
cp "$course/reference/work/$entry" "$course/reference/stages/$dir/$entry"
echo "OK $slug_arg stage $n ($dir): 1..$n passed, snapshotted"
