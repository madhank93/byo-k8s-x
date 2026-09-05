#!/usr/bin/env bash
# vsnap.sh <stage-N>
#
# Verifies the authoring copy (courses/kubectl/reference/work) against stages
# 1..N and snapshots it as stage N *only* on a full pass. Nothing unverified
# is ever stored, which is what stops a broken snapshot becoming the starting
# point of the next stage.
set -euo pipefail

n="${1:?usage: vsnap.sh <stage-N>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
course="$root/courses/kubectl"

slug=$(awk '/^  - slug: /{c++; if (c=='"$n"') {print $3; exit}}' "$course/course.yml")
[ -n "$slug" ] || { echo "no stage $n in course.yml"; exit 1; }
dir=$(printf "%02d-%s" "$n" "$slug")

# Grade a scratch copy so the learner's own app/ is never touched.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/app"
cp "$course"/reference/work/* "$tmp/app/"

cases=$(awk '/^  - slug: /{c++; if (c<='"$n"') printf "%s{\"slug\":\"%s\",\"title\":\"Stage #%d\"}", (c>1?",":""), $3, c} END{}' "$course/course.yml")
BYOK8S_SUBMISSION_DIR="$tmp" BYOK8S_TEST_CASES_JSON="[$cases]" go run "$root/cmd/tester"

mkdir -p "$course/reference/stages/$dir"
cp "$course"/reference/work/main.go "$course/reference/stages/$dir/main.go"
echo "OK stage $n ($dir): 1..$n passed, snapshotted"
