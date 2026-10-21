#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../../.." && pwd)
evidence_dir=${1:?Usage: reproduce.sh NEW_EVIDENCE_DIR NEW_FIXTURE_ROOT}
fixture_root=${2:?Usage: reproduce.sh NEW_EVIDENCE_DIR NEW_FIXTURE_ROOT}
if [[ -e "$evidence_dir" || -e "$fixture_root" ]]; then
  printf '%s\n' 'Evidence and fixture paths must both be new; existing paths are refused.' >&2
  exit 1
fi
mkdir -p -- "$evidence_dir"
evidence_dir=$(cd -- "$evidence_dir" && pwd)
cd -- "$repo_root"

cargo build --offline --locked --release --bin coop --example replay_baseline \
  >"$evidence_dir/build.stdout.log" 2>"$evidence_dir/build.stderr.log"

{
  date --utc --iso-8601=seconds
  git rev-parse HEAD
  git status --short
  rustc --version --verbose
  cargo --version
  uname -a
  df -T /tmp
  sha256sum Cargo.toml Cargo.lock examples/replay_baseline.rs \
    docs/impl/acceptance-20261020/replay/reproduce.sh \
    target/release/coop target/release/examples/replay_baseline
  rg --files src | sort | xargs sha256sum
} >"$evidence_dir/build-environment.txt"

/usr/bin/time -v -o "$evidence_dir/overall-time.txt" \
  target/release/examples/replay_baseline \
  --fixture-root "$fixture_root" \
  --output-dir "$evidence_dir" \
  --coop-binary target/release/coop \
  --records 1000000 --samples 3 --remove-fixture-on-success \
  >"$evidence_dir/stdout.jsonl" 2>"$evidence_dir/harness.stderr.log"

cat "$evidence_dir/results.jsonl"
