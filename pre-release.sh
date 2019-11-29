#!/bin/bash
set -euo pipefail

cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
./test.sh

cargo publish -v --dry-run

# Then do the following:
# * Check version in Cargo.toml is updated
# * cargo publish
# * git tag <new-version>
# * git push --tags
