#!/bin/sh

set -e

export RUSTFLAGS="-Zinstrument-coverage -Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
export CARGO_INCREMENTAL=0
export RUSTDOCFLAGS="-Cpanic=abort"
cargo build
cargo test

grcov . \
	-s . \
	--binary-path ./target/debug/ \
       	-t html \
       	--branch \
	--ignore-not-existing \
       	-o ./target/debug/coverage/ \
	--commit-sha $(git rev-parse HEAD) \
	--ignore "**/*-derive" \
	--ignore "/"

