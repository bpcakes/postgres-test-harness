# Contributing

Contributions to `postgres-test-harness` are welcome. Bug reports should include
the harness version, Rust version, PostgreSQL image or server version, selected
Cargo features, and the smallest reproduction available.

For substantial behavior or API changes, open an issue before implementation so
the intended contract and compatibility impact can be discussed.

## Development requirements

- Rust 1.88 or newer
- Cargo
- A Testcontainers-compatible Docker daemon for owned-container lifecycle tests
- PostgreSQL 18 when exercising external-server mode

The ordinary unit, compile, and documentation tests do not require a running
PostgreSQL server. The ignored lifecycle tests do.

## Local checks

Run the same format, lint, feature, and documentation checks used by CI:

```console
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --no-default-features --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --no-default-features --all-targets
cargo test --locked --doc
cargo test --locked --no-default-features --doc
```

Run the owned-container lifecycle suite separately:

```console
docker pull postgres:18
cargo test --locked --test postgres -- --ignored --test-threads=1
```

To exercise the external-only reference adapter, start PostgreSQL 18 with an
administrative role that can create and drop databases, then run:

```console
POSTGRES_TEST_ADMIN_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
  cargo run --locked --no-default-features --example downstream_adapter
```

Documentation examples in `README.md` are crate doctests and must compile with
both default features and `--no-default-features`. Changes that affect benchmark
semantics or defaults should also update the
[performance workflow](docs/performance.md) and its recorded baseline.
