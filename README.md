# rustplate

Rust project boilerplate with `tracing`, `anyhow`, and version bumping.

## Quick Start

```bash
gh repo create my-project --template CooperBigFoot/rustplate --clone --private
cd my-project
bash init.sh my-project
```

## Development

```bash
cargo build                   # build
cargo run                     # run
cargo test                    # test
cargo clippy                  # lint
cargo fmt                     # format
```

## Adding Dependencies

```bash
cargo add <crate>             # runtime dependency
cargo add --dev <crate>       # dev dependency
```
