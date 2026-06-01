default:
    @just --list

build:
    cargo build --workspace

ci:
    cargo build --workspace
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt -p agui-acp-bridge-core -p agui-acp-bridge-policy -p agui-acp-bridge-server -p agui-acp-bridge-cli -- --check
    cargo test --workspace --all-targets

test:
    cargo test --workspace

fmt:
    cargo fmt -p agui-acp-bridge-core -p agui-acp-bridge-policy -p agui-acp-bridge-server -p agui-acp-bridge-cli
