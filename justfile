default: tokio-test

test:
    cargo test -- --nocapture

tokio-test:
    RUSTFLAGS="--cfg tokio_unstable" cargo run

doc:
    cargo doc --no-deps
