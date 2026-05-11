export RUST_LOG := "info"

run:
    cargo run

check:
    cargo check

fix:
    cargo fix --allow-dirty --allow-staged

test name="":
    cargo test {{ name }} -- --nocapture --test-threads=1

doc:
    cargo doc --open

update:
    cargo update

clean:
    cargo clean

publish:
    cargo publish

tree:
    cargo tree

fmt:
    cargo fmt

clippy:
    cargo clippy

docker:
    docker build -t myapp .

tb:
    npm run tauri build

tr:
    npm run tauri dev

macos:
    cargo install --path . --root ~/.dev --bin "s3cli" --force

# s3cli tmp ls s3://tmp
# s3cli tmp ls s3://tmp/rust-
# s3cli tmp getr s3://tmp/rust- .
# s3cli tmp put tests/test.rs s3://tmp
# s3cli tmp put tests/test.rs s3://tmp/11.rs

install-macos:
    cargo install --path . --root ~/.dev --bin "s3cli" --force

install-linux:
    cargo install --path . --bin "s3cli"

package-macos:
    cargo build --bin "s3cli" --release
    rm -rf bin && mkdir -p bin && cp target/release/s3cli bin/s3cli && chmod +x bin/s3cli
    du -sh bin/s3cli

package-linux:
    if [ ! -f .cargo/config.toml ];then \
        mkdir -p .cargo && echo '[target.x86_64-unknown-linux-musl]\nlinker = "x86_64-linux-musl-gcc"' > .cargo/config.toml; \
    fi
    cargo build --bin "s3cli" --release --target x86_64-unknown-linux-musl;
    rm -rf bin && mkdir -p bin && mv target/x86_64-unknown-linux-musl/release/s3cli bin/s3cli  && chmod +x bin/s3cli
    du -sh bin/s3cli
