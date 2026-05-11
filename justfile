export RUST_LOG := "info"
bin_name := "s3cli"

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

install-macos:
    cargo install --path . --root ~/.dev --bin "{{ bin_name }}" --force

install-linux:
    cargo install --path . --bin "{{ bin_name }}"

package-macos:
    cargo build --bin "{{ bin_name }}" --release
    rm -rf bin && mkdir -p bin && cp target/release/{{ bin_name }} bin/{{ bin_name }} && chmod +x bin/{{ bin_name }}
    du -sh bin/{{ bin_name }}

package-linux:
    if [ ! -f .cargo/config.toml ];then \
        mkdir -p .cargo && echo '[target.x86_64-unknown-linux-musl]\nlinker = "x86_64-linux-musl-gcc"' > .cargo/config.toml; \
    fi
    cargo build --bin "{{ bin_name }}" --release --target x86_64-unknown-linux-musl;
    rm -rf bin && mkdir -p bin && mv target/x86_64-unknown-linux-musl/release/{{ bin_name }} bin/{{ bin_name }} && chmod +x bin/{{ bin_name }}
    du -sh bin/{{ bin_name }}
