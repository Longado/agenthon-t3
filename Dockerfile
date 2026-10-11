# DIAGNOSTIC image (not for the Final): the native engine alone, statically linked, on an empty
# base. No Python, so no fallback. Tests whether image contents change the grader's fixed
# per-container time. Same engine and malloc tunables as the r7 image.
FROM --platform=linux/amd64 rust:1.98-slim-trixie AS native
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src ./src
RUN RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --locked --bin t3sim \
        --target x86_64-unknown-linux-gnu \
    && strip target/x86_64-unknown-linux-gnu/release/t3sim

FROM scratch
LABEL qfbench2.interface_version="2.0"
ENV PATH=/usr/local/bin \
    GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=1073741824:glibc.malloc.arena_max=1
COPY --from=native /src/target/x86_64-unknown-linux-gnu/release/t3sim /usr/local/bin/simulate
COPY --from=native /src/target/x86_64-unknown-linux-gnu/release/t3sim /usr/local/bin/simulate-batch
CMD ["simulate", "--help"]
