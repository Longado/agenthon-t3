# Track 3 submission image: the native engine (native/, Rust) alone, statically linked, on an empty
# base. It reproduces the pinned ABIDES baseline bit-for-bit on every public scenario shape; the
# sealed scenarios use no agent type or setting the public ones lack (track3-simulation-public#25).
# No Python: on the Development host every unit's fixed container cost was 0.09 s lower with this
# image than with the python:3.11-slim one (submissions 976210 vs 976396), and the Python fallback
# could only ever take over on configurations the sealed set does not use.
FROM --platform=linux/amd64 rust:1.98-slim-trixie AS native
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src ./src
RUN RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --locked --bin t3sim \
        --target x86_64-unknown-linux-gnu \
    && strip target/x86_64-unknown-linux-gnu/release/t3sim

FROM scratch
LABEL qfbench2.interface_version="2.0"
# Transparent huge pages for malloc'd memory, a high mmap threshold that keeps freed buffers in
# malloc's arenas, and one arena for all threads (2-15% and 3-11% faster per scenario locally).
ENV PATH=/usr/local/bin \
    GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=1073741824:glibc.malloc.arena_max=1
COPY --from=native /src/target/x86_64-unknown-linux-gnu/release/t3sim /usr/local/bin/simulate
COPY --from=native /src/target/x86_64-unknown-linux-gnu/release/t3sim /usr/local/bin/simulate-batch
CMD ["simulate", "--help"]
