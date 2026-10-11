# Track 3 submission image: ABIDES (pinned commit f9cbe51 + the 4 official patches, vendored under
# abides/) with output-preserving speedups, plus the official `abides_fork` simulate adapter.
# Same Python and dependency pins as the official baseline image, so RNG streams and float math match.
# The native engine (native/, Rust) reproduces the baseline bit-for-bit on supported scenarios and
# hands anything else to the Python adapter, so `simulate` / `simulate-batch` are the Rust binary.
# Built on the same Debian release as the runtime base so it links the same glibc/libm.
FROM --platform=linux/amd64 rust:1.98-slim-trixie AS native
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src ./src
RUN cargo build --release --locked --bin t3sim

FROM --platform=linux/amd64 python:3.11-slim

LABEL qfbench2.interface_version="2.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt

RUN pip install \
        numpy==1.26.4 \
        pandas==1.5.3 \
        scipy==1.17.1 \
        pyarrow==15.0.2 \
        coloredlogs==15.0.1

COPY abides /tmp/abides
RUN pip install --no-deps /tmp/abides/abides-core /tmp/abides/abides-markets && rm -rf /tmp/abides

COPY abides_fork /opt/abides_fork
COPY --from=native /src/target/release/t3sim /usr/local/bin/simulate
# Transparent huge pages for malloc'd memory: hosts default to THP=madvise, where the engine's
# few hundred MB of event logs otherwise cost ~50 ms of 4 KiB page faults on a large scenario.
# A high mmap threshold keeps freed buffers in malloc's arenas for reuse instead of returning them
# to the kernel and faulting fresh zeroed pages in again (measured 2-15% faster per scenario), and
# one arena for all threads stops every encoder thread from faulting in its own huge pages
# (another 3-11%).
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=1073741824:glibc.malloc.arena_max=1
RUN ln -s simulate /usr/local/bin/simulate-batch
# Precompile bytecode: PYTHONDONTWRITEBYTECODE stops caching at runtime, so every container start
# otherwise recompiles the adapter (and anything pip left uncompiled).
# ponytail: `|| true` because a few stdlib test fixtures are deliberately unparsable; missing .pyc only costs speed.
RUN python -m compileall -q /opt/abides_fork /usr/local/lib/python3.11 || true

WORKDIR /work
CMD ["simulate", "--help"]
