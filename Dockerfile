# Track 3 submission image: ABIDES (pinned commit f9cbe51 + the 4 official patches, vendored under
# abides/) with output-preserving speedups, plus the official `abides_fork` simulate adapter.
# Same Python and dependency pins as the official baseline image, so RNG streams and float math match.
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
COPY simulate /usr/local/bin/simulate
COPY simulate-batch /usr/local/bin/simulate-batch
RUN chmod +x /usr/local/bin/simulate /usr/local/bin/simulate-batch

WORKDIR /work
CMD ["simulate", "--help"]
