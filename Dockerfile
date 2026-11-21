# Shopping Sessions starter: DuckDB skeleton.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.
FROM rust:1.85.0-bookworm AS native-build
WORKDIR /build/native/ingest
COPY native/ingest/Cargo.toml native/ingest/Cargo.lock native/ingest/rust-toolchain.toml ./
COPY native/ingest/src/ ./src/
# Build for the target Linux platform, without host-specific CPU instructions.
RUN cargo build --release --locked --jobs 2

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY --from=native-build /build/native/ingest/target/release/shopping-ingest /usr/local/bin/shopping-ingest
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RUST_INGEST_BIN=/usr/local/bin/shopping-ingest

WORKDIR /app
CMD ["/app/run.sh"]
