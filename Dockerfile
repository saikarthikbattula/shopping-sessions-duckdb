# The scorer builds natively for linux/amd64; ARM64 remains available for local checks.
FROM rust:1.85.0-alpine AS native-build
RUN apk add --no-cache musl-dev binutils
WORKDIR /build/native/ingest
COPY native/ingest/Cargo.toml native/ingest/Cargo.lock native/ingest/rust-toolchain.toml ./
COPY native/ingest/src/ ./src/
RUN cargo build --release --locked --jobs 2 \
    && ! readelf -l target/release/shopping-ingest | grep -q INTERP

# BusyBox supplies chmod for the restricted runner's output cleanup. Processing
# starts directly in the static Rust executable: no shell or Python is launched.
FROM busybox:1.37.0-musl
COPY --from=native-build /build/native/ingest/target/release/shopping-ingest /usr/local/bin/shopping-ingest
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch
USER 1000:1000
CMD ["/usr/local/bin/shopping-ingest"]
