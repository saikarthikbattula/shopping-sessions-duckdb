# Evaluate only on Linux x86_64 machines supporting Cascade Lake's AVX-512 features.
FROM rust:1.95.0-alpine AS build
RUN apk add --no-cache musl-dev binutils
WORKDIR /build
COPY native/express-lane/Cargo.toml native/express-lane/Cargo.lock ./
COPY native/express-lane/.cargo ./.cargo
COPY native/express-lane/src ./src
RUN cargo build --release --locked \
    && ! readelf -l target/release/pipeline | grep -q INTERP

FROM busybox:1.37.0-musl
COPY --from=build /build/target/release/pipeline /usr/local/bin/pipeline
COPY native/express-lane/LICENSE /licenses/express-lane.LICENSE
ENV DATA_DIR=/data OUTPUT_DIR=/output HOME=/scratch
USER 1000:1000
CMD ["/usr/local/bin/pipeline"]
