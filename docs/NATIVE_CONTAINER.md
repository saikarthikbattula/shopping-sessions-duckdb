> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Rust-only runtime image

The root Dockerfile builds native/ingest with Rust 1.85.0 on Alpine/musl, checks
that the ELF has no program interpreter, and copies only the static executable
to a minimal BusyBox musl image. BusyBox supplies chmod for the restricted runner's
output-permission cleanup; the processing command invokes Rust directly.

No Python interpreter, DuckDB, Python packages, Python pipeline, Cargo toolchain,
source tree or generated data is installed in the final image. run.sh is only a
local convenience wrapper, with an explicit error if its Rust executable is absent.
The container needs no shell wrapper at startup.

The binary handles DATA_DIR, OUTPUT_DIR, NCPU and cgroup CPU detection. The runner
retains uid 1000:1000, no network, read-only root and inputs, 64 MB /tmp, writable
output/scratch mounts, and process/CPU/memory limits. No host CPU specialization
is applied to the portable binary. Native ARM64 images remain possible for local
checks; the competition target is linux/amd64.

The optional native/express-lane/Dockerfile similarly builds a Rust-only static
candidate, but its Cascade Lake flags require a compatible AVX-512 x86_64 CPU.
It is isolated from the default image and must pass its own correctness/performance
gates before selection.

Docker is unavailable in this local environment. CI must build the new images,
verify architecture, size, absence of Python and static linkage, and run official
fixtures/dev fingerprints under the restricted runner. Historical Python-backed
image results do not validate this new image.
