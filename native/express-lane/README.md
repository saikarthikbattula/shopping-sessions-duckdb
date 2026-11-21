# Express Lane evaluation candidate

Imported from the user's `express-lane-main` archive on 2026-11-21. The original
MIT license and copyright are preserved in LICENSE. UPSTREAM.json records SHA-256
hashes of the supplied source files, Cargo manifests and compiler configuration.
Agent-directed banner comments were removed; functional source is initially
unchanged. No source AGENTS.md or CLAUDE.md was imported.

This candidate is isolated from the default Rust executable. It uses Linux
syscalls and Cascade Lake AVX-512 instructions and cannot run on an ARM Mac.
Rust 1.90.0 is pinned for its AVX-512 intrinsics. Builds must run from this folder
so Cargo loads .cargo/config.toml. A newer CPU alone does not prove compatibility;
the evaluation workflow checks required CPU features before executing the binary.

```sh
cd native/express-lane
cargo build --release --locked
NCPU=2 target/release/pipeline /absolute/data /absolute/output
```

The binary takes positional input/output paths or DATA_DIR/OUTPUT_DIR, rather
than the trusted executable's named arguments. It has its own exact Rust generic
fallback. Fixtures, public dev fingerprints, independent differential checks and
both-seed benchmark comparisons gate selection. Its claimed upstream runtime is
not a measurement of this imported candidate or a competition result for our repo.

The evaluation sweeps the existing XL_SPIN worker-wait parameter at stock, 0,
and 50 microseconds. These are experimental variants until exact comparisons
and whole-process wall/CPU measurements establish an improvement. The benchmark
records a recommendation only when the same variant improves wall time and CPU
against stock on both seeds. No unmeasured setting becomes the production default.

Build its optional Rust-only container from the repository root:

```sh
docker build -f native/express-lane/Dockerfile -t shopping-express-candidate .
```

Do not run this image on a CPU without the required AVX-512 features. The default
Dockerfile remains the portable, validated Rust implementation.
