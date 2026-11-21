IMAGE ?= shopping-sessions:latest
CPUS ?= 8
MEM ?= 24g
PLATFORM ?= linux/amd64
DEV ?= dev
OUT ?= out
FIXTURES ?= fixtures
PYTHON ?= .venv/bin/python
CARGO ?= cargo
RUST_JOBS ?= 2
NATIVE_INGEST ?= $(CURDIR)/native/ingest/target/release/shopping-ingest
GENERATOR ?= tools/generator/target/release/generate
BASELINE_BIN ?= target/baseline/shopping-ingest
BENCH_REPORT ?= out-benchmark/report.json
export IMAGE CPUS MEM PLATFORM

.PHONY: setup native-ingest test test-native build fixtures fixtures-local generator dev run-dev run-local check-dev check-dev-fingerprint benchmark

# Python dependencies are local validation tools; none are installed in the image.
setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements-dev.txt

native-ingest:
	$(CARGO) build --release --locked --jobs $(RUST_JOBS) --manifest-path native/ingest/Cargo.toml

test-native:
	$(CARGO) test --locked --jobs $(RUST_JOBS) --manifest-path native/ingest/Cargo.toml

test: native-ingest test-native
	REQUIRE_RUST_INGEST=1 RUST_INGEST_BIN="$(NATIVE_INGEST)" $(PYTHON) -m unittest discover -s tests -v

fixtures-local: native-ingest
	$(PYTHON) scripts/run_fixtures.py "$(FIXTURES)" -- "$(NATIVE_INGEST)"

build:
	docker buildx build --platform "$(PLATFORM)" --load -t "$(IMAGE)" .

fixtures: build
	$(PYTHON) scripts/run_fixtures.py "$(FIXTURES)" -- scripts/docker_run.sh

generator:
	$(CARGO) build --release --locked --manifest-path tools/generator/Cargo.toml

dev: generator
	"$(GENERATOR)" --seed 1 --events 1000000 --clicks 250000 --output "$(DEV)"

run-local: native-ingest
	DATA_DIR="$(DEV)" OUTPUT_DIR="$(OUT)" ./run.sh

run-dev: build
	scripts/docker_run.sh "$(DEV)" "$(OUT)"

ANSWER ?= dev-answer
check-dev:
	$(PYTHON) scripts/compare.py "$(OUT)" "$(ANSWER)"

check-dev-fingerprint:
	$(PYTHON) scripts/check_dev_fingerprints.py "$(OUT)"

benchmark: native-ingest
	$(PYTHON) scripts/benchmark_cpu_candidate.py "$(BASELINE_BIN)" "$(NATIVE_INGEST)" "$(DEV)" "$(BENCH_REPORT)" --workers "$(CPUS)"
