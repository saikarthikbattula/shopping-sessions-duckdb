.DEFAULT_GOAL := engine
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
NATIVE_INGEST ?= $(CURDIR)/target/release/pipeline
GENERATOR ?= tools/generator/target/release/generate
BASELINE_BIN ?= target/baseline/pipeline
BENCH_REPORT ?= out-benchmark/report.json
export IMAGE CPUS MEM PLATFORM

.PHONY: setup engine native-ingest test test-native build fixtures fixtures-local generator dev run-dev run-local check-dev check-dev-fingerprint benchmark
setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements-dev.txt

engine native-ingest:
	$(CARGO) build --release --locked --jobs $(RUST_JOBS)

test-native:
	$(CARGO) test --release --locked --jobs $(RUST_JOBS)

test: engine test-native
	REQUIRE_RUST_INGEST=1 NATIVE_CLI_STYLE=positional RUST_INGEST_BIN="$(NATIVE_INGEST)" $(PYTHON) -m unittest discover -s tests -v

fixtures-local: engine
	$(PYTHON) scripts/run_fixtures.py "$(FIXTURES)" -- "$(NATIVE_INGEST)"

build:
	docker buildx build --platform "$(PLATFORM)" --load -t "$(IMAGE)" .

fixtures: build
	$(PYTHON) scripts/run_fixtures.py "$(FIXTURES)" -- scripts/docker_run.sh

generator:
	$(CARGO) +1.85.0 build --release --locked --manifest-path tools/generator/Cargo.toml

dev: generator
	"$(GENERATOR)" --seed 1 --events 1000000 --clicks 250000 --output "$(DEV)"

run-local: engine
	DATA_DIR="$(DEV)" OUTPUT_DIR="$(OUT)" NCPU="$(CPUS)" ./run.sh

run-dev: build
	scripts/docker_run.sh "$(DEV)" "$(OUT)"

ANSWER ?= dev-answer
check-dev:
	$(PYTHON) scripts/compare.py "$(OUT)" "$(ANSWER)"

check-dev-fingerprint:
	$(PYTHON) scripts/check_dev_fingerprints.py "$(OUT)"

benchmark: engine
	$(PYTHON) scripts/benchmark_express_lane.py "$(BASELINE_BIN)" "$(NATIVE_INGEST)" "$(DEV)" "$(BENCH_REPORT)" --workers "$(CPUS)"
