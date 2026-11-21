IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
PYTHON ?= .venv/bin/python
SCRATCH ?= $(CURDIR)/scratch
GENERATOR ?= tools/generator/target/release/generate
CARGO ?= cargo
RUST_JOBS ?= 2
NATIVE_INGEST ?= $(CURDIR)/native/ingest/target/release/shopping-ingest
export IMAGE CPUS MEM PLATFORM

.PHONY: setup test native-ingest test-native build fixtures fixtures-local generator dev run-dev run-local check-dev check-dev-fingerprint

## Create an isolated environment with pinned runtime and checker dependencies
setup:
	python3 -m venv .venv
	.venv/bin/python -m pip install -r requirements-dev.txt

## Unit, contract, and independent staged/raw differential checks
test:
	$(PYTHON) -m unittest discover -s tests -v

## Build the optional serial Rust backend; Python remains the default
native-ingest:
	cd native/ingest && $(CARGO) build --release --locked --jobs $(RUST_JOBS)

## Require the real native executable; never silently skip native parity checks
test-native: native-ingest
	cd native/ingest && $(CARGO) test --release --locked --jobs $(RUST_JOBS)
	REQUIRE_RUST_INGEST=1 RUST_INGEST_BIN="$(NATIVE_INGEST)" $(PYTHON) -m unittest discover -s tests -p 'test_ingest*.py' -v

## Official fixtures without Docker for the native development loop
fixtures-local:
	mkdir -p "$(SCRATCH)"
	SCRATCH_DIR="$(SCRATCH)" $(PYTHON) scripts/run_fixtures.py $(FIXTURES) -- $(PYTHON) -m pipeline.pipeline

## Build the official generator; requires Rust 1.85+
generator:
	cargo build --release --manifest-path tools/generator/Cargo.toml

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	$(PYTHON) scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set (make generator first)
dev:
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Native loop; does not reproduce container limits or scorer timings
run-local:
	mkdir -p "$(SCRATCH)" out
	DATA_DIR="$(DEV)" OUTPUT_DIR="$(CURDIR)/out" SCRATCH_DIR="$(SCRATCH)" $(PYTHON) -m pipeline.pipeline

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	$(PYTHON) scripts/compare.py out $(ANSWER)

## Public seed-1 comparison without committing or downloading answer rows
check-dev-fingerprint:
	$(PYTHON) scripts/check_dev_fingerprints.py out
