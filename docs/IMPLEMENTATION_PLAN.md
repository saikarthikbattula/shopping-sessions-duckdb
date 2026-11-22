# Current implementation plan

Production is a complete Rust CSV-to-Parquet executable. The former Python
production pipeline and Parquet staging boundary are retired at the user's
request. Original workstream records are historical; see DATA_CONTRACT.md for
current interfaces and README.md for commands.

1. Keep the portable Rust implementation and exact Rust fallback correct under
   all official rules. Preserve fixtures, dev-answer comparison and independent
   local boundary/randomized checks.
2. Build a static musl executable and run it directly in a minimal container.
   No Python, DuckDB, runtime downloads or intermediate staging are shipped.
   Linux image and restricted-container checks remain required manual gates;
   GitHub Actions workflows were removed at `29cf0f5`.
3. Evaluate the imported, MIT-licensed Express Lane source as an isolated
   AVX-512 Linux candidate. Preserve its license and provenance. Do not infer its
   actual performance from an upstream claim or an ARM Mac development run.
4. Gate candidate selection on all 36 fixtures, published dev fingerprints,
   independent tests, and exact both-table comparisons on 5M and 20M seeds 1/2.
   Compare stock and worker-spin variants with whole-process wall and CPU metrics.
5. Promote a candidate only after measured improvements on both seeds. Record
   hardware, binary hashes, resource usage, fallbacks and unrun checks. A 20M
   scorer run on 8 CPUs/24 GB is necessary to establish the sub-second target.

Every change uses a task branch based on updated origin/main and a PR targeting
main. Never merge, auto-merge, force-push, or publish a competition submission.
A human maintainer reviews and merges. GitHub branch protections are separate.

Generated data, answers, outputs, credentials and local build environments remain
outside Git and the image build context. The official Rust generator is retained
unchanged. Keep the committed repository below 50 MB and image below 8 GB.
