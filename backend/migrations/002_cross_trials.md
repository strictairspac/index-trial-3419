# Schema v2

Adds `cross_trials` (SQLAlchemy metadata, created on startup). Each row stores the immutable
workload/candidate snapshot, the frozen telemetry sample descriptor (row count, id watermark
`max_id`, content fingerprint, gzip artifact path and artifact SHA-256), per-side execution
state/results (`sides`), and the reconciled `comparison` or `mismatch` report.

`telemetry` now exists identically on two databases: the source (`SOURCE_DATABASE_URL`, the
current/baseline version) and the target (`TARGET_DATABASE_URL`, the upgrade candidate). Trials
only read/repair rows with `id <= max_id`; concurrent online writes take higher auto-increment
ids and are never modified. Measurement is per-side under `GET_LOCK('index_trial_serial')`, and
the temporary `trial_candidate` index is dropped after each side.

Schema v1 note: SQLAlchemy metadata creates users, audits, workloads, candidates, experiments,
cross_trials and telemetry on first startup. Workload/candidate natural names are UNIQUE;
experiment snapshots are immutable JSON. Foreign keys prevent orphan references. No production
database connection is accepted.
