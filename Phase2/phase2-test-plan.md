# Phase 2 Test Plan

## 1. Purpose

This document defines executable acceptance tests for Quantalytics Phase 2. Passing these tests is part of the Phase 2 Definition of Done.

Every test records:

```text
Test ID
Purpose
Setup
Input
Action
Expected Result
Failure Condition
```

No test uses fabricated production results. Fixtures are synthetic/frozen test artifacts clearly labeled as such.

## 2. Test environment

Use:

- local PySpark/Delta-compatible test environment where feasible for unit/integration;
- Databricks Free Edition for final execution evidence;
- frozen raw fixtures;
- isolated test catalog/schema or temporary paths.

Tests must not require live source access except collector-specific integration tests. Core Bronze/Silver tests run from frozen artifacts.

## 3. Unit tests

### UT-001 — Canonical timestamp parsing

**Purpose:** ensure supported source times normalize to the same UTC instant.

**Setup:** timestamp helper.

**Input:** equivalent ISO timestamp forms.

**Action:** parse.

**Expected:** same UTC value; original raw value retained separately.

**Failure:** silent null, local-time ambiguity, or unequal UTC values.

### UT-002 — Invalid timestamp distinction

**Input:** non-null invalid timestamp text.

**Action:** parse.

**Expected:** parse failure flag/category; record not treated as legitimate null.

**Failure:** accepted null with no error.

### UT-003 — Stable hash/key generation

**Input:** same canonical key components across three calls.

**Expected:** identical SHA-256.

Change one key component.

**Expected:** different key.

### UT-004 — Null-sentinel key safety

Verify:

```text
["ab", null, "c"]
!=
["ab", "", "c"]
```

under key canonicalization.

### UT-005 — Unit conversion seconds to microseconds

Known test value:

```text
0.0001 s -> 100.0 µs
```

Expected exact/controlled floating result.

### UT-006 — Unit conversion seconds to nanoseconds

```text
1e-7 s -> 100 ns
```

### UT-007 — Probability validation

Inputs: `0`, `0.5`, `1`, `-0.1`, `1.1`.

Expected first three accepted; last two invalid.

### UT-008 — Qubit ID validation

Inputs: `0`, positive, `-1`.

Negative is invalid.

### UT-009 — Parameter parser

Verify all modes parse required/optional parameters and reject impossible combinations, such as backfill with neither range nor files.

### UT-010 — Merge-key construction

Verify keys match documented table-specific business keys and never include `load_timestamp`.

## 4. Schema tests

### ST-001 — Explicit envelope schema

**Setup:** valid raw envelope fixture.

**Action:** read with `RAW_ARTIFACT_ENVELOPE_SCHEMA`.

**Expected:** exact documented types.

**Failure:** any inference path used.

### ST-002 — Missing required envelope field

Remove `batch_id`.

Expected batch/file structural failure; no Bronze write.

### ST-003 — Additive source field

Add unknown IBM payload key.

Expected:
- raw preserved;
- drift logged as `ADDITIVE`;
- compatible processing continues;
- Silver schema does not automatically change.

### ST-004 — Parseable type drift

Represent a test numeric property as an explicitly supported numeric string.

Expected:
- strict normalization if configured;
- normalization logged;
- final value typed.

### ST-005 — Incompatible type drift

Provide object/invalid text where numeric required.

Expected affected record quarantined; no fake null accepted.

### ST-006 — Breaking IBM structure

Remove required `qubits`/change expected structure.

Expected:
- affected stage fails;
- raw retained;
- Silver unchanged;
- watermark unchanged;
- drift event logged.

## 5. Bronze integration tests

### IT-BR-001 — IBM raw to Bronze

**Setup:** frozen valid IBM artifact envelope.

**Action:** run IBM Bronze.

**Expected:**
- one immutable payload-version row;
- hash verified;
- timestamps normalized;
- `record_key`, `batch_id`, `run_id`, `load_timestamp` populated.

### IT-BR-002 — IBM exact rerun

Run same fixture twice.

Expected second run:

```text
rows_inserted = 0
duplicate logical Bronze record = 0
existing load_timestamp unchanged
```

### IT-BR-003 — IBM same snapshot/different hash

Create a source-revision fixture with same backend/calibration but changed payload.

Expected:
- second Bronze row with new `record_key`;
- same `snapshot_business_key`;
- revision/drift/audit event.

### IT-BR-004 — NOAA Kp Bronze

Use frozen Kp fixture.

Expected every observation is traceable to source artifact and typed observation timestamp.

### IT-BR-005 — NOAA F10.7 Bronze

Same expectations for flux fixture.

### IT-BR-006 — Hash mismatch

Tamper payload after recorded hash.

Expected quarantine/rejection before Bronze insert.

## 6. Silver integration tests

### IT-SV-001 — Backend normalization

Given IBM payload with N qubit arrays:

Expected `silver_backend.qubit_count=N`.

### IT-SV-002 — Qubit normalization

Given two known qubit-property arrays:

Expected two Silver qubit rows, correct IDs, typed T1/T2/readout values and lineage.

### IT-SV-003 — T1/T2 unit conversion

Fixture source values in seconds with verified source unit.

Expected stored `t1_us`, `t2_us` in microseconds.

### IT-SV-004 — Initialization error optionality

Payload lacks `init_error`.

Expected row accepted with `init_error=null`; no value invented.

### IT-SV-005 — Canonical single-gate selection

Fixture has `x` and `sx` 1Q errors.

Config selects `sx`.

Expected `single_gate_error_type='sx'` and corresponding error.

### IT-SV-006 — Missing canonical single gate

Config selects unavailable instruction.

Expected qubit row accepted with single-gate fields null plus warning; no arbitrary substitute.

### IT-SV-007 — Two-qubit gate normalization

Known two-qubit gate with error/length.

Expected ordered source/target, gate type, generic error, converted length.

### IT-SV-008 — CZ/RZZ compatibility fields

For CZ fixture, `cz_error=gate_error`, `rzz_error=null`.

For RZZ fixture, inverse.

### IT-SV-009 — Unsupported gate arity

Three-qubit gate fixture.

Expected raw preserved, Phase2 gate Silver skipped with explicit warning/category; no invalid source/target flattening.

### IT-SV-010 — NOAA Kp Silver

Expected:
- `observation_type=planetary_kp`;
- Kp populated;
- solar flux null.

### IT-SV-011 — NOAA F10.7 Silver

Expected:
- `observation_type=f10_7_flux`;
- solar flux populated;
- Kp null.

### IT-SV-012 — No premature environmental alignment

Provide Kp and F10.7 at different times.

Expected separate rows; no nearest-time join.

## 7. Data-quality tests

### DQT-001 — T1/T2 positive

Zero/negative source value -> affected row quarantine.

### DQT-002 — Error range

Error outside `[0,1]` -> quarantine.

### DQT-003 — Gate length positive

Non-positive -> quarantine.

### DQT-004 — Gate endpoint referential check

Endpoint absent from accepted qubits for same calibration.

Expected configured error/warning; acceptance policy explicitly recorded.

### DQT-005 — Expected vs observed qubit count

Remove one qubit row after normalization.

Expected reconciliation discrepancy.

### DQT-006 — Cross-field environment rule

Populate Kp and solar flux in same environment row.

Expected quarantine.

### DQT-007 — Conflicting duplicate candidate

Two different candidate rows for same Silver business key in same merge batch.

Expected ambiguous merge blocked before Delta `MERGE`.

## 8. Idempotency tests

### ID-001 — Full-load rerun

**Setup:** empty target; frozen full-load artifacts.

**Action:**
1. full load;
2. capture target business-key counts and deterministic content hashes;
3. run identical full load again.

**Expected:**
- target business-key count unchanged;
- no duplicates;
- content hash unchanged;
- second run inserts zero duplicate logical records;
- unchanged Silver load timestamps remain unchanged.

### ID-002 — Incremental rerun

Repeat the exact incremental artifact twice.

Expected second pass no analytical changes.

### ID-003 — Backfill rerun

Run historical range twice.

Expected stable historical content; no duplicate keys.

### ID-004 — Replay rerun

Replay same batch multiple times.

Expected stable analytical state each time.

### ID-005 — Three-run proof

Run same batch 3 times.

Expected:

```text
after run1 == after run2 == after run3
```

for analytical content.

## 9. Incremental tests

### INC-001 — Newer observation

Initial watermark at T1; provide T2 > T1.

Expected T2 accepted and watermark advances to committed maximum.

### INC-002 — Overlap duplicate

Provide T1 again inside overlap plus T2.

Expected T1 no-op, T2 accepted.

### INC-003 — Late record inside overlap

Record timestamp slightly before prior watermark but not previously seen.

Expected candidate processed, stable-key merge inserts it, watermark remains max committed time.

### INC-004 — Failed incremental write

Force Silver failure.

Expected watermark after = watermark before.

### INC-005 — Source revision in overlap

Same key/new payload hash.

Expected Bronze version preserved; Silver revision quarantined by default; watermark policy does not cause data loss.

## 10. Backfill tests

### BF-001 — Arbitrary date range

Provide valid historical fixtures within configured start/end.

Expected only selected range processed.

### BF-002 — Backfill does not move normal watermark

Capture watermark, execute backfill, compare.

Expected unchanged.

### BF-003 — Backfill older than existing Silver

Expected older keys inserted without touching newer unrelated keys.

### BF-004 — Controlled historical source revision

Same historical business key/new hash.

Default expected review/quarantine.

With approved update flag + valid fixture, expected controlled update and `rows_updated=1`.

## 11. Replay tests

### RP-001 — No external access during replay

Mock collector/network as unavailable.

Replay frozen artifact.

Expected success without external call.

### RP-002 — Remediated quarantine

Start with parse failure; fix parser/schema version; replay raw record.

Expected accepted record plus quarantine status resolved.

### RP-003 — Replay lineage

Expected new run ID but same source artifact/batch lineage.

## 12. Failure/recovery tests

### FR-001 — Collector transient failure

Simulate two transient failures then success.

Expected bounded retry/backoff and one final immutable artifact.

### FR-002 — Collector authentication failure

Expected no repeated blind retries; explicit auth failure.

### FR-003 — Bronze succeeds, Silver fails

Expected Bronze retained; Silver consistent; watermark unchanged; replay succeeds after fix.

### FR-004 — Audit includes failure

Expected terminal failure stage, error class/message, start/end.

### FR-005 — Partial valid batch

Mix valid and record-level invalid rows.

Expected valid rows processed if batch structural contract remains valid; invalid rows quarantined; audit `PARTIAL` or warning status.

## 13. Schema-drift tests

### SD-001 — Additive

Expected log + continue compatible path.

### SD-002 — Parseable representation drift

Expected explicit normalization + log.

### SD-003 — incompatible type

Expected quarantine affected record.

### SD-004 — breaking removal

Expected stage failure, no Silver corruption, no watermark advance.

### SD-005 — attempted blind schema evolution

Test configuration does not enable global automatic evolution.

Expected schema remains contract-controlled.

## 14. Reconciliation tests

### RC-001 — Source accounting

Verify:

```text
source = accepted/new + duplicate + quarantined/rejected
```

### RC-002 — Qubit accounting

Expected qubit entries equal accepted + quarantined.

### RC-003 — Gate accounting

Eligible 2Q gate entries equal accepted + quarantined/skipped reason.

### RC-004 — NOAA accounting

Parsed source observations equal accepted + duplicate + quarantine.

### RC-005 — unexplained loss

Remove one candidate from accounting.

Expected reconciliation failure.

## 15. Audit tests

### AU-001 — required metrics present

For successful run, assert non-null:

```text
run_id
pipeline_name
layer
stage
source_system
execution_mode
start_time
end_time
status
rows_read
rows_inserted
rows_updated
rows_quarantined
pipeline_version
schema_version
```

### AU-002 — no secret leakage

Inject recognizable fake token into runtime secret.

Expected token absent from logs/artifacts/audit.

### AU-003 — watermark before/after

Incremental audit stores both values.

### AU-004 — unchanged count

Exact rerun records unchanged/duplicate metrics without changing target.

## 16. Security tests

### SEC-001 — `.gitignore`

Verify `.env`, secret configs/token caches excluded.

### SEC-002 — repository secret scan

No known token pattern in tracked files.

### SEC-003 — source URI sanitization

Credential-bearing test query string/header must not persist.

## 17. Research-readiness tests

### RR-001 — temporal grain preserved

Two calibrations for same qubit remain two distinct temporal records.

### RR-002 — gate endpoints preserved

Ordered source/target and gate type survive normalization.

### RR-003 — environment source grain preserved

No forced calibration-time aggregation.

### RR-004 — no composite score substitution

Raw Silver reliability variables remain present; no Phase 2 transformation replaces them with only one score.

### RR-005 — no temporal leakage feature creation

Phase 2 Silver does not create features using future calibration values.

## 18. Acceptance suite

Phase 2 acceptance requires:

- all unit tests pass;
- all explicit schema tests pass;
- integration full/incremental/backfill/replay pass;
- idempotency tests pass;
- source revision behavior passes;
- failure recovery passes;
- schema drift passes;
- reconciliation passes;
- security checks pass;
- golden-path Databricks demonstration evidence captured.

## 19. Test evidence

Capture:

- command/notebook run ID;
- test summary;
- failing test details if any;
- target business-key duplicate checks;
- before/after counts;
- deterministic content hash for idempotency;
- audit rows;
- quarantine/drift evidence.

Do not mark a checklist complete from code inspection alone when the requirement calls for runtime behavior.
