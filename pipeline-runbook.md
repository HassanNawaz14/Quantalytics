# Phase 2 Pipeline Runbook

## 1. Purpose

This runbook defines how to operate, recover, replay, validate and demonstrate the Quantalytics Phase 2 pipeline. It is written for developers, evaluators and coding agents.

The runbook assumes the architecture and schemas in:

- `../../README.md`
- `../architecture/phase2-architecture.md`
- `../data_contracts/bronze-silver-data-contract.md`

## 2. Operating principles

1. External collection is separate from Databricks/Spark transformation.
2. Immutable raw artifacts are the replay source.
3. Spark reads raw artifacts with explicit schemas.
4. Bronze is preservation-oriented.
5. Silver is normalized and contract-enforced.
6. No watermark advances until the corresponding analytical write succeeds.
7. Same input can be run repeatedly without analytical duplication.
8. A quarantined record remains traceable and replayable.
9. Failed runs do not require re-downloading already captured data.
10. No operator fixes a problem by editing historical Silver rows manually.

## 3. Run initialization

Every Spark execution generates a new `run_id`.

Every collector invocation generates or receives a `batch_id`.

Required run context:

```text
run_id
batch_id
run_mode
source_system
input_path
pipeline_version
schema_version
configuration snapshot/reference
operator/job identity where available
start_time
```

At initialization:

1. validate runtime parameters;
2. resolve table/location names through configuration;
3. verify target tables/contracts exist or initialize them in setup mode;
4. read current watermark if relevant;
5. insert `RUNNING` audit event;
6. never log credential values.

## 4. External collection

### 4.1 IBM full/current collection

Purpose: capture one or more current backend property snapshots.

Required parameters:

```text
backend_list
batch_id
output_root
collector_version
```

Procedure:

1. resolve IBM token from environment/secret manager;
2. validate backend identifier;
3. request backend properties;
4. capture retrieval timestamp in UTC;
5. retain the exact response body;
6. extract/record source calibration timestamp if available;
7. calculate SHA-256;
8. create deterministic artifact ID;
9. write immutable artifact/envelope;
10. write batch manifest;
11. return per-backend status.

Acceptance:

- payload file exists;
- payload hash verifies;
- source metadata exists;
- no token appears in artifact/log;
- a collector retry does not overwrite an existing immutable artifact with different bytes.

### 4.2 IBM historical backfill collection

Parameters:

```text
backend
start_timestamp
end_timestamp
batch_id
output_root
```

Algorithm:

```text
cursor = end_timestamp

while cursor > start_timestamp:
    request properties using updated_before=cursor

    if no historical record:
        record gap/no-record result
        stop or move according to verified API behavior

    persist returned payload before moving cursor

    returned_time = payload last_update_date

    if returned_time < start_timestamp:
        stop

    cursor = strictly earlier than returned_time
```

`VALIDATION REQUIRED`: implement the exact cursor step only after confirming live API return/pagination behavior. Do not assume multiple results per call if the API actually returns one snapshot.

Failure rules:

- authentication/permission -> fail collection item; no endless retry;
- rate limiting/transient network -> bounded exponential backoff;
- invalid backend -> item failure;
- malformed JSON -> preserve response body separately as failed artifact evidence and fail item;
- successfully collected earlier snapshots remain preserved if a later call fails.

### 4.3 NOAA collection

Required observational products:

```text
planetary_kp
f10_7_flux
```

Procedure:

1. fetch configured product;
2. record collection timestamp;
3. preserve full source bytes;
4. validate source JSON container shape;
5. calculate whole-artifact hash;
6. create one immutable artifact;
7. optionally emit record-level envelopes referencing the parent artifact;
8. do not align source times to IBM.

Missing NOAA observations are treated as source gaps:

- do not create an interpolated/synthetic Phase 2 observation;
- record the gap through reconciliation/audit when an expected cadence can be established;
- preserve later-arriving observations normally through stable-key incremental processing;
- leave interpolation/window methodology to Phase 3 research.

## 5. Full-load mode

### Purpose

Create the initial Bronze/Silver baseline from supplied/frozen sample artifacts or an approved baseline collection.

### Inputs

```text
run_mode=full
source_system
input_path
batch_id
```

### Procedure

1. start audit;
2. enumerate input artifacts deterministically;
3. apply explicit envelope schema;
4. validate structural contract;
5. verify hashes;
6. detect schema drift;
7. persist Bronze candidates using immutable merge key;
8. parse source payload with explicit source schema;
9. build Silver candidates;
10. execute DQ checks;
11. quarantine failures;
12. deduplicate Silver business keys;
13. merge accepted Silver rows;
14. reconcile;
15. initialize/advance relevant watermark only after successful analytical commit;
16. close audit.

### Failure behavior

- invalid whole-file structural contract -> affected file rejected; no unsafe parse;
- record-level invalidity -> quarantine when isolation is possible;
- Silver merge failure -> watermark unchanged;
- reconciliation failure -> run status according to configured severity, normally `PARTIAL` or `FAILURE`.

### Acceptance

- expected Bronze/Silver tables populated;
- no duplicate business keys;
- audit row(s) complete;
- source-to-Silver lineage demonstrated;
- watermark reflects only committed data.

## 6. Incremental mode

### Purpose

Process newly arrived or potentially revised data without assuming perfect source delivery.

### Discovery

For each logical stream:

```text
previous_watermark = read watermark
candidate_start = previous_watermark - overlap_window
```

Use overlap when collecting/discovering candidate artifacts.

### Processing

1. discover candidate artifacts;
2. deduplicate immutable artifacts;
3. Bronze merge;
4. normalize Silver;
5. route source-revision conflicts;
6. merge new/approved updates;
7. reconcile;
8. compute new watermark from successfully committed source timestamps;
9. update watermark atomically after success;
10. close audit.

### Watermark advancement

Allowed:

```text
watermark_after >= watermark_before
```

only when corresponding target state is committed.

Not allowed:

- advance on validation-only;
- advance after failed target merge;
- advance because a source was merely downloaded;
- advance normal incremental watermark because a historical backfill ran.

### Incremental acceptance

- only genuinely new/approved revised logical records alter Silver;
- exact repeats are unchanged;
- watermark does not skip failed candidate data;
- overlap duplicates do not duplicate target rows.

## 7. Backfill mode

### Purpose

Process an arbitrary historical interval or explicit file set without editing source code.

### Required parameters

At least one of:

```text
start_timestamp + end_timestamp
input_path/file list
```

and:

```text
backend/source
batch_id
```

### Policy

Backfill is logically isolated from normal incremental state.

Default:

```text
update_normal_watermark = false
```

Procedure:

1. collect/freeze historical artifacts externally if needed;
2. run explicit-schema validation;
3. Bronze merge by immutable key;
4. Silver transform/merge by business key;
5. apply normal revision controls;
6. reconcile requested interval;
7. record audit mode `backfill`;
8. leave normal watermark unchanged.

### Rerun

Same backfill twice:

```text
Run 1 -> inserts historical logical records
Run 2 -> zero duplicate logical rows
```

If a different payload appears for an existing historical business key, it follows source-revision policy.

### Backfill acceptance

- arbitrary requested interval can be supplied without code changes;
- historical rows do not corrupt later records;
- rerun is idempotent;
- normal incremental watermark remains correct.

## 8. Replay mode

### Purpose

Prove deterministic behavior or recover from fixed transformation logic using already captured raw input.

### Parameters

```text
run_mode=replay
source_artifact_id or batch_id
input_path
```

### Procedure

1. locate immutable raw artifact(s);
2. verify stored/recomputed hash;
3. create a new `run_id`;
4. run the same schema/validation/transformation contracts;
5. perform normal idempotent merges;
6. compare accepted analytical content with prior expected state;
7. record replay audit;
8. do not call external source;
9. do not advance normal watermark by default.

### Acceptance

- source bytes are unchanged;
- exact valid replay changes no logical analytical content;
- if code was corrected, only documented/approved data changes occur;
- replay is traceable to original batch and new run.

## 9. Validation-only mode

### Purpose

Inspect source schema and quality without mutating analytical tables.

### Allowed writes

- execution/audit log;
- schema drift diagnostic log;
- optional quarantine diagnostic rows clearly marked as validation-only.

### Prohibited writes

- Bronze;
- Silver;
- normal watermark;
- accepted source-revision state.

### Acceptance

Before/after hashes or counts of Bronze/Silver/watermark are identical.

## 10. Bronze operational flow

```text
Read staged artifact
 -> apply RAW_ARTIFACT_ENVELOPE_SCHEMA
 -> verify required metadata
 -> verify SHA-256
 -> parse source event timestamp
 -> calculate snapshot_business_key
 -> calculate record_key
 -> deduplicate candidate record_key
 -> MERGE insert-only
 -> audit
```

If the same raw payload is supplied again, `rows_inserted=0` for Bronze.

## 11. Silver operational flow

```text
Read accepted Bronze payload version
 -> explicit payload parse
 -> explode/normalize
 -> strict casts
 -> unit conversions
 -> business-key generation
 -> DQ
 -> candidate-key dedup
 -> source-revision resolution
 -> MERGE
 -> reconciliation
 -> audit
```

## 12. Source revision handling

A source revision is:

```text
same Silver business key
AND source_payload_hash changed
```

Default action:

1. keep both raw versions in Bronze;
2. write revision/drift/audit event;
3. quarantine new Silver candidate under `SOURCE_REVISION_REQUIRES_REVIEW`;
4. keep current Silver scientific values unchanged.

Controlled update is permitted only when:

```text
allow_source_revision_update=true
```

and:

- operator/reconciliation context is explicit;
- new candidate passes all current validations;
- update is logged;
- prior raw version remains replayable;
- rows updated are counted.

## 13. Schema drift operations

### Additive drift

Example: IBM adds a new optional key.

Operator action:

- verify required structure still exists;
- Bronze continues;
- drift log receives event;
- do not promote field to Silver until contract/version is updated.

### Parseable type representation

Example: numeric value serialized as a string.

Only accept if transformation explicitly recognizes and validates it. Record normalization. Do not enable generic coercion that hides type drift.

### Breaking drift

Procedure:

1. fail affected stage;
2. retain raw;
3. write drift event and failure audit;
4. do not mutate target Silver;
5. do not advance watermark;
6. update schema/transform under code review;
7. increment schema version if contract changed;
8. replay raw artifact.

## 14. Quarantine operations

### Common categories

```text
HASH_MISMATCH
MISSING_REQUIRED_FIELD
TIMESTAMP_PARSE_FAILURE
CAST_FAILURE
VALUE_RANGE_FAILURE
REFERENTIAL_FAILURE
CONFLICTING_DUPLICATE
SOURCE_REVISION_REQUIRES_REVIEW
UNSUPPORTED_GATE_ARITY_FOR_PHASE2_SILVER
BREAKING_SCHEMA_DRIFT
```

### Remediation

1. inspect `raw_record`, source file and failure category;
2. fix parser/config/source contract, not historical Silver manually;
3. mark quarantine item for replay;
4. execute replay with new run ID;
5. if accepted, update `replay_status=RESOLVED` and `resolved_run_id`;
6. if intentionally ignored, store reason.

## 15. Audit inspection

Typical queries:

```sql
SELECT *
FROM pipeline_execution_logs
WHERE run_id = '<run-id>'
ORDER BY start_time, layer, stage;
```

```sql
SELECT status,
       SUM(rows_read) AS rows_read,
       SUM(rows_inserted) AS inserted,
       SUM(rows_updated) AS updated,
       SUM(rows_quarantined) AS quarantined
FROM pipeline_execution_logs
WHERE run_id = '<run-id>'
GROUP BY status;
```

```sql
SELECT *
FROM pipeline_schema_drift_logs
WHERE run_id = '<run-id>';
```

```sql
SELECT *
FROM quarantine_records
WHERE run_id = '<run-id>'
ORDER BY quarantined_at;
```

Examples are query templates; actual catalog/schema qualification comes from configuration.

## 16. Reconciliation procedure

After each analytical run:

### 16.1 Source-level

```text
source_records
= newly_inserted_bronze
+ exact_duplicates
+ quarantined/rejected
```

### 16.2 IBM qubits

For each accepted calibration:

```text
payload qubit count
= accepted Silver qubit rows
+ quarantined qubit rows
```

### 16.3 Gates

For eligible two-qubit gate entries:

```text
eligible source entries
= accepted Silver gate rows
+ quarantine/skip-with-reason entries
```

### 16.4 Environment

For each product artifact:

```text
parsed source observations
= accepted Silver environment observations
+ quarantined observations
+ exact duplicates
```

Unexplained differences fail reconciliation.

## 17. Recovery scenarios

### Scenario A — Spark fails before Bronze merge

- raw artifact remains;
- audit failure;
- no watermark change;
- rerun same artifact.

### Scenario B — Bronze succeeds, Silver fails

- Bronze remains valid;
- Silver is unchanged/transactionally consistent;
- watermark remains unadvanced;
- fix transformation;
- replay from Bronze/raw.

### Scenario C — Silver succeeds, audit-finalization fails

- inspect Delta target and transaction history;
- do not blindly rerun with altered assumptions;
- replay is safe because merge is idempotent;
- reconstruct/repair operational audit with explicit recovery event.

### Scenario D — watermark incorrectly advanced

Treat as correctness incident:

1. identify last successfully committed source event;
2. reset watermark to that value under controlled operator action;
3. record correction in audit;
4. rerun with overlap;
5. confirm reconciliation.

### Scenario E — source artifact corrupted

- recompute hash;
- if mismatch, do not process;
- use immutable backup/frozen source artifact;
- if no valid copy exists, recollect only when source can reproduce the exact record and document provenance difference.

## 18. Retry policy

External collector:

- exponential backoff;
- bounded attempts;
- jitter recommended;
- obey source retry/rate-limit hints where exposed.

No infinite retry.

Spark:

- normal job/task retries are acceptable;
- application-level merge remains idempotent;
- repeated task execution must not create duplicate analytical rows.

## 19. Golden-path academic demonstration

Perform in this order:

```text
1. Show repository tree
2. Show explicit schemas
3. Provide/freeze Full Load samples
4. Run validation-only
5. Run Full Load
6. Inspect Bronze
7. Inspect Silver
8. Inspect audit
9. Record baseline row counts/content hashes
10. Provide Incremental Load sample
11. Run incremental
12. Show new accepted rows
13. Re-run same incremental
14. Show zero logical duplicates / zero unchanged-record updates
15. Run a historical backfill
16. Re-run same backfill
17. Confirm idempotency
18. Introduce an additive drift fixture
19. Show drift log with processing continuing
20. Introduce incompatible type/breaking fixture
21. Show quarantine/failure behavior
22. Remediate and replay quarantined record
23. Show watermark correctness
24. Run reconciliation
25. Run test suite
26. Commit evidence and documentation
```

Do not fabricate expected row numbers. Capture actual outputs after implementation.

## 20. Pre-run checklist

- [ ] config file selected;
- [ ] secrets injected;
- [ ] source list/date range checked;
- [ ] input path immutable/frozen;
- [ ] batch ID known;
- [ ] schema version correct;
- [ ] pipeline version correct;
- [ ] target tables accessible;
- [ ] enough quota available for demonstration;
- [ ] no "today" hard-coded in paths.

## 21. Post-run checklist

- [ ] run status terminal;
- [ ] audit start/end populated;
- [ ] insert/update/quarantine counts present;
- [ ] no unexplained duplicate keys;
- [ ] reconciliation passed/understood;
- [ ] watermark changed only if allowed;
- [ ] source revision conflicts reviewed;
- [ ] no secrets in output/logs;
- [ ] evidence captured where required.

## 22. Runbook acceptance criteria

The pipeline is operationally ready when a developer can execute every supported mode from parameters, recover from a failed Silver transformation without redownloading raw source data, explain every rejected record through audit/quarantine, and prove that identical reruns do not alter analytical content.
