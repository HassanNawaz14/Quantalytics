# Bronze–Silver Data Contract

## 1. Purpose

This document is the executable data contract for Quantalytics Phase 2. Coding agents must implement these grains, columns, types, nullability rules, keys, transformations, validations, merge semantics and schema-drift behavior unless an explicit architecture change is approved.

All Spark schemas are explicit. Schema inference is prohibited.

## 2. Global conventions

### 2.1 Types

Use Spark/Delta types:

- `STRING`
- `INT`
- `LONG`
- `DOUBLE`
- `BOOLEAN`
- `TIMESTAMP`

Complex vendor JSON is initially preserved as `STRING` in the raw artifact envelope and parsed with an explicit nested schema.

### 2.2 Timestamps

- persisted analytical timestamps are UTC;
- raw timestamp strings are preserved where needed;
- timestamp parse failure is observable;
- `load_timestamp` is record materialization time, not source event identity;
- seconds or finer source precision is preserved as supported by Spark `TIMESTAMP`.

### 2.3 Hashes and deterministic keys

Hash function: SHA-256 over canonical UTF-8 concatenation with an unambiguous delimiter and explicit null sentinel.

Conceptual helper:

```text
stable_hash(parts...) =
SHA256(
  canonical(part_1) + "|" +
  canonical(part_2) + "|" + ...
)
```

Canonical timestamps use UTC ISO-8601 representation.

### 2.4 Naming decision

Phase 1 labels `T1_us`, `T2_us`, `CZ_error`, `RZZ_error` are represented physically in snake case:

- `t1_us`
- `t2_us`
- `cz_error`
- `rzz_error`

This is an explicit naming normalization, not a semantic change.

## 3. Collector raw-envelope schemas

### 3.1 IBM collector envelope

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `source_system` | STRING | no | Constant `IBM`. |
| `source_artifact_id` | STRING | no | Deterministic artifact identifier/filename logical ID. |
| `source_uri` | STRING | no | Non-secret logical REST/resource reference. |
| `backend_id` | STRING | no | IBM backend identifier. |
| `collection_timestamp_raw` | STRING | no | Exact collector retrieval timestamp text. |
| `raw_calibration_timestamp` | STRING | yes | Source calibration/last-update timestamp text if available. |
| `calibration_id` | STRING | yes | IBM calibration ID if supplied/known. |
| `observation_type` | STRING | yes | Null for IBM in Phase 2. |
| `raw_payload` | STRING | no | Exact JSON response text. |
| `source_payload_hash` | STRING | no | SHA-256 of exact payload bytes/text under collector canonicalization contract. |
| `batch_id` | STRING | no | Collector batch identity. |
| `collector_version` | STRING | no | Collector release/version. |
| `source_schema_fingerprint` | STRING | yes | Optional hash of observed structural signature/top-level key set. |

### 3.2 NOAA collector envelope

Same shared envelope; differences:

- `source_system = NOAA_SWPC`;
- `backend_id = null`;
- `raw_calibration_timestamp = null`;
- `observation_type` is required (`planetary_kp` or `f10_7_flux` for current Phase 2);
- `collection_timestamp_raw` is required;
- raw payload is preserved exactly.

## 4. Explicit Spark source schemas

### 4.1 Artifact envelope schema

```python
from pyspark.sql.types import (
    StructType, StructField, StringType
)

RAW_ARTIFACT_ENVELOPE_SCHEMA = StructType([
    StructField("source_system", StringType(), False),
    StructField("source_artifact_id", StringType(), False),
    StructField("source_uri", StringType(), False),
    StructField("backend_id", StringType(), True),
    StructField("collection_timestamp_raw", StringType(), False),
    StructField("raw_calibration_timestamp", StringType(), True),
    StructField("calibration_id", StringType(), True),
    StructField("observation_type", StringType(), True),
    StructField("raw_payload", StringType(), False),
    StructField("source_payload_hash", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("collector_version", StringType(), False),
    StructField("source_schema_fingerprint", StringType(), True),
])
```

Never call `.option("inferSchema", "true")`.

### 4.2 IBM backend-properties payload schema

IBM's documented backend-properties object is modeled with name/date/unit/value property objects and gate objects.

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)

IBM_NDUV_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("name", StringType(), False),
    StructField("unit", StringType(), True),
    StructField("value", DoubleType(), True),
])

IBM_GATE_PROPERTIES_SCHEMA = StructType([
    StructField("qubits", ArrayType(IntegerType(), containsNull=False), False),
    StructField("gate", StringType(), False),
    StructField("parameters", ArrayType(IBM_NDUV_SCHEMA, containsNull=False), True),
])

IBM_BACKEND_PROPERTIES_SCHEMA = StructType([
    StructField("backend_name", StringType(), True),
    StructField("backend_version", StringType(), True),
    StructField("last_update_date", StringType(), True),
    StructField(
        "qubits",
        ArrayType(
            ArrayType(IBM_NDUV_SCHEMA, containsNull=False),
            containsNull=False
        ),
        True
    ),
    StructField(
        "gates",
        ArrayType(IBM_GATE_PROPERTIES_SCHEMA, containsNull=False),
        True
    ),
    StructField(
        "general",
        ArrayType(IBM_NDUV_SCHEMA, containsNull=False),
        True
    ),
])
```

`VALIDATION REQUIRED`: compare the frozen project IBM payloads to this contract before productionizing. Additive vendor keys remain in `raw_payload` and are logged as drift even if Spark's `from_json` ignores them.

### 4.3 NOAA current observational schemas

Current source examples verified 2026-09-30:

```python
NOAA_KP_SOURCE_SCHEMA = StructType([
    StructField("time_tag", StringType(), False),
    StructField("Kp", DoubleType(), False),
    StructField("a_running", IntegerType(), True),
    StructField("station_count", IntegerType(), True),
])

NOAA_F107_SOURCE_SCHEMA = StructType([
    StructField("time_tag", StringType(), False),
    StructField("flux", DoubleType(), False),
])
```

If the collector stores one full array payload per artifact, parse using:

```python
ArrayType(NOAA_KP_SOURCE_SCHEMA, containsNull=False)
ArrayType(NOAA_F107_SOURCE_SCHEMA, containsNull=False)
```

Do not infer schema from the downloaded JSON.

## 5. Bronze: `bronze_ibm_backend_properties`

### 5.1 Purpose and grain

One row = one immutable payload version for one IBM backend calibration/property snapshot.

A single logical snapshot may have more than one Bronze row only when the source payload hash actually differs.

### 5.2 Keys

```text
snapshot_business_key =
SHA256(backend_id | calibration_timestamp_utc)

record_key =
SHA256(snapshot_business_key | source_payload_hash)
```

Bronze merge key: `record_key`.

### 5.3 Schema

| Column | Type | Nullable | Unit | Description | Source | Transformation | Validation / key role |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Immutable payload-version identity. | derived | SHA-256 | PK/merge key; unique |
| `snapshot_business_key` | STRING | no | — | Logical backend/calibration identity. | derived | SHA-256 | revision grouping key |
| `backend_id` | STRING | no | — | IBM backend. | envelope / payload | trim, compare sources | non-empty |
| `collection_timestamp` | TIMESTAMP | no | UTC | Collector retrieval time. | envelope | strict UTC parse | parse required |
| `raw_calibration_timestamp` | STRING | no | source text | Exact source calibration timestamp representation. | envelope/payload | preserve | non-empty |
| `calibration_timestamp_utc` | TIMESTAMP | no | UTC | Parsed source calibration time. | raw timestamp | strict parse | business identity |
| `calibration_id` | STRING | yes | — | IBM calibration identifier if available. | envelope | preserve | optional |
| `raw_payload` | STRING | no | — | Exact backend-properties JSON. | source | preserve | non-empty |
| `source` | STRING | no | — | Source identifier, e.g. `IBM_QUANTUM_COMPUTE`. | config/envelope | standardized constant | enum/configured |
| `source_uri` | STRING | no | — | Logical source reference with no secrets. | envelope | preserve | no credentials |
| `source_artifact_id` | STRING | no | — | Immutable artifact ID. | envelope | preserve | non-empty |
| `source_file_name` | STRING | no | — | Staged file name. | Spark input metadata | derive | non-empty |
| `source_payload_hash` | STRING | no | — | SHA-256 payload digest. | collector | recompute/verify | 64 hex chars |
| `source_schema_fingerprint` | STRING | yes | — | Structural fingerprint. | collector | preserve | optional |
| `batch_id` | STRING | no | — | Collector batch. | envelope | preserve | lineage |
| `run_id` | STRING | no | — | Spark execution. | runtime | assign | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Bronze materialization time. | runtime | current UTC once on insert | not a key |
| `collector_version` | STRING | no | — | Collector release. | envelope | preserve | non-empty |
| `pipeline_version` | STRING | no | — | Spark release. | config | assign | non-empty |
| `schema_version` | STRING | no | — | Bronze/Silver contract version. | config | assign | non-empty |

### 5.4 Merge behavior

- `record_key` match -> unchanged; do not update load/run timestamps.
- no `record_key` match -> insert.
- same `snapshot_business_key`, new payload hash -> insert new Bronze version + source-revision event.
- duplicate candidate `record_key` inside one run -> collapse identical records; conflicting duplicates fail validation.

## 6. Bronze: `bronze_noaa_observation`

### 6.1 Purpose and grain

One row = one immutable source observation payload version at a source observation type/time.

If the raw artifact contains an array, the Bronze implementation may either:
1. preserve one row per original artifact plus a child observation table, or
2. explode to one observation row while retaining exact full artifact reference.

For Phase 2, use option 2 **only if** `raw_payload` for each exploded observation preserves the exact source record and `source_artifact_id` links back to the frozen full source artifact.

### 6.2 Keys

```text
snapshot_business_key =
SHA256(source | observation_type | observation_timestamp_utc)

record_key =
SHA256(snapshot_business_key | source_payload_hash)
```

### 6.3 Schema

| Column | Type | Nullable | Unit | Description | Source | Transformation | Validation / key role |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Immutable observation payload version. | derived | SHA-256 | PK/merge key |
| `snapshot_business_key` | STRING | no | — | Source/type/time identity. | derived | SHA-256 | revision grouping |
| `observation_timestamp_raw` | STRING | no | source text | Exact NOAA time. | source `time_tag` | preserve | required |
| `observation_timestamp_utc` | TIMESTAMP | no | UTC | Parsed observation time. | raw time | strict parse | business identity |
| `observation_type` | STRING | no | — | `planetary_kp` or `f10_7_flux`. | collector | standardized | supported enum |
| `raw_payload` | STRING | no | — | Exact source observation JSON. | source | preserve | non-empty |
| `source` | STRING | no | — | `NOAA_SWPC`. | config | standardized | required |
| `source_uri` | STRING | no | — | Logical product reference. | envelope | preserve | no secrets |
| `source_artifact_id` | STRING | no | — | Frozen file/artifact identity. | collector | preserve | required |
| `source_file_name` | STRING | no | — | Staged file. | input metadata | derive | required |
| `source_payload_hash` | STRING | no | — | SHA-256 of observation raw JSON under documented canonicalization. | collector/Spark | verify | required |
| `collection_timestamp` | TIMESTAMP | no | UTC | Retrieval time. | envelope | strict parse | required |
| `batch_id` | STRING | no | — | Collector batch. | envelope | preserve | lineage |
| `run_id` | STRING | no | — | Spark execution. | runtime | assign | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Bronze insert time. | runtime | assign on insert | not key |
| `collector_version` | STRING | no | — | Collector version. | envelope | preserve | required |
| `pipeline_version` | STRING | no | — | Spark version. | config | assign | required |
| `schema_version` | STRING | no | — | Data-contract version. | config | assign | required |

## 7. Silver: `silver_backend`

### 7.1 Grain

One row per `backend_id + calibration_timestamp`.

### 7.2 Business key

```text
backend_id + calibration_timestamp
```

### 7.3 Schema

| Column | Type | Nullable | Unit | Description | Source mapping | Transformation | Validation / key |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Hash of Silver business key. | derived | SHA-256 | unique |
| `backend_id` | STRING | no | — | IBM backend ID. | Bronze/backend payload | standardized | PK part |
| `processor_family` | STRING | yes | — | Processor family/generation if verified metadata supplies it. | backend metadata | direct | do not infer from name |
| `backend_version` | STRING | yes | — | Backend version if supplied. | payload `backend_version` | direct | optional |
| `qubit_count` | INT | no | qubits | Number of physical qubit entries represented. | payload `qubits` | `size(qubits)` | > 0 |
| `calibration_timestamp` | TIMESTAMP | no | UTC | Source calibration state time. | Bronze | direct | PK part |
| `collection_timestamp` | TIMESTAMP | no | UTC | Retrieval time. | Bronze | direct | required |
| `calibration_id` | STRING | yes | — | Source calibration ID if available. | Bronze | direct | optional |
| `source_payload_hash` | STRING | no | — | Source payload version. | Bronze | direct | revision detection |
| `batch_id` | STRING | no | — | Source batch. | Bronze | direct | lineage |
| `run_id` | STRING | no | — | Run that materialized accepted Silver version. | runtime | direct | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Accepted Silver version materialization time. | runtime | assign on insert/update | not key |
| `pipeline_version` | STRING | no | — | Transformation version. | config | direct | required |
| `schema_version` | STRING | no | — | Contract version. | config | direct | required |

## 8. Silver: `silver_qubit_calibration`

### 8.1 Grain

One physical qubit for one backend calibration timestamp.

### 8.2 Business key

```text
backend_id + calibration_timestamp + qubit_id
```

### 8.3 Schema

| Column | Type | Nullable | Unit | Description | Source mapping | Transformation | Validation / key |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Hash of business key. | derived | SHA-256 | unique |
| `backend_id` | STRING | no | — | Backend. | Bronze | direct | PK part |
| `calibration_timestamp` | TIMESTAMP | no | UTC | Calibration time. | Bronze | direct | PK part |
| `qubit_id` | INT | no | index | Physical qubit index. | position in `qubits` array / verified source ID | explicit index | PK part, >= 0 |
| `t1_us` | DOUBLE | yes | µs | Energy relaxation time. | qubit property `T1` where present | source unit -> microseconds | > 0 when present |
| `t2_us` | DOUBLE | yes | µs | Coherence/dephasing time. | qubit property `T2` where present | source unit -> microseconds | > 0 when present |
| `readout_error` | DOUBLE | yes | fraction | Readout error probability. | `readout_error` | strict double | 0..1 when present |
| `init_error` | DOUBLE | yes | fraction | Initialization error if source provides it. | `init_error` | strict double | 0..1 when present |
| `measurement_error` | DOUBLE | yes | fraction | Distinct measurement/instruction error only if separately provided. | source-specific | strict mapping | never alias silently to readout error |
| `single_gate_error` | DOUBLE | yes | fraction | Error for configured canonical 1Q instruction. | gate properties / target | deterministic configured selection | 0..1 |
| `single_gate_error_type` | STRING | yes | — | Instruction used for `single_gate_error`. | gate name | direct | must accompany error |
| `operational` | BOOLEAN | yes | — | Qubit operational status where source supports it. | backend property logic/source | direct/derived | nullable |
| `source_payload_hash` | STRING | no | — | IBM payload version. | Bronze | direct | revision detection |
| `batch_id` | STRING | no | — | Batch. | Bronze | direct | lineage |
| `run_id` | STRING | no | — | Accepted version run. | runtime | direct | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Silver materialization time. | runtime | insert/update only | not key |
| `pipeline_version` | STRING | no | — | Transform version. | config | direct | required |
| `schema_version` | STRING | no | — | Contract version. | config | direct | required |

### 8.4 Single-gate policy

`OPEN DECISION`: Phase 1 expects a single `single_gate_error`, while modern backends may expose multiple one-qubit instructions.

Implementation rule:

- config contains `canonical_single_qubit_gate`;
- if configured gate exists, populate error/type;
- if no gate is configured or source does not provide it, leave both null and emit warning;
- never choose "lowest error", first array element, or another gate silently.

## 9. Silver: `silver_gate_calibration`

### 9.1 Grain

One calibrated **two-qubit instruction** on one ordered qubit pair for one backend calibration timestamp.

### 9.2 Business key

```text
backend_id
+ calibration_timestamp
+ gate_type
+ source_qubit
+ target_qubit
```

The qubit order is source order; do not sort endpoints.

### 9.3 Schema

| Column | Type | Nullable | Unit | Description | Source mapping | Transformation | Validation / key |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Hash business key. | derived | SHA-256 | unique |
| `backend_id` | STRING | no | — | Backend. | Bronze | direct | PK part |
| `calibration_timestamp` | TIMESTAMP | no | UTC | Calibration time. | Bronze | direct | PK part |
| `gate_type` | STRING | no | — | Source instruction/gate name. | gate `gate` | normalize case only by contract | PK part |
| `source_qubit` | INT | no | index | First ordered endpoint. | gate `qubits[0]` | direct | PK part, >=0 |
| `target_qubit` | INT | no | index | Second ordered endpoint. | gate `qubits[1]` | direct | PK part, >=0 |
| `gate_error` | DOUBLE | yes | fraction | Generic error for this gate. | parameter named gate error / supported API | strict mapping | 0..1 |
| `cz_error` | DOUBLE | yes | fraction | Phase 1 CZ-specific error. | `gate_error` when gate type is CZ | conditional copy | null unless CZ |
| `rzz_error` | DOUBLE | yes | fraction | Phase 1 RZZ-specific error. | `gate_error` when gate type is RZZ | conditional copy | null unless RZZ |
| `gate_length_ns` | DOUBLE | yes | ns | Gate duration. | gate-length parameter/API | convert source seconds to ns where applicable | >0 |
| `operational` | BOOLEAN | yes | — | Gate operational status if source supports it. | source/API | direct/derived | optional |
| `source_payload_hash` | STRING | no | — | IBM payload version. | Bronze | direct | revision detection |
| `batch_id` | STRING | no | — | Batch. | Bronze | direct | lineage |
| `run_id` | STRING | no | — | Accepted version run. | runtime | direct | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Accepted version time. | runtime | insert/update only | not key |
| `pipeline_version` | STRING | no | — | Transform version. | config | direct | required |
| `schema_version` | STRING | no | — | Contract version. | config | direct | required |

A gate payload whose `qubits` length is not exactly 2 is preserved in Bronze and logged as `UNSUPPORTED_GATE_ARITY_FOR_PHASE2_SILVER`; it is not squeezed into source/target columns.

## 10. Silver: `silver_environment`

### 10.1 Grain

One source environmental observation type at one observation timestamp.

### 10.2 Business key

```text
source + observation_type + observation_timestamp
```

### 10.3 Schema

| Column | Type | Nullable | Unit | Description | Source mapping | Transformation | Validation / key |
|---|---|---:|---|---|---|---|---|
| `record_key` | STRING | no | — | Hash business key. | derived | SHA-256 | unique |
| `source` | STRING | no | — | `NOAA_SWPC`. | Bronze | direct | PK part |
| `observation_type` | STRING | no | — | `planetary_kp` / `f10_7_flux`. | Bronze | direct | PK part |
| `observation_timestamp` | TIMESTAMP | no | UTC | NOAA source time. | `time_tag` | strict parse | PK part |
| `kp_index` | DOUBLE | yes | index | Planetary Kp value. | Kp `Kp` | strict double | required for Kp row |
| `solar_flux` | DOUBLE | yes | sfu | F10.7 solar radio flux. | F10.7 `flux` | strict double | required for F10.7 row |
| `source_unit` | STRING | no | — | `index` or `sfu`. | contract | derived by observation type | required |
| `source_payload_hash` | STRING | no | — | Source observation payload version. | Bronze | direct | revision detection |
| `batch_id` | STRING | no | — | Batch. | Bronze | direct | lineage |
| `run_id` | STRING | no | — | Accepted version run. | runtime | direct | lineage |
| `load_timestamp` | TIMESTAMP | no | UTC | Silver accepted version time. | runtime | insert/update only | not key |
| `pipeline_version` | STRING | no | — | Transform version. | config | direct | required |
| `schema_version` | STRING | no | — | Contract version. | config | direct | required |

Cross-field rules:

```text
planetary_kp -> kp_index non-null AND solar_flux null
f10_7_flux  -> solar_flux non-null AND kp_index null
```

No Phase 2 temporal join between them.

## 11. Casting and normalization contract

### 11.1 Timestamp parse

Allowed parse formats must be explicitly enumerated in code. Parse returns:

- parsed UTC timestamp;
- parse-status flag;
- parse-error category.

A source value that is non-null but fails parsing is **not** converted to a silently accepted null.

### 11.2 Numeric casts

Use a two-step pattern:

```text
raw value present?
    -> attempt cast
    -> if cast succeeds: validate domain
    -> if cast fails: CAST_FAILURE quarantine
```

Legitimate source null and invalid cast are separate states.

### 11.3 Units

- IBM T1/T2 returned in seconds by supported APIs are converted to microseconds for Silver.
- IBM gate lengths returned in seconds are converted to nanoseconds for Silver.
- NOAA F10.7 stored as solar flux units (`sfu`).
- Kp is dimensionless/index-like.

`VALIDATION REQUIRED`: inspect source property `unit` before converting. Do not apply a seconds conversion if an artifact already contains another unit.

## 12. Data-quality contract

| Rule ID | Condition | Severity | Action | Logging | Quarantine |
|---|---|---|---|---|---|
| DQ-001 | required envelope column missing | ERROR/BATCH | reject file/batch stage | audit + drift | file/batch reference |
| DQ-002 | payload hash mismatch | ERROR | quarantine record; do not Bronze insert | audit | yes |
| DQ-003 | backend ID null/empty for IBM | ERROR | quarantine | audit | yes |
| DQ-004 | calibration timestamp cannot parse | ERROR | quarantine | audit | yes |
| DQ-005 | observation timestamp cannot parse | ERROR | quarantine | audit | yes |
| DQ-006 | `qubit_id < 0` | ERROR | quarantine qubit row | audit | yes |
| DQ-007 | T1/T2 present and `<= 0` | ERROR | quarantine affected qubit row | audit | yes |
| DQ-008 | probability/error present outside `[0,1]` | ERROR | quarantine affected row | audit | yes |
| DQ-009 | gate length present and `<= 0` | ERROR | quarantine gate row | audit | yes |
| DQ-010 | gate endpoints not found in same backend/calibration qubit set | ERROR/WARN configurable | quarantine or warning based on source completeness | audit | conditional |
| DQ-011 | observed qubit row count differs from backend `qubit_count` | ERROR/WARN | reconciliation failure/warning | audit | no individual quarantine by default |
| DQ-012 | duplicate candidate business key with identical content | INFO | deterministic collapse | audit duplicate count | no |
| DQ-013 | duplicate candidate key with conflicting content | ERROR | block ambiguous merge | audit | yes |
| DQ-014 | calibration time materially after collection time | WARN/ERROR configurable | warning/quarantine based on clock-skew config | audit | conditional |
| DQ-015 | Kp row has flux populated or F10.7 row has Kp populated | ERROR | quarantine | audit | yes |
| DQ-016 | Silver required business-key field null | ERROR | quarantine | audit | yes |
| DQ-017 | unsupported two-qubit table gate arity | WARN | preserve Bronze, skip Silver gate | audit | optional diagnostic |
| DQ-018 | unexpected additive vendor field | WARN/DRIFT | preserve raw, continue compatible processing | drift log | no |
| DQ-019 | missing required vendor structure | ERROR/BREAKING_DRIFT | fail affected stage | drift/audit | source file retained |
| DQ-020 | source business key exists but payload hash changed | WARN/REVISION | Bronze insert; Silver review by default | audit/drift | Silver candidate yes |
| DQ-021 | observed NOAA timestamp materially after collection time | WARN/ERROR configurable | flag/quarantine using configured clock skew; do not reinterpret as forecast | audit | conditional |
| DQ-022 | expected source observation is absent/gap in source product | WARN/RECONCILIATION | preserve gap; do not interpolate in Phase 2 | audit/reconciliation | no synthetic record |

No "poor qubit" threshold exists in Phase 2.

## 13. Schema drift contract

### 13.1 Additive

Detection: observed key/path not in versioned expected source schema.

Behavior:

1. preserve raw payload;
2. log `ADDITIVE`;
3. continue if required paths remain compatible;
4. do not auto-promote new field to Silver;
5. contract change requires schema version increment + test update.

### 13.2 Type change

If safe and explicitly accepted:

```text
"0.0123" -> validated DOUBLE 0.0123
```

Record normalization event.

If incompatible:

- quarantine affected record;
- preserve raw;
- continue valid records if batch-level contract remains valid.

### 13.3 Breaking change

Examples:

- required `qubits` structure disappears;
- gate object shape becomes incompatible;
- required source event time unavailable.

Behavior:

- fail affected transformation stage;
- no partial Silver corruption;
- watermark unchanged;
- write drift event;
- preserve raw;
- require contract remediation + replay.

### 13.4 Drift log schema

`pipeline_schema_drift_logs`

| Column | Type | Nullable |
|---|---|---:|
| `drift_event_id` | STRING | no |
| `run_id` | STRING | no |
| `batch_id` | STRING | yes |
| `source_system` | STRING | no |
| `source_file` | STRING | no |
| `table_name` | STRING | yes |
| `column_name` | STRING | yes |
| `expected_type` | STRING | yes |
| `observed_type` | STRING | yes |
| `drift_type` | STRING | no |
| `action_taken` | STRING | no |
| `record_count_affected` | LONG | no |
| `event_timestamp` | TIMESTAMP | no |
| `schema_version` | STRING | no |

## 14. Quarantine contract

`quarantine_records`

| Column | Type | Nullable | Purpose |
|---|---|---:|---|
| `quarantine_id` | STRING | no | unique event/record ID |
| `run_id` | STRING | no | execution |
| `batch_id` | STRING | yes | source batch |
| `source_system` | STRING | no | IBM/NOAA |
| `source_file` | STRING | no | input artifact |
| `table_name` | STRING | yes | intended target |
| `record_key` | STRING | yes | candidate key |
| `raw_record` | STRING | no | replayable source/candidate representation |
| `failure_reason` | STRING | no | human-readable reason |
| `failure_category` | STRING | no | stable machine category |
| `quarantined_at` | TIMESTAMP | no | UTC |
| `pipeline_version` | STRING | no | code version |
| `schema_version` | STRING | no | contract version |
| `replay_status` | STRING | no | `PENDING`, `REPLAYED`, `RESOLVED`, `IGNORED_WITH_REASON` |
| `resolved_run_id` | STRING | yes | successful remediation run |

A quarantined record never disappears without terminal status/reason.

## 15. Execution log contract

`pipeline_execution_logs`

| Column | Type | Nullable |
|---|---|---:|
| `run_id` | STRING | no |
| `batch_id` | STRING | yes |
| `pipeline_name` | STRING | no |
| `layer` | STRING | no |
| `stage` | STRING | no |
| `source_system` | STRING | no |
| `execution_mode` | STRING | no |
| `parameter` | STRING | yes |
| `input_artifact` | STRING | yes |
| `start_time` | TIMESTAMP | no |
| `end_time` | TIMESTAMP | yes |
| `duration_seconds` | DOUBLE | yes |
| `status` | STRING | no |
| `rows_read` | LONG | no |
| `rows_inserted` | LONG | no |
| `rows_updated` | LONG | no |
| `rows_unchanged` | LONG | no |
| `rows_duplicates` | LONG | no |
| `rows_rejected` | LONG | no |
| `rows_quarantined` | LONG | no |
| `error_count` | LONG | no |
| `watermark_before` | TIMESTAMP | yes |
| `watermark_after` | TIMESTAMP | yes |
| `source_payload_count` | LONG | yes |
| `data_quality_status` | STRING | yes |
| `error_class` | STRING | yes |
| `error_message` | STRING | yes |
| `pipeline_version` | STRING | no |
| `schema_version` | STRING | no |

Statuses:

```text
RUNNING
SUCCESS
SUCCESS_WITH_WARNINGS
PARTIAL
FAILURE
VALIDATION_ONLY_SUCCESS
```

Audit rows can be stage-level; `run_id + layer + stage + source/input` must be sufficient to inspect execution.

## 16. Watermark state contract

`pipeline_watermarks`

| Column | Type | Nullable | Meaning |
|---|---|---:|---|
| `stream_id` | STRING | no | deterministic source stream identity |
| `source_system` | STRING | no | IBM/NOAA |
| `backend_id` | STRING | yes | IBM stream dimension |
| `observation_type` | STRING | yes | NOAA stream dimension |
| `watermark_timestamp` | TIMESTAMP | no | latest successfully committed source event time |
| `overlap_seconds` | LONG | no | configured discovery overlap |
| `updated_run_id` | STRING | no | run that advanced watermark |
| `updated_at` | TIMESTAMP | no | UTC update time |

Business key: `stream_id`.

Backfill/replay do not update this table by default.

## 17. Delta MERGE contract

### 17.1 Candidate precondition

Before every merge:

```text
count(candidate business keys)
==
count(distinct candidate business keys)
```

or a documented deterministic deduplication rule must produce one winner per key.

Conflicting duplicates are quarantined rather than arbitrarily resolved.

### 17.2 Bronze merge example

```sql
MERGE INTO bronze_ibm_backend_properties AS t
USING bronze_ibm_candidate AS s
ON t.record_key = s.record_key
WHEN NOT MATCHED THEN INSERT *
```

No matched update.

### 17.3 Silver merge policy

Conceptual SQL:

```sql
MERGE INTO silver_qubit_calibration AS t
USING accepted_qubit_candidate AS s
ON  t.backend_id = s.backend_id
AND t.calibration_timestamp = s.calibration_timestamp
AND t.qubit_id = s.qubit_id

WHEN MATCHED
 AND t.source_payload_hash <> s.source_payload_hash
 AND s.revision_update_approved = true
THEN UPDATE SET
  t.t1_us = s.t1_us,
  t.t2_us = s.t2_us,
  t.readout_error = s.readout_error,
  t.init_error = s.init_error,
  t.measurement_error = s.measurement_error,
  t.single_gate_error = s.single_gate_error,
  t.single_gate_error_type = s.single_gate_error_type,
  t.operational = s.operational,
  t.source_payload_hash = s.source_payload_hash,
  t.batch_id = s.batch_id,
  t.run_id = s.run_id,
  t.load_timestamp = s.load_timestamp,
  t.pipeline_version = s.pipeline_version,
  t.schema_version = s.schema_version

WHEN NOT MATCHED THEN INSERT *
```

Exact same hash => no matched clause => target remains unchanged.

Different hash without approval never reaches `accepted_qubit_candidate`; it is routed to source-revision quarantine.

Equivalent logic applies to all Silver tables using their own business keys.

## 18. Idempotency proof contract

For any fixed input batch A:

```text
Run 1:
  candidate accepted
  inserts occur

Run 2:
  same keys + same hashes
  inserts = 0
  updates = 0
  target analytical content unchanged

Run 3:
  same result as Run 2
```

Verification:

- no duplicate target business keys;
- stable row count;
- stable deterministic content hash excluding operational audit tables;
- load timestamps of unchanged analytical records do not mutate merely because rerun occurred.

## 19. Relationships

```text
silver_backend
  1 ---- many silver_qubit_calibration
  on backend_id + calibration_timestamp

silver_backend
  1 ---- many silver_gate_calibration
  on backend_id + calibration_timestamp

silver_environment
  independent source-time series
  no Phase 2 FK to IBM tables
```

Referential validation for gate endpoints uses the qubit table for the same backend/calibration when source completeness allows.

## 20. Reconciliation contracts

### Source artifact reconciliation

```text
source records discovered
=
Bronze newly inserted
+ Bronze exact duplicates skipped
+ quarantined/rejected records
```

### IBM calibration reconciliation

```text
expected physical qubit entries in payload
=
accepted silver_qubit_calibration rows
+ quarantined qubit rows
```

### Gate reconciliation

Where source semantics permit:

```text
eligible 2Q gate entries
=
accepted silver_gate_calibration rows
+ quarantined/skipped-with-reason gate entries
```

Reconciliation discrepancy is written to audit and can fail acceptance.

## 21. Schema versioning

Every breaking or Silver-promoted additive contract change:

1. updates schema code;
2. increments `schema_version`;
3. updates this document;
4. adds migration/replay test;
5. adds ADR or architecture-change note when behavior changes;
6. does not rely on global auto-schema evolution.

## 22. Data contract acceptance criteria

- every listed column is implemented with the documented type/nullability;
- explicit input schemas are imported from `schemas/`;
- no schema inference is used;
- every Bronze/Silver row contains `load_timestamp`;
- business keys are enforced and tested;
- source revisions are preserved/audited;
- error metrics are not fabricated;
- NOAA cadence remains unaligned in Silver;
- Delta merge has a single candidate per target key;
- no exact rerun changes scientific analytical content.
