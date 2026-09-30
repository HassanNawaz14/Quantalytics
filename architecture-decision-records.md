# Architecture Decision Records

## ADR format

Each decision records:

```text
ADR ID
Title
Status
Context
Problem
Decision
Reasoning
Alternatives Considered
Consequences
Implementation Impact
Research Impact
```

---

## ADR-001 — Separate external collection from Spark processing

**Status:** Accepted — Phase 2

**Context:** Databricks Free Edition is serverless and can restrict outbound internet. The project also requires reproducible historical backfills.

**Problem:** Direct API calls inside Spark notebooks would couple source availability/network policy to transformation correctness and make replays repeatedly hit vendor APIs.

**Decision:** Use external Python collectors for IBM/NOAA. Stage immutable artifacts. Databricks primarily processes staged artifacts into Bronze/Silver.

**Reasoning:** Supports restricted networking, replay, lower API load, deterministic testing and cleaner failure boundaries.

**Alternatives considered:**
- direct Spark/HTTP ingestion;
- notebook-only collector and transformation.

**Consequences:**
- one extra staging step;
- source acquisition can be tested independently;
- raw history survives Spark reruns.

**Implementation impact:** implement `ingestion/` collector modules and artifact envelope.

**Research impact:** historical analyses can be reproduced from frozen source artifacts.

---

## ADR-002 — Use Databricks Free Edition as the default no-cost execution environment

**Status:** Accepted subject to instructor environment

**Context:** legacy Community Edition was retired in 2025; Free Edition is the current no-cost student product.

**Problem:** Phase 1/official wording may reference Community Edition.

**Decision:** Document Free Edition unless instructor mandates Azure.

**Reasoning:** current-platform accuracy.

**Alternatives considered:** Azure student environment.

**Consequences:** serverless/Free Edition limitations influence networking and resource strategy.

**Implementation impact:** avoid assumptions about classic clusters/DBFS mounts; use supported managed/Unity Catalog patterns available in the selected workspace.

**Research impact:** none to scientific design.

---

## ADR-003 — Preserve immutable raw source artifacts

**Status:** Accepted

**Context:** source APIs can change, source values can be revised, and Phase 2 requires replay/backfill.

**Problem:** If only normalized data is kept, source interpretation cannot be audited or reprocessed later.

**Decision:** preserve exact payload bytes/text plus SHA-256 and source metadata.

**Reasoning:** auditability, reproducibility, schema-drift recovery.

**Alternatives considered:** store only parsed Bronze columns.

**Consequences:** small storage overhead; much stronger replay.

**Implementation impact:** artifact writer, hashes, source IDs.

**Research impact:** published analyses can trace to exact source versions.

---

## ADR-004 — Explicit Spark schemas; no inference

**Status:** Accepted / Mandatory

**Context:** official Phase 2 requirement.

**Problem:** inference can change types across files and hide source drift.

**Decision:** every raw input type uses version-controlled `StructType`/`StructField`.

**Reasoning:** deterministic typing and detectable drift.

**Alternatives considered:** `inferSchema`.

**Consequences:** schema maintenance required when source evolves.

**Implementation impact:** central `schemas/` modules.

**Research impact:** stable analytical types.

---

## ADR-005 — Bronze payload-version identity and Silver analytical business keys

**Status:** Accepted

**Context:** source revisions and idempotency have different preservation needs.

**Problem:** one key strategy cannot both preserve revised raw payloads and provide one stable analytical row.

**Decision:**

Bronze:

```text
snapshot_business_key = source logical identity
record_key = snapshot_business_key + payload hash
```

Silver:

```text
table-specific analytical business key
```

**Reasoning:** Bronze can retain source revisions; Silver remains analytically unique.

**Alternatives considered:**
- business key only in Bronze (would overwrite raw revisions);
- load timestamp in key (breaks idempotency).

**Consequences:** revision conflicts require explicit Silver policy.

**Implementation impact:** key helper functions and revision detection.

**Research impact:** exact source versions remain recoverable.

---

## ADR-006 — Delta `MERGE` for idempotent Bronze/Silver writes

**Status:** Accepted / Mandatory

**Context:** official requirement requires `MERGE INTO`.

**Problem:** append-only writes duplicate data on rerun.

**Decision:** use Delta merge with explicit keys after candidate deduplication.

**Reasoning:** idempotent upsert and transactionality.

**Alternatives considered:** append + periodic dedup; full overwrite.

**Consequences:** source candidate must have at most one effective row per target key.

**Implementation impact:** shared merge module, idempotency tests.

**Research impact:** repeated pipeline execution does not alter analytical meaning.

---

## ADR-007 — Conservative source-revision update policy

**Status:** Accepted default; precedence rule remains open

**Context:** same logical snapshot can theoretically appear with changed payload.

**Problem:** blindly updating historical scientific values may replace prior evidence without knowing whether a change is correction, revision or corruption.

**Decision:** preserve new version in Bronze; default Silver action is `SOURCE_REVISION_REQUIRES_REVIEW`. Controlled Silver update requires explicit approval flag and passing validations.

**Reasoning:** safest behavior under incomplete source-revision semantics.

**Alternatives considered:** last-write-wins; first-write-wins permanently.

**Consequences:** some revisions require operator/reconciliation action.

**Implementation impact:** revision quarantine and optional controlled update path.

**Research impact:** historical scientific values do not silently mutate.

---

## ADR-008 — Watermark plus configurable overlap

**Status:** Accepted

**Context:** incremental source delivery may be late or revised.

**Problem:** strict `>` last timestamp can miss late/revised source records.

**Decision:** discover from `watermark - overlap`, deduplicate with stable identities, advance watermark only after successful commit.

**Reasoning:** resilience to imperfect delivery.

**Alternatives considered:** no overlap; file-arrival-only state.

**Consequences:** repeated candidates are normal and must be idempotent.

**Implementation impact:** `pipeline_watermarks`, overlap config, incremental tests.

**Research impact:** fewer historical gaps.

---

## ADR-009 — Quarantine instead of silent coercion/discard

**Status:** Accepted

**Context:** invalid casts, malformed structures and conflicting revisions can occur.

**Problem:** dropping bad records or turning them into null can corrupt research datasets invisibly.

**Decision:** preserve failing raw/candidate record, categorized reason and replay status in `quarantine_records`.

**Reasoning:** auditability and recoverability.

**Alternatives considered:** fail all batches; silently drop bad records.

**Consequences:** operational table and replay process required.

**Implementation impact:** quarantine writer and categories.

**Research impact:** exclusions are traceable.

---

## ADR-010 — Dedicated operational audit log

**Status:** Accepted / Mandatory

**Context:** Phase 2 requires layer/file/parameter/start/end/status/insert/update observability.

**Problem:** notebook console output is not a durable audit trail.

**Decision:** persist `pipeline_execution_logs` with row metrics, status, versions, watermarks and errors.

**Reasoning:** academic evidence and troubleshooting.

**Alternatives considered:** text logs only.

**Consequences:** each stage emits structured audit events.

**Implementation impact:** shared audit module.

**Research impact:** data lineage and run reproducibility improve.

---

## ADR-011 — NOAA environment remains at source observation grain

**Status:** Accepted

**Context:** Kp, solar flux and IBM calibrations have different cadences.

**Problem:** joining them in Silver would prematurely choose an alignment methodology.

**Decision:** `silver_environment` stores one row per source/observation type/time. Phase 3 aligns series.

**Reasoning:** preserves analytical flexibility and avoids hidden look-ahead choices.

**Alternatives considered:** nearest-time join in Phase 2; hourly/daily aggregation.

**Consequences:** sparse metric columns by observation type.

**Implementation impact:** observation-type key and cross-field rule.

**Research impact:** supports lag/window sensitivity analysis.

---

## ADR-012 — UTC standardization with raw timestamp preservation

**Status:** Accepted

**Context:** longitudinal comparisons require unambiguous time semantics.

**Problem:** local/offset ambiguity can invalidate ordering/freshness.

**Decision:** parse persisted analytical timestamps to UTC and preserve raw source timestamp text in Bronze.

**Reasoning:** stable temporal comparisons plus auditability.

**Alternatives considered:** retain mixed source zones only.

**Consequences:** strict timestamp parser required.

**Implementation impact:** timestamp utility and tests.

**Research impact:** prevents false ordering caused by timezone handling.

---

## ADR-013 — Canonical single-qubit error is configurable

**Status:** Accepted with open configuration value

**Context:** Phase 1 requires `single_gate_error`, while a backend can expose several one-qubit instructions.

**Problem:** selecting one arbitrarily would encode an undocumented research assumption.

**Decision:** configuration names the canonical one-qubit gate. If unavailable/unset, the convenience metric remains null with warning.

**Reasoning:** preserves Phase 1 field without fabricating semantics.

**Alternatives considered:** minimum error; first source gate; average all 1Q errors.

**Consequences:** one configuration decision required after sample inspection.

**Implementation impact:** `canonical_single_qubit_gate` config and validation.

**Research impact:** avoids hidden cherry-picking/aggregation.

---

## ADR-014 — Initial Delta tables are not forcibly partitioned

**Status:** Accepted recommendation

**Context:** expected academic dataset is relatively small.

**Problem:** partitioning by backend/date can create many small files and complexity without measurable benefit.

**Decision:** start with simple/unpartitioned managed Delta tables; add partitioning only from measured access/volume needs.

**Reasoning:** correctness and maintainability over premature optimization.

**Alternatives considered:** always partition by backend/date.

**Consequences:** simpler setup; future optimization remains possible.

**Implementation impact:** no partition key required in Phase 2 contract.

**Research impact:** none.

---

## ADR-015 — Modules own business logic; notebooks are thin entry points

**Status:** Accepted

**Context:** implementation must be testable and coding-agent friendly.

**Problem:** a single large notebook hides dependencies and is hard to unit test.

**Decision:** reusable logic lives under `ingestion/`, `spark/`, `schemas/`; notebooks parameterize and demonstrate execution.

**Reasoning:** testability, reuse and maintainability.

**Alternatives considered:** notebook-only implementation.

**Consequences:** small module structure required.

**Implementation impact:** repository layout specified in README.

**Research impact:** later Gold code can reuse stable modules/contracts.


---

## ADR-016 — Separate Bronze preservation from Silver normalization

**Status:** Accepted

**Context:** the research needs exact replayable source history and separately needs stable analytical tables.

**Problem:** a single layer either becomes too raw for research use or too transformed for source-faithful replay.

**Decision:** Bronze preserves source payload versions with minimal semantic enrichment; Silver performs typed normalization, business-key uniqueness, validation and research-ready relational structure.

**Reasoning:** enforces the Medallion responsibility boundary and prevents analytical transformations from destroying source evidence.

**Alternatives considered:** one normalized table directly from raw; aggressive normalization in Bronze.

**Consequences:** two explicit contracts and one additional transformation boundary.

**Implementation impact:** independent Bronze and Silver modules/tables with reconciliation between them.

**Research impact:** Phase 3 uses stable Silver while raw provenance remains recoverable.
