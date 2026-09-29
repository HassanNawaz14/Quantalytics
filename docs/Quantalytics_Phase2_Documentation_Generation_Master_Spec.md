# Quantalytics --- Phase 2 Documentation-Generation Master Specification

**Project:** Quantalytics --- Longitudinal Reliability Drift and
Calibration Freshness Analysis of Real Quantum Processors\
**Project repository:** `HassanNawaz14/Quantalytics`\
**Current planning boundary:** Phase 2 --- Bronze + Silver engineering
foundation\
**Downstream scope:** Phase 3+ research/Gold/dashboard work must remain
compatible with this Phase 2 contract.\
**Prepared:** 2026-09-29

------------------------------------------------------------------------

## 0. Purpose of This File

This is **not the final project documentation**.

It is a **documentation-generation specification**. It is intended to be
supplied as the sole substantive project input to another AI agent whose
job is to generate the project's complete, rigid, development-oriented
Markdown documentation.

The downstream documentation agent must treat this specification as the
project design authority for Phase 2 while preserving Phase 1's research
idea and official Phase 2 requirements.

The generated documentation must be:

-   implementation-oriented rather than motivational;
-   explicit enough that another developer can implement the pipeline
    without guessing;
-   strict about schemas, keys, contracts, failure modes, idempotency,
    and auditability;
-   designed for the current student/free-tier environment but not
    architecturally trapped by it;
-   compatible with future Gold/research/dashboard phases;
-   clear about which statements are requirements, which are design
    decisions, which are assumptions, and which remain configurable;
-   faithful to Phase 1 rather than silently replacing its research
    objectives.

------------------------------------------------------------------------

# 1. Source-of-Truth Hierarchy

The documentation agent must resolve conflicts using this order:

1.  **Official Phase 2 requirements** --- mandatory academic/technical
    requirements.
2.  **Phase 1 project document** --- authoritative statement of the
    research idea, source data, planned medallion structure, metrics,
    business questions, and intended future direction.
3.  **This specification** --- engineering clarification, architecture
    hardening, additional safeguards, and future-proofing decisions.
4.  **Current official vendor documentation** --- used to validate
    implementation details that can change over time.
5.  General engineering best practice --- only where it does not
    conflict with 1--4.

Never silently change a research question, data source, layer
responsibility, or required deliverable. If a contradiction cannot be
resolved safely, the generated documentation must explicitly flag it as
an open decision.

------------------------------------------------------------------------

# 2. Project Identity and Research Intent

## 2.1 Core project

Quantalytics is a data engineering and research project studying whether
the reliability of real quantum processors changes over time in ways
that are not visible from a single calibration snapshot.

The project collects longitudinal calibration information from real IBM
quantum processors, stores it through a Spark-based Medallion
architecture, normalizes it into analytical tables, and ultimately uses
the historical record to study temporal drift, spatial patterns,
calibration freshness, hardware-selection stability, prediction, and
possible associations with space-weather variables.

## 2.2 Primary research question

> Do real quantum processors exhibit predictable temporal and spatial
> reliability-drift patterns that are not visible from individual
> calibration snapshots?

## 2.3 Supporting research questions

The Phase 1 questions must remain part of the project:

1.  Do qubits that rank best at one calibration remain the best
    performers later?
2.  Do degraded qubits and gates cluster in specific regions of the
    processor topology?
3.  Can past calibration behavior predict which qubits are likely to
    enter a poor-reliability state?
4.  How quickly does calibration information become stale?
5.  How much does calibration staleness change hardware-selection
    decisions?
6.  Do publicly available space-weather indicators show a statistically
    detectable association with QPU reliability changes?

The project must never assume that the hypotheses are true. Results may
be positive, negative, null, mixed, or inconclusive.

## 2.4 Operational interpretation

The project connects two ideas:

-   **How does a real QPU drift over time?**
-   **How long can calibration information reasonably be trusted for
    hardware-selection decisions?**

The future Gold/research layer must consume Phase 2 Silver data without
needing to reinterpret raw source records.

------------------------------------------------------------------------

# 3. Phase 2 Objective

Phase 2 is the transition from a conceptual/roughly planned project into
an engineered data pipeline.

The primary Phase 2 objective is:

> Build a robust, reproducible, parameterized, auditable Apache Spark
> pipeline that ingests the approved raw IBM/NOAA datasets into Bronze
> and transforms them into contract-enforced Silver analytical tables.

Phase 2 is **not** the phase to complete all research analysis,
prediction, or dashboards.

However, Phase 2 must make those future phases straightforward.

Therefore, the Silver layer must preserve the temporal, spatial,
backend, qubit, gate, and environmental information required by the
planned Gold/research questions.

------------------------------------------------------------------------

# 4. Official Phase 2 Requirements --- Mandatory Compliance Matrix

The final documentation must contain a requirement-to-implementation
matrix.

  -----------------------------------------------------------------------------------
  Official requirement                Required implementation/documentation
  ----------------------------------- -----------------------------------------------
  Databricks Community Edition or     Use the currently available Databricks Free
  Azure                               Edition unless the instructor/environment
                                      mandates Azure. Do not document Community
                                      Edition as a current product.

  GitHub version control              All source
                                      notebooks/scripts/configuration/documentation
                                      committed to the repository.

  Bronze + Silver data models         Full data dictionary: column, type, nullable,
                                      description, key role, source, transformation,
                                      validation.

  No Spark schema inference           Explicit `StructType` / `StructField`
                                      definitions before reading raw files. Never use
                                      `inferSchema=True`.

  Strict casting                      Explicit, documented casts into Silver; invalid
                                      values must be detected and handled.

  `load_timestamp`                    Every Bronze and Silver record must contain a
                                      record-level `load_timestamp`.

  Idempotency                         Re-running identical input must not create
                                      duplicates or alter the analytical result.

  `MERGE INTO`                        Demonstrate Delta `MERGE INTO`/upsert patterns
                                      using explicit business keys.

  Parameterized backfills             Pipeline must accept run mode and
                                      historical/file parameters; no "today-only"
                                      hardcoded path.

  Schema drift                        Detect new columns/type changes and either
                                      safely evolve or quarantine non-conforming data
                                      without destroying the batch.

  Dedicated logging                   Create operational pipeline execution log
                                      table(s).

  Audit metrics                       Record layer, file/parameter, start/end,
                                      status, inserted/updated counts, and enough run
                                      metadata to diagnose failures.

  GitHub submission                   Repository must contain executable pipeline
                                      code.

  README                              README must include Bronze/Silver models and
                                      execution instructions.

  Backfill guide                      README/docs must explain incremental vs
                                      backfill invocation.
  -----------------------------------------------------------------------------------

------------------------------------------------------------------------

# 5. Current Platform Reality and Architectural Consequences

The documentation must reflect the platform as it exists now, not the
historical wording in Phase 1.

## 5.1 Databricks

Databricks Community Edition was retired in 2025. The current no-cost
student-oriented product is Databricks Free Edition.

Free Edition is serverless-only and quota-limited. Outbound internet
access is restricted unless the account has the applicable
access/verification that enables it. Therefore:

**Do not make Spark notebooks responsible for downloading IBM and NOAA
data directly.**

Instead:

-   external ingestion/collection should occur outside the Databricks
    Spark execution environment when outbound access is unavailable;
-   collected raw files should be staged into a controlled input
    location;
-   Databricks should primarily perform Bronze/Silver processing;
-   the pipeline must remain usable with uploaded sample files for the
    semester submission.

This separation is both more robust and more reproducible.

## 5.2 IBM historical retrieval

Current IBM documentation supports the backend-properties REST endpoint
with `updated_before` and `calibration_id` query parameters.

Qiskit's current `backend.properties(refresh=False, datetime=None)` API
documents historical lookup behavior, but the current API documentation
also notes that `datetime` is not implemented for some cloud
configurations.

Therefore:

-   the **REST `updated_before` approach is the authoritative historical
    extraction strategy**;
-   Qiskit may be used as a convenience/current-snapshot collector;
-   historical extraction must not depend exclusively on
    `backend.properties(datetime=...)`;
-   the collector must record the exact source retrieval timestamp and
    source calibration timestamp separately.

IBM documentation confirms that backend properties contain qubit/gate
information and change after calibration.

## 5.3 Delta / MERGE

The Silver layer should use Delta tables where available.

`MERGE` must be implemented against stable business keys, not against
`load_timestamp`.

Schema evolution must be deliberate. Do not globally enable automatic
schema evolution merely to make errors disappear. First detect and
classify drift; then either accept a documented additive change or
quarantine incompatible records.

------------------------------------------------------------------------

# 6. Architectural Principle: Separate Collection from Spark Processing

The system should be treated as two connected but independently testable
stages.

``` text
IBM Quantum / NOAA
        |
        v
External Collector / Staging
        |
        v
Immutable Raw Input
        |
        v
Spark Bronze
        |
        v
Spark Silver
        |
        +----> Phase 3 Gold / Research
        |
        +----> Dashboard / BI
```

## 6.1 Why this separation matters

It solves several problems simultaneously:

-   Databricks Free Edition network restrictions;
-   reproducibility;
-   repeatable backfills;
-   preservation of exact source payloads;
-   independent testing of ingestion and transformation;
-   ability to rerun Spark transformations without repeatedly calling
    external APIs;
-   reduced API load;
-   cleaner audit boundaries.

## 6.2 Collector responsibilities

The collector is responsible for:

-   source authentication;
-   backend discovery/configuration;
-   current/historical API requests;
-   retry/backoff;
-   rate-limit awareness;
-   raw payload preservation;
-   source metadata capture;
-   deterministic filenames/object identifiers;
-   producing immutable input artifacts;
-   returning meaningful failures rather than silently producing partial
    data.

The collector is **not** responsible for Silver business logic.

## 6.3 Spark responsibilities

Spark is responsible for:

-   explicit schema application;
-   Bronze persistence;
-   Bronze deduplication/upsert;
-   schema validation;
-   malformed-record quarantine;
-   Silver normalization;
-   strict data typing;
-   key generation;
-   data-quality checks;
-   audit logging;
-   deterministic transformation;
-   parameterized incremental/backfill processing.

------------------------------------------------------------------------

# 7. Environment and Execution Model

## 7.1 Primary environment

Recommended Phase 2 environment:

-   Databricks Free Edition;
-   PySpark;
-   Delta tables;
-   GitHub repository;
-   external Python collector for API acquisition;
-   optional GitHub Actions orchestration where feasible.

If Azure/student credits are actually selected instead, the logical
architecture remains unchanged.

## 7.2 Configuration

Never hardcode:

-   API credentials;
-   backend-specific input paths;
-   current date;
-   research cutoff dates;
-   table names inside transformation logic;
-   arbitrary top-k values inside metric logic;
-   environment-specific paths.

Use a configuration layer.

Suggested configuration concepts:

``` text
environment
catalog
schema
bronze_location
silver_location
quarantine_location
audit_location
input_root
run_mode
backend_list
start_timestamp
end_timestamp
batch_id
source_system
```

Use defaults only where safe.

------------------------------------------------------------------------

# 8. Execution Modes

The pipeline must explicitly support at least:

## 8.1 Incremental mode

Purpose: process only newly arrived source snapshots.

Inputs:

-   backend(s);
-   input/staging location;
-   optional watermark override;
-   batch ID.

Expected behavior:

1.  Identify candidate new snapshots.
2.  Validate source metadata.
3.  Compare against existing watermark/business key.
4.  Process genuinely new observations.
5.  Merge into Bronze/Silver.
6.  Record audit metrics.
7.  Produce no duplicates on repeated execution.

## 8.2 Backfill mode

Purpose: process an arbitrary historical interval or explicitly supplied
files.

Inputs:

-   backend;
-   start/end time or file/folder;
-   optional batch ID;
-   optional overwrite/reconciliation mode.

Backfill must not require modifying source code.

## 8.3 Replay mode

Recommended addition.

Purpose: re-run a previously captured batch from immutable raw files to
verify deterministic behavior.

This is especially useful for research reproducibility.

## 8.4 Validation-only mode

Recommended addition.

Purpose: validate files/schema/data quality without modifying analytical
tables.

------------------------------------------------------------------------

# 9. Data Lineage and Run Identity

Every pipeline run should have a unique:

`run_id`

Every externally collected source batch should have:

`batch_id`

These must not be conflated.

Suggested lineage chain:

``` text
source_system
    -> source_artifact
        -> batch_id
            -> run_id
                -> bronze_record
                    -> silver_record
```

Recommended metadata fields:

-   `run_id`
-   `batch_id`
-   `source_system`
-   `source_file_name`
-   `source_uri` or logical source path
-   `source_payload_hash`
-   `ingested_at`
-   `load_timestamp`
-   `pipeline_version`

The final documentation must explain which metadata belongs to the raw
artifact, the pipeline run, and the analytical record.

------------------------------------------------------------------------

# 10. Bronze Layer Contract

## 10.1 Bronze philosophy

Bronze is the preservation layer.

It should preserve source information with minimal semantic
modification.

Do not calculate research metrics in Bronze.

Do not aggressively normalize Bronze.

Do not discard raw fields merely because Silver does not currently use
them.

## 10.2 IBM Bronze recommended model

The Phase 1 fields remain:

-   `backend_id`
-   `collection_timestamp`
-   `raw_calibration_timestamp`
-   `raw_payload`
-   `source`

Recommended engineering metadata:

-   `record_key`
-   `batch_id`
-   `run_id`
-   `source_file_name`
-   `source_payload_hash`
-   `load_timestamp`
-   `pipeline_version`

### IBM Bronze key

Recommended natural/business identity:

`backend_id + raw_calibration_timestamp`

If the source can contain multiple distinct payloads for the same
calibration timestamp, use:

`backend_id + raw_calibration_timestamp + source_payload_hash`

The documentation must explicitly state which identity is chosen after
validating the actual source behavior.

## 10.3 NOAA Bronze

Phase 1 fields:

-   `observation_timestamp`
-   `source`
-   `raw_payload`

Recommended additions:

-   `record_key`
-   `batch_id`
-   `run_id`
-   `source_file_name`
-   `source_payload_hash`
-   `load_timestamp`
-   `pipeline_version`

The natural identity should be based on source observation time plus any
source-specific discriminator necessary to avoid collapsing legitimate
observations.

## 10.4 Raw payload preservation

Prefer storing the original payload in a representation that can be
reproduced exactly enough for reprocessing.

If the collector receives JSON, retain the raw JSON payload/string or
immutable raw artifact and keep a hash.

If CSV is the supplied raw sample, preserve the original file and
separately store the structured Bronze representation.

------------------------------------------------------------------------

# 11. Silver Layer Contract

Silver is the normalized, typed, validated analytical foundation.

The final documentation must include complete data dictionaries.

## 11.1 `silver_backend`

Purpose: one backend/calibration-level record.

Recommended fields:

  ---------------------------------------------------------------------------------
  Field                     Type              Key               Purpose
  ------------------------- ----------------- ----------------- -------------------
  `backend_id`              STRING            PK part           IBM backend
                                                                identifier

  `processor_family`        STRING                              Processor
                                                                generation/family

  `qubit_count`             INT                                 Number of physical
                                                                qubits represented

  `calibration_timestamp`   TIMESTAMP         PK part           Source calibration
                                                                timestamp

  `collection_timestamp`    TIMESTAMP                           Collector retrieval
                                                                time

  `load_timestamp`          TIMESTAMP                           Record processing
                                                                time

  `batch_id`                STRING                              Source batch

  `run_id`                  STRING                              Pipeline execution

  `source_payload_hash`     STRING                              Payload
                                                                lineage/integrity

  `pipeline_version`        STRING                              Transformation
                                                                version
  ---------------------------------------------------------------------------------

## 11.2 `silver_qubit_calibration`

Phase 1 fields must remain conceptually intact:

-   `backend_id`
-   `calibration_timestamp`
-   `qubit_id`
-   `T1_us`
-   `T2_us`
-   `readout_error`
-   `init_error`
-   `single_gate_error`
-   `operational`

Recommended additions:

-   `measurement_error`
-   `single_gate_error_type` or source gate discriminator if needed;
-   `load_timestamp`
-   `batch_id`
-   `run_id`
-   `source_payload_hash`
-   `record_key`
-   `pipeline_version`

Primary key:

`backend_id + calibration_timestamp + qubit_id`

Do not use `load_timestamp` as part of the business key.

## 11.3 `silver_gate_calibration`

Phase 1 fields:

-   `backend_id`
-   `calibration_timestamp`
-   `source_qubit`
-   `target_qubit`
-   `CZ_error`
-   `RZZ_error`
-   `gate_length_ns`

Recommended addition:

-   `gate_type` / `instruction_type`

This is important because multiple instruction types can exist for the
same qubit pair.

Primary key should therefore normally be:

`backend_id + calibration_timestamp + gate_type + source_qubit + target_qubit`

If the real source structure proves that gate identity is different, the
documentation must adapt based on actual observed payloads.

## 11.4 `silver_environment`

Phase 1 fields:

-   `timestamp`
-   `kp_index`
-   `solar_flux`

Recommended additions:

-   `source`
-   `observation_type`
-   `load_timestamp`
-   `batch_id`
-   `run_id`
-   `source_payload_hash`

The time semantics must be documented carefully because environmental
observations and IBM calibration timestamps may not have identical
cadence.

------------------------------------------------------------------------

# 12. Timestamp Semantics

This is a critical design area and must receive its own documentation
section.

The project has multiple times that must never be conflated:

1.  **Source calibration time** --- when IBM says the
    calibration/property state applies.
2.  **Collection time** --- when the external collector retrieved the
    source.
3.  **Load time** --- when Spark wrote/processed the record.
4.  **Environmental observation time** --- when NOAA measured the
    environmental variable.
5.  **Analysis reference time** --- a later analytical "now" used to
    calculate freshness.

All timestamps should be normalized to UTC.

The documentation must explicitly state that:

`load_timestamp != calibration_timestamp`

and explain why.

------------------------------------------------------------------------

# 13. Explicit Schema Enforcement

No Spark schema inference is permitted.

Never use:

``` python
.option("inferSchema", "true")
```

or equivalent implicit inference.

Every raw input type must be represented by an explicit `StructType`.

The documentation must include:

-   IBM Bronze schema;
-   NOAA Bronze schema;
-   each Silver schema;
-   expected nullability;
-   units;
-   allowed ranges where known;
-   source field mapping;
-   conversion rules.

## 13.1 Why explicit schema matters

It prevents:

-   numeric fields becoming strings;
-   inconsistent timestamp parsing;
-   accidental type changes across files;
-   silent schema drift;
-   downstream analytical instability.

## 13.2 Casting policy

Casting must be explicit and observable.

Example conceptual mapping:

``` text
raw string -> validated numeric -> DOUBLE
raw ISO string -> TIMESTAMP
raw integer identifier -> INT
operational indicator -> BOOLEAN / documented status type
```

Invalid casts must not silently become valid-looking NULLs.

They must be measurable and, where appropriate, quarantined.

------------------------------------------------------------------------

# 14. Data Quality Contract

Every ingestion/transformation stage should have data-quality checks.

## 14.1 Structural checks

-   required columns exist;
-   no forbidden unexpected type substitutions;
-   required metadata fields exist;
-   expected nested structures exist where applicable.

## 14.2 Value checks

Examples:

-   `T1_us > 0` when present;
-   `T2_us > 0` when present;
-   error metrics within physically/semantically meaningful ranges;
-   qubit IDs non-negative where applicable;
-   gate source/target IDs valid;
-   timestamps parse successfully;
-   backend ID non-null.

Do not invent arbitrary scientific thresholds unless justified by source
documentation or research methodology.

## 14.3 Referential checks

-   gate qubits should correspond to known physical qubits for the same
    backend/calibration where the source permits validation;
-   backend-level qubit count should be consistent with observed qubit
    records, allowing documented exceptions.

## 14.4 Completeness checks

Track:

-   expected vs observed qubit count;
-   expected vs observed gate records;
-   null rates;
-   malformed record counts;
-   duplicate counts;
-   rejected/quarantined counts.

## 14.5 Quality outcomes

Each record/batch should fall into a controlled outcome:

-   accepted;
-   accepted_with_warning;
-   quarantined;
-   rejected_batch.

A single bad record should not automatically destroy an otherwise valid
batch unless it violates a batch-level contract.

------------------------------------------------------------------------

# 15. Schema Drift Strategy

Schema drift must be handled intentionally.

## 15.1 Additive drift

Example:

IBM adds a new field.

Recommended behavior:

1.  Detect it.
2.  Record drift in a schema-drift log.
3.  Preserve the raw field in Bronze.
4.  Decide whether it is promoted to Silver.
5.  If promoted, version/update the Silver contract and data dictionary.
6.  Keep backward compatibility where possible.

## 15.2 Type drift

Example:

A numeric source value arrives as a string.

If safely parseable:

-   normalize/cast;
-   record the normalization.

If incompatible:

-   quarantine the affected record;
-   do not crash the entire valid batch unnecessarily.

## 15.3 Breaking drift

Example:

A field disappears or its structure becomes incompatible.

The pipeline should:

-   fail the affected transformation stage safely;
-   record a precise schema-drift error;
-   preserve the raw input;
-   avoid corrupting the existing Silver table;
-   allow remediation and replay.

## 15.4 Do not blindly enable schema evolution

Automatic evolution can be useful for additive changes, but uncontrolled
evolution weakens the project's explicit-contract requirement.

Schema evolution must be:

-   deliberate;
-   logged;
-   versioned;
-   reviewable.

------------------------------------------------------------------------

# 16. Idempotency Design

Idempotency is one of the most important Phase 2 requirements.

The pipeline must satisfy:

> Same input + same business state =\> same analytical result,
> regardless of how many times the pipeline is run.

## 16.1 Idempotency must not use load time

This is wrong:

``` text
unique key = business_key + load_timestamp
```

because every rerun creates a new identity.

Instead use source/business identity.

## 16.2 Bronze merge

Use a deterministic key based on:

-   backend;
-   calibration timestamp;
-   payload hash if necessary.

## 16.3 Silver merge

Use table-specific business keys.

Examples:

``` text
silver_backend:
backend_id + calibration_timestamp

silver_qubit_calibration:
backend_id + calibration_timestamp + qubit_id

silver_gate_calibration:
backend_id + calibration_timestamp + gate_type + source_qubit + target_qubit

silver_environment:
source + observation_timestamp + source-specific discriminator
```

## 16.4 Required demonstration

Documentation must include an idempotency test:

1.  Run batch A.
2.  Record row counts and hashes.
3.  Run batch A again.
4.  Verify no duplicate business keys.
5.  Verify inserted/updated behavior is zero or logically unchanged.
6.  Verify analytical content is identical.
7.  Show the `MERGE INTO` logic.

------------------------------------------------------------------------

# 17. Merge Semantics

The documentation must define what "matched" means for every table.

For example:

``` text
WHEN MATCHED:
    update mutable source/metadata fields only if the source payload differs

WHEN NOT MATCHED:
    insert new record
```

Do not blindly overwrite historical scientific values with a new
`load_timestamp`.

For immutable source snapshots, a matched record may simply be treated
as unchanged.

If the source payload for the same business key changes, the system must
distinguish:

-   legitimate correction;
-   source revision;
-   duplicate ingestion;
-   data corruption.

This distinction should be logged.

------------------------------------------------------------------------

# 18. Watermark Strategy

Incremental ingestion should maintain a watermark.

Recommended watermark:

`max(source calibration timestamp successfully committed)`

But the design must not rely solely on `>` timestamp filtering if source
APIs can return revised records.

Recommended incremental discovery:

1.  Read previous watermark.
2.  Query slightly before the watermark using a small overlap window.
3.  Retrieve candidate snapshots.
4.  Deduplicate by business identity/payload hash.
5.  Merge.
6.  Advance watermark only after successful commit.

This protects against late-arriving/revised source data.

The overlap duration should be configurable rather than hardcoded into
business logic.

------------------------------------------------------------------------

# 19. Fault Tolerance and Retry Strategy

The external collector should distinguish:

-   authentication failure;
-   rate limiting;
-   transient network failure;
-   source unavailable;
-   malformed response;
-   no historical record available;
-   invalid backend;
-   permission failure.

Recommended behavior:

-   exponential backoff for transient errors;
-   bounded retries;
-   clear final error;
-   no partial-success ambiguity;
-   batch-level status;
-   preservation of successful artifacts even if a later backend fails.

Spark transformation failures should:

-   preserve raw inputs;
-   avoid partial Silver corruption;
-   write failure details to audit logs;
-   allow replay.

------------------------------------------------------------------------

# 20. Audit and Operational Logging

Create at least:

## `pipeline_execution_logs`

Recommended fields:

  Field                Purpose
  -------------------- ----------------------------------
  `run_id`             Unique execution
  `batch_id`           Source batch
  `pipeline_name`      Pipeline identity
  `layer`              Bronze/Silver
  `stage`              Exact operation
  `source_system`      IBM/NOAA
  `parameter`          File/path/date/backend parameter
  `start_time`         Execution start
  `end_time`           Execution end
  `status`             SUCCESS/FAILURE/PARTIAL
  `rows_read`          Input count
  `rows_inserted`      Insert count
  `rows_updated`       Update count
  `rows_quarantined`   Rejected/quarantined count
  `error_message`      Failure summary
  `pipeline_version`   Code/data contract version

Additional recommended fields:

-   `duration_seconds`;
-   `watermark_before`;
-   `watermark_after`;
-   `source_payload_count`;
-   `schema_version`;
-   `data_quality_status`.

## 20.1 Audit principles

Logging must occur for:

-   full load;
-   incremental load;
-   backfill;
-   replay;
-   Bronze processing;
-   Silver processing;
-   failed runs;
-   schema drift events where applicable.

The audit table is itself operational data and should be queryable.

------------------------------------------------------------------------

# 21. Schema Drift Log

Recommended additional table:

`pipeline_schema_drift_logs`

Fields:

-   `drift_event_id`
-   `run_id`
-   `batch_id`
-   `source_system`
-   `source_file`
-   `table_name`
-   `column_name`
-   `expected_type`
-   `observed_type`
-   `drift_type`
-   `action_taken`
-   `record_count_affected`
-   `event_timestamp`

This makes the "schema drift handling" requirement demonstrable rather
than merely theoretical.

------------------------------------------------------------------------

# 22. Quarantine Design

Recommended location/table:

`quarantine_records`

Minimum metadata:

-   `quarantine_id`
-   `run_id`
-   `batch_id`
-   `source_system`
-   `source_file`
-   `table_name`
-   `record_key`
-   `raw_record`
-   `failure_reason`
-   `failure_category`
-   `quarantined_at`
-   `pipeline_version`

Quarantine should preserve enough information to fix and replay the
record.

------------------------------------------------------------------------

# 23. Bronze-to-Silver Transformation Flow

Recommended flow:

``` text
Read staged source
      |
      v
Explicit schema
      |
      v
Structural validation
      |
      +---- invalid structure ---> quarantine / batch failure
      |
      v
Metadata enrichment
      |
      v
Normalize timestamps
      |
      v
Explicit casting
      |
      v
Value validation
      |
      +---- invalid record ---> quarantine
      |
      v
Deduplicate candidate records
      |
      v
Generate stable keys
      |
      v
Delta MERGE
      |
      v
Audit metrics
```

The final documentation must describe this sequence precisely.

------------------------------------------------------------------------

# 24. Repository Structure

Phase 1 proposed the following broad structure. Preserve the intent but
refine it for Phase 2.

Recommended:

``` text
Quantalytics/
├── ingestion/
│   ├── ibm/
│   ├── noaa/
│   ├── common/
│   └── README.md
│
├── spark/
│   ├── common/
│   ├── bronze/
│   ├── silver/
│   └── validation/
│
├── notebooks/
│   ├── phase2/
│   └── experiments/
│
├── schemas/
│   ├── bronze/
│   └── silver/
│
├── config/
│   ├── defaults/
│   └── examples/
│
├── sample_data/
│   ├── full_load/
│   └── incremental_load/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── data_quality/
│   └── idempotency/
│
├── research/
│
├── dashboard/
│
├── docs/
│   ├── architecture/
│   ├── data_contracts/
│   ├── operations/
│   ├── research/
│   └── phase2/
│
├── .github/
│   └── workflows/
│
├── requirements.txt
├── .gitignore
├── README.md
└── LICENSE
```

The final agent may simplify this if the actual repository needs a
smaller structure, but it must not collapse all logic into one notebook.

------------------------------------------------------------------------

# 25. Notebook vs Python Module Strategy

Prefer reusable Python modules for business logic.

Notebooks should primarily provide:

-   execution entry points;
-   demonstrations;
-   visual validation;
-   submission-friendly walkthroughs.

Core logic should be importable/testable.

Avoid a project where the only implementation is a 500-line notebook.

Recommended conceptual modules:

``` text
config
schemas
io
validation
keys
bronze
silver
audit
quarantine
watermark
```

------------------------------------------------------------------------

# 26. Testing Strategy

Phase 2 documentation must include a real testing plan.

## 26.1 Unit tests

Test:

-   timestamp parsing;
-   numeric casting;
-   stable-key generation;
-   validation functions;
-   schema comparison;
-   parameter parsing;
-   merge-key construction.

## 26.2 Integration tests

Test:

-   raw -\> Bronze;
-   Bronze -\> Silver;
-   IBM payload -\> normalized tables;
-   NOAA payload -\> environment table;
-   audit writes;
-   quarantine writes.

## 26.3 Idempotency tests

Required.

Test repeated execution of:

-   same full-load file;
-   same incremental file;
-   same backfill range.

Expected result:

-   no duplicate business keys;
-   stable analytical row counts;
-   stable data content.

## 26.4 Failure tests

Test:

-   malformed JSON/CSV;
-   missing required column;
-   wrong data type;
-   null timestamp;
-   impossible numeric value;
-   schema drift;
-   duplicate source records;
-   simulated source/API failure.

## 26.5 Backfill tests

Verify that:

-   arbitrary historical ranges work;
-   rerunning a backfill is safe;
-   backfill does not corrupt newer records;
-   watermark handling remains correct.

------------------------------------------------------------------------

# 27. Data Reconciliation

The documentation should define reconciliation checks such as:

``` text
source snapshot count
    vs
Bronze accepted snapshot count
    + quarantined snapshot count
    + intentionally skipped duplicate count
```

For a valid IBM snapshot:

``` text
expected qubits
    vs
silver_qubit_calibration rows
```

and where source semantics allow:

``` text
expected gate records
    vs
silver_gate_calibration rows
```

Reconciliation failures must be visible in audit output.

------------------------------------------------------------------------

# 28. Research Readiness Requirements

Although Phase 2 stops before Gold, it must preserve future analytical
validity.

Silver must retain:

-   backend identity;
-   calibration time;
-   collection time;
-   qubit identity;
-   gate endpoints;
-   gate type;
-   reliability/error measures;
-   T1/T2;
-   operational state;
-   environment observation time;
-   source lineage.

Do not aggregate away the raw qubit/gate temporal grain.

Do not replace raw metrics with only a composite health score.

Composite scores belong in Gold/research.

------------------------------------------------------------------------

# 29. Future Gold Contract

Phase 2 documentation must include a short "downstream dependency
contract" explaining what Phase 3 will expect.

Planned Gold concepts from Phase 1:

-   `gold_qpu_health`
-   `gold_qubit_reliability`
-   `gold_drift_events`
-   `gold_calibration_staleness`
-   `gold_prediction`
-   `gold_environment_association`

Phase 2 must not prematurely implement these as if their research
methodology were already finalized.

However, the Silver design must support:

-   consecutive-calibration comparisons;
-   drift percentage;
-   calibration age;
-   ranking;
-   top-k turnover;
-   stale-selection loss;
-   spatial/topological analysis;
-   time-aware prediction;
-   lagged environmental analysis.

------------------------------------------------------------------------

# 30. Scientific Methodology Guardrails

The documentation agent must preserve the distinction between:

### Observation

Example:

"Qubit 27's readout error increased between calibration A and
calibration B."

### Statistical association

Example:

"Periods with higher Kp were associated with higher observed drift under
the specified model."

### Prediction

Example:

"The model estimated a probability of the next calibration entering the
defined poor-reliability class."

### Causation

Do **not** state that space weather causes QPU drift merely because
correlation/regression is significant.

The Phase 1 design explicitly requires environmental results to be
reported as associations, not automatic causal conclusions.

------------------------------------------------------------------------

# 31. Drift Metric Caution

Phase 1 defines:

`Drift% = ((Metric_t - Metric_t-1) / Metric_t-1) × 100`

The final documentation must preserve this metric but add safeguards:

-   division-by-zero handling;
-   missing predecessor handling;
-   extremely small denominators;
-   directionality differences between "higher is better" and "lower is
    better" metrics;
-   separate treatment of error metrics and coherence metrics.

For example:

-   lower error is generally better;
-   higher T1/T2 is generally better.

Therefore, a raw percentage change should not automatically be called
"worse" or "better" without metric-specific interpretation.

Composite reliability scoring should be defined later, not silently
invented in Phase 2.

------------------------------------------------------------------------

# 32. Calibration Freshness Design

Phase 1 defines:

`Calibration Age = CurrentTime − CalibrationTimestamp`

Phase 2 must preserve the timestamp information needed to calculate this
later.

Important distinction:

-   Phase 2 stores source facts.
-   Gold calculates freshness relative to an analysis reference time.

Do not continuously mutate historical Silver records merely because
"current time" has advanced.

Freshness should be derived analytically.

------------------------------------------------------------------------

# 33. Spatial Analysis Readiness

The planned dashboard includes a living processor map.

Therefore Silver should retain:

-   physical qubit ID;
-   gate source qubit;
-   gate target qubit;
-   backend identity;
-   calibration timestamp.

A future Gold/research layer can construct topology.

Do not hardcode a topology into Bronze.

If topology metadata is not present in calibration payloads, acquire it
as a separate documented metadata source in a later phase.

------------------------------------------------------------------------

# 34. Prediction Readiness

Future prediction must use time-aware splits.

Phase 2 must not leak future data into Silver-derived training features.

The future documentation should enforce:

``` text
past calibration(s) -> features
next calibration -> target
```

and prohibit random row-level train/test splitting for temporal
prediction.

------------------------------------------------------------------------

# 35. Space-Weather Alignment

The NOAA feed has a different temporal cadence from IBM calibration
events.

Phase 2 should store environmental observations at source granularity.

Do not prematurely aggregate NOAA to IBM calibration timestamps in
Silver.

Gold can later define:

-   nearest observation;
-   rolling average;
-   lagged value;
-   event-window summary.

This preserves analytical flexibility.

------------------------------------------------------------------------

# 36. Security

Required:

-   IBM credentials never committed;
-   secrets through environment variables, repository secrets, or
    platform secret management;
-   `.gitignore` includes local credential/config files;
-   sample configuration contains placeholders only;
-   logs must never print tokens;
-   raw payload logging must be checked for credentials before
    persistence;
-   public repository must contain no secrets.

The project is not a PII-heavy system, but "no PII" does not mean "no
security controls."

------------------------------------------------------------------------

# 37. Git and Development Discipline

The final documentation should define:

-   feature branches where practical;
-   meaningful commit messages;
-   no generated credentials;
-   no huge binary artifacts unless explicitly required;
-   reproducible sample data;
-   tagged/identified Phase 2 milestone;
-   README kept synchronized with schema changes.

Recommended commit categories:

``` text
feat:
fix:
docs:
test:
refactor:
data:
chore:
```

------------------------------------------------------------------------

# 38. CI / Automation

If GitHub Actions is used, it should initially automate safe tasks such
as:

-   Python syntax checks;
-   unit tests;
-   schema validation;
-   linting;
-   repository structure checks;
-   secret scanning where available.

Do not make the academic submission dependent on a fragile
production-like deployment system.

The collector may be scheduled externally, but the project must still
work manually with supplied sample files.

------------------------------------------------------------------------

# 39. FinOps / Free-Tier Strategy

Keep Phase 1's intent:

-   one historical baseline extraction;
-   incremental collection thereafter;
-   compact Delta/Parquet storage;
-   avoid unnecessary full-table recomputation;
-   bounded research window;
-   small Spark workloads.

Add:

-   never re-download history simply because a Spark job was rerun;
-   cache/stage raw data externally;
-   avoid continuous Spark compute;
-   process only candidate new files;
-   use partitioning only when it provides real benefit;
-   avoid over-partitioning small datasets.

The dataset is expected to be small enough that correctness and
maintainability are more important than extreme optimization.

------------------------------------------------------------------------

# 40. Partitioning Guidance

Phase 1 proposes partitioning by backend and calibration date.

Treat this as a design option, not an unconditional rule.

Because the expected dataset is relatively small, over-partitioning can
create unnecessary small files.

Recommended principle:

> Partition only when it improves actual read/write patterns; otherwise
> retain simple Delta tables.

The documentation must explain the chosen decision.

------------------------------------------------------------------------

# 41. Documentation Deliverables the Downstream Agent Must Generate

The final documentation package should be divided by development
concern.

## Document A --- `README.md`

Must contain:

-   project purpose;
-   architecture;
-   Phase 2 scope;
-   prerequisites;
-   repository structure;
-   environment setup;
-   configuration;
-   execution modes;
-   incremental example;
-   backfill example;
-   test commands;
-   Bronze model;
-   Silver model;
-   audit model;
-   troubleshooting;
-   security note;
-   Phase 2 acceptance checklist.

## Document B --- `docs/architecture/phase2-architecture.md`

Must contain:

-   architecture diagram;
-   component responsibilities;
-   ingestion boundary;
-   Bronze/Silver flow;
-   lineage;
-   failure boundaries;
-   environment constraints;
-   future Gold interface.

## Document C --- `docs/data_contracts/bronze-silver-data-contract.md`

Must contain:

-   complete schema tables;
-   data types;
-   nullable;
-   units;
-   keys;
-   source mapping;
-   transformations;
-   validation rules;
-   metadata fields;
-   schema versioning.

## Document D --- `docs/operations/pipeline-runbook.md`

Must contain:

-   incremental execution;
-   backfill;
-   replay;
-   validation-only mode;
-   watermark;
-   retries;
-   failures;
-   quarantine;
-   schema drift;
-   audit inspection;
-   recovery.

## Document E --- `docs/testing/phase2-test-plan.md`

Must contain:

-   unit;
-   integration;
-   data-quality;
-   idempotency;
-   backfill;
-   failure;
-   reconciliation;
-   acceptance tests.

## Document F --- `docs/research/phase2-to-gold-contract.md`

Must contain:

-   research questions;
-   Silver fields supporting each question;
-   timestamp semantics;
-   leakage prevention;
-   future Gold expectations;
-   limitations.

## Document G --- `docs/decisions/architecture-decision-records.md`

Recommended ADRs:

1.  External ingestion vs Spark ingestion.
2.  Databricks Free Edition environment.
3.  Delta tables.
4.  Business-key-based MERGE.
5.  Explicit schema enforcement.
6.  Schema-drift policy.
7.  Watermark + overlap strategy.
8.  Immutable raw preservation.

------------------------------------------------------------------------

# 42. Phase 2 Definition of Done

Phase 2 is complete only when all of the following are demonstrably
true.

## Environment

-   [ ] Databricks environment initialized.
-   [ ] Repository connected/usable.
-   [ ] Dependencies documented.

## Bronze

-   [ ] IBM Bronze implemented.
-   [ ] NOAA Bronze implemented.
-   [ ] Explicit schemas used.
-   [ ] Raw payload preserved.
-   [ ] `load_timestamp` present.
-   [ ] Stable identity present.
-   [ ] Source lineage present.

## Silver

-   [ ] `silver_backend` implemented.
-   [ ] `silver_qubit_calibration` implemented.
-   [ ] `silver_gate_calibration` implemented.
-   [ ] `silver_environment` implemented.
-   [ ] Explicit casts implemented.
-   [ ] UTC timestamps enforced.
-   [ ] Null handling implemented.
-   [ ] Duplicate handling implemented.
-   [ ] Malformed records handled.
-   [ ] Stable business keys implemented.
-   [ ] `load_timestamp` present.

## Robustness

-   [ ] `MERGE INTO` demonstrated.
-   [ ] Repeated execution produces no duplicates.
-   [ ] Incremental mode works.
-   [ ] Backfill mode works.
-   [ ] Replay mode is documented or implemented.
-   [ ] Schema drift is detected.
-   [ ] Quarantine exists.
-   [ ] Failures are auditable.

## Logging

-   [ ] Pipeline execution log exists.
-   [ ] Start/end timestamps logged.
-   [ ] Layer logged.
-   [ ] Parameter/file logged.
-   [ ] Status logged.
-   [ ] Inserted/updated counts logged.
-   [ ] Error information logged.

## Documentation

-   [ ] README updated.
-   [ ] Bronze data model documented.
-   [ ] Silver data model documented.
-   [ ] Backfill execution documented.
-   [ ] Incremental execution documented.
-   [ ] Testing documented.
-   [ ] Architecture documented.

------------------------------------------------------------------------

# 43. Evidence Required for Academic Demonstration

The final documentation should tell the developer exactly what evidence
to capture.

Recommended evidence:

1.  Repository tree.
2.  Explicit `StructType` definitions.
3.  Bronze table schema.
4.  Silver table schemas.
5.  Successful full-load run.
6.  Successful incremental run.
7.  Successful backfill run.
8.  `MERGE INTO` implementation.
9.  Repeated-run/idempotency demonstration.
10. Audit-log records.
11. Schema-drift demonstration.
12. Quarantined-record example.
13. README execution examples.

This turns requirements into verifiable evidence instead of claims.

------------------------------------------------------------------------

# 44. Risks and Mitigations

## Risk: IBM source/API changes

Mitigation:

-   isolate source connector;
-   preserve raw payload;
-   version collector;
-   keep source-to-Bronze mapping documented.

## Risk: backend catalogue changes

Mitigation:

-   configurable backend list;
-   discovery metadata where practical;
-   never hardcode "exactly three forever."

## Risk: calibration cadence changes

Mitigation:

-   timestamp-based ingestion;
-   watermark;
-   overlap window;
-   no daily-file assumption.

## Risk: Databricks networking limitations

Mitigation:

-   external collector;
-   staged raw files;
-   Spark-independent source acquisition.

## Risk: schema drift

Mitigation:

-   explicit contracts;
-   drift detector;
-   quarantine;
-   controlled evolution.

## Risk: duplicate ingestion

Mitigation:

-   stable business keys;
-   payload hashes;
-   Delta MERGE;
-   idempotency tests.

## Risk: research leakage

Mitigation:

-   temporal ordering;
-   Gold-layer time-aware split;
-   immutable Silver facts.

## Risk: overengineering

Mitigation:

-   prioritize academic requirements;
-   avoid unnecessary production infrastructure;
-   keep interfaces simple;
-   document optional features separately from mandatory features.

------------------------------------------------------------------------

# 45. Things the Documentation Agent Must NOT Invent

Do not invent:

-   scientific conclusions;
-   a predetermined drift magnitude;
-   a causal relationship with space weather;
-   a specific "best" backend;
-   arbitrary reliability thresholds;
-   unsupported IBM fields;
-   unsupported NOAA fields;
-   production-scale infrastructure that is unnecessary for the project;
-   fake execution results;
-   fake row counts;
-   fake benchmark results;
-   fake API responses;
-   credentials;
-   undocumented platform capabilities.

Where an exact value depends on actual source payloads, write the
documentation so the developer can populate it after inspecting the
sample.

------------------------------------------------------------------------

# 46. Things the Documentation Agent SHOULD Add

The documentation agent should add implementation detail where Phase 1
was intentionally rough:

-   exact interfaces;
-   parameter names;
-   schema contracts;
-   key strategy;
-   watermark logic;
-   audit model;
-   quarantine model;
-   drift handling;
-   test strategy;
-   recovery procedures;
-   evidence checklist;
-   development order;
-   acceptance criteria;
-   future Gold dependencies.

These additions are not changes to the research idea. They are
engineering formalization.

------------------------------------------------------------------------

# 47. Recommended Development Order

The generated implementation documentation should instruct development
in this order:

### Stage 1 --- Repository/environment

Set up repository, Databricks workspace, dependencies, configuration,
and secret handling.

### Stage 2 --- Source artifacts

Freeze and validate the supplied Full Load and Incremental Load samples.

### Stage 3 --- Schemas

Implement explicit IBM/NOAA schemas.

### Stage 4 --- Bronze

Implement raw ingestion, metadata, stable keys, and Delta MERGE.

### Stage 5 --- Bronze validation

Implement structural validation, duplicate detection, drift detection,
and quarantine.

### Stage 6 --- Silver

Implement normalization, casting, timestamp handling, and analytical
tables.

### Stage 7 --- Audit

Implement execution logs and reconciliation.

### Stage 8 --- Parameterization

Implement incremental/backfill/replay/validation modes.

### Stage 9 --- Testing

Implement unit, integration, failure, idempotency, and backfill tests.

### Stage 10 --- Evidence

Run the complete demonstration workflow and capture proof.

### Stage 11 --- Documentation

Finalize README and all Phase 2 documentation only after implementation
behavior is verified.

------------------------------------------------------------------------

# 48. Recommended End-to-End Demonstration

The documentation should provide a reproducible "golden path":

``` text
1. Upload/provide Full Load sample
2. Run validation
3. Run Full Load
4. Inspect Bronze
5. Inspect Silver
6. Inspect audit log
7. Run Incremental Load sample
8. Inspect new records
9. Re-run Incremental Load
10. Confirm no duplicates
11. Run historical backfill
12. Re-run same backfill
13. Confirm idempotency
14. Inject/observe schema drift
15. Confirm quarantine/drift log
16. Verify README instructions
17. Commit repository
```

This should become the central Phase 2 acceptance demonstration.

------------------------------------------------------------------------

# 49. Documentation Style Requirements for the Downstream Agent

The final generated documentation must:

-   use Markdown;
-   use clear headings;
-   use tables for schemas;
-   use Mermaid diagrams where useful;
-   use code blocks for commands/configuration;
-   use precise terminology;
-   distinguish mandatory vs recommended;
-   distinguish current behavior vs future phase;
-   include "Why" explanations for important engineering decisions;
-   avoid vague phrases such as "handle errors properly";
-   state exact failure behavior;
-   include examples;
-   include assumptions;
-   include acceptance criteria;
-   include cross-references between documents.

The documentation should read like a compact engineering specification
plus implementation runbook, not like a generic university report.

------------------------------------------------------------------------

# 50. Required Direct Prompt for the Documentation-Generation Agent

Use the following prompt directly with the downstream agent:

> You are the lead data architect, senior data engineer,
> research-methodology-aware analyst, and technical documentation
> engineer for the Quantalytics project.
>
> Your task is to generate the complete Phase 2 engineering
> documentation for the project based **only on the project
> specification supplied below plus the official Phase 2 requirements
> and authoritative vendor documentation where implementation details
> need current verification**.
>
> Do not treat this as a generic data-engineering assignment. Understand
> the actual research purpose: Quantalytics is studying longitudinal
> reliability drift and calibration freshness in real IBM quantum
> processors. Phase 2 must build the Bronze/Silver foundation that makes
> later drift, freshness, spatial, predictive, and environmental
> research possible.
>
> ## Non-negotiable principles
>
> 1.  Preserve the Phase 1 research questions and conceptual direction.
> 2.  Satisfy every official Phase 2 requirement explicitly.
> 3.  Do not silently invent scientific conclusions.
> 4.  Do not use Spark schema inference.
> 5.  Use explicit `StructType`/`StructField` schemas.
> 6.  Put `load_timestamp` on every Bronze and Silver record.
> 7.  Implement real idempotency using stable business keys and Delta
>     `MERGE INTO`.
> 8.  Support both incremental processing and arbitrary historical
>     backfills through parameters.
> 9.  Handle schema drift explicitly and safely.
> 10. Provide operational logging with start/end/status/parameters/row
>     counts.
> 11. Preserve raw scientific payloads sufficiently for
>     replay/reprocessing.
> 12. Keep source collection separated from Spark transformation because
>     the current Databricks Free Edition environment can restrict
>     outbound internet.
> 13. Treat IBM historical retrieval through the documented REST
>     `updated_before` mechanism as the robust baseline; do not make the
>     design depend exclusively on Qiskit's historical `datetime`
>     behavior.
> 14. Make every implementation decision traceable to a requirement,
>     Phase 1 intention, platform constraint, or engineering rationale.
>
> ## Your output
>
> Generate a coherent Markdown documentation package, not a loose
> collection of notes.
>
> Produce:
>
> -   `README.md`
> -   `docs/architecture/phase2-architecture.md`
> -   `docs/data_contracts/bronze-silver-data-contract.md`
> -   `docs/operations/pipeline-runbook.md`
> -   `docs/testing/phase2-test-plan.md`
> -   `docs/research/phase2-to-gold-contract.md`
> -   `docs/decisions/architecture-decision-records.md`
>
> If you cannot physically create multiple files, present the complete
> contents under clearly separated filenames.
>
> ## Required documentation depth
>
> For every major component, document:
>
> -   purpose;
> -   inputs;
> -   outputs;
> -   schema;
> -   keys;
> -   transformations;
> -   validations;
> -   failure behavior;
> -   idempotency behavior;
> -   lineage;
> -   parameters;
> -   test cases;
> -   operational considerations.
>
> ## Required architecture
>
> Document this logical flow:
>
> `IBM Quantum / NOAA -> external collector/staging -> immutable raw input -> Spark Bronze -> Spark Silver -> future Gold/research/dashboard`
>
> Explain why source collection is separated from Spark processing.
>
> ## Required Bronze models
>
> Document IBM Bronze and NOAA Bronze with:
>
> -   source fields;
> -   explicit Spark types;
> -   metadata fields;
> -   `load_timestamp`;
> -   batch/run identifiers;
> -   source payload hash;
> -   business/record key;
> -   deduplication rules.
>
> ## Required Silver models
>
> Fully document:
>
> -   `silver_backend`
> -   `silver_qubit_calibration`
> -   `silver_gate_calibration`
> -   `silver_environment`
>
> Include column name, Spark/Delta type, nullable, unit, description,
> source mapping, transformation, validation, and key role.
>
> Pay particular attention to:
>
> -   `backend_id`
> -   `calibration_timestamp`
> -   `qubit_id`
> -   gate source/target qubits
> -   gate type
> -   T1/T2
> -   readout/init/single-gate/CZ/RZZ errors
> -   operational state
> -   environment timestamps
> -   `load_timestamp`
>
> Do not use `load_timestamp` as an analytical business key.
>
> ## Required timestamp model
>
> Clearly distinguish:
>
> -   source calibration timestamp;
> -   collection timestamp;
> -   Spark load timestamp;
> -   NOAA observation timestamp;
> -   later analytical freshness/reference time.
>
> Normalize stored analytical timestamps to UTC.
>
> ## Required idempotency model
>
> Explain exact MERGE keys for every table.
>
> Include pseudocode or PySpark/Delta examples.
>
> Show how the pipeline behaves when the exact same input is run twice.
>
> Include a concrete idempotency test.
>
> ## Required incremental/backfill model
>
> Document:
>
> -   incremental mode;
> -   backfill mode;
> -   replay mode if included;
> -   validation-only mode if included;
> -   watermark;
> -   overlap window;
> -   parameter passing;
> -   examples;
> -   expected audit output.
>
> Never hardcode "today".
>
> ## Required schema-drift model
>
> Explain:
>
> -   additive drift;
> -   type drift;
> -   breaking drift;
> -   drift detection;
> -   quarantine;
> -   controlled evolution;
> -   schema versioning;
> -   audit logging.
>
> Do not blindly enable automatic schema evolution.
>
> ## Required audit model
>
> Define a `pipeline_execution_logs` table containing at minimum:
>
> -   layer;
> -   processed parameter/file;
> -   start/end;
> -   status;
> -   inserted rows;
> -   updated rows.
>
> Add run ID, batch ID, source, rows read, quarantined rows, watermark,
> pipeline version, and error details where appropriate.
>
> ## Required quarantine model
>
> Define a replayable quarantine structure containing the raw
> problematic record and failure reason.
>
> ## Required testing
>
> Include:
>
> -   unit tests;
> -   integration tests;
> -   data-quality tests;
> -   idempotency tests;
> -   backfill tests;
> -   schema-drift tests;
> -   malformed-input tests;
> -   reconciliation tests.
>
> Each test should specify:
>
> -   setup;
> -   action;
> -   expected result.
>
> ## Research preservation
>
> Explain how Phase 2 supports future:
>
> -   QPU health;
> -   per-qubit reliability;
> -   drift events;
> -   calibration staleness;
> -   top-k turnover;
> -   stale-selection loss;
> -   spatial topology analysis;
> -   prediction;
> -   space-weather association.
>
> Do not implement or claim final Gold research methodology unless it is
> explicitly required.
>
> ## Scientific guardrails
>
> Never turn correlation into causation.
>
> Preserve the distinction between:
>
> -   observed change;
> -   statistical association;
> -   prediction;
> -   causal inference.
>
> Never assume drift exists at a particular magnitude.
>
> ## Current-platform verification
>
> Where you discuss Databricks or IBM/Qiskit behavior that may change
> over time, verify against current official documentation and clearly
> mark time-sensitive implementation facts.
>
> Do not use outdated references to Databricks Community Edition as the
> current platform.
>
> ## Quality bar
>
> The result must be sufficiently precise that a competent developer can
> implement Phase 2 without asking basic architectural questions.
>
> Do not write vague recommendations.
>
> Do not produce fake execution results.
>
> Do not invent source fields.
>
> Do not invent scientific thresholds.
>
> Do not overengineer the project into an enterprise platform that
> cannot reasonably be implemented by students on a free-tier
> environment.
>
> Optimize for:
>
> **correctness + reproducibility + auditability + academic compliance +
> future research usability + practical implementability.**
>
> End the documentation with:
>
> 1.  Phase 2 Definition of Done.
> 2.  Requirement-to-implementation traceability matrix.
> 3.  Evidence/demo checklist.
> 4.  Known assumptions and open decisions.
> 5.  Explicit handoff contract for Phase 3.

------------------------------------------------------------------------

# 51. Final Architectural Position

The project should not be thought of as:

> "a Spark assignment that happens to use quantum data."

It should be engineered as:

> **a small but genuinely research-oriented longitudinal telemetry
> lakehouse whose Phase 2 contribution is a reproducible,
> contract-enforced historical data foundation.**

The strongest architectural decision is to make the raw source history
immutable and independently replayable, then make Bronze/Silver
deterministic and idempotent.

That creates the foundation required for the later research questions
without prematurely baking research conclusions into the
data-engineering layer.
