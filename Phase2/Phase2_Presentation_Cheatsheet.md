# Phase 2 Presentation Cheat Sheet

**Use live. Show the code and saved output, not README claims.**  
All paths are relative to the repository root. Rubric: `Phase2/Phase_2_Rubric.pdf`.

## Opening: 30 seconds

1. **Sources:** IBM Quantum calibration data and NOAA SWPC Kp/F10.7 data.
2. **Flow:** GitHub Actions collectors -> Unity Catalog staging volume -> Bronze Delta -> Silver Delta -> Phase 3 Gold/dashboard.
3. **Phase 2 stops at Silver.** Gold, dashboard, and modelling are Phase 3.

## Rubric map: exact proof

### 1. Infrastructure and GitHub
**Show:** `README.md:18-30` (repository layout); `ingestion/`, `notebooks/`, `spark/bronze/`, `spark/silver/`, `.github/workflows/`.

**Say:** PySpark code is in GitHub in separate collector, setup, Bronze, Silver, and test folders. Databricks is the execution environment; tables live in the `workspace` catalog.

### 2. Data models and README
**Show:** `README.md:63-176` (all Bronze/Silver tables, columns, types, logical keys); `Phase2/README.md:179-237`; `docs/diagrams/quantalytics_data_models.dbml` and `.png`.

**Say:** All four data tables plus the two operational tables are documented. Delta does not enforce primary keys, so the README states the logical key and the mechanism that prevents duplicates.

### 3. Schemas and data types
**Show:**
- IBM Bronze schema cell: `IBM_SCHEMA`, lines 6-32.
- IBM Bronze load cell: `build_file_schema` lines 13-26; `.schema(file_schema)` and `.option("inferSchema", False)` lines 48-56.
- NOAA Bronze Cell 2: `NOAA_INPUT_SCHEMA` lines 1-6; Cell 4 line 35: `.schema(NOAA_INPUT_SCHEMA).json(...)`.
- Silver Cell 4: `IBM_PAYLOAD_SCHEMA` lines 3-12, `from_json` lines 22-24, `try_cast` lines 37-45.
- Silver Cell 5: `try_cast` for timestamp/Kp/flux, lines 28 and 37-38.

**Say:** No `inferSchema`. Bronze keeps IBM values as strings to preserve raw values; every field promoted to Silver is explicitly cast. Gate-level IBM fields remain in Bronze `raw_payload` by design. `IBM_SCHEMA` is the declared 23-column contract; `build_file_schema` validates the header against it and builds the StructType used by Spark, preserving new columns for drift handling.

### 4. `load_timestamp`
**Show:** IBM Bronze table cell (`CREATE TABLE`, lines 2-12; `load_timestamp` line 9) and load cell (`F.current_timestamp()`, line 66); NOAA Bronze Cell 3 (DDL lines 2-10; `load_timestamp` line 7) and Cell 4 MERGE lines 74-84; Silver Cell 4 line 77 and Cell 5 line 61; `README.md:105,119,138,155`.

**Say:** Every Bronze and Silver data row carries `load_timestamp`. The log table also has one. Historical IBM Bronze timestamps came from a one-time migration, not the original ingestion time.

### 5. Bronze layer and lineage
**Show:** IBM load cell lines 59-67 (`raw_payload`, `source`, `source_file`, `collection_timestamp`); NOAA Bronze Cell 4 lines 61-86.

**Say:** Bronze preserves the source row as raw text/JSON and tracks source, file, and load time; IBM also tracks collection time. IBM deduplicates by source filename; NOAA merges on `(source, observation_timestamp)`.

### 6. Silver layer and data quality
**Show:**
- Silver Cell 3: `select_input` lines 1-11; `quarantine` lines 14-28; `merge_silver` lines 31-44.
- Silver Cell 4: validation rules lines 47-69; dedupe/key lines 71-84; MERGE line 85.
- Silver Cell 5: NOAA structure checks lines 3-18; validation lines 44-56; dedupe lines 66-68.
- `spark/silver/tests/3. Quarantine Test.ipynb`: actual assertions and output.

**Say:** Silver casts types, checks required fields and ranges, quarantines bad rows, deduplicates logical keys, and merges only valid rows. Bad rows are not silently dropped.

### 7. MERGE and idempotency
**Show:** Silver Cell 3 `merge_silver` condition line 38 (`source_payload_hash <>`); NOAA Bronze Cell 4 MERGE lines 61-86; Silver Cell 4 saved output `read 33228, inserted 0, updated 0`; Silver Cell 6 duplicate check `0`.

**Say:** Re-running unchanged input leaves Silver business data unchanged. The audit table intentionally grows because each attempted run writes a new log row.

### 8. Parameters and backfills
**Show:** Silver Cell 1 widgets lines 20-47; Silver Cell 3 `select_input` lines 1-11; `README.md:182-204`; saved log output in Silver Cell 6 (`backfill`, `4680`, `0`, `0`).

**Say:** Silver supports `full`, `incremental`, and `backfill` through `dataset`, `mode`, `source_file`, `start_date`, and `end_date`. A backfill is a date range; no code edit is needed.

**Bronze caveat:** there is no Bronze widget. Bronze takes all files in `new/`; the file/folder is the batch input. To replay an old batch, stage the historical file in `new/` and rerun Bronze. Do not claim a runtime Bronze parameter.

### 9. Schema drift
**Show:**
- IBM Bronze load cell: required-column validation and `SchemaBreak` lines 13-26; new-column warning lines 72-77; rejected-file handling lines 81-84.
- Silver Cell 4: extra-key detection lines 24-25 and `SCHEMA_DRIFT_NEW_COLUMN` line 52.
- Silver Cell 5: NOAA structure UDF lines 3-18.
- `spark/silver/tests/2. Schema Drift Test.ipynb` (manual demo) and `3. Quarantine Test.ipynb` (assertions).

**Say:** If IBM adds a column, Bronze loads it and preserves it in `raw_payload`; Silver quarantines affected rows until the Silver contract is extended. If a required IBM column is missing/renamed, that file is rejected and the rest of the batch continues. Type changes become `try_cast` nulls and are quarantined. No `mergeSchema` is used; the implemented path is quarantine/rejection.

### 10. Execution logs
**Show:** IBM Bronze log cell `LOG_SCHEMA` lines 12-27 and `log_run` lines 37-43; NOAA Bronze log cell lines 12-43; Silver Cell 3 `audit` lines 47-53; `README.md:161-176,236-241`; saved log table in Silver Cell 6.

**Say:** `pipeline_execution_logs` is one shared audit table in the Silver schema; `stage` distinguishes `Raw-to-Bronze` from `Bronze-to-Silver`. It records `run_id`, layer, dataset, mode, parameters, start/end time, status, input rows, inserted/updated/quarantined rows, error, and load time. Bronze writes per file; Silver writes per dataset, including failures. The saved output in Silver Cell 6 shows Bronze-to-Silver rows; the Bronze log cells write the `Raw-to-Bronze` rows.

### 11. Code and run guide
**Show:** `README.md:178-208` (incremental and backfill procedures); `Phase2/README.md:329-359`; `spark/silver/1. Silver Pipeline.ipynb` “How to demonstrate” cell.

**Say:** The runbook covers setup, normal incremental load, backfill, rerunning a file, and idempotency verification.

## Five-minute demo route

1. **Repo and models (30s):** show folder layout and `README.md:63-176`; mention logical keys.
2. **Bronze proof (45s):** open IBM schema cell and load cell. Point to explicit schema, `inferSchema=False`, raw payload, source/file lineage, `load_timestamp`.
3. **Backfill (45s):** open Silver Cell 1 widgets; show `mode=backfill`, date fields; jump to Silver Cell 6 saved log row: `backfill`, `4680`, `0`, `0`.
4. **Idempotency (45s):** show `merge_silver` payload-hash condition; show saved incremental row `33228` read, `0` inserted, `0` updated; show duplicate count `0`.
5. **Schema change and quarantine (60s):** run/show `3. Quarantine Test.ipynb`: `2` input, `2` quarantined, `0` in Silver, assertions pass. Mention Test 2 for the manual drift demonstration.
6. **Logs (30s):** show `LOG_SCHEMA`/`log_run` for the Raw-to-Bronze stage and the saved Silver-stage log table; both write to the same table.
7. **Close (30s):** Phase 2 ends at Silver; Gold/dashboard are Phase 3; state limitations honestly.

**If time is short:** steps 2, 4, 5, and 6. Prefer saved outputs; do not run a full load during the demo.

## Proof already saved in the repo

| Evidence | Open | Proves |
|---|---|---|
| NOAA full load | `spark/bronze/noaa_bronze_load.ipynb`, Cell 4 output | `rows: 1822` merged into Bronze |
| IBM incremental replay | `spark/silver/1. Silver Pipeline.ipynb`, Cell 4 output | `read 33228, inserted 0, updated 0, quarantined 0` |
| Final Silver counts | same, Cell 6 output | IBM `456612`, NOAA `2194`, duplicate keys `0` |
| Backfill and full-run logs | same, Cell 6 output | backfill `4680` selected, `0` inserted/updated; full runs also `0`/`0` |
| Quarantine proof | `spark/silver/tests/3. Quarantine Test.ipynb` | `2` bad rows quarantined, `0` reach QA Silver, assertions pass |
| Schema-drift demo | `spark/silver/tests/2. Schema Drift Test.ipynb` | invalid number and extra column are classified for quarantine |
| Evidence summary | `README.md:255-270`; `Phase2/README.md:269-327` | recorded results and limitations |

## Likely questions and answers

| Question | Answer |
|---|---|
| Walk me through the pipeline. | Collectors fetch IBM/NOAA data -> GitHub Actions upload files to the UC staging volume -> Bronze notebooks impose schemas and load Delta -> Silver casts, validates, quarantines, deduplicates, and MERGEs -> logs record every run. |
| Why Bronze stores everything as strings? | It preserves the source values and survives source type changes; Silver is where types and business rules are enforced. |
| Where is the explicit schema? | IBM Bronze `IBM_SCHEMA`/`build_file_schema`; NOAA Bronze Cell 2; Silver `from_json` and `try_cast`; no `inferSchema`. |
| Why does IBM build the schema from the header? | It validates the fixed 23-column contract, then creates an explicit `StructType` that also retains new columns for drift handling. Spark still reads with that StructType; no inference is used. |
| How do you prevent duplicates? | IBM file-level dedupe; NOAA Bronze MERGE on source+timestamp; Silver SHA-256 `record_key`, pre-MERGE `row_number`, and MERGE update only on payload change. |
| Prove idempotency. | Saved incremental run: 33,228 read, 0 inserted, 0 updated. Final duplicate check: 0. Audit rows grow by design; business data does not. |
| How does backfill work? | Silver: set `mode=backfill` plus `start_date`/`end_date`. Bronze: stage the historical file in `new/` and rerun; no code edit. |
| What happens if a new column appears? | IBM Bronze loads and preserves it in `raw_payload`; Silver quarantines affected rows as `SCHEMA_DRIFT_NEW_COLUMN`; other rows continue. |
| What if a type is invalid? | Silver `try_cast` produces null, the row is tagged with an error reason, and it goes to `quarantined_records`; valid rows continue. |
| What validations run? | Required identifiers/timestamps, positive T1/T2 when present, readout/init in `[0,1]`, non-negative qubit id, Kp in `[0,9]`, and flux `> 0`; failures are quarantined. |
| What if a required IBM column is missing? | The file is moved to `rejected/`, logged as `FAILURE`, and other files still process. |
| Where are bad records? | `workspace.silver.quarantined_records`, with dataset, source file, raw payload, reason, and load timestamp. |
| Where are the logs? | `workspace.silver.pipeline_execution_logs`; one row per Bronze file or Silver dataset attempt, with stage, parameters, times, status, and counts. |
| Is Bronze parameterized? | Partially: it consumes the `new/` folder and logs each `source_file`, but there is no widget. Silver has full widgets. This is a known Phase 2 gap. |
| Is the data complete? | No. IBM history is capped/sampled; NOAA Kp mixes 3-hourly history with minute-level live data, and the live window is short. |
| Is this orchestrated? | Only collectors are scheduled. Bronze/Silver are manual notebook runs in Phase 2; end-to-end jobs are Phase 3. |
| Are the tests automated? | Partly. The quarantine test and final duplicate check have assertions; schema-drift and Silver test notebooks are manual; there is no CI workflow. |
| What are the biggest gaps? | No Bronze widget, no end-to-end jobs, no CI, NOAA Kp/F10.7 semantics are not normalized, quarantine has no per-run `run_id`, and `Operational` is a constant. |
| Where is Gold? | Not in Phase 2. Gold/dashboard/modeling are Phase 3; see `Phase3/Phase3_Implementation_Guide.md`. |
| Why is a primary key not enforced? | Delta Lake does not enforce PK constraints here. The logical keys are enforced by file dedupe, MERGE, and pre-merge deduplication. |

## Do not overclaim

- Do not say Bronze has a runtime widget or date parameter.
- Do not say `mergeSchema` is used; the implemented handling is schema preservation plus quarantine/rejection.
- Do not say all 23 IBM fields are promoted to Silver; only core calibration fields are, gate metrics stay in Bronze.
- Do not say the schema-drift test is automated; it is a manual demonstration.
- Do not say tests or CI are complete; there is no CI workflow.
- Do not say NOAA is a complete regular time series.
- Do not say Gold, dashboard, or modelling is done.
- Do not say historical IBM `load_timestamp` is the original ingestion time.
- Do not say Delta enforces the primary keys.
- Do not say audit rows are idempotent; only Silver business rows are.

## Numbers to remember

| Item | Value |
|---|---:|
| IBM Silver total | 456,612 |
| IBM full + increments | 422,916 + 33,228 + 468 |
| IBM snapshots | 2,711 |
| NOAA Silver total | 2,194 |
| NOAA full + live | 1,822 + 372 |
| IBM backfill, 25-26 Sep 2026 | 4,680 selected; 0 inserted; 0 updated |
| Duplicate Silver keys | 0 for IBM and NOAA |
| Quarantine test | 2 quarantined; 0 in Silver |

## One-line close

“Phase 2 is a working Raw -> Bronze -> Silver Delta pipeline with explicit schemas, lineage, validation, quarantine, MERGE-based idempotency, backfill parameters, and both-layer execution logs; Gold and automation are deliberately deferred to Phase 3.”
