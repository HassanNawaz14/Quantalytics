# Quantalytics

**Data Analysis and Visualisation — Semester Project**  
**Phase 2: Apache Spark Bronze and Silver Lakehouse Pipeline**

## 1. Project overview

Quantalytics studies the reliability of IBM quantum processors over time and compares their calibration measurements with space-weather observations from NOAA. The project uses **Databricks, PySpark and Delta Lake** with the Medallion Architecture:

`IBM Quantum / NOAA source files → Bronze (raw data) → Silver (validated data) → Gold (future analysis and dashboard)`

**Phase 2 scope:** Ingest both data sources into Bronze Delta tables; transform them into typed Silver tables; provide idempotent upserts, selectable incremental loads and historical backfills, data-quality checks, quarantine and execution logs. Gold analytics and the dashboard belong to a later phase.

## 2. Technologies and data sources

- **Platform:** Databricks (Apache Spark / PySpark and Delta Lake).
- **IBM Quantum:** Historical and incremental backend-calibration CSV files, covering measurements such as T1, T2 and readout error.
- **NOAA SWPC:** Staged JSON Lines observations for planetary Kp and F10.7 solar radio flux.
- **Repository:** [HassanNawaz14/Quantalytics](https://github.com/HassanNawaz14/Quantalytics).
- **Security:** The datasets describe hardware/environmental observations rather than individual people. API keys and account credentials must never be committed to GitHub.

## 3. Notebooks and responsibilities

| Location | Purpose |
| --- | --- |
| `notebooks/00_setup_databricks.ipynb` | Prepares project schemas and the staging volume. |
| `spark/bronze/ibm_bronze_load.ipynb` | Reads staged IBM CSV files into the IBM Bronze Delta table using an explicit schema. |
| `spark/bronze/noaa_bronze_load.ipynb` | Reads staged NOAA JSONL files into the NOAA Bronze Delta table using an explicit schema. |
| `spark/silver/1. Silver Pipeline.ipynb` | Builds both Silver tables; handles validation, Delta `MERGE`, full/incremental/backfill parameters, logging and quarantine. |
| `spark/silver/tests/` | Separate schema-drift and automatic-quarantine QA notebooks. |

The exact filenames of the two test notebooks may differ; they are supporting tests, not additional production layers.

## 4. Data dictionary and logical keys

All tables use the `workspace` catalog. **Bronze tables preserve original values**; **Silver tables have typed analytical columns**. Delta Lake does not automatically enforce the *logical keys* listed below as traditional relational primary-key constraints. The pipeline enforces uniqueness during its merges.

### IBM Bronze — `workspace.bronze.ibm_calibration_raw`

| Column | Type | Meaning |
| --- | --- | --- |
| `backend_id` | STRING | IBM processor identifier. |
| `collection_timestamp` | TIMESTAMP | Timestamp associated with collection. |
| `raw_calibration_timestamp` | STRING | Original calibration time, not yet cast. |
| `raw_payload` | STRING | Original flattened IBM record stored as JSON text. |
| `source` | STRING | Source system identifier. |
| `source_file` | STRING | Originating CSV file. |
| `load_timestamp` | TIMESTAMP | Bronze load time; added to existing data during migration. |

**Grain:** One source row representing a qubit's calibration measurement. The source-level identifier includes its processor, calibration time and qubit ID (the qubit ID is inside `raw_payload`). **Current Bronze design has no declared/enforced primary key.**

### NOAA Bronze — `workspace.bronze.noaa_space_weather_raw`

| Column | Type | Meaning |
| --- | --- | --- |
| `observation_timestamp` | STRING | Original observation time. |
| `source` | STRING | NOAA feed (`noaa_swpc_kp` or `noaa_swpc_f107`). |
| `raw_payload` | STRING | Source and parsed observations stored as JSON text. |
| `source_file` | STRING | Originating JSONL file. |
| `load_timestamp` | TIMESTAMP | Bronze load time; added to existing data during migration. |

**Grain / logical merge key:** One observation per `(source, observation_timestamp)`.

### IBM Silver — `workspace.silver.ibm_qubit_calibration`

| Column | Type | Meaning |
| --- | --- | --- |
| `record_key` | STRING | SHA-256 logical key. |
| `backend_id` | STRING | IBM processor. |
| `calibration_timestamp` | TIMESTAMP | Parsed calibration time (UTC session). |
| `qubit_id` | INT | Qubit identifier. |
| `t1_us` | DOUBLE | Qubit T1 lifetime, microseconds. |
| `t2_us` | DOUBLE | Qubit T2 lifetime, microseconds. |
| `readout_error` | DOUBLE | Readout assignment error. |
| `init_error` | DOUBLE | Initialisation error, nullable when unavailable. |
| `operational` | BOOLEAN | Whether the source marks the qubit as operational. |
| `source_payload_hash` | STRING | Hash used to detect changed source records. |
| `source_file` | STRING | Source lineage. |
| `load_timestamp` | TIMESTAMP | When the record was processed into Silver. |

**Logical key:** `(backend_id, calibration_timestamp, qubit_id)`, represented by `record_key`. Measurements are cast with `try_cast`; invalid keys and invalid non-empty measurements are rejected.

### NOAA Silver — `workspace.silver.noaa_environment`

| Column | Type | Meaning |
| --- | --- | --- |
| `record_key` | STRING | SHA-256 logical key. |
| `source` | STRING | Source feed. |
| `observation_type` | STRING | `planetary_kp` or `f10_7_flux`. |
| `observation_timestamp` | TIMESTAMP | Parsed observation time. |
| `kp_index` | DOUBLE | Kp geomagnetic index (when applicable). |
| `solar_flux` | DOUBLE | F10.7 solar radio flux (when applicable). |
| `source_unit` | STRING | `index` or `sfu`. |
| `source_payload_hash` | STRING | Detects changed source values. |
| `source_file` | STRING | Source lineage. |
| `load_timestamp` | TIMESTAMP | When the record was processed into Silver. |

**Logical key:** `(source, observation_type, observation_timestamp)`, represented by `record_key`. The pipeline checks source labels, JSON structure, timestamps and measurement ranges.

### Operational tables

- `workspace.silver.pipeline_execution_logs`: `run_id`, `stage`, `dataset`, `mode`, `parameters`, `started_at`, `finished_at`, `status`, `input_rows`, `rows_inserted`, `rows_updated`, `rows_quarantined`, `error_message`, `load_timestamp`.
- `workspace.silver.quarantined_records`: `record_key`, `dataset`, `source_file`, `raw_payload`, `error_reason`, `load_timestamp`.

The log records a row for each attempted **Bronze-to-Silver** dataset run. Invalid records are retained for inspection instead of silently discarded.

## 5. How to run the Silver pipeline

**Prerequisites:** Databricks Spark compute, access to populated Bronze tables under `workspace.bronze`, and permission to create/write `workspace.silver` tables.

1. Open `spark/silver/1. Silver Pipeline.ipynb` in Databricks.
2. Select Serverless or another supported Spark compute option.
3. Run notebook cells **from top to bottom**. Cell 1 defines widgets and variables required by later cells.
4. Choose the parameters below and use **Run All**:

| Use case | `dataset` | `mode` | `source_file` | `start_date` | `end_date` |
| --- | --- | --- | --- | --- | --- |
| Initial/full | `both` | `full` | *(blank)* | *(blank)* | *(blank)* |
| Specific incremental IBM file | `ibm` | `incremental` | `ibm_incremental_20261009T124928Z.csv` | *(blank)* | *(blank)* |
| Historical IBM backfill | `ibm` | `backfill` | *(blank)* | `2026-09-25` | `2026-09-26` |

The dates are **inclusive**. Replace the example file/date parameters with the batch you need. `source_file` is matched to the `source_file` column **already stored in Bronze**; this notebook does not fetch live API data. The notebook supports `dataset=ibm`, `noaa` or `both`.

**Implementation:** PySpark reads the Bronze Delta tables with `spark.table()`. The IBM JSON payload uses an explicit `StructType` and safe casts; the NOAA payload is validated and parsed into typed fields. Valid records are deduplicated by logical key and written by Delta `MERGE`. Existing records are updated **only when the source payload hash changes**. Each run records counts and status in `pipeline_execution_logs`.

## 6. Phase 2 QA evidence (10 October 2026)

| Test | Observed result |
| --- | --- |
| IBM initial full processing | **456,612** Silver records; 456,612 inserted. |
| NOAA initial full processing | **2,194** Silver records; 2,194 inserted. |
| Duplicate logical-key check | **0** duplicate keys in either Silver table. |
| Repeated full processing | **0 inserts, 0 updates** for unchanged IBM and NOAA records. |
| IBM backfill, 25–26 September 2026 | **4,680** historical rows selected; **0 inserts, 0 updates** on unchanged data. |
| IBM file-based incremental reprocessing | **33,228** rows from one chosen file; **0 inserts, 0 updates** because they were already in Silver. |
| Schema-drift test | Invalid numeric type and unexpected field correctly classified in isolated QA. |
| Automatic quarantine QA | **2** invalid records rejected to QA quarantine; **0** invalid records reached QA Silver. |
| Production audit logs | `SUCCESS` entries with timestamps and insert/update/rejection counts. |

The isolated schema-drift/quarantine tests demonstrate the validation behaviour without modifying the main datasets. The file-based incremental test demonstrates **selection and safe replay**, not ingestion of a genuinely new batch.

### Quick verification SQL

```sql
SELECT COUNT(*) FROM workspace.silver.ibm_qubit_calibration;
SELECT COUNT(*) FROM workspace.silver.noaa_environment;

SELECT dataset, mode, status, input_rows, rows_inserted,
       rows_updated, rows_quarantined, started_at, finished_at
FROM workspace.silver.pipeline_execution_logs
ORDER BY started_at DESC;

SELECT dataset, error_reason, COUNT(*) AS rejected_rows
FROM workspace.silver.quarantined_records
GROUP BY dataset, error_reason;
```

## 7. Remaining Phase 2 integration checks

**Bronze ingestion metadata and logging:** Both Bronze tables contain `load_timestamp`. For historical records, the timestamps were populated during a migration and therefore do **not** represent the original ingestion times. The pipeline still needs to be verified so future Bronze writes populate `load_timestamp` automatically and Raw-to-Bronze runs capture start/end times, status, and inserted/updated row counts. Any necessary notebook improvements should be tested and committed to the repository.

**Additional verification:** A genuinely new incremental insert and a changed-record update have not yet been demonstrated. The isolated schema-drift tests show correct QA behaviour but do not establish that every malformed raw file is handled by the production ingestion route. Keep Databricks run outputs as evidence alongside the notebooks.

## 8. Next phase

**Phase 3 (Gold and dashboard)** will calculate processor and qubit reliability trends, comparisons, and suitable exploratory analyses connecting calibration records with NOAA observations. The Gold layer and dashboard are **not part of the current implemented Silver pipeline**.
