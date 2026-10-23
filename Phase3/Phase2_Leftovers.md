# Phase 2 Leftovers

**Status:** Deferred to Phase 3
**Source:** Phase 2 as-built analysis, 2026-10-23
**Phase 2 implementation:** Frozen. Nothing in this file was applied to Phase 2.

This file is the Phase 3 remediation backlog for defects, gaps, and risks discovered while validating the Phase 2 implementation against the actual collectors, workflows, notebooks, and saved outputs.

Items are ordered by severity. The affected files are listed so Phase 3 work can start without redoing the investigation.

## 1. Priority summary

| Priority | Item | Primary impact |
|---|---|---|
| P0 | NOAA Kp coverage and semantics | Silver Kp series is incomplete and analytically inconsistent |
| P0 | End-to-end orchestration | Bronze and Silver are manual; data freshness is not guaranteed |
| P0 | IBM history reproducibility | "Full" load is bounded and cannot be resumed or backfilled |
| P1 | IBM operational flag | Operational status is not a real signal |
| P1 | IBM cutoff guard | Incremental collection can exceed its requested time window |
| P1 | Incremental overlap and deduplication | No revision handling; NOAA docs and code disagree |
| P1 | Silver-to-Gold contract gap | Gate metrics and collection time are unavailable in Silver |
| P1 | Quarantine and QA isolation | Audit counts are misleading; test data can enter production tables |
| P1 | Automated test coverage | Several tests are manual and have no assertions; no CI |
| P1 | Documentation and repository hygiene | Several docs, links, dependencies, and manifests are stale |
| P2 | Credential and supply-chain hardening | Long-lived PATs and an unpinned GitHub Action |

## 2. P0: Fix before building Gold

### P0.1 Rebuild NOAA Kp ingestion

**Evidence**

- The scheduled NOAA workflow runs weekly.
- The live Kp source is `json/planetary_k_index_1m.json`, a minute-level feed.
- At analysis time the feed returned only 358 rows covering roughly six hours.
- The full-load NOAA batch contains 1,613 3-hourly Kp rows.
- The recorded Silver total is 2,194 rows, which is 372 rows above the 1,822-row full-load batch; the difference matches 358 live Kp rows plus 14 live F10.7 rows.
- The archive cutoff is around 2026-09-25. The live snapshot is around 2026-10-23. Approximately four weeks of Kp are therefore absent.
- Historical Kp is decimal and timestamped at the start of a 3-hour interval.
- Live Kp contains both integer `kp_index` and decimal `estimated_kp`.
- Silver coalesces `source_record.Kp`, then `source_record.kp_index`, then the historical estimated value. Live rows can therefore use the integer field while historical rows use the decimal value.
- F10.7 has a related problem: historical records are date-only and become midnight, while live records are timestamped at 20:00. The same calendar day can appear twice in Silver.

**Required Phase 3 changes**

- Treat the 3-hourly NOAA planetary K-index product as the canonical Kp source.
- Normalize one Kp definition and precision, preferably decimal estimated Kp.
- Use one timestamp convention: the observation time at the start of the 3-hour interval.
- Backfill from the NOAA archive after the latest trusted Silver timestamp.
- Fetch a rolling overlap window on every incremental run.
- Make Kp unique by observation time.
- Add validation for expected cadence, maximum gap, value range, and duplicate timestamps.
- Make F10.7 unique by observation date and document the canonical timestamp.
- Decide whether minute-level Kp is needed at all; if retained, store it separately from the canonical 3-hourly Silver series.
- A candidate source for recent 3-hourly Kp is `https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json`, which returned 59 decimal rows covering about seven days during analysis.

**Affected files**

- `ingestion/noaa_collect.py`
- `.github/workflows/noaa-collect.yml`
- `spark/bronze/noaa_bronze_load.ipynb`
- `spark/silver/1. Silver Pipeline.ipynb`

**Acceptance criteria**

- The Silver Kp series has the expected 3-hour cadence over the selected period, with zero unexplained gaps.
- No duplicate observation timestamps exist.
- All Kp values use one documented definition and decimal precision.
- An incremental run followed by a re-run produces zero new rows.
- A backfill from the latest trusted timestamp closes the gap without duplicating existing rows.
- NOAA Silver totals reconcile exactly with source full-load plus incremental batches.

### P0.2 Add end-to-end orchestration

**Evidence**

- Only `.github/workflows/ibm-collect.yml` and `.github/workflows/noaa-collect.yml` are committed and scheduled.
- No Databricks job definitions are committed.
- The IBM setup guide instructs manual creation of an `ibm_bronze_weekly` job.
- The NOAA guide describes manual notebook execution.
- `spark/silver/1. Silver Pipeline.ipynb` always requires a manual run and a manually supplied source-file widget for incremental processing.
- No Gold notebook or job exists.
- There is no stage dependency, retry policy, or failure propagation between collection, Bronze, and Silver.

**Required Phase 3 changes**

- Define Databricks jobs or workflows for IBM Bronze, NOAA Bronze, and Silver.
- Trigger Bronze after a successful collector run.
- Trigger Silver after successful Bronze processing.
- Pass `dataset`, `mode`, `source_file`, and date-range parameters instead of relying on manual widget entry.
- Add retries, failure propagation, and run-level observability.
- Ensure full/incremental/backfill modes are explicit parameters, not inferred from optional notebook state.
- Add a Gold job only after the Silver contract is finalized.

**Affected files**

- `.github/workflows/ibm-collect.yml`
- `.github/workflows/noaa-collect.yml`
- Databricks job definitions to be added in Phase 3
- `spark/bronze/ibm_bronze_load.ipynb`
- `spark/bronze/noaa_bronze_load.ipynb`
- `spark/silver/1. Silver Pipeline.ipynb`

**Acceptance criteria**

- A single scheduled workflow can collect, load Bronze, and build Silver for both datasets without manual notebook execution.
- A failed stage prevents downstream stages and is visible in logs.
- Re-running the workflow is idempotent.
- A backfill can be launched with explicit parameters.

### P0.3 Make IBM history complete and reproducible

**Evidence**

- The IBM collector uses `TARGET_MB = 200` and `LOOKBACK_DAYS = 365`.
- The 200 MB target stops the full load before the requested period is exhausted.
- Only five snapshots per day are requested.
- Full-load data contains 422,916 rows and 2,711 backend snapshots: approximately 925 fez, 887 kingston, and 899 marrakesh.
- That is far below the roughly 1,825 snapshots per backend expected from five samples per day over a year.
- Kingston has an early gap from approximately 2026-02-24 to 2026-03-06.
- There is no checkpoint/resume or backfill mode after the full-load watermark exists.
- The CSV upload and watermark update are not atomic.
- Bronze deduplicates IBM files by filename, so a retry after a partial failure can duplicate Bronze rows even though Silver deduplicates by logical key.

**Required Phase 3 changes**

- Remove or make configurable the 200 MB full-load target.
- Replace or make configurable the five-snapshots-per-day sampling cap.
- Add checkpointing and resumable backfill.
- Add an explicit historical backfill mode independent of the initial full-load watermark.
- Write the watermark only after successful Bronze completion, or make the upload/watermark transaction recoverable.
- Deduplicate Bronze by a logical key or content hash rather than filename alone.
- Document the resulting history policy: every available snapshot versus a deliberate sample.
- Backfill and reconcile the Kingston gap.

**Affected files**

- `ingestion/ibm_collect.py`
- `.github/workflows/ibm-collect.yml`
- `spark/bronze/ibm_bronze_load.ipynb`
- `spark/silver/1. Silver Pipeline.ipynb`

**Acceptance criteria**

- A full backfill can run to completion, resume after failure, and verify its covered date range.
- Retrying an upload does not duplicate Bronze rows.
- The retained snapshot rate is documented and verifiable.
- The known Kingston gap is either filled or explicitly documented as unavailable at the source.

## 3. P1: Correctness and reliability

### P1.1 Replace the hard-coded IBM operational flag

**Evidence**

- `ingestion/ibm_collect.py:181` writes `"Yes"` for every IBM row.
- All full-load `Operational` values are `"Yes"`.
- The Silver `operational` field is therefore constant.

**Required Phase 3 changes**

- Read the actual backend or qubit operational status from the source payload.
- Preserve the raw value and document any mapping to a boolean.
- Add validation for missing or unexpected status values.

**Affected files**

- `ingestion/ibm_collect.py`
- IBM Bronze and Silver transformations

**Acceptance criteria**

- At least one non-operational source state can be represented.
- The Silver flag is derived from source data, not a constant.
- Known source status values are documented.

### P1.2 Restore the IBM incremental cutoff guard

**Evidence**

- The legacy generator rejects API results newer than the requested cutoff (`data/incremental_generation.py:267-276`).
- The production collector does not implement that guard (`ingestion/ibm_collect.py:189-245`).

**Required Phase 3 changes**

- Reject or clip records whose calibration timestamp exceeds the requested end time.
- Log rejected or clipped records.
- Add a test for future-dated API responses.

**Affected files**

- `ingestion/ibm_collect.py`

**Acceptance criteria**

- No production row can exceed the collection request's end boundary without an explicit override.
- The behavior is covered by an automated test.

### P1.3 Define incremental overlap and revision semantics

**Evidence**

- NOAA incremental filtering uses strict `dt <= saved_dt`.
- No overlap or revision window is implemented.
- The revised NOAA guide claims a small overlap that does not exist.
- IBM incremental behavior relies on timestamps and filenames rather than a documented overlapping merge.
- NOAA historical/live timestamp conventions make duplicate-day handling fragile.

**Required Phase 3 changes**

- Add a documented overlap window for both sources.
- Re-fetch and merge the overlap so revised records can update.
- Define whether newer source data wins and what constitutes a revision.
- Record the overlap window in execution logs.
- Add automated tests for revised records and duplicate-day inputs.

**Affected files**

- `ingestion/ibm_collect.py`
- `ingestion/noaa_collect.py`
- Bronze and Silver notebooks

**Acceptance criteria**

- A revised source record within the overlap window updates Silver.
- A re-run outside the overlap window inserts zero rows.
- The overlap policy is documented and covered by tests.

### P1.4 Close the Silver-to-Gold contract gap

**Evidence**

- The former Phase 2 Gold contract expected gate-level metrics and collection time.
- Silver promotes only T1, T2, readout error, initialization error, and operational status.
- Gate metrics remain inside Bronze `raw_payload`.
- There is no Silver gate table.
- Silver has no per-row `collection_timestamp`; IBM Bronze has only file/batch-level collection time.

**Required Phase 3 changes**

- Decide and document the canonical Gold contract before implementation.
- Add a gate-level Silver table or explicitly exclude gate analysis from Gold.
- Add per-row collection time if collection-lag analysis is required.
- Document units, nullability, and grain for every promoted metric.
- Reconcile the root README and final Phase 2 documentation with the chosen contract.

**Affected files**

- `spark/silver/1. Silver Pipeline.ipynb`
- `README.md`
- Future Gold implementation

**Acceptance criteria**

- Gold requirements can be answered directly from documented Silver columns, or the missing requirements are explicitly out of scope.
- Every metric used by Gold has a documented unit, grain, and lineage.

### P1.5 Isolate QA and make quarantine audit counts truthful

**Evidence**

- `workspace.silver.quarantined_records` has no `run_id`.
- `rows_quarantined` counts bad rows in the selected batch, not rows newly inserted by the quarantine merge.
- The production Silver notebook inserts `QA_TEST_ONLY.json` into the production quarantine table and later deletes it.
- An interrupted run can leave that test record behind.
- Repeated runs can log the same quarantine count even when zero new rows were inserted.

**Required Phase 3 changes**

- Add `run_id` and insertion counts to quarantine processing.
- Separate QA tables or schemas from production tables.
- Remove production writes whose only purpose is a test.
- Make quarantine metrics idempotent and reconcile them with inserted rows.

**Affected files**

- `spark/silver/1. Silver Pipeline.ipynb`
- Quarantine table schema
- QA notebooks

**Acceptance criteria**

- A repeated unchanged run reports zero newly quarantined rows.
- QA never writes to production operational tables.
- Every quarantine row is traceable to a run.

### P1.6 Add real automated tests and CI

**Evidence**

- Test 3 has real assertions and passes.
- Test 2 is a manual schema-drift demonstration.
- Test 4 is manual SQL and file-count verification.
- There are no NOAA-specific tests.
- There is no CI workflow, linting, or automated notebook validation.

**Required Phase 3 changes**

- Add executable tests for IBM and NOAA parsing, key generation, validation, quarantine, and idempotent merges.
- Assert expected counts and rejected-record categories.
- Add a CI workflow that runs non-Databricks unit tests and static checks.
- Separate integration tests that require Databricks credentials from local tests.
- Document how to run the tests.

**Affected files**

- `spark/silver/tests/*`
- `.github/workflows/*`
- Test harness to be added in Phase 3

**Acceptance criteria**

- Every test exits non-zero on failure.
- CI runs on pushes and pull requests.
- NOAA parsing and Kp/F10.7 semantics have dedicated coverage.

### P1.7 Clean up stale and contradictory documentation

**Evidence**

- The IBM setup guide contradicts itself on snapshot retention.
- The revised NOAA guide references `f107_cm_flux.json`, while the code uses `products/10cm-flux-30-day.json`.
- The older `docs/Quantalytics_NOAA_Bronze_Setup_Guide.md` is superseded by the revised guide but remains in the repository.
- `spark/Silver README.md` is stale and says there is no primary key.
- The Silver notebook references a missing `Bronze_Fixes_For_Friend.md`.
- `requirements.txt` omits dependencies needed by the legacy generation scripts.
- No `LICENSE` file exists.
- Placeholder files remain in `docs/init_plan.txt` and `docs/diagrams/empty`.

**Required Phase 3 changes**

- Update the IBM and NOAA setup guides to match the deployed code.
- Remove or rewrite `spark/Silver README.md`.
- Remove the missing-notebook reference from Silver cell 0.
- Add a license.
- Complete dependency metadata for legacy scripts or mark those scripts unsupported.
- Remove or populate empty placeholders.
- Keep the root README, `Phase2/README.md`, and future Phase 3 docs synchronized with code.

**Affected files**

- `docs/Quantalytics_IBM_Setup_Guide.md`
- `docs/Quantalytics_NOAA_Bronze_Setup_Guide_REVISED.md`
- `docs/Quantalytics_NOAA_Bronze_Setup_Guide.md`
- `spark/Silver README.md`
- `spark/silver/1. Silver Pipeline.ipynb`
- `requirements.txt`
- `docs/init_plan.txt`
- `docs/diagrams/empty`
- Repository root for `LICENSE`

**Acceptance criteria**

- A reader can follow each setup guide against the current repository without encountering nonexistent files or contradictory behavior.
- Dependency installation for supported scripts succeeds from a clean environment.
- No stale duplicate README remains.

## 4. P2: Security and supply chain

### P2.1 Harden credentials and actions

**Evidence**

- No secrets are committed.
- The workflows use long-lived `DATABRICKS_HOST` and `DATABRICKS_TOKEN` credentials.
- `databricks/setup-cli@main` is unpinned.
- There is no OIDC-based authentication.

**Required Phase 3 changes**

- Evaluate OIDC or short-lived Databricks credentials.
- Pin GitHub Actions to version tags or commit SHAs.
- Limit token scope and document rotation.
- Add dependency and action update policy.

**Affected files**

- `.github/workflows/ibm-collect.yml`
- `.github/workflows/noaa-collect.yml`

**Acceptance criteria**

- Workflows do not depend on an unpinned floating action.
- Credential lifetime and scope are documented.
- Secrets remain outside the repository.

## 5. Resolved during the Phase 2 documentation freeze

These items were documentation-only and were completed without changing the Phase 2 implementation:

- Deleted the obsolete generated Phase 2 documentation package.
- Replaced `Phase2/README.md` with the final as-built Phase 2 documentation.
- Deleted the obsolete Phase 2 documentation-generation prompt and master specification.
- Corrected the root README QA claims and added the current Phase 2 known limits.
- Added this Phase 3 backlog.

## 6. Explicit non-goals for this cleanup

- No collector changes in Phase 2.
- No workflow schedule changes in Phase 2.
- No notebook logic, schema, table, or data changes in Phase 2.
- No attempted NOAA backfill in Phase 2.
- No Gold implementation in Phase 2.

## 7. Phase 3 exit criteria

Phase 3 can close this backlog when:

1. NOAA Kp and F10.7 are canonical, complete over the supported period, and validated for cadence and duplicates.
2. Collection, Bronze, and Silver run end-to-end from a scheduled job with failure propagation.
3. IBM history can be fully backfilled, resumed, and reconciled without duplicate Bronze rows.
4. The operational flag and cutoff guard reflect real source behavior.
5. Incremental overlap and revision semantics are documented and tested.
6. The Gold contract is satisfied by documented Silver fields, or explicitly narrowed.
7. Quarantine metrics are run-scoped and QA is isolated from production.
8. Automated tests and CI cover both datasets and fail on regressions.
9. Setup documentation matches the code, and repository hygiene and credential risks are resolved.
