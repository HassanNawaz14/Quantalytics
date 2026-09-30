#### **README.md**	

The front door to the whole project. Explains what Quantalytics is, Phase 2 scope, architecture, environment setup, repository structure, configuration, execution modes, Bronze/Silver overview, security, development order, Definition of Done, traceability matrix, evidence checklist, open decisions, and coding-agent handoff. If someone reads only one file first, this is it.



#### **docs/architecture/phase2-architecture.md**	

Explains how the system is built structurally: IBM/NOAA → external collectors → immutable raw data → Spark Bronze → Silver → future Gold. It defines component responsibilities, boundaries, lineage, replay, failure boundaries, watermarks, security boundaries, and why collection is kept outside Databricks Spark.

docs/data\_contracts/bronze-silver-data-contract.md	Probably the most important file for actual coding. It defines the exact schemas, grains, keys and rules for Bronze and all four Silver tables. It contains Spark StructType examples, field types/nullability/units, stable keys, validation rules, schema drift, quarantine tables, audit tables, watermark tables, and MERGE behavior.



#### **docs/operations/pipeline-runbook.md**	

Tells a developer how to operate the pipeline. It defines full load, incremental, backfill, replay and validation-only modes; what parameters they accept; what happens on failure; how watermarks move; how quarantine replay works; how to recover from failures; and the exact academic demo sequence.



#### **docs/testing/phase2-test-plan.md**	

The QA/testing contract. Contains actual test cases for schemas, casting, Bronze/Silver, idempotency, incremental loads, backfills, replay, failures, schema drift, reconciliation, audit logs and security. Each test says what to set up, what action to perform and what should happen.



#### **docs/research/phase2-to-gold-contract.md**	

Protects the research side of your project. Explains exactly what Phase 3/Gold can rely on from Silver and how later drift, freshness, top-k turnover, stale-selection loss, prediction, topology and space-weather analysis should use the Phase 2 data. It also prevents temporal leakage and correlation→causation mistakes.



#### **docs/decisions/architecture-decision-records.md**	

Records the important “why did we design it this way?” decisions. For example: external collection vs Spark downloads, explicit schemas, Bronze vs Silver separation, Delta MERGE, watermark overlap, quarantine, UTC timestamps, NOAA source granularity, configurable single-qubit gate, etc. This stops future coding agents from randomly changing the architecture.



#### **docs/phase2/documentation-qa-report.md**	

My self-QA report on the documentation itself. I checked that required tables have grains, keys and schemas; execution modes exist; idempotency matches MERGE logic; watermarks are consistent; research requirements are preserved; secrets aren't hardcoded; open decisions are clearly marked, etc.



#### **docs/phase2/package-manifest.md**	

A small integrity file containing SHA-256 hashes of the generated Markdown files. It lets you verify the documentation package hasn't changed after the QA-final version was produced.

