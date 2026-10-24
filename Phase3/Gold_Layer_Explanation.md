# Quantalytics Gold Layer Architecture & Dimensional Model

This document provides a comprehensive, non-code explanation of the Gold layer architecture, dimensional modeling choices, business question mapping, and data contracts built for the Quantalytics project.

---

## 1. Purpose of the Gold Layer

The Gold layer represents the business-ready, analytical core of the Quantalytics lakehouse architecture. While Bronze preserves raw landing data and Silver cleans, validates, and types the data into structured operational tables, **Gold transforms Silver into a high-performance dimensional model (Star Schema)** designed specifically for analytical query performance, BI semantic modeling, and executive visualization.

Gold bridges the gap between complex quantum telemetry and space-weather environmental data, answering core longitudinal research questions about quantum processor reliability under solar influence.

---

## 2. Dimensional Model Architecture (Star Schema with Snowflaking)

To balance query performance, storage efficiency, and BI tool usability, we implemented a star schema with a single snowflaked dimension (`dim_qubit`).

### A. Dimension Tables (Descriptive Attributes)
Dimensions provide the contextual context (who, what, when, where) for our metrics:
- **`dim_date`**: Calendar dimension (`date_key` as `yyyyMMdd`, full date, year, quarter, month name, sort order, weekday/weekend flags). Enables time-series slicing from days to years.
- **`dim_time`**: Time-of-day dimension (`time_key` as `HHmmss`, hour, minute, second, 3-hour NOAA collection buckets).
- **`dim_backend`**: IBM quantum processor identifiers (`backend_id`, display name, provider, first/last seen timestamps).
- **`dim_qubit`**: Qubit entities (`qubit_id`, linked via foreign key to `dim_backend`). This is our single snowflaked dimension, preventing backend attribute repetition while keeping hierarchy shallow.
- **`dim_observation_type`**: NOAA environmental observation types (`planetary_kp` and `f10_7_flux`, descriptions, default units).
- **`dim_stage`, `dim_dataset`, `dim_mode`, `dim_status`, `dim_error_reason`**: Operational and quarantine dimensions providing classification context for pipeline execution logs and rejected records.

### B. Fact Tables (Numerical Events & Metrics)
Facts store the measurable, quantitative events and aggregated summaries:
- **`fact_qubit_calibration`**: Atomic qubit-level calibration measurements (`t1_us`, `t2_us`, `readout_error`, `init_error`, `operational` status). Grain: one qubit calibration per timestamp.
- **`fact_environment`**: Canonical NOAA space-weather observations (`kp_index`, `solar_flux`). Grain: one observation per timestamp.
- **`fact_backend_daily`**: Pre-aggregated daily performance layer (`calibration_count`, `qubit_count`, average/min/max T1 & T2, mean readout/init error, operational percentage, daily average/max Kp, daily F10.7 solar flux). Grain: one backend per calendar day. Optimizes dashboard trend rendering by pre-aggregating millions of atomic rows.
- **`fact_pipeline_run`**: Audit log events tracking execution duration, input rows, and status across pipeline stages.
- **`fact_quarantine`**: Quarantined error events tracking invalid records and failure reasons.

---

## 3. Mapping Business Questions (BQ1–BQ4) to Gold

The Gold schema directly satisfies the core project analytical requirements:

| ID | Business Question | Gold Tables Used | Dashboard Visualizations |
|---|---|---|---|
| **BQ1** | How do IBM QPU calibration metrics change over time by backend and qubit? | `fact_backend_daily`, `fact_qubit_calibration`, `dim_date`, `dim_backend`, `dim_qubit` | Daily T1/T2 time-series trends, backend comparisons, qubit heatmaps. |
| **BQ2** | Which backends or qubits show the largest reliability degradation or variability? | `fact_qubit_calibration`, `fact_backend_daily`, `dim_qubit` | Distribution charts, top/bottom reliable qubits, variance error bars. |
| **BQ3** | Do QPU calibration metrics correlate with space-weather activity? | `fact_backend_daily`, `fact_environment`, `dim_observation_type` | Kp/F10.7 trend lines, dual-axis charts, scatter plots with correlation metrics. |
| **BQ4** | Is the pipeline reliable and is bad data under control? | `fact_pipeline_run`, `fact_quarantine`, `dim_status`, `dim_error_reason` | Pipeline execution run timeline, failure logs, quarantine Pareto charts. |

---

## 4. Data Governance & Design Decisions

- **No PII:** Public machine telemetry only; zero personal data, credentials, or user information. No masking required.
- **Idempotency & Late-Arriving Data:** Fact tables use deterministic SHA-256 keys and Delta `MERGE` upserts, ensuring re-runs and historical backfills do not duplicate records.
- **Optimization:** Fact tables are optimized and Z-ordered by `date_key` and `backend_key` to accelerate partition pruning and join performance during BI queries.
