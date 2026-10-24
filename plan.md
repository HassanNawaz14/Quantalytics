# Quantalytics Phase 3 Comprehensive Execution Plan & Remediation Roadmap

This plan establishes the definitive protocol for human-agent collaboration to finalize data engineering pipelines, resolve all Phase 2 leftovers (specifically including the NOAA weather collection gap), and prepare the environment before connecting the Power BI dashboard and compiling the final architectural report.

---

## 1. NOAA Weather Collection Remediation Protocol (P0.1)

### Problem Analysis (from `Phase2_Leftovers.md`)
- **The Gap:** The live NOAA planetary K-index feed (`planetary_k_index_1m.json`) is a rolling 30-day feed. At analysis time, it covered only ~6 hours (approx. 2026-10-23), while the IBM full-load archive cutoff was `2026-09-25`. This left an unpopulated gap of approximately 4 weeks (late September to mid-October 2026).
- **Cadence & Precision Discrepancy:** Historical Kp records are 3-hourly decimal values. Live Kp records contain both integer `kp_index` and decimal `estimated_kp`. F10.7 historical records are date-only (midnight UTC), while live F10.7 records are timestamped at 20:00 UTC, causing potential date duplication in Silver.

### Step-by-Step Resolution Steps
1. **Adopt Official NOAA Archives for Historical Gap:**
   - Modify or instruct the NOAA collector (`ingestion/noaa_collect.py`) to fetch NOAA's quarterly geomagnetic archive (`.txt` / `.json` summary files from NGDC/SWPC) for the missing historical window (`2026-03-01` to `2026-09-25`).
2. **Canonical Data Standardization in Silver:**
   - Standardize all Kp observations to use decimal estimated Kp and 3-hourly start timestamps.
   - Separate minute-level live Kp records into a dedicated stream or normalize them to the 3-hourly cadence so they do not conflict with historical archive data.
3. **F10.7 Date Normalization:**
   - Force all F10.7 observation timestamps to calendar-date midnight UTC (`yyyy-MM-dd 00:00:00Z`) to prevent duplicate-day entries in Silver and Gold.
4. **Validation Check:**
   - Run verification queries against `workspace.bronze.noaa_space_weather_raw` and `workspace.silver.noaa_environment` to ensure zero gaps between March 2026 and current dates, with unique observation timestamps.

---

## 2. Division of Labor: Agent vs. Human Tasks

### 🤖 Agent Responsibilities (Opencode)
- **Code & Pipeline Implementation:**
  - Create and refine PySpark pipeline notebooks (Bronze, Silver, Gold).
  - Implement and test collection scripts and validation logic.
- **Orchestration & Testing:**
  - Generate Databricks job JSON templates (`quantalytics_ibm_job.json`, `quantalytics_noaa_job.json`).
  - Maintain automated unit tests (`tests/test_collectors.py`) and GitHub Actions CI (`.github/workflows/ci.yml`).
- **Documentation:**
  - Maintain technical design guides (`Phase3/Gold_Layer_Explanation.md`, `plan.md`).

### 👤 Human Responsibilities (User)
- **Databricks Workspace Execution:**
  - Upload/sync notebooks into the Databricks workspace repository.
  - Execute `00_setup_databricks.ipynb`, Bronze notebooks, Silver pipeline, and `spark/gold/1. Gold Pipeline.ipynb` on Databricks serverless compute.
  - Deploy and test Databricks Jobs via the Workflows UI.
- **BI Dashboard Development (`dashboard/Quantalytics.pbix`):**
  - Open Power BI Desktop locally.
  - Connect to the Databricks SQL endpoint / workspace.
  - Import Gold dimension and fact tables.
  - Configure star schema relationships (single active filter path: `dim_backend → dim_qubit → fact_qubit_calibration`).
  - Build interactive visuals answering BQ1–BQ4 and audit/quarantine KPIs.
- **Reporting & FinOps Evidence:**
  - Capture Spark UI DAG screenshots and Azure/Databricks credit consumption metrics.
  - Compile the final architectural PDF report (`report/Quantalytics_Phase3_Report.pdf`).

---

## 3. Action Checklist Before BI Dashboard

- [ ] **Step 1:** Review and approve NOAA archival backfill strategy for the Kp gap.
- [ ] **Step 2:** Execute Databricks setup and Bronze/Silver/Gold notebook runs in the Databricks workspace.
- [ ] **Step 3:** Verify Gold acceptance queries return expected non-zero row counts and zero orphan keys.
- [ ] **Step 4:** Confirm automated CI workflow passes successfully on GitHub.
- [ ] **Step 5:** Proceed to Power BI Desktop semantic model connection and dashboard creation.
