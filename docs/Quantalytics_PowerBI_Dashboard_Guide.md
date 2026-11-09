# Quantalytics Power BI Dashboard – Page and Visual Guide

**Project:** `dashboard/Quantalytics_pbip_v2/Quantalytics/Quantalytics.pbip`
**Semantic model:** `workspace.gold` (Databricks) – 5 fact tables, 10 dimensions
**Report size:** 1280 × 720, 12 pages, Import storage mode
**Last verified against the PBIP definitions:** 2026-11-09

This guide explains every page and every visual in the current report: what the visual is
built from, what it represents, and the insight it is designed to give. It also records the
known limitations that affect how the numbers should be read.

---

## Contents

1. [How to read the report](#1-how-to-read-the-report)
2. [Global controls and shared behaviour](#2-global-controls-and-shared-behaviour)
3. [Page 1 – QPU Health Overview](#3-page-1--qpu-health-overview)
4. [Page 2 – Backend & Qubit Reliability](#4-page-2--backend--qubit-reliability)
5. [Page 3 – Space Weather vs QPU Calibration](#5-page-3--space-weather-vs-qpu-calibration)
6. [Page 4 – Pipeline & Data Quality](#6-page-4--pipeline--data-quality)
7. [Page 5 – Exec Insights](#7-page-5--exec-insights)
8. [Page 6 – Storm Impact & Event Study](#8-page-6--storm-impact--event-study)
9. [Page 7 – Lag & Backend Sensitivity](#9-page-7--lag--backend-sensitivity)
10. [Page 8 – Solar Regimes](#10-page-8--solar-regimes)
11. [Page 9 – Rhythms](#11-page-9--rhythms)
12. [Page 10 – Coherence Physics](#12-page-10--coherence-physics)
13. [Page 11 – Coverage & Trust](#13-page-11--coverage--trust)
14. [Page 12 – About & Data Notes](#14-page-12--about--data-notes)
15. [Metric glossary](#15-metric-glossary)
16. [Known issues and caveats](#16-known-issues-and-caveats)
17. [Maintaining the report](#17-maintaining-the-report)

---

## 1. How to read the report

### 1.1 What the report is

The report is a **Power BI Project (PBIP)** – the report and semantic model are stored as
text definitions, not as a binary `.pbix`. Every table is loaded from Databricks
(`workspace.gold`) on refresh; **no data is embedded in the file**. Insights therefore
reflect the **last successful refresh**, not live IBM or NOAA values.

Data flows:

```
GitHub Actions collectors -> Databricks landing -> bronze -> silver -> gold -> Power BI Import
```

### 1.2 Business questions the report answers

The report is organised around four core business questions (BQ1–BQ4) plus seven
analytical "combined analysis" pages added in v2:

| # | Page | Business question |
|---|---|---|
| 1 | QPU Health Overview | BQ1 – How do IBM QPU calibration metrics change over time, by backend and qubit? |
| 2 | Backend & Qubit Reliability | BQ2 – Which backends or qubits show the largest degradation or variability? |
| 3 | Space Weather vs QPU Calibration | BQ3 – Do calibration metrics correlate with space-weather activity? |
| 4 | Pipeline & Data Quality | BQ4 – Is the pipeline reliable and is bad data under control? |
| 5 | Exec Insights | Which QPUs are healthiest, which react most to space weather, and is the pipeline trustworthy? |
| 6 | Storm Impact & Event Study | How do metrics behave on storm days (Kp ≥ 5) vs quiet days (Kp < 3)? |
| 7 | Lag & Backend Sensitivity | Does calibration respond to Kp with a delay, and which backend is most sensitive? |
| 8 | Solar Regimes | How do QPUs behave across F10.7 and Kp regimes? |
| 9 | Rhythms | When are QPUs calibrated, and do calibration and space weather share daily/weekly/monthly rhythms? |
| 10 | Coherence Physics | Do T1 and T2 behave physically (T2 ≤ 2·T1), and how do error types compare? |
| 11 | Coverage & Trust | How complete is each dataset across backends and time? |
| 12 | About & Data Notes | How the report maps to the Gold layer, KPI definitions, caveats |

### 1.3 Colour and units

- Coherence times **T1 and T2 are in microseconds (µs)**; higher is better.
- **Readout error and init error are fractions** in [0, 1] and are displayed as percentages; lower is better.
- **Kp index** is the geomagnetic activity index (0–9): `< 3` quiet, `3–5` unsettled, `≥ 5` storm.
- **F10.7** is solar radio flux in solar flux units (sfu); higher means a more active Sun.
- A **backend-day** is one row per backend per calendar day in `fact_backend_daily`.
- A **calibration row** is one qubit in one IBM calibration snapshot. One snapshot normally has 156 rows per backend.
- The report uses a dark theme; positive/negative values are not colour-coded by sign, so read the number and the axis.

---

## 2. Global controls and shared behaviour

Every analytical page is built from the same Gold tables and shares the same navigation
and filter conventions.

| Control | Field | Where | What it does |
|---|---|---|---|
| Page navigator (`hdrNav`) | Report pages | Every page | Jumps to any other page. |
| Page header (`hdrTitle`) | Static text | Every page | Page title plus its business question. |
| Date slicer (`slDate`) | `dim_date[Date]` | All pages except About | Filters every visual by calendar date. |
| Backend slicer (`slBackend`) | `dim_backend[Backend]` | All analytical pages except Pipeline | Filters to one or more IBM backends. |
| Qubit slicer (`slQubit`) | `dim_qubit[Qubit Label]` | Page 1 only | Filters to one or more individual qubits (label format `backend · Qnnn`). |
| Dataset slicer (`slDataset`) | `dim_dataset[Dataset]` | Page 4 only | Filters pipeline and quarantine visuals by dataset. |
| Stage slicer (`slStage`) | `dim_stage[Stage]` | Page 4 only | Filters pipeline visuals by pipeline stage. |

**Slicer synchronisation.** The Date slicer uses sync group `gDate` and the Backend slicer
uses `gBackend`. Both groups are present on pages 1–4 and 5–11, **not only on pages 1–4**
(the README predates the v2 pages). A date or backend selection therefore carries across
almost the whole report. The Pipeline page has no backend slicer because pipeline runs are
not backend-specific.

**Filter context matters.** Every KPI is an aggregate over the current slicer selection.
Changing the backend or date selection changes both the values and the ranking-based
metrics (Health Index, "healthiest backend", strongest lag).

---

## 3. Page 1 – QPU Health Overview

**Header:** "QPU Health Overview" – BQ1 · How do IBM QPU calibration metrics change over
time, by backend and qubit?

**Purpose:** the landing page. It gives a one-screen health summary of the selected QPUs,
then lets the user drill into time trends and per-backend comparisons.

**Page chrome**

- `hdrTitle` – title and business question (static text).
- `hdrNav` – page navigator.
- `slDate` – shared `gDate` date slicer.
- `slBackend` – shared `gBackend` backend slicer.
- `slQubit` – page-local qubit slicer (`dim_qubit[Qubit Label]`), useful for investigating one physical qubit.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Calibration Rows** (`kRows`) | Card | `fact_qubit_calibration[Calibration Rows]` | Count of qubit-snapshot rows in the current filter context. | How much calibration evidence the KPIs are based on. A low count means averages are fragile; a high count is dominated by whichever backend was sampled most. |
| **Qubits Measured** (`kQubits`) | Card | `fact_qubit_calibration[Qubits Measured]` | Distinct qubits observed in the selection. | Whether the report is covering the full device (156 qubits per backend) or only a subset. Compare with `Calibration Rows ÷ snapshots` when something looks off. |
| **Avg T1 (µs)** (`kT1`) | Card | `fact_qubit_calibration[Avg T1 (µs)]` | Arithmetic mean T1 over all selected qubit-snapshots. | The headline coherence level. Higher is better; a falling card is the first sign of degradation. |
| **Avg T2 (µs)** (`kT2`) | Card | `fact_qubit_calibration[Avg T2 (µs)]` | Arithmetic mean T2 over the same rows. | Dephasing/coherence level alongside T1. T2 should normally sit below 2×T1 (see page 10). |
| **Avg Readout Error** (`kRO`) | Card | `fact_qubit_calibration[Avg Readout Error]` | Mean readout assignment error (fraction). | Measurement fidelity. Lower is better; a rising value can invalidate comparisons of other metrics. |
| **Data Freshness (h)** (`kFresh`) | Card | `fact_qubit_calibration[Data Freshness (hours)]` | Hours between the newest Silver load timestamp and `NOW()`. | Whether the report is fresh. Because this is **load time**, not IBM calibration time, a healthy card does not prove IBM published new data. |
| **Daily Avg T1 (µs) by Backend** (`lnT1`) | Line chart | X: `dim_date[Date]`; series: `dim_backend[Backend]`; Y: `fact_backend_daily[Daily Avg T1 (µs)]` | Daily backend-level T1 trend, one line per backend. | Long-term degradation, recovery after maintenance, and which backend is consistently stronger. Gaps are days without calibration data. |
| **Daily Avg T2 (µs) by Backend** (`lnT2`) | Line chart | X: `dim_date[Date]`; series: `dim_backend[Backend]`; Y: `fact_backend_daily[Daily Avg T2 (µs)]` | Daily T2 trend per backend. | Whether dephasing degrades in step with T1 or independently (for example after a recalibration). |
| **Avg T1 & T2 by Backend** (`colBackend`) | Clustered column chart | X: `dim_backend[Backend]`; Y: `Avg T1 (µs)`, `Avg T2 (µs)` | Side-by-side average coherence per backend. | A fast "which backend is best right now" comparison without reading the trend lines. |
| **Calibration Snapshots per Day** (`arSnap`) | Area chart | X: `dim_date[Date]`; Y: `fact_backend_daily[Daily Calibration Snapshots]` | Number of calibration snapshots captured per day (sum of `calibration_count`). | Sampling density. Sudden drops or gaps explain flat or noisy KPIs and may indicate collection problems. |
| **Backend Scorecard** (`tbBackend`) | Table | Backend, `Avg T1`, `Avg T2`, `Avg Readout Error`, `Calibration Rows` | Per-backend comparison table. | Compact ranking of coherence, readout quality, and sample size in one place. Use the row count to judge how much to trust each row. |

**How to use this page:** start with the cards for the headline state, check Data Freshness,
then use the trend lines to see whether the state is stable, improving, or degrading. Use the
qubit slicer when a backend-level average hides a single bad qubit.

---

## 4. Page 2 – Backend & Qubit Reliability

**Header:** "Backend & Qubit Reliability" – BQ2 · Which backends or qubits show the largest
degradation or variability?

**Purpose:** separates "average quality" from "stability". A backend can have a good average
T1 but be unusable if it swings wildly between calibrations.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **T1 Std Dev (µs)** (`kStd`) | Card | `fact_qubit_calibration[T1 Std Dev (µs)]` | Population standard deviation of T1 across selected rows. | Absolute spread of coherence. Larger means less predictable hardware. |
| **T1 Variability (CoV)** (`kCov`) | Card | `fact_qubit_calibration[T1 Variability (CoV)]` | T1 standard deviation ÷ mean T1. | Relative stability, comparable across backends with different average T1. Lower is better. |
| **T1 Change vs Previous 7 Days** (`kChg`) | Card | `fact_qubit_calibration[T1 Change vs Prev 7d]` | Last 7 days' average T1 vs the previous 7 days, as a percentage change. | Recent drift. Negative means the latest week is worse than the week before; positive means recovery. The window is anchored to the newest date in the filter context. |
| **Readout Error Std Dev** (`kROs`) | Card | `fact_qubit_calibration[Readout Error Std Dev]` | Standard deviation of readout error. | Whether measurement fidelity is stable. A stable average with a large spread means some qubits are much worse than the headline number. |
| **Heat Map · Daily Avg T1 by Backend × Month** (`mxHeat`) | Pivot table | Rows: Backend; columns: `dim_date[Year-Month]`; values: `Daily Avg T1 (µs)` | Month-by-month T1 matrix per backend. | Monthly seasonality and step changes that a daily line chart can hide. Empty cells are months without data. |
| **T1 Distribution (50 µs buckets)** (`hist`) | Clustered column chart | X: `T1 Bucket (µs)` (50 µs bins); Y: `Calibration Rows` | Histogram of all selected T1 values. | The shape of the distribution: a tight cluster means uniform qubits; a long left tail means a subset of poor qubits; a bimodal shape suggests two device populations. |
| **Qubits Ranked by Avg T1 (lowest first)** (`barLow`) | Clustered bar chart | X: `dim_qubit[Qubit Label]`; Y: `Avg T1 (µs)` | Per-qubit average T1, sorted with the worst first. | Maintenance triage: the qubits at the top of the chart are the lowest-coherence candidates. |
| **Qubit Quality · T1 vs Readout Error** (`scQ`) | Scatter chart | Detail: `Qubit Label`; series: Backend; X: `Avg T1 (µs)`; Y: `Avg Readout Error` | One point per qubit, coloured by backend. | The quality trade-off. The best region is **high T1 and low readout error** (right and low). Outliers show qubits whose readout is poor despite good coherence, or vice versa. |
| **Degradation & Variability by Backend** (`tbDeg`) | Table | Backend, `T1 Change vs Prev 7d`, `T1 Std Dev`, `T1 Variability (CoV)` | Per-backend degradation and stability summary. | Ranks backends by recent drift and variability rather than just average T1. |

**How to use this page:** read the four cards together. A backend with a good average but a
high CoV is less trustworthy than a slightly lower-average, stable backend. Use the histogram
to see whether the average is representative, and the ranked bar chart to find specific qubits
to exclude or investigate.

---

## 5. Page 3 – Space Weather vs QPU Calibration

**Header:** "Space Weather vs QPU Calibration" – BQ3 · Do QPU calibration metrics correlate
with space-weather activity? *(exploratory association, not causation)*

**Purpose:** overlays geomagnetic (Kp) and solar (F10.7) activity on backend calibration to
look for associations. This page is deliberately exploratory.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers, plus a note textbox.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Avg Kp** (`kKp`) | Card | `fact_environment[Avg Kp]` | Average Kp over the selected environment observations. | Overall geomagnetic activity level in the period. |
| **Max Kp** (`kMaxKp`) | Card | `fact_environment[Max Kp]` | Highest Kp observed in the period. | Whether the period contains storm-level activity at all. Without a value ≥ 5, storm comparisons have no storm days. |
| **Avg F10.7 (sfu)** (`kF`) | Card | `fact_environment[Avg F10.7 (sfu)]` | Average daily solar radio flux. | Solar activity regime. Higher F10.7 means a more active Sun. |
| **Storm Days (Kp ≥ 5)** (`kStorm`) | Card | `fact_environment[Days with Kp ≥ 5]` | Count of distinct days whose maximum Kp reached 5 or more. | The sample size behind every "storm" comparison. A small count makes storm conclusions weak. |
| **Correlation · T1 vs Kp** (`kCorr`) | Card | `fact_backend_daily[Corr T1 vs Kp]` | Pearson correlation between daily T1 and daily Kp across backend-days with both values. | Direction and strength of the T1–Kp association. Near zero means no linear association; negative means T1 tends to fall when Kp rises. |
| **Daily Avg Kp (columns) vs Daily Avg T1 (line)** (`cmbKp`) | Line and clustered column combo | X: Date; columns: `Daily Avg Kp`; line (Y2): `Daily Avg T1 (µs)` | Two time series on one time axis. | Visual co-movement: do T1 dips line up with Kp peaks? The shared axis makes timing easy to inspect. |
| **Daily Avg Kp vs Daily Avg T1 (by Backend)** (`scKp`) | Scatter chart | Detail: Date; series: Backend; X: `Daily Avg Kp`; Y: `Daily Avg T1 (µs)` | One point per backend-day. | The shape of the relationship (linear, flat, or threshold-like) and outliers. Each backend gets its own colour. |
| **Solar Flux F10.7 (sfu) Trend** (`lnF107`) | Line chart | X: Date; Y: `Daily Avg F10.7 (sfu)` | Solar-cycle context over time. | Whether the selected period spans a meaningful change in solar activity, which is needed before F10.7 comparisons can mean anything. |
| **Avg T1 & T2 by Geomagnetic Activity** (`colBand`) | Clustered column chart | X: `Kp Band` (Quiet < 3, Unsettled 3–5, Storm ≥ 5, No Kp data); Y: `Daily Avg T1`, `Daily Avg T2` | Average coherence in each Kp band. | A direct quiet-vs-storm comparison. Look for a lower T1/T2 in the Storm band and check the "No Kp data" band before drawing conclusions. |
| **Correlation Summary (Pearson r)** (`mcCorr`) | Multi-row card | `Corr T1 vs Kp`, `Corr T2 vs Kp`, `Corr Readout Error vs Kp`, `Corr T1 vs F10.7`, `Correlation Strength` | Five association metrics together. | A one-glance association dashboard. `Correlation Strength` labels only the **T1–Kp** correlation (Negligible < 0.2, Weak < 0.4, Moderate < 0.6, Strong ≥ 0.6). |
| **Note** (`tbNote`) | Textbox | Static text | Caveat: daily Kp is shared by all backends, and Kp is historical 3-hourly only until NOAA ingestion is rebuilt. | Prevents over-reading correlations that are driven by common daily exposure rather than backend-specific physics. |

**How to use this page:** check Storm Days and Max Kp first. If the period has no storm days,
the storm visuals cannot show a storm effect. Then read the correlation cards as exploratory
signals and confirm them on pages 6–8 before acting.

---

## 6. Page 4 – Pipeline & Data Quality

**Header:** "Pipeline & Data Quality" – BQ4 · Is the pipeline reliable and is bad data under
control?

**Purpose:** operational health of the data platform itself. Everything else in the report
depends on these runs and on the quarantine rules.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date slicer, page-local Dataset and Stage slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Pipeline Runs** (`kRuns`) | Card | `fact_pipeline_run[Pipeline Runs]` | Count of pipeline log entries (tasks/runs) in the selection. | Overall activity volume. The About page clarifies that "success %" is based on log entries, not distinct job invocations. |
| **Success Rate** (`kSucc`) | Card | `fact_pipeline_run[Pipeline Success %]` | Successful log entries ÷ all log entries. | Reliability at a glance. Anything below 100% means at least one failed entry in the selection. |
| **Failed Runs** (`kFail`) | Card | `fact_pipeline_run[Failed Runs]` | Count of entries with status `FAILURE`. | Absolute failure volume; pairs with Success Rate to distinguish "one failure among many" from "everything failed". |
| **Quarantined Rows** (`kQ`) | Card | `fact_quarantine[Quarantined Rows]` | Count of records rejected or flagged into quarantine. | The volume of bad data being kept out of the trusted tables. A rising trend is an early warning even when success rate looks fine. |
| **Avg Run Duration (s)** (`kDur`) | Card | `fact_pipeline_run[Avg Run Duration (s)]` | Mean run duration in seconds. | Performance baseline. Compare across stages on the bar chart to find bottlenecks. |
| **Last Run Finished** (`kLast`) | Card | `fact_pipeline_run[Last Run Finished]` | Newest `Finished At` timestamp. | Freshness of pipeline telemetry. An old value can mean the pipeline stopped, or that the fact table was not refreshed. |
| **Pipeline Runs per Day by Status** (`colRuns`) | Column chart | X: Date; series: `dim_status[Status]`; Y: `Pipeline Runs` | Daily run volume split by SUCCESS/FAILURE. | Failure bursts and trends over time. Isolated failures are less worrying than a growing failure band. |
| **Avg Run Duration by Stage** (`barDur`) | Clustered bar chart | X: `dim_stage[Stage]`; Y: `Avg Run Duration (s)` | Average duration per pipeline stage. | Which stage dominates runtime, and therefore where optimisation or timeout tuning matters. |
| **Rows Inserted & Updated by Stage** (`barRows`) | Clustered bar chart | X: Stage; Y: `Total Rows Inserted`, `Total Rows Updated` | Data volume per stage. | Throughput profile: which stages insert new rows and which mostly update existing rows. |
| **Quarantine Pareto by Error Reason** (`cmbPareto`) | Line and clustered column combo | X: `dim_error_reason[Error Reason]`; columns: `Quarantined Rows`; line (Y2): `Cumulative Quarantine %` | Quarantine volume per reason with a cumulative-share line. | A Pareto view: the few reasons that account for most quarantined rows. Fix the left-hand bars first. |
| **Quarantine by Dataset** (`dnDataset`) | Donut chart | Category: `dim_dataset[Dataset]`; Y: `Quarantined Rows` | Share of quarantined rows by dataset. | Which source is producing the most bad data. This is the first place to look when quarantine volume rises. |
| **Latest Failed Runs** (`tbFail`) | Table | Dataset, Stage, Mode, `Finished At`, `Error Message`, `Failed Runs` | Recent failure detail. | Triage view: the exact dataset/stage/mode and error text for the latest failures. |

**How to use this page:** if Success Rate is below 100%, read the daily status chart and the
Latest Failed Runs table. If quarantine is rising, use the Pareto and dataset donut to find
the dominant reason and source. Check Last Run Finished before trusting any other page.

---

## 7. Page 5 – Exec Insights

**Header:** "Executive Summary" – Which QPUs are healthiest, which react most to space
weather, and is the pipeline trustworthy?

**Purpose:** a one-page decision summary for someone who will not read the detail pages. It
combines the coherence, storm-sensitivity, and pipeline views into ranking cards and one
scorecard.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Healthiest Backend** (`kHealthy`) | Card | `fact_qubit_calibration[Healthiest Backend]` | Name of the backend with the highest Backend Health Index. | The current best overall backend under the selection. |
| **Most Storm-Sensitive** (`kSens`) | Card | `fact_backend_daily[Most Storm-Sensitive Backend]` | Backend with the largest absolute T1 storm impact. | The backend whose coherence is most disturbed by geomagnetic storms. |
| **Most Variable T1** (`kVar`) | Card | `fact_qubit_calibration[Most Variable Backend]` | Backend with the highest T1 coefficient of variation. | The least predictable backend; useful for scheduling and maintenance risk. |
| **T1 on Storm vs Quiet Days** (`kImpact`) | Card | `fact_backend_daily[T1 Storm Impact %]` | Aggregate relative T1 change between storm and quiet days. | The size of the storm effect across the selection. Negative means T1 is lower on storm days. |
| **Storm Days (Kp ≥ 5)** (`kStorm`) | Card | `fact_environment[Days with Kp ≥ 5]` | Count of storm days in the period. | The evidence base for the storm cards. |
| **Pipeline Success** (`kPipe`) | Card | `fact_pipeline_run[Pipeline Success %]` | Pipeline success rate. | Whether the data behind the page is trustworthy. |
| **Backend Health Index (0-100)** (`barHealth`) | Clustered bar chart | X: Backend; Y: `Backend Health Index` | Composite score per backend. | Ranking of overall health. The score weights T1 40%, T2 30%, readout 30%, each normalised against the best backend in the current selection. It is **relative**, not an absolute device rating. |
| **Resilience Quadrant · Health vs Storm Impact** (`scQuad`) | Scatter chart | Detail: Backend; X: `Backend Health Index`; Y: `T1 Storm Impact %` | One point per backend. | Robustness positioning. High health and low storm sensitivity is the ideal quadrant; high health but strongly negative storm impact identifies a good device that is fragile during storms. |
| **Monthly Avg T1 (columns) vs Avg Kp (line)** (`cmbMonth`) | Line and clustered column combo | X: `Year-Month`; columns: `Daily Avg T1 (µs)`; line: `Avg Kp` | Monthly coherence trend with space-weather overlay. | Long-term drift and whether monthly T1 moves with monthly geomagnetic activity. |
| **Backend Scorecard** (`tbScore`) | Table | Backend, Health Index, Avg T1, Avg T2, Avg Readout Error, T1 CoV, T1 Storm Impact % | Full per-backend summary. | The detail behind the ranking cards, including the dimensions that drive the composite score. |
| **Storm vs Quiet Days · % Change by Metric** (`colImpact`) | Clustered column chart | X: Backend; Y: T1, T2, and Readout storm impact % | Per-backend storm effect on three metrics. | Whether storms mainly hurt coherence (T1/T2) or measurement fidelity (readout), and which backend is most affected on each axis. |

**How to use this page:** treat it as a decision snapshot. Confirm the sample size with Storm
Days and Pipeline Success, then use the quadrant and scorecard to choose a backend. Because
the Health Index is normalised to the best backend in the selection, do not compare scores
across different slicer selections.

---

## 8. Page 6 – Storm Impact & Event Study

**Header:** "Storm Impact & Event Study" – Combined analysis · How do calibration metrics
behave on geomagnetic storm days (Kp ≥ 5) vs quiet days (Kp < 3)?

**Purpose:** quantifies the storm effect and shows its timing around the storm day. This is
the detailed evidence behind the executive "storm-sensitive" card.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Storm Days (Kp ≥ 5)** (`kS`) | Card | `fact_environment[Days with Kp ≥ 5]` | Number of storm days. | How much storm evidence exists. |
| **Quiet Days (Kp < 3)** (`kQ`) | Card | `fact_environment[Quiet Days (Kp < 3)]` | Number of quiet days. | The baseline sample size. A large quiet sample and a tiny storm sample produce unstable impact percentages. |
| **Avg T1 · Storm Days** (`kT1S`) | Card | `fact_backend_daily[T1 Storm Days (µs)]` | Average daily T1 on storm-band days. | The storm-condition coherence level. |
| **Avg T1 · Quiet Days** (`kT1Q`) | Card | `fact_backend_daily[T1 Quiet Days (µs)]` | Average daily T1 on quiet-band days. | The comparison baseline. |
| **T1 Storm Impact** (`kImp`) | Card | `fact_backend_daily[T1 Storm Impact %]` | (Storm T1 − Quiet T1) ÷ Quiet T1. | The relative change. Negative = T1 lower on storm days (degradation); positive = higher. |
| **Event Study · T1 change vs baseline around storm days** (`colEvent`) | Clustered column chart | Y: `T1 Δ Day -3`, `-2`, `-1`, `T1 Δ Storm day`, `+1`, `+2`, `+3` | Relative T1 change on the three days before, the storm day, and the three days after, each versus the average of all selected days. | Timing: whether T1 declines before the storm, on the storm day, or recovers afterwards. A pre-storm decline suggests the effect starts earlier than the Kp threshold; a post-storm decline suggests lagged impact. |
| **Avg T1 · Quiet vs Storm Days by Backend** (`colSQ`) | Clustered column chart | X: Backend; Y: `T1 Quiet Days (µs)`, `T1 Storm Days (µs)` | Side-by-side quiet and storm averages per backend. | Which backend loses the most absolute T1 during storms, and whether the ranking of backends changes under storm conditions. |
| **Storm Impact % by Backend & Metric** (`barImp`) | Clustered bar chart | X: Backend; Y: T1, T2, and Readout storm impact % | Relative storm effect for each metric and backend. | Whether a backend is sensitive in coherence, in readout, or both. This distinguishes a physics effect from a measurement effect. |
| **Qubits Most Sensitive to Storms (largest T1 drop first)** (`tbQ`) | Table | Qubit Label, Avg T1 Quiet Days, Avg T1 Storm Days, Qubit T1 Storm Impact % | Per-qubit storm response. | The individual qubits driving the backend-level effect. These are the candidates for exclusion or extra calibration around storms. |
| **Backend-Days by Geomagnetic Band** (`dnBand`) | Donut chart | Category: `Kp Band`; Y: `Backend-Days` | Share of backend-days in Quiet, Unsettled, Storm, and No-Kp bands. | Sample composition. If Storm is a thin slice, the storm comparisons rest on few observations. |

**How to use this page:** verify the storm and quiet sample sizes first. Read the impact card
as a percentage, not a raw µs difference. Use the event study to see whether the effect is
concentrated on the storm day or spread around it. Use the qubit table to move from a
backend-level conclusion to specific hardware.

---

## 9. Page 7 – Lag & Backend Sensitivity

**Header:** "Lag & Backend Sensitivity" – Combined analysis · Does calibration respond to Kp
with a delay, and which backend is most sensitive?

**Purpose:** tests whether the calibration response to geomagnetic activity is immediate or
delayed, up to seven days.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Strongest Lag (days)** (`kLag`) | Card | `fact_backend_daily[Strongest T1~Kp Lag (days)]` | The lag (0, 1, 2, 3, 5, or 7 days) with the largest absolute T1–Kp correlation. | Whether the strongest association is same-day or delayed. A non-zero lag suggests a response that outlasts the storm day. |
| **Strongest \|r\| (T1 ~ Kp)** (`kR`) | Card | `fact_backend_daily[Strongest T1~Kp r]` | The largest absolute correlation across those lags. | The best explanatory strength found. Correlations are still exploratory; a large value with a small sample is not robust. |
| **Same-day r (T1 ~ Kp)** (`kR0`) | Card | `fact_backend_daily[Corr T1 vs Kp]` | Pearson correlation at lag 0. | The immediate association, for comparison with the strongest lag. |
| **Strength (same-day)** (`kStr`) | Card | `fact_backend_daily[Correlation Strength]` | Text label for the same-day T1–Kp correlation: Negligible, Weak, Moderate, or Strong. | A plain-language reading of the correlation card. It uses absolute value thresholds of 0.2, 0.4, and 0.6. |
| **r(T1, Kp) by Lag (days)** (`cLagT1`) | Clustered column chart | Y: `r T1 vs Kp · lag 0d`, `1d`, `2d`, `3d`, `5d`, `7d` | Correlation at each lag, as six series. | The lag profile for T1. Compare the columns to see where the association peaks. |
| **r(T2, Kp) by Lag (days)** (`cLagT2`) | Clustered column chart | Y: `r T2 vs Kp · lag 0d` … `lag 7d` | Correlation at each lag for T2. | Whether T2 responds on the same schedule as T1 or with a different delay. |
| **r(Readout Error, Kp) by Lag (days)** (`cLagRO`) | Clustered column chart | Y: `r Readout vs Kp · lag 0d` … `lag 7d` | Correlation at each lag for readout error. | Whether measurement fidelity responds to space weather, and when. Readout changes can be an instrumentation effect rather than a coherence effect. |
| **Same-day r(T1, Kp) by Backend** (`barT1`) | Clustered bar chart | X: Backend; Y: `Corr T1 vs Kp` | Per-backend same-day correlation. | Which backend's T1 is most associated with Kp, independent of the strongest-lag summary. |
| **Same-day r(Readout Error, Kp) by Backend** (`barRO`) | Clustered bar chart | X: Backend; Y: `Corr Readout Error vs Kp` | Per-backend same-day readout correlation. | Which backend's measurement error tracks geomagnetic activity. |
| **Backend Space-Weather Sensitivity** (`tbSens`) | Table | Backend, `T1 Storm Impact %`, `Corr T1 vs Kp`, `Corr T2 vs Kp`, `Corr Readout Error vs Kp` | Sensitivity summary per backend. | A ranked, combined view of storm impact and lag-0 associations. Use it to choose the most and least sensitive backend. |

**How to use this page:** start with the Strongest Lag and Same-day r cards. If they disagree,
read the lag charts. Check the sample size and the date slicer: lag correlations need enough
overlapping days, and the measures return blank when fewer than five paired observations exist.

**Layout note:** the three lag charts put the lag measures directly on the value axis and
bind no category field. They therefore render as one clustered group with a measure legend,
rather than as a lag-ordered x-axis. The numbers are correct, but the visual reading is a
little harder than a true lag axis would be (see Known issues).

---

## 10. Page 8 – Solar Regimes

**Header:** "Solar Regimes" – Combined analysis · How do QPUs behave across solar-flux
(F10.7) and geomagnetic (Kp) regimes?

**Purpose:** moves beyond a single scalar correlation and compares calibration across
combined solar and geomagnetic conditions.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Avg F10.7 (sfu)** (`kF`) | Card | `fact_environment[Avg F10.7 (sfu)]` | Average daily solar flux. | The solar activity level of the selection. |
| **Max F10.7 (sfu)** (`kFm`) | Card | `fact_environment[Max F10.7 (sfu)]` | Peak daily solar flux. | Whether the period contains high-solar-flux days, not just an elevated average. |
| **r(Readout Error, F10.7)** (`kC2`) | Card | `fact_backend_daily[Corr Readout Error vs F10.7]` | Pearson correlation between daily readout error and F10.7. | Whether measurement fidelity tracks solar flux. Exploratory only. |
| **r(T1, F10.7)** (`kC1`) | Card | `fact_backend_daily[Corr T1 vs F10.7]` | Pearson correlation between daily T1 and F10.7. | Whether coherence tracks solar flux. Compare with the Kp correlation on page 3. |
| **F10.7 vs Readout Error (backend-days)** (`scF`) | Scatter chart | Detail: Date; series: Backend; X: `Daily Avg F10.7 (sfu)`; Y: `Daily Avg Readout Error` | One point per backend-day. | The shape of the F10.7–readout relationship and outliers. A flat cloud means no meaningful association. |
| **Avg T1 (columns) & Readout Error (line) by F10.7 Band** (`cmbBand`) | Line and clustered column combo | X: `F10.7 Band` (Low < 100, Moderate 100–150, High 150–200, Very high ≥ 200, No data); columns: `Daily Avg T1`; line: `Daily Avg Readout Error` | Average T1 and readout error per solar-flux band. | Whether calibration changes across solar regimes, and whether T1 and readout move together or in opposite directions. |
| **Kp (columns) vs F10.7 (line) over Time** (`cmbTr`) | Line and clustered column combo | X: Date; columns: `Daily Avg Kp`; line: `Daily Avg F10.7 (sfu)` | Two space-weather series on one time axis. | Whether Kp and F10.7 are correlated in the selected period. If they move together, it is hard to attribute an effect to one rather than the other. |
| **Regime Matrix · Avg T1 (µs)** (`mxT1`) | Pivot table | Rows: `Kp Band`; columns: `F10.7 Band`; values: `Daily Avg T1 (µs)` | T1 across nine combined regimes. | Which combination of geomagnetic and solar conditions coincides with the lowest or highest T1. |
| **Regime Matrix · Readout Error** (`mxRO`) | Pivot table | Rows: `Kp Band`; columns: `F10.7 Band`; values: `Daily Avg Readout Error` | Readout error across the same regimes. | Whether the regime effect is on coherence, on measurement, or both. |
| **Regime Matrix · Backend-Days (sample size)** (`mxN`) | Pivot table | Rows: `Kp Band`; columns: `F10.7 Band`; values: `Backend-Days` | Number of backend-days in each regime cell. | The reliability check for the two matrices. A strong-looking T1 cell with a sample size of 1 or 2 is not evidence. |

**How to use this page:** read `mxN` first, then `mxT1` and `mxRO`. Ignore regime cells with
very few backend-days. Use the Kp-vs-F10.7 line to judge whether the two drivers can be
separated in this period at all.

---

## 11. Page 9 – Rhythms

**Header:** "Rhythms · Time of Day & Seasonality" – Combined analysis · When are QPUs
calibrated, and do calibration and space weather share daily, weekly or monthly rhythms?

**Purpose:** separates real calibration rhythms from artifacts of the calibration schedule.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **When Does IBM Calibrate? Snapshots by Hour (UTC)** (`colHrN`) | Clustered column chart | X: `dim_time[Hour]`; Y: `Calibration Rows` | Calibration-row volume by hour of day (UTC). | The calibration schedule. Strong concentration in a few hours means time-of-day comparisons are really comparisons of the same calibration batches. |
| **Avg T1 by Calibration Hour (UTC)** (`colHrT`) | Clustered column chart | X: `dim_time[Hour]`; Y: `Avg T1 (µs)` | Average T1 for rows calibrated in each hour. | Apparent diurnal T1 variation. It can be a real rhythm or an artifact of which backend/calibration batch runs at that hour; use the backend slicer to check. |
| **Avg Kp by 3-Hour Window (UTC)** (`colKpW`) | Clustered column chart | X: `dim_time[Time Window]` (3-hour UTC window); Y: `Avg Kp` | Average geomagnetic activity per 3-hour window. | The daily rhythm of Kp itself. Compare with the calibration-hour chart before attributing a T1 rhythm to space weather. |
| **Weekday · Avg T1 (columns) vs Avg Kp (line)** (`cmbWd`) | Line and clustered column combo | X: `dim_date[Weekday Short]`; columns: `Daily Avg T1`; line: `Avg Kp` | Weekly pattern of T1 and geomagnetic activity. | Whether apparent weekday effects are actually space-weather effects, or vice versa. |
| **Month · Avg T1 (columns) vs Avg Kp (line)** (`cmbMo`) | Line and clustered column combo | X: `Year-Month`; columns: `Daily Avg T1`; line: `Avg Kp` | Monthly pattern of T1 and geomagnetic activity. | Seasonal drift and whether it tracks monthly Kp. This is a coarser version of the page 5 monthly combo. |
| **Heat Map · Calibration Snapshots, Weekday × Hour** (`mxWH`) | Pivot table | Rows: `Weekday Short`; columns: `Hour`; values: `Calibration Rows` | Snapshot density by weekday and hour. | The detailed calibration calendar. Empty or pale cells are low-sampling windows; dark cells are over-represented in averages. |

**How to use this page:** read `colHrN` and `mxWH` before interpreting any "rhythm" as
hardware behaviour. In a sampled dataset, when calibration happens can be a stronger driver of
the daily average than any physical effect.

---

## 12. Page 10 – Coherence Physics

**Header:** "Coherence Physics" – Combined analysis · Do T1 and T2 behave physically
(T2 ≤ 2·T1), and how do gate errors compare across backends?

**Purpose:** a physical-plausibility and error-mechanism view. It checks whether the numbers
obey the expected T2 ≤ 2·T1 relationship and compares readout and initialization errors.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Avg T1 (µs)** (`kT1`) | Card | `fact_qubit_calibration[Avg T1 (µs)]` | Mean T1. | Coherence level for context. |
| **Avg T2 (µs)** (`kT2`) | Card | `fact_qubit_calibration[Avg T2 (µs)]` | Mean T2. | Dephasing level for context. |
| **T2 / T1 Ratio** (`kR`) | Card | `fact_qubit_calibration[T2/T1 Ratio]` | Mean T2 ÷ mean T1. | Physical consistency at a glance. Values around 1–2 are expected; a value well below 1 means T2 is much shorter than T1. |
| **Rows with T2 > 2×T1** (`kV`) | Card | `fact_qubit_calibration[T2 > 2×T1 Rate]` | Share of calibration rows where T2 exceeds twice T1. | A data-quality flag. Physically, T2 cannot exceed 2·T1; a non-trivial rate suggests bad fits, label errors, or inconsistent calibration payloads. |
| **Readout ÷ Init Error** (`kRI`) | Card | `fact_qubit_calibration[Readout/Init Error Ratio]` | Mean readout error ÷ mean init error. | Whether measurement error or state-preparation error dominates. A ratio above 1 means readout is the larger error source. |
| **Qubit Coherence · T1 vs T2** (`scT`) | Scatter chart | Detail: Qubit Label; series: Backend; X: `Avg T1 (µs)`; Y: `Avg T2 (µs)` | One point per qubit. | The joint coherence profile. Points should sit on or below the T2 = 2·T1 bound; points above it deserve a data-quality check. Clusters reveal whether backends have distinct coherence populations. |
| **T2/T1 Ratio Distribution (values above 2.0 are implausible)** (`histR`) | Clustered column chart | Y: `Calibration Rows` | Intended histogram of the T2/T1 ratio. | Intended to show how many qubit-snapshots are near the physical bound and how many are implausible. **Current issue:** the category field (`T2/T1 Ratio Bucket`) is not bound, so the visual collapses to one total bar instead of a distribution. See Known issues. |
| **T2/T1 Ratio by Backend** (`barR`) | Clustered bar chart | X: Backend; Y: `T2/T1 Ratio` | Average ratio per backend. | Which backend has the most (or least) T2 relative to T1. A backend with a low ratio may have a dephasing problem despite acceptable T1. |
| **Qubit Errors · Init vs Readout** (`scE`) | Scatter chart | Detail: Qubit Label; series: Backend; X: `Avg Init Error`; Y: `Avg Readout Error` | One point per qubit. | Whether initialization and readout errors are correlated, and which qubits are bad on both axes (top-right). Qubits with high init but low readout point to a preparation problem rather than a measurement problem. |
| **Readout vs Init Error by Backend** (`colE`) | Clustered column chart | X: Backend; Y: `Avg Readout Error`, `Avg Init Error` | Side-by-side error averages per backend. | Which backends have a readout-heavy or init-heavy error profile. This is the backend-level version of the scatter. |
| **T2/T1 Ratio over Time** (`lnR`) | Line chart | X: Date; series: Backend; Y: `T2/T1 Ratio` | Daily ratio trend. | Whether the physical relationship is stable over time or shifts after recalibration, maintenance, or a data-pipeline change. |

**How to use this page:** check the "Rows with T2 > 2×T1" card and the scatter first. If the
implausible rate is high, treat other coherence conclusions with caution. Then use the error
scatter and backend bars to separate measurement problems from coherence problems.

**Header note:** the page subtitle says "gate errors", but the visuals on this page compare
**readout and initialization errors**. Gate-level metrics are not in Silver, as the About page
states. Read "gate errors" as a shorthand for the error metrics shown here.

---

## 13. Page 11 – Coverage & Trust

**Header:** "Coverage & Trust" – Read this before trusting the insights · how complete is each
dataset across backends and time?

**Purpose:** the honesty check for the whole report. Every other page can look convincing on a
thin sample; this page shows how thin the sample actually is.

**Page chrome:** `hdrTitle`, `hdrNav`, shared Date and Backend slicers.

### Content visuals

| Visual | Type | Fields | What it represents | Insight it gives |
|---|---|---|---|---|
| **Calendar Days** (`kCal`) | Card | `fact_backend_daily[Calendar Days]` | Number of calendar days in the selected date range. | The denominator for coverage. |
| **Backend-Days with Data** (`kBD`) | Card | `fact_backend_daily[Backend-Days]` | Number of backend-day rows with calibration data. | The numerator for calibration coverage. |
| **Calibration Coverage** (`kCov`) | Card | `fact_backend_daily[Data Coverage %]` | Backend-days ÷ (calendar days × distinct backends). | How complete the calibration dataset is. 100% means every backend has data every day in the selection. |
| **Kp Coverage** (`kKp`) | Card | `fact_environment[Kp Coverage %]` | Days with a Kp value ÷ calendar days. | Whether space-weather comparisons have a complete geomagnetic record. A low value limits pages 3, 6, 7, 8, and 9. |
| **F10.7 Coverage** (`kF`) | Card | `fact_environment[F10.7 Coverage %]` | Days with an F10.7 value ÷ calendar days. | Completeness of the solar-flux record. |
| **Qubits Measured** (`kQ`) | Card | `fact_qubit_calibration[Qubits Measured]` | Distinct qubits observed. | Whether the calibration data covers the full device or a subset. |
| **Calibration Coverage % by Backend** (`barCov`) | Clustered bar chart | X: Backend; Y: `Data Coverage %` | Coverage per backend. | Which backend has the most gaps. A low-coverage backend contributes fewer rows to every average and may look artificially stable or noisy. |
| **Heat Map · Backend-Days by Month** (`mxCov`) | Pivot table | Rows: Backend; columns: `Year-Month`; values: `Backend-Days` | Monthly data density per backend. | Where the gaps are in time. It shows whether a backend's coverage is consistently good or concentrated in a few months. |
| **Space-Weather Coverage by Month** (`colKp`) | Clustered column chart | X: `Year-Month`; Y: `Kp Coverage %`, `F10.7 Coverage %` | Monthly completeness of the two space-weather series. | Months where the space-weather record is missing or partial. Correlations should be discounted for those months. |
| **Qubits Measured per Day by Backend** (`lnQ`) | Line chart | X: Date; series: Backend; Y: `Qubits Measured` | Distinct qubits captured per day. | Partial calibrations. A drop from the normal 156 means the day's average covers only part of the device, which can bias trend comparisons. |
| **Backend Coverage Summary** (`tbCov`) | Table | Backend, First Calibration Date, Last Calibration Date, Backend-Days, Data Coverage %, Qubits Measured, Calibration Rows | Per-backend coverage and volume summary. | The definitive "how much data do I have?" table. Use the first/last dates to see whether backends cover the same period, and the row counts to judge sample balance. |

**How to use this page:** before quoting an insight from any other page, check Calibration
Coverage, Kp Coverage, F10.7 Coverage, and the per-backend table. If coverage is low, narrow
the date range or state the limitation with the insight.

---

## 14. Page 12 – About & Data Notes

**Header:** "About & Data Notes" – How this report maps to the Quantalytics Gold layer.

**Purpose:** in-report documentation. It is the authoritative explanation of the model,
KPI definitions, and known limitations.

**Page chrome:** `hdrTitle`, `hdrNav`, and four static textboxes.

| Visual | Type | Content | Why it matters |
|---|---|---|---|
| **Business questions** (`note0`) | Textbox | BQ1–BQ4 and the pages they map to (pages 1–4). | Explains the report's original structure. The v2 pages extend it with combined analyses. |
| **Semantic model** (`note1`) | Textbox | Star schema over `workspace.gold`: dimensions `dim_date`, `dim_time`, `dim_backend`, `dim_qubit` (snowflaked), `dim_observation_type`, `dim_stage`, `dim_dataset`, `dim_mode`, `dim_status`, `dim_error_reason`; facts `fact_qubit_calibration`, `fact_environment`, `fact_backend_daily`, `fact_pipeline_run`, `fact_quarantine`; all relationships many-to-one, single direction. | Documents the model contract. |
| **KPI definitions** (`note2`) | Textbox | T1/T2 arithmetic mean in µs with nulls ignored; readout/init error as fractions in [0, 1] shown as %; pipeline success = successful log entries ÷ all log entries; data freshness = hours since latest Silver load, not source collection lag. | Prevents misreading the headline numbers. |
| **Coverage notes and known limitations** (`note3`) | Textbox | Kp is historical 3-hourly only until NOAA ingestion is rebuilt; F10.7 is one value per calendar day; IBM history is sampled and bounded by the collector; Operational Rate is not a real health signal until the collector reads true operational state; gate-level metrics and collection-lag analysis are excluded; correlations are exploratory, not causal; Databricks parameters can be changed in Transform data. | The report's own caveat list. Read it before presenting conclusions. |

---

## 15. Metric glossary

### 15.1 Headline metrics

| Metric | Definition | Direction | Notes |
|---|---|---|---|
| `Avg T1 (µs)` | Average of `fact_qubit_calibration[T1 (µs)]` over rows. | Higher is better | Row-weighted, so backends/days with more snapshots contribute more. |
| `Avg T2 (µs)` | Average of T2 over rows. | Higher is better | T2 should normally be ≤ 2·T1. |
| `Avg Readout Error` | Average readout assignment error. | Lower is better | Fraction in [0, 1]. |
| `Avg Init Error` | Average initialization error. | Lower is better | Fraction in [0, 1]. |
| `Calibration Rows` | Row count in `fact_qubit_calibration`. | Context | One row = one qubit in one snapshot (normally 156 rows per snapshot). |
| `Qubits Measured` | Distinct qubit keys. | Context | Use to detect partial calibrations. |
| `Daily Avg T1/T2 (µs)` | Average of the daily aggregates in `fact_backend_daily`. | Higher is better | Used by all trend visuals; each point is a backend-day. |
| `Data Freshness (hours)` | `DATEDIFF(Latest Silver Load, NOW(), HOUR)`. | Lower is better | Measures the last Silver load, not IBM's calibration time. |

### 15.2 Reliability and storm metrics

| Metric | Definition | How to read it |
|---|---|---|
| `T1 Std Dev (µs)` | Population standard deviation of T1. | Absolute dispersion; larger = less stable. |
| `T1 Variability (CoV)` | `T1 Std Dev ÷ Avg T1`. | Relative dispersion; comparable across backends. |
| `T1 Change vs Prev 7d` | Last 7 days vs previous 7 days, relative change. | Negative = recent week worse; positive = recovery. |
| `Readout Error Std Dev` | Standard deviation of readout error. | Stability of measurement fidelity. |
| `Backend Health Index` | `100 × (0.4·T1 Index + 0.3·T2 Index + 0.3·Readout Index)`. | Relative 0–100 score; **normalised to the best backend in the current selection**. With one backend selected it is always 100. |
| `Days with Kp ≥ 5` | Days whose maximum Kp reaches 5. | Storm sample size. |
| `Quiet Days (Kp < 3)` | Days whose maximum Kp is below 3. | Baseline sample size. |
| `T1 Storm Impact %` | `(Storm T1 − Quiet T1) ÷ Quiet T1`. | Negative = T1 lower on storm days (degradation); positive = higher. |
| `T2 / Readout Storm Impact %` | Same calculation for T2 and readout error. | Tells you whether the storm effect is on coherence, measurement, or both. |
| `T1 Δ Day ±N` | T1 on the day N days from a storm day, vs the all-selected daily average, minus 1. | Negative = below baseline. Use to see pre/post-storm timing. |

### 15.3 Space weather and correlation metrics

| Metric | Definition | How to read it |
|---|---|---|
| `Avg Kp` / `Max Kp` | Average / maximum `fact_environment[Kp Index]`. | Geomagnetic activity level. |
| `Avg F10.7 (sfu)` / `Max F10.7 (sfu)` | Average / maximum daily solar flux. | Solar activity level. |
| `Corr T1 vs Kp` | Pearson r between daily T1 and daily Kp across backend-days. | Exploratory linear association; blank if fewer than five paired observations. |
| `Corr T2 vs Kp`, `Corr Readout Error vs Kp` | Same for T2 and readout. | Check whether the response is metric-specific. |
| `Corr T1 vs F10.7`, `Corr Readout Error vs F10.7` | Pearson r against solar flux. | Exploratory; compare with the Kp correlations. |
| `Correlation Strength` | Label from \|r\| for T1–Kp: Negligible < 0.2, Weak < 0.4, Moderate < 0.6, Strong ≥ 0.6. | Plain-language reading; only applies to T1–Kp. |
| `r ... lag Nd` | Pearson r using Kp from N days earlier. | Lag profile; lags are 0, 1, 2, 3, 5, and 7 days. |
| `Strongest T1~Kp Lag (days)` | Lag with the largest absolute T1–Kp correlation. | 0, 1, 2, 3, 5, or 7; blank if no valid correlation. |

### 15.4 Coverage metrics

| Metric | Definition | How to read it |
|---|---|---|
| `Calendar Days` | Count of days in `dim_date` under the date filter. | Coverage denominator. |
| `Backend-Days` | Row count of `fact_backend_daily`. | One row per backend per day with data. |
| `Data Coverage %` | `Backend-Days ÷ (Calendar Days × distinct backends)`. | 100% = every backend has data every day. |
| `Kp Coverage %` | Days with a Kp value ÷ Calendar Days. | Completeness of the geomagnetic record. |
| `F10.7 Coverage %` | Days with an F10.7 value ÷ Calendar Days. | Completeness of the solar-flux record. |
| `Qubits per Backend-Day` | Distinct qubits ÷ distinct days. | Defined but not currently placed on a page; useful as a partial-calibration check. |

### 15.5 Pipeline and quarantine metrics

| Metric | Definition | How to read it |
|---|---|---|
| `Pipeline Runs` | Row count of `fact_pipeline_run` (log entries). | Activity volume. |
| `Pipeline Success %` | Successful log entries ÷ all log entries. | Reliability; 100% = no failed entries. |
| `Failed Runs` | Entries with status `FAILURE`. | Absolute failure volume. |
| `Avg Run Duration (s)` | Mean run duration. | Performance baseline. |
| `Total Rows Inserted / Updated` | Sums of the corresponding log fields. | Throughput profile by stage. |
| `Quarantined Rows` | Row count of `fact_quarantine`. | Bad-data volume kept out of trusted tables. |
| `Cumulative Quarantine %` | Cumulative share of quarantined rows when reasons are ranked descending. | Pareto reading. |

---

## 16. Known issues and caveats

These are behaviours of the current report definition, not opinions about the data.

### 16.1 Report-definition issues

1. **`histR` (T2/T1 Ratio Distribution) has no category field.** The visual only binds the
   `Calibration Rows` measure. The calculated column `fact_qubit_calibration[T2/T1 Ratio Bucket]`
   exists in the model but is not used by any visual. As a result, the chart renders a single
   total bar instead of a distribution. Fix: add `T2/T1 Ratio Bucket` to the visual's
   **Category** role.
2. **The three lag charts and the event-study chart use measure series without a category
   axis.** `cLagT1`, `cLagT2`, `cLagRO`, and `colEvent` bind multiple measures to the value
   axis and no field to the category axis. The values are correct, but the lags/offsets appear
   as a colour legend in one clustered group rather than as an ordered x-axis. Fix: reshape the
   lag measures into a dimension (or use a line chart with a lag field) if the profile needs to
   read left-to-right.
3. **The Coherence Physics header mentions "gate errors", but the page shows readout and
   initialization errors.** Gate-level metrics are not in Silver, as the About page states.
   The header is broader than the visuals.
4. **The README's slicer statement is out of date.** It says Date and Backend slicers are
   synced across pages 1–4; in the PBIP, the v2 pages also use the same `gDate` and `gBackend`
   sync groups. Selections therefore carry across almost the entire report.
5. **The About page is last in the page order but is the active page on open.** This is a
   presentation choice; it means the report opens on documentation rather than on page 1.

### 16.2 Data and statistical caveats

1. **Correlations are exploratory, not causal.** Kp is a daily value shared by all backends,
   so backend-level correlations share a common exposure. Use pages 6–8 to check timing and
   sensitivity before making a causal claim.
2. **Small storm samples.** `Days with Kp ≥ 5` can be small relative to quiet days. Check the
   storm/quiet cards and the Backend-Days by Geomagnetic Band donut before trusting a storm
   percentage.
3. **Coverage denominator.** `Data Coverage %` divides by `Calendar Days × distinct backends
   present in fact_backend_daily`. A backend with **zero** rows in the selection is not counted
   in the denominator, which can inflate the percentage. Use the Backend Coverage Summary and
   the monthly heat map to check for fully missing backends.
4. **Health Index is relative.** Each index is normalised against the best backend in the
   current selection. Scores are not comparable across different backend/date selections, and a
   single selected backend always scores 100.
5. **Row-weighted averages.** T1/T2/readout cards average over qubit-snapshot rows. A backend
   or day with more calibration snapshots contributes more weight than one with fewer. Use the
   daily aggregates on the trend pages for a balanced view.
6. **Data Freshness measures load time, not source time.** A fresh card means Silver loaded
   recently, not that IBM published a new calibration.
7. **Kp history is incomplete.** The About page states that Kp is historical 3-hourly only
   until NOAA ingestion is rebuilt; F10.7 is one value per calendar day. Coverage cards on
   page 11 quantify the resulting gaps.
8. **IBM history is sampled and bounded.** The collector asks five times per day and stops the
   full load at a target size, so the report reflects available calibration history, not a
   complete one-year record.
9. **`Operational Rate` is not a health signal.** The collector writes `Operational = Yes` for
   every row, so the measure cannot detect a non-operational qubit. It is not used on any page,
   but it is present in the model.
10. **Import mode.** All tables are imported on refresh. Insights are as current as the last
    successful refresh; there is no DirectQuery live view.
11. **Correlation minimum sample.** Correlation measures return blank when fewer than five
    paired observations exist. Blank cells in lag or regime visuals often mean "not enough
    data", not "zero correlation".
12. **Time zone.** `dim_time[Hour]` is UTC, and the calibration-hour visuals are labelled UTC.
    Do not read them as Pakistan local time.

---

## 17. Maintaining the report

### 17.1 Refresh

1. Open `dashboard/Quantalytics_pbip_v2/Quantalytics/Quantalytics.pbip` in Power BI Desktop
   with the PBIP and PBIR preview features enabled.
2. Home → Transform data → Edit parameters:
   - `DatabricksHost` – workspace hostname, without `https://`
   - `DatabricksHttpPath` – SQL warehouse HTTP path
   - `DatabricksCatalog` – `workspace`
   - `DatabricksSchema` – `gold`
3. Refresh and authenticate in Desktop. Credentials are not stored in the project.
4. Verify page 11 Coverage & Trust before publishing insights.

### 17.2 Where to change what

| Change | File / place |
|---|---|
| Page order, names, size | `Quantalytics.Report/definition/pages/pages.json` and each page's `page.json` |
| Visual layout, fields, titles | `Quantalytics.Report/definition/pages/<page>/visuals/<visual>/visual.json` |
| Measures, calculated columns, display folders | `Quantalytics.SemanticModel/definition/tables/*.tmdl` |
| Relationships | `Quantalytics.SemanticModel/definition/relationships.tmdl` |
| Databricks connection parameters | `expressions.tmdl` / Transform data in Desktop |
| Theme | `Quantalytics.Report/StaticResources/RegisteredResources/Quantalytics.json` |
| This guide | `docs/Quantalytics_PowerBI_Dashboard_Guide.md` |

### 17.3 Definition of "current"

This guide was generated from the PBIP definitions on 2026-11-09. If a page, visual, measure,
or field is added later, update the corresponding section here. The PBIP files are the source
of truth; this guide is the human-readable map.
