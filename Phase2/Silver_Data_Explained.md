# Silver Data - A Plain-English Guide

**Purpose:** understand the Silver data well enough to explain it clearly in the Phase 2 presentation and to suggest useful BI views. This is a data guide, not a code guide.

**Prepared:** 23 Oct 2026, for Phase 2 presentation preparation.

**In one sentence:** Silver gives us a clean timeline of "how healthy was each qubit" plus "what was the space weather around it" - but it is a narrow dataset built by watching real systems, not a controlled experiment, so it cannot prove cause and effect.

**Companion document:** `Phase2/Phase2_Presentation_Cheatsheet.md` covers the code, notebook cells, and saved proof for the rubric.

---

## 1. What "Silver" means here

The project has three layers:

| Layer | Plain meaning | What it is for |
| --- | --- | --- |
| **Bronze** | The raw copy | Keeps everything exactly as it arrived, including raw text/JSON |
| **Silver** | The cleaned, typed layer with repeated rows removed | Reliable rows that can be filtered, grouped, and analysed |
| **Gold** | Future summaries and models | Not built in Phase 2 |

Silver is **not** a complete copy of the source. It is deliberately narrow: it promotes the important measurements, adds technical tracking columns, and leaves bulky raw details in Bronze.

Silver contains **four tables**:

| Table | What it holds | Is it measurement data? |
| --- | --- | --- |
| `workspace.silver.ibm_qubit_calibration` | Qubit health measurements from 3 IBM quantum computers | Yes |
| `workspace.silver.noaa_environment` | Kp and F10.7 space-weather readings | Yes |
| `workspace.silver.pipeline_execution_logs` | What the pipeline did on each run | No - audit data |
| `workspace.silver.quarantined_records` | Records that failed validation | No - rejected data |

A useful way to think about it:

- **IBM table** = report cards for each qubit at each health check.
- **NOAA table** = a weather diary for the space around Earth.
- **Logs** = the pipeline's diary.
- **Quarantine** = the "rejected" tray, kept for investigation instead of being silently deleted.

---

## 2. Three things to remember

1. **One IBM row is not one calibration.** A calibration covers 156 qubits, so it produces 156 rows. Count **distinct calibration timestamps**, not rows, when counting health checks.
2. **Blank is not zero.** A blank T1, T2, or init error means "not reported" or "not available." It does not mean the value was zero.
3. **IBM and NOAA do not share a key.** They are joined by **time**, and only after both sides are summarised. Raw-row joins multiply rows and produce misleading results.

A fourth practical rule: **use the median for the typical value and the 95th percentile (p95) for the high end.** A small number of very poor readings can pull the average away from what a typical qubit experiences.

---

## 3. Data at a glance

| | IBM qubit data | NOAA space-weather data |
| --- | --- | --- |
| **Silver rows at freeze** | **456,612** | **2,194** |
| **What one row means** | One qubit on one machine at one calibration time | One observation type (Kp or F10.7) at one time |
| **Main entities** | 3 backends, 156 qubits each | Kp index and F10.7 solar radio flux |
| **Time span** | 24 Feb 2026 to 30 Sep 2026 | 1 Mar 2026 to 25 Sep 2026, plus a short recent addition |
| **Natural rhythm** | About 5 calibration snapshots per machine per day | Kp: 3-hourly in history, minute-level live. F10.7: daily |
| **Core measures** | T1, T2, readout error | Kp, solar flux |
| **Technical columns** | Key, hash, source file, load time | Key, hash, source file, load time |

The larger source samples used for the numbers in this guide are:

- IBM full load: **422,916 rows**, **2,711 snapshots**.
- NOAA archive: **1,822 rows** = **1,613 Kp + 209 F10.7**.
- Silver adds the later increments: IBM **456,612** = **422,916** full + **33,228** increment + **468** backfill, with **2,927** snapshots; NOAA **2,194** rows.

Here, **full load** means the first large batch, and **increments** are later additions. **Archive** means historical NOAA data; **live** means the recent NOAA feed.

**Important:** the IBM medians below come from the large full-load sample. The official Silver count is 456,612. The increments can shift a median slightly; the values are for explanation, not a promise that the final table will match to the last decimal.

---

## 4. IBM qubit calibration table

**Table:** `workspace.silver.ibm_qubit_calibration`

**One row means:** one qubit, on one quantum computer, at one calibration timestamp.

### 4.1 A real example row

On **IBM Kingston**, at **2026-09-30 19:22:44 UTC**, for **qubit 0**:

| Column | Value | Plain meaning |
| --- | --- | --- |
| `backend_id` | `ibm_kingston` | This is the Kingston machine |
| `calibration_timestamp` | `2026-09-30T19:22:44Z` | The health check happened then (UTC) |
| `qubit_id` | `0` | Qubit number 0 on that machine |
| `t1_us` | `298.09` | Its memory lasted about 298 microseconds |
| `t2_us` | `395.11` | Its timing stayed clear for about 395 microseconds |
| `readout_error` | `0.02124` | About a 2.1% chance of being read incorrectly |
| `init_error` | blank | The source did not report a start-up error |
| `operational` | `Yes` | IBM marked it as usable |

That one row is a snapshot of one qubit. It is not a full calibration run, and it is not a time range.

### 4.2 What the 12 columns mean

| Column | Kind | Unit | One-line meaning |
| --- | --- | --- | --- |
| `record_key` | Text | Fingerprint | Unique ID for this row |
| `backend_id` | Text | Name | Which quantum computer |
| `calibration_timestamp` | Date/time | UTC | When the health check was measured |
| `qubit_id` | Number | Index | Which qubit on that machine (0-155) |
| `t1_us` | Number | Microseconds | How long the qubit keeps its stored energy |
| `t2_us` | Number | Microseconds | How long the qubit keeps its timing pattern |
| `readout_error` | Number | Chance (0-1) | Chance the qubit's answer is read incorrectly |
| `init_error` | Number | Chance (0-1) | Chance the qubit starts in the wrong state |
| `operational` | Yes/No | Flag | IBM's "usable" marker |
| `source_payload_hash` | Text | Fingerprint | Fingerprint of the raw source record |
| `source_file` | Text | Filename | Which batch file supplied the row |
| `load_timestamp` | Date/time | UTC | When the pipeline wrote the Silver row |

### 4.3 The analysis columns in detail

#### `backend_id` - which quantum computer

**Plain meaning:** The name of the machine that produced the calibration: `ibm_fez`, `ibm_kingston`, or `ibm_marrakesh`.

**Use it for:** filtering to one machine; comparing machines; grouping fleet summaries; checking whether a pattern appears on more than one machine.

**Watch out:** There are only three machines, and their physical locations are not in Silver. A difference between machines can come from the machine, the time window, the mix of qubits, or operational conditions. Do not crown a "best machine" from one snapshot.

#### `calibration_timestamp` - when the health check happened

**Plain meaning:** The UTC date and time when IBM measured that qubit. All 156 rows from the same check share the same timestamp.

**Use it for:** the time axis; counting snapshots; spotting gaps; aligning with Kp/F10.7; before/after comparisons; time-of-day views; finding sudden changes.

**Watch out:** The time spacing is irregular - roughly five snapshots per machine per day, not a regular hourly feed. The full history is bounded by the collector's 200 MB target and five-snapshot-per-day sampling. It is not a complete year. Kingston also has a sparse early period from about 24 Feb to 6 Mar 2026.

#### `qubit_id` - which qubit

**Plain meaning:** A seat number from 0 to 155 on that machine. Qubit 5 on Kingston and qubit 5 on Fez are not the same physical object.

**Use it for:** qubit-level heatmaps; best/worst lists; repeated-problem detection; recovery tracking; comparing the same qubit over time.

**Watch out:** It is only an index. Silver does not include the physical layout, connectivity map, or calibration recipe. Do not assume qubit IDs line up meaningfully across machines.

#### `t1_us` - memory lifetime

**Plain meaning:** After a qubit is given energy, T1 is roughly how long it holds that energy before it leaks away. It is measured in microseconds (one millionth of a second). **Generally, longer is better.**

**Use it for:** tracking memory stability; ranking qubits; spotting sudden drops or recoveries; comparing machines; selecting stable qubits; before/after changes; one input to a relative health score.

**Watch out:** Blank is not zero. Compare a qubit with its own history and with the same time window on other machines. There is no universal pass/fail line. One low reading can be a transient calibration result, so look for repeated readings. T1 and T2 tend to move together somewhat (their full-load relationship score is about 0.50, where 0 is no link and 1 is a perfect link), but they are not the same thing.

#### `t2_us` - timing/phase memory

**Plain meaning:** T2 is roughly how long the qubit keeps its delicate timing pattern before it blurs. It is also in microseconds, and **generally, longer is better.**

**Use it for:** timing stability; comparing T2 with T1; detecting noise or degradation; tracking recovery; checking whether a bad period affects both memory and timing; space-weather exploration.

**Watch out:** T2 can be larger than T1 in a row; that is allowed because they measure different things. It is not an error rate. Blank is not zero. Use repeated readings before declaring a trend.

#### `readout_error` - chance of a wrong read

**Plain meaning:** When the qubit's answer is measured, this is the chance the answer is read incorrectly. `0.01` means **1%** (about 1 in 100); `0.10` means **10%** (about 1 in 10). **Lower is better.**

**Use it for:** ranking measurement quality; finding qubits that are hard to read; tracking drift; estimating measurement error; comparing machines; combining with T1/T2 for a fuller health view; spotting outliers.

**Watch out:** It is a chance from 0 to 1, not a percentage. A few values are much higher than the rest: the full-load median is 1.07%, but about 9.5% of rows are above 5% and 4.3% are above 10%. A simple average is pulled upward by a minority of poor readings. It is not an error from a quantum operation (a gate) and it is not a guarantee for every circuit.

#### `init_error` - chance of a wrong start

**Plain meaning:** The chance the qubit is prepared in the wrong starting state. `0.002` means **0.2%** (about 2 in 1,000). **Lower is better.**

**Use it for:** if the source supplies it, separating start-up quality from measurement quality; comparing preparation with readout; tracking data completeness.

**Watch out:** In the production full load and the committed incremental/latest files, this column is **entirely blank**. That means "not reported," not zero. A sample file does contain values (125 of 156 rows, from 0.0002 to 0.042), so the column is wired for data but is not usable in a current production dashboard. Do not build a chart that treats blank as zero.

#### `operational` - IBM's usable flag

**Plain meaning:** A yes/no marker from IBM saying whether the qubit is considered operational. `Yes` means usable.

**Use it for:** filtering out a qubit if IBM ever reports `No`; basic data-quality checks.

**Watch out:** Every production row currently says `Yes`, so the column has no variation and is **not a health signal**. It is not uptime, it is not a failure log, and it does not mean the qubit is error-free. In some tools, converting the text to a number can make it look blank; the actual source value is `Yes`.

### 4.4 Technical columns

These are useful for tracking and joining, but they are not measurements to chart.

| Column | What it is | Use it for | Do not use it as |
| --- | --- | --- | --- |
| `record_key` | A unique fingerprint built from backend, timestamp, and qubit | De-duplication, row identity, joins | A quality score or a business label |
| `source_payload_hash` | A fingerprint of the raw source record | Detecting whether the source content changed; deciding whether an update is needed | A health measure |
| `source_file` | The batch filename that supplied the row | Where the row came from; finding the original batch; distinguishing full vs incremental files; investigating older batches | A time axis |
| `load_timestamp` | When the pipeline wrote the Silver row | Freshness checks; batch debugging | The measurement time |

A few details:

- The Silver data tables do **not** keep the raw source record. Raw detail stays in Bronze. Quarantine is the one Silver table that keeps the raw record.
- The data tables do not have a `dataset`, `mode`, `run_id`, or `quarantine` flag. Those live in `source_file` and the logs.
- `load_timestamp` is not the same as `calibration_timestamp`. Historical Bronze load times came from a one-time migration, not the original collection, so do not use load time to measure how long collection took.
- There is no per-row collection timestamp in Silver. If that question matters, it is a Bronze/collector question.

### 4.5 What the IBM numbers show

**Overall full-load sample (422,916 rows):**

| Measure | Median | Mean | Range | p95 |
| --- | --- | --- | --- | --- |
| T1 | **171.2 µs** | 185.8 µs | 1.68 - 565.87 µs | 338.9 µs |
| T2 | **103.7 µs** | 121.2 µs | 1.18 - 823.43 µs | 292.0 µs |
| Readout error | **1.07%** | 2.63% | 0.049% - 57.1% | 8.74% |

The mean is higher than the median for all three, especially readout. That is a sign that a few very high values are pulling the average up: most readings are decent, but a minority are much worse. For BI, show both the median and a high-end measure such as p95.

**By machine, using the full-load sample for medians and Silver for snapshot counts:**

| Backend | Full-load rows | Median T1 | Median T2 | Median readout | Silver snapshots |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ibm_kingston` | 138,372 | **239.7 µs** | **134.6 µs** | **0.90%** | 959 |
| `ibm_marrakesh` | 140,244 | 173.9 µs | 88.0 µs | 1.16% | 971 |
| `ibm_fez` | 144,300 | 132.4 µs | 96.1 µs | 1.17% | 997 |

In this slice, Kingston has the highest median T1 and T2; Fez has the lowest T1; Marrakesh has the lowest T2. Readout medians are close. These are plain descriptions, not an official ranking: the machines have different coverage, and Kingston's early period is sparse. For a fair comparison, use the same date window and show snapshot counts.

**Other important facts:**

- Missing T1: **1,121 of 422,916** (0.265%). Missing T2: **1,741** (0.412%). Readout has **no** missing values.
- No negative or zero T1/T2 values. Readout values are inside 0-1 with no out-of-range values.
- No duplicate IBM logical keys in the full load. The Silver quality check reports **0 duplicate keys** in either Silver data table.
- T1 and T2 are positively related (about 0.50), but they are not interchangeable. A qubit can be good at one and weaker at the other.
- T2's maximum (823 µs) is much higher than its p95 (292 µs). Very high values are unusual but not automatically errors. Do not delete them without a stated rule.
- The Silver table has 2,927 snapshots. The full load has 2,711; the later batches add 216.

---

## 5. NOAA space-weather table

**Table:** `workspace.silver.noaa_environment`

**One row means:** one observation type at one observation time.

The table holds two different kinds of observation:

- **Kp** - a 0-9 score for how disturbed Earth's magnetic field is.
- **F10.7** - a daily measure of the Sun's radio brightness at a wavelength of 10.7 cm.

They share one table, but they do **not** share a value column.

### 5.1 Two real example rows

| `source` | `observation_type` | `observation_timestamp` | `kp_index` | `solar_flux` | `source_unit` |
| --- | --- | --- | ---: | ---: | --- |
| `noaa_swpc_kp` | `planetary_kp` | `2026-09-25T12:00:00Z` | **3.67** | blank | `index` |
| `noaa_swpc_f107` | `f10_7_flux` | `2026-09-25T00:00:00Z` | blank | **105** | `sfu` |

The first row says: at noon UTC on 25 Sep 2026, the Kp index was 3.67 on the 0-9 scale - disturbed, but below the 5.0 storm threshold.

The second row says: for 25 Sep 2026, the F10.7 solar radio flux was 105 sfu - a moderate level.

These are **two different observations on the same day**, not duplicates. If you summarise by date, you get one Kp summary and one F10.7 value.

### 5.2 What the 10 columns mean

| Column | Kind | Unit | One-line meaning |
| --- | --- | --- | --- |
| `record_key` | Text | Fingerprint | Unique ID for this observation |
| `source` | Text | Feed name | Which NOAA feed supplied it |
| `observation_type` | Text | Category | `planetary_kp` or `f10_7_flux` |
| `observation_timestamp` | Date/time | UTC | When the observation applies |
| `kp_index` | Number | Index (0-9) | Earth's magnetic disturbance score |
| `solar_flux` | Number | sfu | F10.7 solar radio brightness |
| `source_unit` | Text | Unit | `index` or `sfu` |
| `source_payload_hash` | Text | Fingerprint | Fingerprint of the raw source record |
| `source_file` | Text | Filename | Which batch file supplied the row |
| `load_timestamp` | Date/time | UTC | When the pipeline wrote the Silver row |

### 5.3 The analysis columns in detail

#### `source` - which NOAA feed

**Plain meaning:** A label for the feed: `noaa_swpc_kp` or `noaa_swpc_f107`. "SWPC" is NOAA's Space Weather Prediction Center.

**Use it for:** grouping by feed; seeing where a row came from; a quick filter when you already know the feed name.

**Watch out:** It does **not** tell you whether the row is historical archive data or live data. Both use the same two labels. Use `source_file` when archive-vs-live matters. `observation_type` is the friendlier filter for analysis.

#### `observation_type` - Kp or F10.7

**Plain meaning:** The kind of observation: `planetary_kp` for the Earth's magnetic disturbance index, `f10_7_flux` for solar radio brightness.

**Use it for:** filtering to one measure; labelling charts; preventing accidental mixing of Kp and F10.7.

**Watch out:** One row is one type, not both. A Kp row will have a blank flux, and an F10.7 row will have a blank Kp. That is normal, not missing data.

#### `observation_timestamp` - when the observation applies

**Plain meaning:** The UTC time attached to the observation. The meaning of the time depends on the feed:

- Historical Kp: the **start of a 3-hour interval** (`00`, `03`, `06`, ... `21`). The value describes that interval.
- Live Kp: minute-level reporting.
- Historical F10.7: daily, stamped at `00:00`.
- Live F10.7: daily, stamped at `20:00`.

**Use it for:** the time axis; matching IBM calibrations to the Kp interval that contains them; assigning a daily F10.7 value; checking coverage; checking delayed effects.

**Watch out:** The time spacing changes between archive and live. Do not compare raw row frequencies. The archive has 202 Kp dates, including one final partial day, and 209 F10.7 dates; 7 dates have F10.7 but no Kp. The archive/live overlap can produce two F10.7 rows for the same calendar day because their timestamps differ.

#### `kp_index` - how disturbed the magnetic field is

**Plain meaning:** A 0-9 scale for how disturbed Earth's magnetic field is around the planet. **Higher means more disturbed.** `0` is calm; `5` and above is the usual storm threshold. It is not a percentage and it is not a pass/fail score for a qubit; it describes conditions.

**Use it for:** storm flags; daily maximum and daily mean; background conditions; overlaying with IBM qubit trends; testing whether unusually disturbed days line up with unusually poor qubit readings; checking delayed effects.

**Watch out:** Historical Kp is decimal and often in thirds (`2.67`, `3.67`); live Kp uses an integer `kp_index` (`2`, `3`). Mixed precision is expected. Kp is global, not local to a machine. Blank on F10.7 rows is normal; do not fill it with zero. A zero is a real calm reading, while a blank is simply not a Kp row.

#### `solar_flux` - how radio-bright the Sun is

**Plain meaning:** F10.7 is the Sun's radio brightness at 10.7 cm, measured in **solar flux units (sfu)**. **Higher means more solar activity.** It is not a particle count, it is not a percentage, and it is not a pass/fail score for a qubit; it describes conditions.

**Use it for:** daily solar background; long-term activity trend; grouping high/low activity days; comparing with Kp; checking delayed effects; context for qubit-performance periods.

**Watch out:** It is a daily global measure, not a local measurement at a quantum computer. Blank on Kp rows is normal and is not zero. Do not average it with Kp; they have different units and meanings. The archive has no duplicate F10.7 dates, but archive/live overlap can duplicate a calendar day.

#### `source_unit` - the unit label

**Plain meaning:** `index` for Kp rows and `sfu` for F10.7 rows. It is a guardrail that tells you what the number means.

**Use it for:** chart labels; sanity checks; making sure a calculation only uses one unit at a time.

**Watch out:** Never add or average Kp and F10.7 together. The unit column makes that mistake easier to catch. It is a label, not an analysis measure.

### 5.4 Technical columns

The same four technical columns as IBM, with the same roles:

| Column | What it is | Use it for | Do not use it as |
| --- | --- | --- | --- |
| `record_key` | Unique fingerprint built from source, type, and timestamp | De-duplication, row identity, joins | A weather measure |
| `source_payload_hash` | Fingerprint of the raw source record | Change detection | A quality score |
| `source_file` | The batch filename | Where it came from; distinguishing archive vs live; finding the original batch | A time axis |
| `load_timestamp` | When the pipeline wrote the row | Freshness; pipeline debugging | The observation time |

### 5.5 What the NOAA numbers show

**Archive sample (1,822 rows):**

| Feed | Rows | Median | Mean | Range | Coverage |
| --- | ---: | ---: | ---: | ---: | --- |
| Kp | 1,613 | **2.0** | 2.14 | 0 - 7.33 | 202 dates; 8 readings/day on 201 full days, 5 on the final partial day; 7 dates missing |
| F10.7 | 209 | **122 sfu** | 125.3 | 93 - 203 sfu | 209 daily dates; no duplicate archive dates |

At freeze, the NOAA Silver total is **2,194** = **1,822 archive + 372 live** (358 Kp + 14 F10.7). A later live fetch returned 388 rows because the live F10.7 endpoint returned 30 daily values; that is a moving window, not a correction to the frozen Silver count. Use 2,194 for the official total.

**What stands out:**

- Kp was mostly quiet: median 2.0, p95 4.33, p99 5.67.
- There were **58** Kp readings at or above 5.0, spread across **26 days** with a daily maximum at or above 5.0. Only **2 days** had a daily mean at or above 5.0.
- F10.7 was moderate: median 122, p95 158.6, p99 200.5, maximum 203. There were **20 days** at or above 150 sfu and **3 days** at or above 200 sfu.
- The maximum Kp was **7.33**, which is a strong storm reading on the 0-9 scale.
- Same-day Kp mean and F10.7 had almost no link in this archive slice (a relationship score of about **0.03**, where 0 is no link and 1 is a perfect link). They are different signals, and any effect may be delayed. Do not use one as a proxy for the other.
- The current live sample is short and mostly quiet (Kp 0-2, F10.7 92-137). Do not treat that as the overall climate; the archive contains much stronger activity.
- Kp has 7 full dates missing (F10.7 is present on those dates) and a partial final day. Do not assume every date has all 8 Kp readings.
- The Silver quality check reports **0 duplicate logical keys** in the NOAA table as well.

---

## 6. The two support tables

These are part of Silver, but they are **not** qubit or weather measurements. They tell you whether the pipeline and the data quality are healthy.

| Table | What one row means | Count |
| --- | --- | --- |
| `workspace.silver.pipeline_execution_logs` | One file processed into Bronze, or one dataset processed into Silver | Grows with every run |
| `workspace.silver.quarantined_records` | One record that failed validation | Grows when bad records arrive |

### 6.1 `pipeline_execution_logs` - the pipeline diary

| Column | Plain meaning | Use it for |
| --- | --- | --- |
| `run_id` | ID for that pipeline run | Tracing one run |
| `stage` | `Raw-to-Bronze` or `Bronze-to-Silver` | Splitting pipeline stages |
| `dataset` | Which dataset (`ibm`, `noaa`) | Filtering by source |
| `mode` | Full, incremental, or backfill (loading older data) | Understanding batch type |
| `parameters` | Inputs such as file or date range | Reproducing a run |
| `started_at` | UTC start time | Duration and timing |
| `finished_at` | UTC finish time | Duration and timing |
| `status` | Success or failure | Health checks |
| `input_rows` | Rows read | Volume checks |
| `rows_inserted` | New rows written | Change volume |
| `rows_updated` | Existing rows changed | Change volume |
| `rows_quarantined` | Bad rows rejected | Data-quality checks |
| `error_message` | Error or warning text | Debugging |
| `load_timestamp` | When the log row was written | Audit timing |

How to read it:

- **One row is a file or dataset run, not a data row.** Do not join it to qubit rows and expect one-to-one.
- `input_rows` vs `rows_inserted` + `rows_updated` + `rows_quarantined` shows whether a run was a no-op or did real work.
- A `SUCCESS` row can still contain a warning that the source format changed (schema drift) in `error_message`. Read both status and message.
- The log table grows on every run; it is an audit trail, not a snapshot you should count over time without context.
- Use it for pipeline health, not for hardware health. A quiet pipeline does not mean healthy qubits.

### 6.2 `quarantined_records` - the rejected tray

| Column | Plain meaning | Use it for |
| --- | --- | --- |
| `record_key` | Fingerprint of the rejected record | Matching it to the source |
| `dataset` | Which dataset rejected it | Filtering by source |
| `source_file` | Which batch contained it | Finding the bad batch |
| `raw_payload` | The original record | Investigating the failure |
| `error_reason` | Why it was rejected | Grouping failure types |
| `load_timestamp` | When it was quarantined | Timing the incident |

Examples of `error_reason` values include missing backend, invalid qubit, invalid timestamp, a source-format change (schema drift), invalid T1, T1 not positive, readout out of range, init out of range, invalid operational flag, an unreadable NOAA record, unknown source, invalid Kp, and invalid solar flux.

How to read it:

- A high quarantine count usually means a source change, a schema change, or a pipeline problem - **not** a hardware failure.
- The bad row is kept with its raw record, so you can inspect it instead of guessing.
- There is **no `run_id`** in the quarantine table. Use `source_file` and `load_timestamp` to investigate.
- The quality test intentionally writes one artificial record into the production quarantine table. Do not count that test row as a real data incident.
- Quarantine protects the clean tables. A row here never reached Silver's data tables.

---

## 7. What is deliberately not in Silver

This matters because it explains what you should not promise in the presentation.

- **IBM operation-level (gate-level) errors are not in Silver.** The Bronze raw record contains more fields (ID error, RX, RZ, SX, Pauli-X, XSLOW, CZ, RZZ, operation lengths, readout length, measurement probabilities, MEASURE, MEASURE_2), but there is no Silver operation-level table.
- **No raw source record in the two Silver data tables.** Bronze keeps it; only the quarantine table keeps raw text for failed rows.
- **No backend location, connectivity map, physical qubit layout, or calibration recipe.**
- **No job or workload outcomes.** There is no application success rate, circuit depth, user error, or queue data.
- **No local space-weather measurement.** Kp and F10.7 are global, not measured at the quantum computer.
- **No maintenance, outage, or uptime history.** `operational` is a constant `Yes` in the current data.
- **No per-row collection timestamp.** Silver has the measurement time and the pipeline write time, but not a separate collection time.
- **No `run_id` in the two data tables.** Use the logs for runs.
- **No `dataset`, `mode`, or `quarantine` flag in the data tables.** Use `source_file` and the support tables.
- **No Gold summary, health score, or prediction model.** Those are future work.

If someone asks for operation-level (gate-level) errors, the honest answer is: "The current Silver layer is intentionally narrow. Operation metrics remain in Bronze and are not promoted yet."

---

## 8. How to combine IBM and NOAA without misleading yourself

There is no shared key. `record_key` is source-specific. The only sensible bridge is **time**.

### 8.1 A safe daily recipe

1. **Summarise IBM by backend and UTC date.** For each backend/day, calculate:
   - median T1
   - median T2
   - median readout error
   - count of distinct calibration snapshots
   - count of qubits with data
   - optionally p95 readout error
2. **Summarise NOAA by UTC date.**
   - Kp: daily maximum and daily mean (and count of readings)
   - F10.7: one value per date (remove duplicate days if archive and live both exist)
3. **Join on the UTC date.**
4. **Plot separately by backend.** A three-machine average can hide a machine-specific pattern.
5. **Show coverage.** A day with few IBM snapshots or missing Kp should not look as strong as a day with full coverage.

### 8.2 A safe 3-hour recipe

- Put IBM calibrations into the same 3-hour buckets used by historical Kp (`00-03`, `03-06`, ...).
- Use the Kp value for the bucket that contains the calibration.
- Use the F10.7 value for that date.
- Do not pretend F10.7 varies within the day; it is daily.

### 8.3 Matching on time, and delayed effects

- Use the **latest NOAA reading at or before** the IBM calibration time. Do not use a future value.
- For delayed effects, compare IBM day `D` with Kp/F10.7 on `D-1`, `D-2`, or `D-3`.
- Show all delays you tested or state your rule. Do not quietly pick the delay with the best-looking result.

### 8.4 The main traps

- **Do not join on `record_key`.** IBM and NOAA keys are built from different things.
- **Summarise before joining.** Raw IBM rows are at qubit level; raw NOAA rows are at observation level. A raw join multiplies rows.
- **Do not fill missing Kp with zero.** Missing is not calm.
- **Remove duplicate F10.7 days.** Archive is stamped `00:00`, live at `20:00`; both can exist for one calendar day.
- **Do not average Kp and F10.7 together.** They have different units and meanings.
- **Use the same date window.** The archive overlap is roughly 1 Mar to 25 Sep 2026. IBM extends to 30 Sep; NOAA's recent feed covers some later dates, but the archive does not.
- **Do not claim cause and effect.** This is data from watching real systems, not a controlled experiment. Space weather may move together with qubit performance, but other things are changing at the same time, effects may be delayed, and there are only three machines.
- **Storm days are rare.** With only 26 storm days in the Kp archive, patterns are uncertain. Call it an early, uncertain signal.

---

## 9. BI ideas that fit this data

| BI question | What to show | Suggested visual |
| --- | --- | --- |
| How healthy is the fleet right now? | Latest snapshot per backend: median T1, T2, readout, qubit count | KPI cards + small bars |
| Which qubits are weak? | Per-qubit median and p95 for T1, T2, readout | Heatmap + top/bottom table |
| Is performance drifting? | Weekly medians and p95 by backend | Line chart with snapshot counts |
| Did something change suddenly? | Largest qubit-level drops between snapshots | Annotated line + outlier table |
| Does space weather line up? | Daily IBM medians vs Kp max/mean and F10.7 | Separate small charts, scatter plot, delayed-effect view |
| Is the data complete? | Missing T1/T2, snapshot counts, Kp coverage | Bar chart + coverage table |
| Is the pipeline healthy? | Success rate, inserted/updated/quarantined counts | KPI cards + run table |
| Which batch caused a problem? | `source_file`, `load_timestamp`, log status | Table with click-through detail |
| Can we rank "best" qubits? | A transparent score built from T1/T2/readout, with component columns | Ranked table, not a single hidden number |

Practical rules for the first BI pages:

1. **Fleet overview:** one row per backend with the latest common snapshot, medians, and coverage.
2. **Qubit heatmap:** qubits down the side, time across the top, colour by T1/T2/readout. Let users click through to one qubit.
3. **Trend and outliers:** weekly medians plus a "worst 5%" view. Do not let one snapshot define the story.
4. **Space weather:** early-look page, clearly labelled "lines up with, does not cause."
5. **Data quality:** missingness, quarantine reasons, and pipeline runs. Keep it separate from hardware health.

Use **UTC dates**, show **sample size**, and state the **date window** on every page. A chart without its window and count is easy to misread.

A word on health scores: a combined score can be useful, but only after the components are visible. T1/T2 are "higher is better"; readout is "lower is better." There is no official IBM score in this data. If you build one, label it as the project's own relative indicator and show the inputs.

---

## 10. What this data can and cannot answer

| It can help answer | It cannot answer |
| --- | --- |
| Which backend or qubit looks relatively weaker in a chosen window? | Did a real user's quantum job succeed or fail? |
| Is T1/T2/readout drifting over time? | Which quantum operation is the root cause? (No Silver operation-level table) |
| Which qubits are repeated outliers? | Was a machine down, and for how long? (`operational` is constant) |
| Do bad qubit periods line up with disturbed space weather? | Does space weather *cause* the bad period? |
| How complete is the data? How many snapshots? | What is the local space weather at the machine? |
| Did a batch insert, update, or quarantine rows? | What was the exact collection delay for each row? |
| Which qubits have the best measured T1/T2/readout? | Which qubit is best for every possible circuit? |
| What is the relative fleet picture? | What will happen next? (No prediction model) |

The biggest limits to state clearly:

- Three machines, about seven months, and roughly five snapshots per machine per day. This is a useful sample, not the full universe of IBM hardware.
- `init_error` is blank in production, and `operational` has no variation.
- Operation-level (gate-level) metrics remain in Bronze.
- No location, workload, maintenance, or local weather data.
- Kp/F10.7 are global context, not a controlled experiment.
- Only 26 Kp storm days, so space-weather findings are early and uncertain.

---

## 11. Common traps and simple rules

1. **Blank is not zero.** Do not treat missing T1, T2, init error, Kp, or flux as 0.
2. **Rows are not calibrations.** Count distinct `calibration_timestamp` values.
3. **Use the median for "typical."** Use p95 or worst-case for the risk from the worst readings.
4. **Readout is a chance.** `0.01` = 1%, not 0.01%.
5. **T1/T2 are microseconds.** Bigger is generally better; there is no universal pass mark.
6. **Kp is an index, not a percentage.** `5` is the storm threshold, not 5%.
7. **F10.7 is in sfu, not percent.** It is solar radio brightness, not particle flux.
8. **Kp and F10.7 are different measures.** Keep separate axes and separate rows.
9. **A missing Kp day is not a calm day.** Leave it missing or show coverage.
10. **F10.7 can repeat a calendar day across archive and live.** Remove duplicate days before a daily join.
11. **`load_timestamp` is not the measurement time.** Use it for freshness, not for when the qubit was measured.
12. **Keys and hashes are not metrics.** Hide them from charts.
13. **One reading is not a trend.** Look for repeated readings before flagging a qubit.
14. **Compare like windows.** Kingston's early coverage is sparse; different windows can change the ranking.
15. **A pattern is not proof of cause.** Say "lines up with" or "is linked to," not "caused by."

---

## 12. Quick glossary

| Term | Plain meaning |
| --- | --- |
| **Backend** | A quantum computer (for example, `ibm_kingston`) |
| **Qubit** | The basic unit of quantum information; this data has 156 per machine |
| **Calibration** | A health check that measures qubit properties |
| **Snapshot** | One calibration at one timestamp |
| **Row meaning (grain)** | What one row represents |
| **T1** | Roughly, how long a qubit keeps its stored energy |
| **T2** | Roughly, how long a qubit keeps its timing pattern |
| **Readout error** | Chance of reading the qubit's answer incorrectly |
| **Init error** | Chance of preparing the qubit in the wrong starting state |
| **Operational** | IBM's yes/no "usable" flag |
| **Kp** | A 0-9 score for how disturbed Earth's magnetic field is |
| **F10.7** | Solar radio brightness at 10.7 cm |
| **sfu** | Solar flux unit, the unit for F10.7 |
| **UTC** | Coordinated Universal Time; all timestamps here are UTC |
| **Null / blank** | Unknown or not reported; not zero |
| **Median** | The middle value; half above, half below |
| **Mean** | The average; extreme values can pull it up or down |
| **p95** | A high-end cut-off: 95% of readings are below it |
| **Join** | Match rows from two tables using a shared value; here, the date/time |
| **Hash / key** | A fingerprint or ID, not a measurement |
| **Quarantine** | A rejected record kept for investigation |
| **Full load** | The first large batch |
| **Increment** | A later batch added after the full load |
| **Backfill** | Loading older data after the fact |
| **Pipeline** | The steps that move data from source to Bronze to Silver |
| **Archive / live** | Historical NOAA data / recent NOAA feed |
| **Bronze** | The raw layer |
| **Silver** | The cleaned, typed layer with repeated rows removed |
| **Gold** | Future summaries/models; not built in Phase 2 |
| **BI** | Dashboards and analysis for business questions |

---

## 13. If someone asks...

**"Why is Silver bigger than the full load?"**
Because Silver includes the full load plus later incremental and latest-state loads. IBM Silver is 456,612 rows; the full-load sample is 422,916. NOAA Silver is 2,194 rows; the archive sample is 1,822.

**"Why is `init_error` blank?"**
The source did not report it in these production files. It is not zero. A sample file shows the column can be populated, so the source simply did not supply it here; it is not a broken table design.

**"Why is `operational` always `Yes`?"**
The collector writes a constant `Yes`. It is not a live uptime or failure signal. Do not use it to measure reliability.

**"Why do Kp values look like 3.67 in history but 3 in live data?"**
Historical Kp is an estimated decimal value, often in thirds. Live Kp uses an integer field. They are on the same 0-9 scale but at different precision and time spacing.

**"Why can the same F10.7 day appear twice?"**
Historical F10.7 is stamped at `00:00`; live F10.7 is stamped at `20:00`. Both can represent the same calendar day. Remove duplicate days by date and state your rule.

**"Does space weather hurt the qubits?"**
The data can show whether poor qubit periods line up with disturbed Kp or high F10.7. It cannot prove cause. Use delayed-effect views, show uncertainty, and remember there are only three machines and 26 Kp storm days.

**"Which qubit is best?"**
There is no single answer. T1, T2, and readout measure different things. Rank with a transparent method, show the components, require repeated readings, and state the date window.

**"What is the biggest limitation?"**
The data is narrow and based on watching a small set of real systems: three machines, about seven months, no workload outcomes, no location, no operation-level table in Silver, blank init error, and a constant operational flag.

**"What would you build next?"**
A Gold layer with daily per-backend and per-qubit summaries, a fleet health page, a qubit heatmap, and an early-look space-weather view. Keep cause-and-effect language out of it.

---

## 14. Sources behind this guide

- `README.md` - authoritative table definitions, counts, keys, quality evidence, and known limits.
- `Phase2/README.md` - Phase 2 as-built details and recorded Silver totals.
- `spark/silver/1. Silver Pipeline.ipynb` - Silver schema, parsing, validation, quarantine, merge, and logging rules.
- IBM full-load CSV (`data/Full_Load.rar`) - 422,916-row sample used for the IBM figures.
- `data/Incremental_20260925T093001Z_to_20260930T200523Z.csv`, `data/Latest_State.csv`, and `data/sample/Sample_Incremental_Load.csv` - increment, latest-state, and sample checks.
- NOAA public archive/live feeds collected through `ingestion/noaa_collect.py` - Kp and F10.7 ranges, coverage, and timing.

The official Silver totals are **456,612 IBM rows** and **2,194 NOAA rows**. The IBM figures in this guide are from the full-load sample. NOAA archive numbers are from the 1,822-row archive. Live counts can move with the current endpoint window; use the README totals for the frozen Silver state.

No Databricks job, notebook, table, or pipeline code was changed for this guide.
