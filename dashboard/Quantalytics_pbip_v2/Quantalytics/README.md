# Quantalytics – Power BI Project (Gold layer)

Report + semantic model over `workspace.gold` from the Quantalytics lakehouse. **No data is embedded**; every table loads from Databricks on refresh.

## Open it
1. Power BI Desktop → Options → Preview features: enable **Power BI Project (.pbip) save option** and **Store reports using enhanced metadata format (PBIR)**; restart.
2. File → Open → Browse → `Quantalytics.pbip`.
3. Home → Transform data → Edit parameters:
   - `DatabricksHost` – workspace hostname (no https://)
   - `DatabricksHttpPath` – SQL warehouse HTTP path
   - `DatabricksCatalog` – `workspace`
   - `DatabricksSchema` – `gold`
4. Refresh and sign in (Microsoft Entra ID or personal access token, entered in Desktop, never stored in the project).
5. Optional: File → Save as `.pbix`.

## Model (mirrors docs/diagrams/quantalytics_phase3_gold_model.dbml)
- Facts: `fact_qubit_calibration`, `fact_environment`, `fact_backend_daily`, `fact_pipeline_run`, `fact_quarantine`
- Dimensions: `dim_date` (date table), `dim_time`, `dim_backend`, `dim_qubit` (snowflaked to backend), `dim_observation_type`, `dim_stage`, `dim_dataset`, `dim_mode`, `dim_status`, `dim_error_reason`
- 19 many-to-one, single-direction relationships. No `dim_backend → fact_qubit_calibration` relationship (path goes through `dim_qubit`).
- Keys are hidden; columns have friendly names via `sourceColumn` mapping, so Gold column names must stay unchanged.
- Calculated columns: `dim_qubit[Qubit Label]`, `fact_qubit_calibration[T1 Bucket (µs)]`, `fact_backend_daily[Kp Band]`.
- Measures are grouped in display folders (Volume, Coherence, Errors, Variability, Degradation, Freshness, Daily, Space Weather, Correlation, Runs, Performance, Quarantine).

## Report pages
| Page | Business question |
|---|---|
| QPU Health Overview | BQ1 – calibration trends by backend/qubit |
| Backend & Qubit Reliability | BQ2 – degradation and variability |
| Space Weather vs QPUs | BQ3 – Kp / F10.7 vs calibration (exploratory) |
| Pipeline & Data Quality | BQ4 – run health, quarantine Pareto, failures |
| About & Data Notes | mapping, KPI definitions, coverage caveats |

Date and Backend slicers are synced across pages 1–4. The theme is in `Quantalytics.Report/StaticResources/RegisteredResources/Quantalytics.json`; a copy is in `Theme/` for View → Themes → Browse.

## Notes
- Storage mode is Import. For DirectQuery, change `mode: import` to `mode: directQuery` per table (drop the calculated columns first).
- If Desktop rejects the custom theme, delete `customTheme` and `resourcePackages` from `report.json` and import the theme from `Theme/`.
- Databricks `TIMESTAMP` columns arrive as datetime; if you see timezone issues, convert in the partition M.

## v2 additions (existing pages and model objects are unchanged)
Seven pages were appended after "About & Data Notes", all built from the same Gold tables. New measures and calculated columns were added to the existing tables; nothing was removed or renamed.

| New page | What it compares |
|---|---|
| Exec Insights | Backend Health Index vs storm sensitivity, dynamic "healthiest / most sensitive / most variable" cards, scorecard |
| Storm Impact | T1/T2/readout on storm (Kp ≥ 5) vs quiet (Kp < 3) days, event study around storm days, most sensitive qubits |
| Lag & Sensitivity | Pearson r between Kp and T1/T2/readout error at lags of 0-7 days, backend sensitivity ranking |
| Solar Regimes | Kp band × F10.7 band matrices (T1, readout error, sample size), F10.7 vs readout error |
| Rhythms | Calibration hour-of-day, weekday and monthly patterns vs Kp |
| Coherence Physics | T1 vs T2, T2/T1 ratio vs the physical bound of 2, readout vs init error by backend |
| Coverage & Trust | Calibration, Kp and F10.7 coverage by backend and month |

New calculated columns: `fact_backend_daily[F10.7 Band]`, `fact_qubit_calibration[T2/T1 Ratio Bucket]`, `dim_date[Weekday Short]`, `dim_time[Time Window]`. All insights are exploratory associations, not causal findings; check Coverage & Trust first.
