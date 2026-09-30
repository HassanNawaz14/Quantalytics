# Phase 2 to Gold / Research Contract

## 1. Purpose

This document defines what future Phase 3 Gold, research and dashboard work may rely on from Phase 2, and what it must not assume.

Phase 2 produces source-faithful, typed, replayable Silver facts. Phase 3 defines research methodology.

## 2. Research questions preserved

1. Do qubits that rank best at one calibration remain the best performers later?
2. Do degraded qubits and gates cluster in specific processor regions?
3. Can past calibration behavior predict which qubits are likely to enter a poor-reliability state?
4. How quickly does calibration information become stale?
5. How much does calibration staleness change hardware-selection decisions?
6. Do publicly available space-weather indicators show a statistically detectable association with QPU reliability changes?

No question is assumed to have a positive result.

## 3. Guaranteed Phase 2 analytical facts

Subject to source availability and the documented nullable rules, Gold can rely on:

| Research need | Silver source |
|---|---|
| backend identity | all IBM Silver tables |
| calibration time | backend/qubit/gate |
| collection time | backend, and Bronze lineage for detailed source retrieval |
| qubit identity | `silver_qubit_calibration.qubit_id` |
| T1/T2 | qubit table |
| readout/init/single-gate error | qubit table where source/config supports |
| operational state | qubit table where source supports |
| gate type | gate table |
| gate ordered endpoints | gate table |
| gate error / CZ/RZZ compatibility fields | gate table |
| gate duration | gate table |
| environment observation time | `silver_environment` |
| Kp | environment rows of `planetary_kp` |
| F10.7 | environment rows of `f10_7_flux` |
| source lineage | hash/batch/run/pipeline/schema fields |
| exact raw payload | Bronze -> immutable source artifact |

## 4. Timestamp semantics for research

Gold must distinguish:

```text
calibration_timestamp
collection_timestamp
environment observation_timestamp
analysis reference time
```

### Calibration freshness

Concept:

```text
Calibration Age =
analysis_reference_time - calibration_timestamp
```

Phase 2 does not mutate Silver each day as time passes. Calibration age is derived in Gold relative to an explicit analysis reference time.

### Collection lag

Gold may also inspect:

```text
collection_timestamp - calibration_timestamp
```

This is a separate operational/source-latency measure and must not be confused with calibration freshness at later analysis time.

## 5. Drift concept

Phase 1 drift formula:

```text
Drift% =
((Metric_t - Metric_t-1) / Metric_t-1) * 100
```

Gold safeguards:

1. no predecessor -> drift undefined/null with reason;
2. denominator zero -> do not divide;
3. extremely small denominator -> apply a research-methodology rule documented before analysis;
4. directionality is metric-specific;
5. error-rate metrics and coherence metrics are interpreted differently.

Examples:

- lower error is generally favorable;
- higher T1/T2 is generally favorable.

Therefore raw positive percentage change cannot automatically be labeled "improvement" or "degradation" for every metric.

Phase 2 does not define the final composite reliability score.

## 6. Ranking stability and top-k turnover

Gold can rank qubits per backend/calibration using a documented future metric/rule.

Conceptual turnover:

```text
Turnover =
1 - (|S_old ∩ S_new| / k)
```

Requirements:

- `k` is a research parameter, not hard-coded in Phase 2;
- ranking metric must be documented;
- ties must have deterministic handling;
- comparison must respect backend and calibration ordering.

Phase 2 provides the necessary repeated qubit observations.

## 7. Stale-selection loss

Concept:

1. choose hardware/qubits using calibration A;
2. evaluate that choice under later calibration B;
3. compare against a choice that would have been made using B;
4. quantify difference under a defined objective.

The sign and magnitude are not assumed.

Phase 2 preserves both calibrations and stable qubit/gate identity needed for this analysis.

## 8. Spatial analysis readiness

Phase 2 preserves:

- qubit IDs;
- ordered two-qubit gate endpoints;
- gate type;
- backend;
- calibration timestamp.

This permits construction of observed calibrated interaction edges.

`OPEN DECISION`: a complete static/versioned coupling graph may require a separate topology metadata source because backend calibration properties are not guaranteed to be the complete topology authority.

If Phase 3 adds topology metadata:

- version it;
- keep backend identity and effective time where possible;
- do not rewrite historical Bronze;
- document the join contract.

## 9. Prediction readiness

### 9.1 Leakage rule

Future prediction must use time-aware splits.

Correct conceptual form:

```text
past calibration(s) -> features
next/future calibration -> target
```

Prohibited when it leaks future information:

```text
random row-level train/test split
```

for longitudinal next-calibration prediction.

### 9.2 Candidate evaluation

Acceptable future patterns include:

```text
train period
 -> validation period
 -> future test period
```

or rolling/expanding-window evaluation.

### 9.3 Phase 2 obligation

Silver contains immutable source facts, not future-informed derived labels.

## 10. Environmental association contract

### 10.1 Phase 2 preservation

NOAA Kp and F10.7 remain at source observation time/granularity.

### 10.2 Phase 3 alignment options

Gold may define, after methodology review:

- nearest prior observation;
- nearest observation;
- rolling average;
- lagged value;
- event-window summary;
- other justified temporal feature.

The chosen approach must avoid look-ahead leakage for prediction.

### 10.3 Scientific language

Permitted result categories include:

```text
observed change
statistical association
prediction
```

Causal claim requires a causal design beyond simple correlation/regression.

Therefore:

```text
association != causation
```

The project must be prepared for null, mixed or inconclusive environmental results.

## 11. Planned Gold concepts

Phase 1 planned concepts:

```text
gold_qpu_health
gold_qubit_reliability
gold_drift_events
gold_calibration_staleness
gold_prediction
gold_environment_association
```

These names describe intended areas, not finalized schemas.

Phase 2 does not prematurely implement them.

## 12. Gold dependency matrix

| Gold concept | Required Phase 2 fields | Phase 2 guarantee |
|---|---|---|
| QPU health | backend, time, qubit metrics, gate metrics | source-faithful metrics preserved |
| qubit reliability | backend/time/qubit/T1/T2/errors | repeated qubit temporal grain |
| drift events | ordered calibrations + metrics | timestamps/metrics preserved |
| staleness | calibration time + explicit analysis reference later | calibration timestamp stable |
| top-k turnover | qubit ID + metric at repeated calibrations | stable identity |
| stale-selection loss | repeated calibration state + stable identities | preserved |
| spatial analysis | qubit IDs + gate endpoints/type | preserved at calibration grain |
| prediction | ordered historical records | no future-derived Phase2 features |
| environment association | environment observation timestamp + IBM calibration timestamp | independent series preserved |

## 13. Silver fields that Gold must not reinterpret casually

### `load_timestamp`

Operational materialization time. Not calibration time.

### `collection_timestamp`

Retrieval time. Not calibration time.

### `source_payload_hash`

Lineage/revision identifier. Not a scientific metric.

### `single_gate_error`

Represents only the configured canonical 1Q instruction if available. It is not automatically "overall 1Q reliability".

### `cz_error` / `rzz_error`

Compatibility projections of the generic gate error for those gate types. They are null for other gate types.

## 14. Missing-data semantics

Null can mean:

- source did not provide optional property;
- configured canonical gate unavailable;
- metric not applicable to observation type.

Invalid source values are **not** converted into accepted nulls; they are handled by quarantine/DQ.

Gold must distinguish legitimate null from data-quality exclusion through audit/lineage where needed.

## 15. Source revision semantics for research

Bronze can preserve multiple payload versions for one logical source snapshot.

Silver contains the accepted version.

If Phase 3 requires reproducible publication:

- record the `source_payload_hash`;
- record `pipeline_version` and `schema_version`;
- record analysis code version;
- freeze the accepted Silver snapshot or reproducible table version.

## 16. Research reproducibility contract

A reported analysis should be reproducible from:

```text
source artifact/hash
+ Phase 2 schema/pipeline version
+ accepted Silver state
+ Phase 3 analysis code/config
+ explicit analysis reference time
```

## 17. Limitations

1. Phase 2 documentation does not independently prove availability of every targeted IBM backend at implementation time.
2. Optional IBM properties vary; nullable fields must not be fabricated.
3. Full coupling topology may need a later metadata source.
4. Source cadence is irregular/different between IBM and NOAA.
5. Observational association with environment does not establish causality.
6. A composite health score is intentionally not defined yet.
7. A poor-reliability classification threshold is intentionally not defined yet.
8. Historical source revisions require explicit accepted-version policy.

## 18. Phase 3 acceptance of handoff

Phase 3 can begin when:

- Silver schemas implemented;
- idempotency/replay proven;
- multiple historical calibration times available;
- NOAA observation times preserved;
- lineage verified;
- known null/source mapping limitations documented;
- topology metadata decision made if a full coupling graph is required.

## 19. Phase 3 change rule

If Phase 3 discovers a necessary source field that Phase 2 preserved only in raw payload:

1. do not rewrite old raw artifacts;
2. add/version a Silver contract if appropriate;
3. replay from immutable Bronze/raw;
4. increment schema version;
5. update ADR/data contract/tests;
6. confirm backward compatibility.

This is the intended benefit of immutable source preservation.
