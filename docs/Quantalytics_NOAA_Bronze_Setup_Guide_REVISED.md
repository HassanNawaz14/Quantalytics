# Quantalytics – NOAA Data Setup Guide

> **🔶 REVISION NOTICE — READ BEFORE CONTINUING**
>
> This revision fixes two omissions in the earlier guide: **(1) a real incremental-ingestion/watermark pattern matching the IBM setup, and (2) a historical window aligned to IBM: 2026-03 through the IBM cutoff of 2026-09-25 13:33:33 UTC.**
>
> **You said you completed the old guide through Step 6 and deleted the data it generated afterward.** Keep the repository, existing GitHub secrets, catalog/schemas, and staging volume. Do not repeat IBM setup or recreate shared infrastructure. Start by reviewing the revised Steps 3–6 below, then follow the new recovery instructions in Step 7.
>
> **Important source limitation:** the live `planetary_k_index_1m.json` feed is a rolling recent-data feed (NOAA describes the live planetary K-index product as retaining the last 30 days), so it cannot alone populate March–September 2026. The revised guide therefore distinguishes the historical backfill from ongoing incremental collection. Use NOAA's quarterly historical archive for the backfill, then use the live SWPC JSON endpoints for ongoing arrivals. Do not claim the Bronze table covers March–September until the range check in Step 7 confirms it. NOAA archive references are linked at the end.
>
> **Change labels:** 🔶 **CHANGED** means replace the previous instructions/code; 🆕 **NEW** means add a new file or step; ✅ **UNCHANGED** means keep the existing setup.
>
> ---
>
### Add NOAA space-weather data to the Bronze layer using GitHub Actions + Databricks Free Edition

*This guide adds only the NOAA source to the existing Quantalytics pipeline. It assumes the IBM setup guide has already been followed and the repository, GitHub Actions access, Databricks catalog, schemas, and staging volume are already configured exactly as documented there.*

---

## Contents

1. [What this setup adds](#1-what-this-setup-adds)
2. [Existing setup: what not to repeat](#2-existing-setup-what-not-to-repeat)
3. [Add the NOAA collector](#3-add-the-noaa-collector)
4. [Add a GitHub Actions workflow](#4-add-a-github-actions-workflow)
5. [Run and verify the collector](#5-run-and-verify-the-collector)
6. [Create the NOAA Bronze notebook with schema imposition](#6-create-the-noaa-bronze-notebook-with-schema-imposition)
7. [Backfill March–September 2026, then verify Bronze](#7-backfill-marchseptember-2026-then-verify-bronze)
8. [Make the incremental schedule automatic](#8-make-the-incremental-schedule-automatic)
9. [Troubleshooting and safe reruns](#9-troubleshooting-and-safe-reruns)
10. [Final state and relationship to Phase 1](#10-final-state-and-relationship-to-phase-1)

---

## 1. What this setup adds

Quantalytics already collects IBM calibration data and stages it under the existing Databricks volume. This guide adds a separate NOAA collector and a separate Bronze table; it does not alter the working IBM collector, IBM workflow, or IBM Bronze notebook.

The NOAA Space Weather Prediction Center (SWPC) feeds supply the environmental variables named in the Phase 1 document:

| Dataset | NOAA endpoint | Research variable |
|---|---|---|
| Planetary K-index | `https://services.swpc.noaa.gov/json/planetary_k_index_1m.json` | Planetary K-index (`Kp`) |
| F10.7 cm flux | `https://services.swpc.noaa.gov/json/f107_cm_flux.json` | Solar radio flux (`F10.7`) |

NOAA publishes machine-readable JSON. The collector runs outside Databricks, normalizes each source record into a small JSON Lines staging file, and uploads it to the existing Databricks volume. The historical backfill must use NOAA's dated archive files; the rolling live endpoints are used for incremental arrivals. Databricks then places the original source record in Bronze as a JSON string. No NOAA values are cleaned, combined, or interpreted in this Bronze step.

### 🔶 Historical window aligned with IBM

For the initial comparison dataset, use the same overall period as the IBM guide:

- **Start:** `2026-03-01T00:00:00Z`
- **End/cutoff:** `2026-09-25T13:33:33Z` (the IBM full-load cutoff)
- **Incremental continuation:** after the backfill, collect observations newer than the persisted NOAA watermark.

NOAA sources do not all have the same cadence or history. Kp records and F10.7 records will not have equal row counts or identical timestamp frequencies. “Aligned” means the same date window, not one-to-one timestamp matching.

### Bronze table design

The table will be `workspace.bronze.noaa_space_weather_raw` and will use the fields defined in Phase 1:

| Column | Meaning |
|---|---|
| `observation_timestamp` | Timestamp supplied by the NOAA record, kept as text in Bronze so the source representation is not silently altered |
| `source` | `noaa_swpc_kp` or `noaa_swpc_f107`, identifying which NOAA feed supplied the row |
| `raw_payload` | The complete original NOAA JSON record serialized as a JSON string |
| `source_file` | Staging filename used to trace the row to its delivered batch and prevent loading a batch twice |

`source_file` is an operational lineage column. The three core NOAA Bronze fields remain `observation_timestamp`, `source`, and `raw_payload`, as specified in Phase 1.

🔶 **CHANGED — ingestion strategy:** the previous version downloaded the complete live endpoint response on every run and had no watermark. The revised design uses two phases:
>
> 1. **Historical backfill:** load NOAA archive records in the fixed window `2026-03-01` through `2026-09-25 13:33:33 UTC` so the starting range aligns with the IBM sample window.
> 2. **Incremental runs:** retain a source-specific watermark in `/Volumes/workspace/landing/staging/noaa/_state/noaa_watermark.json`. Each scheduled collection starts from the last successfully committed observation time (with a small overlap to catch late revisions), stages only newer records, and advances the watermark only after the staged batch has uploaded successfully.
>
> Bronze still uses a keyed Delta `MERGE` on `(source, observation_timestamp)`. The watermark controls what the collector requests; the merge protects against duplicate observation keys. Do not confuse the collection batch timestamp in a filename with the NOAA observation timestamp.

### Flow

```text
NOAA SWPC JSON endpoints
        |
        v
GitHub Actions runner (fetch + normalize to JSON Lines)
        |
        v
/Volumes/workspace/landing/staging/noaa/new/
        |
        v
Databricks notebook (imposed schema + keyed MERGE)
        |
        v
workspace.bronze.noaa_space_weather_raw
```

---

## 2. Existing setup: what not to repeat

The IBM setup is already complete. Do **not** repeat these tasks:

- Do not create a new GitHub repository.
- Do not recreate the `workspace` catalog, `landing`, `bronze`, `silver`, or `gold` schemas.
- Do not recreate the `landing.staging` volume.
- Do not change the IBM collector, its watermark, its workflow, or its Bronze table.
- Do not create additional GitHub secrets for NOAA. NOAA's endpoints are public and require no API key. The existing `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets are reused by the new workflow.
- Do not download NOAA files to your own computer. GitHub Actions transfers them directly into the Databricks volume.

The following paths assume the catalog is `workspace`. If your existing IBM guide was adapted to another catalog, use that same catalog consistently in the workflow and notebook below.

### Existing volume versus new folder

The volume already exists. We are only creating a new source-specific folder beneath it:

```text
/Volumes/workspace/landing/staging/noaa/
    new/          <- GitHub Actions uploads staged JSON Lines batches here
    processed/    <- notebook moves successfully handled batches here
    _state/       <- source-specific watermark for incremental collection
```

🔶 **CHANGED:** create `_state/` as well as `new/` and `processed/`. The watermark lives in `_state/` on the Databricks volume so it survives disposable GitHub Actions runners. The Bronze `MERGE` is still required; a watermark alone is not a duplicate-protection mechanism.

---

## 3. Add the NOAA collector

Create one new file in the existing repository:

`ingestion/noaa_collect.py`

On GitHub, open `HassanNawaz14/Quantalytics` → **Add file** → **Create new file**. Enter the path above, paste the code below, and commit it to the same branch used by the IBM workflow.

### 🔶 CHANGED — required collector behavior before you run it

The earlier collector shown in this section is **full-feed-only**. Do not keep using that old code unchanged, because it does not implement IBM-style incremental ingestion and cannot guarantee the March 2026 start date.

Replace `ingestion/noaa_collect.py` with a collector that supports these two explicit modes:

| Mode | Purpose | Required behavior |
|---|---|---|
| `full` | One-time historical backfill | Read the NOAA historical archive for the fixed date window `2026-03-01` through `2026-09-25T13:33:33Z`; write all in-window rows to one JSONL batch; emit an initial watermark based on the newest successfully included observation for each source. |
| `incremental` | Recurring updates | Read `state/noaa_watermark.json` downloaded from the Databricks `_state/` folder; request/collect observations newer than each source watermark (include a small overlap, then deduplicate by `(source, observation_timestamp)`); emit a batch only when new/revised records exist; write the proposed next watermark to `output/noaa_watermark.json`. |

**Watermark safety rule (same principle as IBM):** upload the JSONL batch first. Upload/overwrite `_state/noaa_watermark.json` only after the batch upload succeeds. If collection or upload fails, do not advance the saved watermark; the next run must retry the same time range.

**Archive-source note:** NOAA's quarterly daily archive files are the historical backfill source, not the rolling live JSON endpoint. The quarterly archives have a daily cadence, while the current Kp feed is more frequent. Preserve each archive record's native observation date/time and source record in `raw_payload`; do not fabricate minute-level timestamps for daily archive rows. This is a Bronze raw-data step, so later Silver work can standardize values.

The prior collector code below is retained here for reference only. **Do not paste it as the final collector unless you also implement the two modes and watermark contract above.** The same warning applies to the old workflow in Step 4: it must download the watermark for incremental runs and upload the updated watermark last.

### Full code: `ingestion/noaa_collect.py`

```python
"""Collect NOAA SWPC Kp and F10.7 JSON feeds into a JSON Lines batch.

The NOAA feeds are public; no NOAA credential is required. Each output line
contains the source observation timestamp, a stable source label, and the
original source record serialized as JSON for Bronze preservation.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SOURCES = [
    (
        "noaa_swpc_kp",
        "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    ),
    (
        "noaa_swpc_f107",
        "https://services.swpc.noaa.gov/json/f107_cm_flux.json",
    ),
]


def fetch_json(url, attempts=3):
    """Fetch a public JSON endpoint with a small retry delay."""
    last_error = None
    for attempt in range(attempts):
        try:
            request = Request(
                url,
                headers={"User-Agent": "Quantalytics-Research/1.0"},
            )
            with urlopen(request, timeout=45) as response:
                if response.status != 200:
                    raise RuntimeError("Unexpected HTTP status: " + str(response.status))
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
    raise RuntimeError("Could not fetch " + url + ": " + str(last_error))


def observation_time(record):
    """Return the NOAA record's time field as text without changing it."""
    # NOAA SWPC JSON feeds use time_tag for the observation time.
    value = record.get("time_tag")
    if value is None or str(value).strip() == "":
        # Keep malformed/unexpected records visible for investigation rather
        # than silently dropping source data. The Bronze table accepts text.
        return ""
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="Collect NOAA SWPC feeds")
    parser.add_argument("--out-dir", default="output")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(args.out_dir, "noaa_full_" + stamp + ".jsonl")

    total = 0
    source_counts = {}
    try:
        with open(out_path, "w", encoding="utf-8", newline="\n") as output:
            for source_name, url in SOURCES:
                payload = fetch_json(url)
                if not isinstance(payload, list):
                    raise RuntimeError(
                        source_name + " returned " + type(payload).__name__
                        + " instead of a JSON array"
                    )

                count = 0
                for record in payload:
                    if not isinstance(record, dict):
                        # Preserve unexpected values in a record-shaped wrapper
                        # so they remain inspectable instead of disappearing.
                        original_record = {"_unexpected_value": record}
                    else:
                        original_record = record

                    row = {
                        "observation_timestamp": observation_time(original_record),
                        "source": source_name,
                        "raw_payload": json.dumps(
                            original_record,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        "source_file": os.path.basename(out_path),
                    }
                    output.write(json.dumps(row, ensure_ascii=False) + "\n")
                    count += 1
                    total += 1

                source_counts[source_name] = count
                print("Fetched", source_name, "| records:", count, flush=True)

        if total == 0:
            os.remove(out_path)
            raise RuntimeError("Both NOAA feeds returned zero records; no file was produced.")

        size = os.path.getsize(out_path)
        print("Finished | records:", total, "| bytes:", size, "| output:", out_path, flush=True)
        for name, count in source_counts.items():
            print("Source", name, "->", count, "records", flush=True)
        return 0

    except Exception as exc:
        if os.path.exists(out_path):
            os.remove(out_path)
        print("NOAA collection failed:", str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
```

### How the collector works

1. Fetches both public NOAA JSON arrays over HTTPS.
2. Retries a failed request up to three times with a short delay.
3. Reads each source record's `time_tag` as the observation timestamp. It does not parse it into a different time format.
4. Writes one JSON object per line. Each line contains the three Phase 1 fields plus `source_file` for lineage.
5. Writes a unique timestamped filename, so separate workflow runs do not overwrite each other's staged file.
6. Fails the run if either endpoint cannot be fetched or does not return a JSON array. A partial batch is deleted rather than uploaded as if it were complete.

The filename uses UTC, for example `noaa_full_20261009T150000Z.jsonl`. This timestamp identifies the collection batch; it is not substituted for the NOAA observation timestamp.

---

## 4. Add a GitHub Actions workflow

Create this second new file:

`.github/workflows/noaa-collect.yml`

Use **Add file** → **Create new file**, enter the exact path, paste the workflow, and commit it. Do not edit `ibm-collect.yml`; NOAA has its own workflow so either source can be tested or run independently.

### 🔶 CHANGED — workflow requirements

The old workflow only ran a full-feed collector and had no persistent bookmark. Replace it with a workflow that mirrors the IBM workflow's lifecycle:

- `workflow_dispatch` input `mode` with `smoke`, `full`, and `incremental` choices. Default to `smoke` for manual runs.
- Scheduled runs default to `incremental`.
- For `full`, stop if `/Volumes/workspace/landing/staging/noaa/_state/noaa_watermark.json` already exists, so the historical backfill cannot accidentally be repeated.
- For `incremental`, download `_state/noaa_watermark.json` to `state/noaa_watermark.json` before running the collector.
- Run `python ingestion/noaa_collect.py --mode "$MODE" --out-dir output` (plus `--watermark-in state/noaa_watermark.json` for incremental mode).
- Upload any `output/noaa_*.jsonl` file(s) to `$VOLUME/new/`.
- **Only after the data upload succeeds**, upload `output/noaa_watermark.json` to `$VOLUME/_state/noaa_watermark.json` with overwrite enabled.
- Keep a 14-day GitHub Actions artifact copy for troubleshooting, as the IBM workflow does.

Do not use the previous full-feed-only YAML below unchanged. It is the original version and does not meet the incremental-ingestion requirement.

### Full code: `.github/workflows/noaa-collect.yml`

```yaml
name: NOAA data collector

on:
  # Weekly on Monday at 03:30 UTC (08:30 Pakistan time).
  # This is offset from the IBM workflow's Monday 03:00 UTC schedule.
  schedule:
    - cron: "30 3 * * 1"
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: noaa-collect
  cancel-in-progress: false

env:
  VOLUME: dbfs:/Volumes/workspace/landing/staging/noaa

jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

    steps:
      - name: Get the code
        uses: actions/checkout@v4

      - name: Install Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Check the Databricks connection
        run: databricks fs ls dbfs:/Volumes/workspace/landing/staging

      - name: Create NOAA staging folders
        run: |
          databricks fs mkdir "$VOLUME/new"
          databricks fs mkdir "$VOLUME/processed"

      - name: Collect NOAA feeds
        run: python ingestion/noaa_collect.py --out-dir output

      - name: Upload NOAA batch to Databricks
        run: |
          shopt -s nullglob
          files=(output/noaa_*.jsonl)
          if [ ${#files[@]} -eq 0 ]; then
            echo "No NOAA output file was produced. Stopping."
            exit 1
          fi
          for f in "${files[@]}"; do
            databricks fs cp "$f" "$VOLUME/new/$(basename "$f")"
          done

      - name: Keep a copy of the batch for 14 days
        uses: actions/upload-artifact@v4
        with:
          name: noaa-jsonl-batch
          path: output/noaa_*.jsonl
          retention-days: 14
          if-no-files-found: error
```

### Important workflow notes

- The workflow reuses the existing `DATABRICKS_HOST` and `DATABRICKS_TOKEN` repository secrets. It does not need IBM credentials.
- The workflow creates only the two NOAA folders under the existing volume. It does not create a catalog, schema, or volume.
- The weekly schedule is Monday 03:30 UTC, which is 08:30 in Pakistan. GitHub scheduled jobs can start late; use **Actions → NOAA data collector → Run workflow** when you want to run it manually.
- If your catalog is not `workspace`, update `VOLUME` and the connection-check path to use the existing catalog name.

---

## 5. Run and verify the collector\n\n🔶 **CHANGED:** the original one-click collector test is still useful as a connectivity smoke test, but it does not prove historical coverage. The revised collector should support `smoke`, `full`, and `incremental`; use `smoke` first, then the historical `full` backfill.

Do the first run manually so you can inspect the output before creating the Bronze table.

1. Open the repository on GitHub.
2. Select **Actions**.
3. Select **NOAA data collector**.
4. Click **Run workflow** → **Run workflow**.
5. Open the run and inspect each step. In **Collect NOAA feeds**, expect one record count for each source and a final output line showing the total count and file size.
6. Confirm that **Upload NOAA batch to Databricks** succeeds.

### Check the staged file in Databricks

Open a Databricks notebook using Serverless compute and run:

```python
ROOT = "/Volumes/workspace/landing/staging/noaa"
display(dbutils.fs.ls(f"{ROOT}/new"))
```

✅ **Checkpoint:** at least one `noaa_full_<timestamp>.jsonl` file appears in `new/`.

You can inspect a few staged lines without moving the file:

```python
files = [f for f in dbutils.fs.ls(f"{ROOT}/new") if f.name.endswith(".jsonl")]
print("Staged batches:", len(files))
if files:
    display(spark.read.text(files[0].path).limit(5))
```

Each line should contain `observation_timestamp`, `source`, `raw_payload`, and `source_file`. The `raw_payload` value should itself contain a serialized NOAA record. Do not manually edit the staged file.

---

## 6. Create the NOAA Bronze notebook with schema imposition\n\n✅ **Mostly unchanged:** the Bronze schema and keyed `MERGE` remain appropriate. The staging folder now also has `_state/noaa_watermark.json`, but the notebook should not load the watermark as data. Continue to read only `.jsonl` files from `new/`.

Create a new Databricks notebook named:

`noaa_bronze_load`

Choose **Serverless** compute. Add the following cells in order. This notebook explicitly imposes a schema when reading the staged JSON Lines data; it does not rely on Spark to infer the schema from the current batch.

### Cell 1 – Imports and paths

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "workspace"  # use the catalog already used by the IBM setup

ROOT = f"/Volumes/{CATALOG}/landing/staging/noaa"
NEW_DIR = f"{ROOT}/new"
DONE_DIR = f"{ROOT}/processed"
BRONZE_TABLE = f"{CATALOG}.bronze.noaa_space_weather_raw"

spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.fs.mkdirs(NEW_DIR)
dbutils.fs.mkdirs(DONE_DIR)
```

**What this does:** imports Spark's schema tools, points the notebook at the already-existing volume, names the new table, and sets the session timezone to UTC. It does not touch the IBM directories or table.

### Cell 2 – Explicit input schema

```python
# Schema imposition: Spark must read these fields with the declared types.
# All fields remain STRING in Bronze so source values are not silently cast.
NOAA_INPUT_SCHEMA = StructType([
    StructField("observation_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("raw_payload", StringType(), True),
    StructField("source_file", StringType(), True),
])
```

**Why this cell matters:** schema inference can vary when a batch contains different records or values. This declared schema fixes the staging contract. The observation timestamp is intentionally a string in Bronze; timestamp parsing and normalization belong in Silver, where malformed values can be handled explicitly.

### Cell 3 – Create the Bronze Delta table

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        observation_timestamp STRING,
        source                STRING,
        raw_payload           STRING,
        source_file           STRING
    )
    USING DELTA
""")
```

The table schema matches the staged schema. `source_file` is retained for lineage and idempotent batch handling; the three main data fields match the NOAA Bronze model in Phase 1.

### Cell 4 – Read, impose schema, and merge batches

```python
# A filename already present in the table has been handled before.
already_loaded = {
    row["source_file"]
    for row in spark.table(BRONZE_TABLE)
        .select("source_file")
        .where(F.col("source_file").isNotNull())
        .distinct()
        .collect()
}

jsonl_files = sorted(
    [f for f in dbutils.fs.ls(NEW_DIR) if f.name.endswith(".jsonl")],
    key=lambda f: f.name,
)
print("NOAA batches waiting:", len(jsonl_files))

for f in jsonl_files:
    if f.name in already_loaded:
        print("Batch already represented in Bronze; moving only:", f.name)
        dbutils.fs.mv(f.path, f"{DONE_DIR}/{f.name}")
        continue

    # Explicit schema imposition: do not remove this .schema(...) call.
    raw = (
        spark.read
        .schema(NOAA_INPUT_SCHEMA)
        .json(f.path)
    )

    # Validate required lineage/source fields before merging. Do not discard
    # malformed source payloads silently; stop and leave the file in new/.
    missing_source = raw.filter(
        F.col("source").isNull() | (F.trim(F.col("source")) == "")
    ).limit(1).count()
    missing_payload = raw.filter(F.col("raw_payload").isNull()).limit(1).count()
    if missing_source or missing_payload:
        raise ValueError(
            f"{f.name} contains a row without source or raw_payload. "
            "The file remains in new/ for inspection."
        )

    # Merge each source/timestamp once. The incoming row with the same key
    # updates raw_payload if NOAA revises a record; new keys are inserted.
    raw.createOrReplaceTempView("noaa_incoming_batch")
    spark.sql(f"""
        MERGE INTO {BRONZE_TABLE} AS target
        USING (
            SELECT observation_timestamp, source, raw_payload, source_file
            FROM noaa_incoming_batch
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY source, observation_timestamp
                ORDER BY source_file DESC
            ) = 1
        ) AS incoming
        ON target.source = incoming.source
           AND target.observation_timestamp = incoming.observation_timestamp
        WHEN MATCHED THEN UPDATE SET
            target.raw_payload = incoming.raw_payload,
            target.source_file = incoming.source_file
        WHEN NOT MATCHED THEN INSERT (
            observation_timestamp, source, raw_payload, source_file
        ) VALUES (
            incoming.observation_timestamp, incoming.source,
            incoming.raw_payload, incoming.source_file
        )
    """)

    rows_in_batch = raw.count()
    print("Merged:", f.name, "| rows:", rows_in_batch)
    dbutils.fs.mv(f.path, f"{DONE_DIR}/{f.name}")

    already_loaded.add(f.name)
```

**What happens in this cell:**

1. Lists the `.jsonl` files waiting in `new/`.
2. Skips reprocessing of a batch filename already represented in Bronze.
3. Reads each file using `NOAA_INPUT_SCHEMA` explicitly. This is the schema-imposition step.
4. Checks that each row has a source label and raw payload. If a check fails, the notebook stops before moving that file, so it can be investigated.
5. Uses Delta `MERGE` with `(source, observation_timestamp)` as the observation key. Existing keys are updated and new keys inserted.
6. Moves the successfully handled file to `processed/`.

**Important:** the merge key follows the NOAA observation model described in the Phase 1 document. If later inspection of the live feeds shows that one source can emit multiple distinct records with the same `time_tag`, revise the key before relying on it downstream.

### Cell 5 – Health check

```python
display(spark.sql(f"""
    SELECT
        source,
        COUNT(*) AS observations,
        COUNT(DISTINCT observation_timestamp) AS distinct_timestamps,
        MIN(observation_timestamp) AS oldest_observation,
        MAX(observation_timestamp) AS newest_observation,
        SUM(CASE WHEN raw_payload IS NULL THEN 1 ELSE 0 END) AS null_payloads
    FROM {BRONZE_TABLE}
    GROUP BY source
    ORDER BY source
"""))
```

This summarizes the stored rows by feed. Since the Kp and F10.7 feeds have different cadences and history lengths, their row counts are not expected to match.

---

## 7. Backfill March–September 2026, then verify Bronze

### 🔶 CHANGED — run the historical backfill first

Because you deleted the data produced by the earlier run, treat this as a clean NOAA backfill. Do **not** run the old full-feed collector and assume it has March data.

1. Confirm `new/`, `processed/`, and `_state/` exist under `/Volumes/workspace/landing/staging/noaa/`.
2. Run **GitHub → Actions → NOAA data collector → Run workflow → `full`** using the revised collector/workflow described in Steps 3–4.
3. Inspect the action logs and confirm the batch includes archive observations within the requested window, with the newest included observation no later than `2026-09-25T13:33:33Z`.
4. Confirm the batch reached `new/` and the watermark was written to `_state/` only after upload succeeded.
5. Run `noaa_bronze_load` in Databricks.

1. Confirm the NOAA workflow has placed a `.jsonl` file in `new/`.
2. Open `noaa_bronze_load` and click **Run all**.
3. Read the output from Cell 4. The first run should report a positive number of waiting batches and a `Merged: noaa_full_...jsonl | rows: ...` message.
4. Review the health-check table from Cell 5.
5. In Catalog Explorer, open `workspace` → `bronze` and confirm that `noaa_space_weather_raw` exists.
6. Open the volume and confirm the batch has moved from `new/` to `processed/`.

### 🔶 Required timestamp-range check (do not skip)

Run this after the historical batch is loaded. The purpose is to prove that the source data actually reaches back to March 2026 and forward to the IBM cutoff window.

```sql
%sql
SELECT
  source,
  COUNT(*) AS rows,
  MIN(observation_timestamp) AS oldest_observation,
  MAX(observation_timestamp) AS newest_observation
FROM workspace.bronze.noaa_space_weather_raw
WHERE observation_timestamp >= '2026-03-01'
  AND observation_timestamp <= '2026-09-25T13:33:33Z'
GROUP BY source
ORDER BY source
```

If a source's `oldest_observation` is later than March 2026, or its `newest_observation` is much earlier than the IBM cutoff, **stop and fix the historical archive backfill**. Do not solve this by changing timestamps or filling gaps with fabricated rows. The live Kp JSON feed alone cannot supply the full historical window.

### Inspect a few Bronze records

Run this in a new notebook cell:

```sql
%sql
SELECT observation_timestamp, source, raw_payload, source_file
FROM workspace.bronze.noaa_space_weather_raw
ORDER BY observation_timestamp DESC
LIMIT 10
```

You should see rows from the Kp and F10.7 feeds. Click a `raw_payload` value to inspect the original JSON record stored as text.

### Check the shape of the source data

```sql
%sql
SELECT source, COUNT(*) AS rows, COUNT(DISTINCT observation_timestamp) AS timestamps
FROM workspace.bronze.noaa_space_weather_raw
GROUP BY source
ORDER BY source
```

The table should contain source labels `noaa_swpc_kp` and `noaa_swpc_f107`. If only one label appears, inspect the GitHub Actions log for the other endpoint and rerun the workflow after resolving the error.

---

## 8. Make the incremental schedule automatic

🔶 **CHANGED:** the workflow schedule remains every Monday at **03:30 UTC (08:30 Pakistan time)**, but scheduled runs must use `incremental` mode and read the persistent NOAA watermark. This is the same full-once / incremental-afterward pattern used by IBM.

After the first manual run and successful Bronze load:

1. Open GitHub → **Actions** → **NOAA data collector**.
2. Confirm the workflow is enabled. If GitHub presents an enable button, click it.
3. Keep the repository active. GitHub may disable scheduled workflows in public repositories after a long period without repository activity, and scheduled starts can be delayed.
4. Each Monday, the workflow reads the saved watermark, collects observations newer than it from the live SWPC feeds, stages a uniquely named batch when there are new records, uploads the batch, and only then advances the watermark. The Bronze notebook merges those observations by source and timestamp.

The scheduled workflow only collects and uploads data and updates the watermark. It does **not** start a Databricks notebook by itself. For now, after a scheduled or manual collector run, open `noaa_bronze_load` and click **Run all** to process any new files. This keeps the execution model consistent with the existing IBM setup, where collection and Bronze loading are separate actions. Notebook/job orchestration can be added later if desired.

---

## 9. Troubleshooting and safe reruns

| Problem | Likely cause | What to do |
|---|---|---|
| `databricks fs ls` fails | Existing Databricks secret expired or host/token is incorrect | Check the existing `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets. Do not create NOAA-specific credentials. |
| A NOAA endpoint request fails | Temporary network issue or NOAA service interruption | Read the collector log; rerun the workflow. Failed partial output is deleted and not uploaded. |
| `returned ... instead of a JSON array` | NOAA endpoint response format changed or returned an unexpected response | Inspect the endpoint response and update the collector deliberately before proceeding. |
| `NOAA batches waiting: 0` | No staged files are waiting | This is normal if there is nothing new in `new/`. Check `processed/` and the previous run logs if you expected a file. |\n| Backfill starts after March 2026 | Only the rolling live endpoint was collected, or the archive backfill was skipped | Re-run the historical `full` backfill only after confirming no valid watermark exists; use NOAA's dated quarterly archive, not only the live JSON feed. |\n| Incremental run says watermark missing | The full backfill has not completed, or `_state/noaa_watermark.json` was deleted | Do not run incremental mode. Restore the watermark from a known-good copy or repeat/repair the full backfill first. |
| A file remains in `new/` after a notebook error | Validation or merge failed | Inspect the cell error and the staged file. Fix the issue, then rerun the notebook. Do not move the file manually before it is successfully processed. |
| The same observation appears to be revised | NOAA may have updated the record for that timestamp | The merge updates the existing `(source, observation_timestamp)` row instead of appending a second one. |
| Only one feed appears in Bronze | The other endpoint may have failed or returned no records | Check the collector's two `Fetched ...` log lines and the total. Rerun after fixing the issue. |
| Scheduled workflow did not start | GitHub schedules may be delayed or disabled | Run it manually from the Actions tab and re-enable the schedule if prompted. |

### Safe rerun tests

**Test A – Run the notebook twice without new files.**

- First run processes the files in `new/` and moves them to `processed/`.
- Second run should print `NOAA batches waiting: 0` and should not change the table.

**Test B – Run incremental collection twice.**

- The first run collects only records newer than the saved per-source watermark (plus the documented overlap for late revisions).
- If no new records exist, it should report that there is nothing to upload and should not move the watermark forward.
- Running the Bronze notebook merges by source and timestamp. Existing observations are updated, not duplicated by timestamp.

**Test C – Re-run a previously processed file.**

If you copy a file from `processed/` back into `new/`, the notebook checks `source_file`. If the file's source filename is already represented in Bronze, it moves the file back to `processed/` without loading it again.

> **🔶 Revised scope note:** NOAA now follows the IBM pattern: one historical backfill, then incremental collection driven by a persisted source-specific watermark. The live endpoints are rolling feeds, so the March–September historical range must be loaded from NOAA's dated archive before incremental mode is enabled. The Delta merge remains the final duplicate/revision safeguard.

---

### 🆕 Recovery checklist for your current progress

Since you completed the earlier guide through Step 6 and deleted the generated data:

- **Keep:** repository, `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets, catalog, schemas, volume, and the `noaa_bronze_load` notebook if you already created it.
- **Replace:** `ingestion/noaa_collect.py` and `.github/workflows/noaa-collect.yml` with implementations that meet the `full` / `incremental` / watermark contract in Steps 3–4. The original code blocks in this document are preserved as reference, but are explicitly marked as the old versions.
- **Create:** `/Volumes/workspace/landing/staging/noaa/_state/`.
- **Clean staging only if needed:** remove stale NOAA files from `new/` only after confirming you do not need them. Do not delete IBM folders, tables, or watermark files.
- **Then:** run the historical backfill, load Bronze, verify the March-to-September range, and only then enable the scheduled incremental runs.

## 10. Final state and relationship to Phase 1

After completing this guide, the new NOAA-specific assets should be:

```text
Quantalytics/
├── ingestion/
│   ├── ibm_collect.py                 # existing; unchanged
│   └── noaa_collect.py                # new
├── .github/
│   └── workflows/
│       ├── ibm-collect.yml            # existing; unchanged
│       └── noaa-collect.yml           # new
└── ...

Databricks volume:
/Volumes/workspace/landing/staging/noaa/
├── new/
├── processed/
└── _state/
    └── noaa_watermark.json

Databricks table:
workspace.bronze.noaa_space_weather_raw
    observation_timestamp STRING
    source                STRING
    raw_payload           STRING
    source_file           STRING
```

This completes the requested NOAA addition to Bronze only. It does not implement Silver normalization, the `silver_environment` table, the Silver `MERGE`, Gold analysis, dashboards, or a fully automated Databricks notebook job. Those remain later pipeline stages.

The table follows the Phase 1 Bronze model (`observation_timestamp`, `source`, `raw_payload`) and preserves the raw NOAA record for reproducibility. Later, Silver can parse the timestamp, extract Kp and F10.7 into normalized numeric columns, handle nulls/malformed records, and apply the planned upsert strategy using `observation_timestamp` as the business key.

### Source endpoints

- NOAA SWPC Planetary K-index: <https://services.swpc.noaa.gov/json/planetary_k_index_1m.json>
- NOAA SWPC F10.7 cm flux (live feed): <https://services.swpc.noaa.gov/json/f107_cm_flux.json>\n- NOAA quarterly daily geomagnetic archive (historical K-index data): <https://www.ngdc.noaa.gov/stp/space-weather/swpc-products/annual_reports/daily_solar_indices_summaries/daily_geomagnetic_data/>\n- NOAA quarterly daily solar archive (historical F10.7 data): <https://www.ngdc.noaa.gov/stp/space-weather/swpc-products/annual_reports/daily_solar_indices_summaries/daily_solar_data/>\n- NOAA SWPC data access notes (live feeds vs long-term archive): <https://www.spaceweather.gov/content/data-access>
- Project design: `Quantalytics-Project-Phase1_REVISED.docx`, sections **Secondary Data Source — NOAA SWPC**, **Bronze Layer — Raw**, and **Upsert / MERGE Strategy**.
