# Quantalytics – IBM Data Setup Guide
### From zero to the Bronze table, using GitHub Actions + Databricks Free Edition

*Covers the IBM source only (full load + weekly incremental load). The NOAA source will follow the same pattern later.*

---

## Contents

0. [What we are building (read this first)](#0-what-we-are-building-read-this-first)
1. [Collect your 4 secret values](#part-1--collect-your-4-secret-values)
2. [Set up the storage in Databricks](#part-2--set-up-the-storage-in-databricks)
3. [Put the code into GitHub](#part-3--put-the-code-into-github)
4. [Save the secrets in GitHub](#part-4--save-the-secrets-in-github)
5. [Test run (smoke test)](#part-5--test-run-smoke-test)
6. [Full load (run once)](#part-6--full-load-run-once)
7. [Bronze table in Databricks](#part-7--bronze-table-in-databricks)
8. [Incremental load (test it by hand)](#part-8--incremental-load-test-it-by-hand)
9. [Make everything automatic](#part-9--make-everything-automatic)
10. [If something goes wrong](#part-10--if-something-goes-wrong)
11. [Safety rules and what comes next](#part-11--safety-rules-and-what-comes-next)
- [Appendix A – How this matches your Phase 1 document](#appendix-a--how-this-matches-your-phase-1-document)
- [Appendix B – Final folder layout](#appendix-b--final-folder-layout)

---

## 0. What we are building (read this first)

**The problem.** Databricks Free Edition cannot talk to IBM Quantum (your own test showed this). So Databricks cannot download the IBM data by itself.

**The solution.** A free robot on GitHub (called *GitHub Actions*) does only the download part. It then drops the file into a folder inside Databricks. Everything after that happens inside Databricks.

```
  IBM Quantum        GitHub robot            Databricks folder           Databricks table
  (data source)  ->  downloads new data  ->  workspace.landing.staging  ->  workspace.bronze.
                     every Monday            /ibm/new                       ibm_calibration_raw
                           |                                                  (Bronze = raw copy)
                           v
                  "bookmark" file (watermark.json) remembers
                  the newest reading we already have
```

**Nothing runs on your computer.** The file goes straight from GitHub's temporary machine to Databricks. It is never downloaded to your laptop.

### Two kinds of load

| | Full load | Incremental load |
|---|---|---|
| When | **Once**, at the start | **Every Monday**, automatically |
| What it collects | The history up to 25 Sep 2026 (about 200 MB) | Only readings that are **newer** than the bookmark |
| Result | One big CSV file | One small CSV file (a few MB) |

### Words used in this guide

| Word | Plain meaning |
|---|---|
| **Repository (repo)** | Your project folder on GitHub: `HassanNawaz14/Quantalytics` |
| **Workflow** | The robot's instruction sheet. One file that says what to do and when |
| **Secret** | A locked box in GitHub where passwords and keys are stored. Nobody can read them back, not even you |
| **Token** | A password that a program uses to log in to Databricks for you |
| **Volume** | A folder inside Databricks where you can keep files (CSV files, etc.) |
| **Bookmark (watermark)** | A small file that remembers "the newest calibration I already have" for each IBM machine |
| **Bronze table** | The first table in your pipeline. A raw, untouched copy of the data |
| **Notebook** | A Databricks page where you paste code and press Run |

### What you need before you start

- A GitHub account with your repo (`HassanNawaz14/Quantalytics`)
- A Databricks Free Edition account (you already have it)
- Your IBM Quantum account (the same one you used in VS Code)
- A web browser. **You do not need to install anything.**
- About 1 hour for the setup, plus the waiting time of the full load

### How to read this guide

- ✅ **Checkpoint** = how to check that the step worked. Do not continue until it works.
- 🛑 **Stop** = something that may block the whole plan. Read it carefully.
- Text in `grey boxes` is something you type or paste exactly.

---

## Part 1 – Collect your 4 secret values

You need four values. Write them in a **private notepad on your computer** (not in GitHub, not in a chat, not in a screenshot). You will paste them into GitHub's locked storage in Part 4.

| # | Name (use exactly this) | What it is | Where it comes from |
|---|---|---|---|
| 1 | `IBM_QUANTUM_API_KEY` | Your IBM key | Step 1.1 |
| 2 | `IBM_QUANTUM_INSTANCE` | Your IBM instance address | Step 1.2 |
| 3 | `DATABRICKS_HOST` | Your Databricks web address | Step 1.3 |
| 4 | `DATABRICKS_TOKEN` | Password for GitHub to log in to Databricks | Step 1.4 |

### Step 1.1 – IBM API key
1. Log in to IBM Quantum Platform: <https://quantum.cloud.ibm.com/>
2. On the home page (dashboard), find the **API key** box and copy the key.
3. This is the same key that is in your `.env` file in VS Code. You can copy it from there too.

### Step 1.2 – IBM instance address (CRN)
1. On IBM Quantum Platform, open **Instances**.
2. Click your instance and copy the long text that starts with `crn:v1:`.
3. It is also in your `.env` file as `IBM_QUANTUM_INSTANCE`.

### Step 1.3 – Databricks web address
1. Log in to Databricks.
2. Look at the address bar of your browser. It looks like `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com/...`
3. Copy **only the first part**, ending at `.com`. No slash at the end, nothing after it.

   Correct: `https://dbc-1234abcd-5678.cloud.databricks.com`
   Wrong: `https://dbc-1234abcd-5678.cloud.databricks.com/?o=123456789`

### Step 1.4 – Databricks token
1. In Databricks, click your **name or profile picture** (top right) → **Settings**.
2. Click **Developer**.
3. Next to **Access tokens**, click **Manage**.
4. Click **Generate new token**.
5. Comment: `github-actions`. Lifetime: pick the longest one offered (for example 90 days).
6. Click **Generate** and **copy the token immediately**. Databricks shows it only once.
7. Write the **expiry date** in your calendar. When the token expires, the robot stops working until you make a new one (see Part 10).

> 🛑 **Stop – if you cannot find "Access tokens" or "Generate new token", or the button is disabled:**
> this method will not work on your account, because the robot has no way to log in to Databricks. Do not continue. Tell your TA and me what you see. The only fallback is uploading the files by hand, which breaks the "everything automated" rule.
>
> I could not find a statement in the Databricks documentation that confirms tokens work on Free Edition, so this step is the real test. The smoke test in Part 5 is the second test.

### Step 1.5 – Make sure your IBM key was never made public
Your repo is **public**. Open `https://github.com/HassanNawaz14/Quantalytics` and check that there is **no `.env` file** in it (including in the commit history).

If a `.env` file was ever uploaded, go to IBM Quantum Platform now and **create a new API key**, then use the new key everywhere.

✅ **Checkpoint:** you have 4 values in your private notepad.

---

## Part 2 – Set up the storage in Databricks

We create the folders and schemas (think "drawers" for tables) that the pipeline uses. You do this once.

### Step 2.1 – Check your catalog name
1. In Databricks, click **Catalog** in the left menu.
2. Look at the top-level names. On Free Edition there is normally one called **`workspace`**.

> If yours has a different name, replace `workspace` with your name in **three places**: the setup notebook (Step 2.2), the Bronze notebook (Part 7), and the `VOLUME:` line of the workflow file (Part 3).

### Step 2.2 – Run the setup notebook
1. In Databricks click **+ New** (top left) → **Notebook**.
2. Rename it to `00_setup_databricks` (click the title at the top).
3. At the top right, make sure compute says **Serverless** (pick it if asked).
4. Paste each block below into its own cell. To get a new cell, hover under the existing cell and click **+ Code**.
5. Click **Run all**.

**Cell 1**

```python
CATALOG = "workspace"      # change only if your catalog has another name

for schema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.staging")
```

**Cell 2**

```python
ROOT = f"/Volumes/{CATALOG}/landing/staging/ibm"

for folder in ["new", "processed", "_state"]:
    dbutils.fs.mkdirs(f"{ROOT}/{folder}")

display(dbutils.fs.ls(ROOT))
```

✅ **Checkpoint:** the last cell shows a small table with three rows: `new`, `processed`, `_state`.
Also click **Catalog → workspace**. You should see four schemas: `landing`, `bronze`, `silver`, `gold`. Under `landing` there is a volume called `staging`.

You now have this folder inside Databricks:

```
/Volumes/workspace/landing/staging/ibm/
    new/          <- the robot drops new CSV files here
    processed/    <- files move here after they were loaded into Bronze
    _state/       <- the bookmark file (watermark.json) lives here
```

---

## Part 3 – Put the code into GitHub

You will add **4 files** to your repo. The easiest way needs no installation:

**How to add a file on the GitHub website**
1. Open your repo: `https://github.com/HassanNawaz14/Quantalytics`
2. Click **Add file** → **Create new file**.
3. In the box at the top, type the **file path** (for example `ingestion/ibm_collect.py`). Typing a `/` creates a folder automatically.
4. Paste the content into the big editor.
5. Click **Commit changes…** → **Commit changes** (keep "Commit directly to the main branch". If your default branch is called `master`, that is fine too).

> You can also add the files from VS Code and push them, if you prefer. The paths must be exactly the same.

### File 1 – `ingestion/ibm_collect.py` (the collector)

This is your own script, adapted for GitHub. The four functions that read and convert IBM data (`parameter_map`, `format_gate_value`, `snapshot_to_rows`, `fetch_snapshot`) are **copied unchanged**, so the CSV columns are exactly the same as before.

What is different from your VS Code script:

| Your VS Code script | This version |
|---|---|
| Reads the key from a `.env` file | Reads it from GitHub Secrets (the `.env` file is only used if you test on your own computer) |
| Saves a checkpoint file | No checkpoint. GitHub's machine is thrown away after each run, so a checkpoint would be useless |
| Does one kind of run | Three modes: `smoke` (test), `full` (history), `incremental` (only new data) |
| One output name | Files are named `ibm_full_<time>.csv` and `ibm_incremental_<time>.csv` so each is unique |
| Nothing | Saves the bookmark file (`watermark.json`) |

One small fix: when two results for the same calibration were handled at the same moment, a "skipped duplicate" result could be processed first and cause the real snapshot to be ignored. This is now handled. It is rare on a real network, but the fix costs nothing.

<details>
<summary><b>Click to open the full code of <code>ingestion/ibm_collect.py</code></b></summary>

```python
"""
Quantalytics - IBM calibration collector

Runs on GitHub Actions (or on your own computer for testing).

Modes
  smoke        Fetch ONE snapshot from ONE backend and print it. Writes nothing.
  full         One-time history load. Walks backwards from FULL_LOAD_CUTOFF until
               about TARGET_MB of data is collected. Writes one CSV + watermark.json.
  incremental  Collects every snapshot that is newer than the saved watermark.
               Writes one CSV (only if something is new) + an updated watermark.json.

The IBM key is read from environment variables, never from the code:
  IBM_QUANTUM_API_KEY    your IBM API key
  IBM_QUANTUM_INSTANCE   your IBM instance CRN (starts with crn:v1:)
"""
import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Lock

from qiskit_ibm_runtime import QiskitRuntimeService


BACKENDS = ["ibm_kingston", "ibm_fez", "ibm_marrakesh"]

# The full load stops at this moment. The incremental load continues from here.
FULL_LOAD_CUTOFF = datetime(2026, 9, 25, 13, 33, 33, tzinfo=timezone.utc)
TARGET_MB = 200.0          # full load stops after about this many MB
LOOKBACK_DAYS = 365        # full load never goes further back than this
SNAPSHOTS_PER_DAY = 5      # how many times per day we "ask" IBM (duplicates are skipped)
MAX_WORKERS = 16
MAX_RETRIES = 3
REQUEST_TIMEOUT = 25

HEADERS = [
    "Qubit",
    "T1 (us)",
    "T2 (us)",
    "Readout assignment error",
    "Init error",
    "Prob meas0 prep1",
    "Prob meas1 prep0",
    "Readout length (ns)",
    "ID error",
    "Single-qubit gate length (ns)",
    "RX error",
    "Z-axis rotation (rz) error",
    "√x (sx) error",
    "Pauli-X error",
    "XSLOW error",
    "CZ error",
    "Gate length (ns)",
    "RZZ error",
    "MEASURE error",
    "MEASURE_2 error",
    "Operational",
    "Backend_ID",
    "Calibration_Timestamp",
]

runtime_client = None
cache_lock = Lock()
known_calibrations = {backend: [] for backend in BACKENDS}


def load_env_file():
    """Only for testing on your own computer: reads a .env file in the current folder.
    On GitHub Actions there is no .env file; the secrets arrive as environment variables."""
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect():
    api_key = os.environ.get("IBM_QUANTUM_API_KEY", "").strip()
    instance_crn = os.environ.get("IBM_QUANTUM_INSTANCE", "").strip()
    if not api_key:
        raise RuntimeError("IBM_QUANTUM_API_KEY is missing (check the GitHub secret).")
    if not instance_crn.startswith("crn:v1:"):
        raise RuntimeError(
            "IBM_QUANTUM_INSTANCE must be the full CRN beginning with 'crn:v1:'."
        )
    print("Connecting to IBM Quantum...", flush=True)
    service = QiskitRuntimeService(
        channel="ibm_quantum_platform",
        token=api_key,
        instance=instance_crn,
    )
    client = service._get_api_client(instance_crn)
    client._session._timeout = (5, REQUEST_TIMEOUT)
    print("Connected. Historical metadata requests do not submit quantum jobs.", flush=True)
    return client


# ---------------------------------------------------------------------------
# The four functions below are copied unchanged from full_load_generation_fast.py
# ---------------------------------------------------------------------------

def parameter_map(parameters):
    return {item.get("name"): item.get("value", "") for item in parameters}


def format_gate_value(values):
    return ";".join(values)


def snapshot_to_rows(payload, backend_name):
    timestamp = payload.get("last_update_date", "")
    if hasattr(timestamp, "isoformat"):
        timestamp = timestamp.isoformat().replace("+00:00", "Z")
    if not timestamp:
        return [], ""

    qubits = payload.get("qubits", [])
    gates = payload.get("gates", [])
    indexed_gates = {}
    for gate in gates:
        gate_name = str(gate.get("gate", "")).lower()
        qubit_ids = gate.get("qubits", [])
        for source_qubit in qubit_ids:
            target_qubit = next(
                (qubit for qubit in qubit_ids if qubit != source_qubit),
                None,
            )
            for parameter in gate.get("parameters", []):
                key = (gate_name, source_qubit, parameter.get("name"))
                value = str(parameter.get("value", ""))
                if target_qubit is not None:
                    value = str(target_qubit) + ":" + value
                indexed_gates.setdefault(key, []).append(value)

    rows = []
    for qubit_id, raw_parameters in enumerate(qubits):
        params = parameter_map(raw_parameters)

        def gate_value(gate_names, parameter_name):
            for gate_name in gate_names:
                values = indexed_gates.get((gate_name, qubit_id, parameter_name))
                if values:
                    return format_gate_value(values)
            return ""

        init_error = params.get("prob_init0_prep1", "")
        if init_error == "":
            init_error = params.get("prob_init1_prep0", "")

        rows.append({
            "Qubit": qubit_id,
            "T1 (us)": params.get("T1", ""),
            "T2 (us)": params.get("T2", ""),
            "Readout assignment error": params.get("readout_error", ""),
            "Init error": init_error,
            "Prob meas0 prep1": params.get("prob_meas0_prep1", ""),
            "Prob meas1 prep0": params.get("prob_meas1_prep0", ""),
            "Readout length (ns)": params.get("readout_length", ""),
            "ID error": gate_value(("id",), "gate_error"),
            "Single-qubit gate length (ns)": gate_value(("id",), "gate_length"),
            "RX error": gate_value(("rx", "x"), "gate_error"),
            "Z-axis rotation (rz) error": gate_value(("rz",), "gate_error"),
            "√x (sx) error": gate_value(("sx",), "gate_error"),
            "Pauli-X error": gate_value(("x",), "gate_error"),
            "XSLOW error": gate_value(("xslow",), "gate_error"),
            "CZ error": gate_value(("cz",), "gate_error"),
            "Gate length (ns)": gate_value(("cz",), "gate_length"),
            "RZZ error": gate_value(("rzz",), "gate_error"),
            "MEASURE error": params.get("readout_error", ""),
            "MEASURE_2 error": params.get("prob_meas1_prep0", ""),
            "Operational": "Yes",
            "Backend_ID": backend_name,
            "Calibration_Timestamp": timestamp,
        })

    return rows, timestamp


def fetch_snapshot(job):
    backend_name, cutoff = job
    cutoff_text = cutoff.isoformat().replace("+00:00", "Z")

    with cache_lock:
        for later_cutoff, calibration_time, calibration_text in known_calibrations[backend_name]:
            if later_cutoff > cutoff and calibration_time <= cutoff:
                return {
                    "ok": True,
                    "backend": backend_name,
                    "cutoff": cutoff_text,
                    "calibration": calibration_text,
                    "rows": [],
                    "skipped_duplicate": True,
                    "error": "",
                }

    for attempt in range(MAX_RETRIES):
        try:
            payload = runtime_client.backend_properties(backend_name, datetime=cutoff)
            rows, timestamp = snapshot_to_rows(payload or {}, backend_name)
            if timestamp:
                parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                with cache_lock:
                    known_calibrations[backend_name].append((cutoff, parsed, timestamp))
            return {
                "ok": True,
                "backend": backend_name,
                "cutoff": cutoff_text,
                "calibration": timestamp,
                "rows": rows,
                "skipped_duplicate": False,
                "error": "",
            }
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if status is None:
                status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (401, 403, 404):
                return {
                    "ok": False,
                    "backend": backend_name,
                    "cutoff": cutoff_text,
                    "calibration": "",
                    "rows": [],
                    "error": str(exc),
                }
            if attempt + 1 == MAX_RETRIES:
                return {
                    "ok": False,
                    "backend": backend_name,
                    "cutoff": cutoff_text,
                    "calibration": "",
                    "rows": [],
                    "error": str(exc),
                }
            time.sleep(0.5 * (2 ** attempt))


# ---------------------------------------------------------------------------
# Watermark = "the newest calibration we already have, per backend"
# ---------------------------------------------------------------------------

def parse_time(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def to_text(moment):
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def read_watermark(path):
    if not os.path.exists(path):
        raise RuntimeError(
            "No watermark file found at '" + path + "'. "
            "Run the FULL load once before running incremental loads."
        )
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    watermark = {name: parse_time(value) for name, value in data["backends"].items()}
    missing = [name for name in BACKENDS if name not in watermark]
    if missing:
        raise RuntimeError("Watermark has no entry for: " + ", ".join(missing))
    return watermark


def write_watermark(path, watermark):
    data = {
        "version": 1,
        "updated_at": to_text(datetime.now(timezone.utc)),
        "backends": {name: to_text(value) for name, value in sorted(watermark.items())},
    }
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


# ---------------------------------------------------------------------------
# Work lists
# ---------------------------------------------------------------------------

def grid_cutoffs(latest_moment, days_back):
    """Moments in time at which we ask IBM 'what were the properties then?'"""
    interval_hours = 24.0 / SNAPSHOTS_PER_DAY
    first_day = latest_moment.replace(hour=0, minute=0, second=0, microsecond=0)
    for day_index in range(days_back):
        day = first_day - timedelta(days=day_index)
        for slot_index in range(SNAPSHOTS_PER_DAY):
            yield day + timedelta(hours=slot_index * interval_hours)


def build_full_jobs():
    jobs = []
    for cutoff in grid_cutoffs(FULL_LOAD_CUTOFF, LOOKBACK_DAYS):
        if cutoff >= FULL_LOAD_CUTOFF:
            continue
        for backend in BACKENDS:
            jobs.append((backend, cutoff))
    jobs.sort(key=lambda job: job[1], reverse=True)
    return jobs


def build_incremental_jobs(now, watermark):
    jobs = []
    oldest = min(watermark.values())
    days_back = (now - oldest).days + 2
    for cutoff in grid_cutoffs(now, days_back):
        for backend in BACKENDS:
            if watermark[backend] < cutoff <= now:
                jobs.append((backend, cutoff))
    for backend in BACKENDS:                      # always ask "what is the very latest?"
        jobs.append((backend, now))
    jobs.sort(key=lambda job: job[1], reverse=True)
    return jobs


# ---------------------------------------------------------------------------
# Main collection loop (same structure as the original script, minus the checkpoint)
# ---------------------------------------------------------------------------

def collect(jobs, out_path, watermark, stop_at_target):
    seen_calibrations = set()
    latest = {}                       # newest calibration written, per backend
    stats = {"queries": 0, "ok": 0, "failed": 0, "skipped": 0, "snapshots": 0}
    start_time = time.perf_counter()
    last_report = start_time
    stop = False

    print("Pending requests:", len(jobs), flush=True)
    print("Workers:", MAX_WORKERS, flush=True)

    with open(out_path, "w", newline="", encoding="utf-8", buffering=1024 * 1024) as output_file:
        writer = csv.DictWriter(output_file, fieldnames=HEADERS)
        writer.writeheader()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {}
            next_job = 0
            while (next_job < len(jobs) or future_map) and not stop:
                while next_job < len(jobs) and len(future_map) < MAX_WORKERS:
                    job = jobs[next_job]
                    future_map[executor.submit(fetch_snapshot, job)] = job
                    next_job += 1

                finished = []
                for future in as_completed(tuple(future_map)):
                    finished.append(future)
                    if len(finished) >= MAX_WORKERS:
                        break

                for future in finished:
                    future_map.pop(future, None)
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {"ok": False, "backend": "unknown", "cutoff": "",
                                  "calibration": "", "rows": [], "error": str(exc)}

                    stats["queries"] += 1
                    if result["ok"]:
                        stats["ok"] += 1
                        backend = result["backend"]
                        if result.get("skipped_duplicate"):
                            stats["skipped"] += 1
                        calibration = result["calibration"]
                        # Only results that carry rows count. A "skipped duplicate" has no rows,
                        # and must never block the real result for the same calibration.
                        if calibration and result["rows"]:
                            key = backend + "|" + calibration
                            if key not in seen_calibrations:
                                seen_calibrations.add(key)
                                calibration_time = parse_time(calibration)
                                is_new = (backend not in watermark
                                          or calibration_time > watermark[backend])
                                if is_new:
                                    writer.writerows(result["rows"])
                                    stats["snapshots"] += 1
                                    if backend not in latest or calibration_time > latest[backend]:
                                        latest[backend] = calibration_time
                    else:
                        stats["failed"] += 1
                        print("ERROR |", result["backend"], "|", result["error"], flush=True)

                    now = time.perf_counter()
                    size_mb = output_file.tell() / (1024.0 * 1024.0)
                    if stats["queries"] == 1 or now - last_report >= 10:
                        print(
                            "Progress | queries:", stats["queries"],
                            "| unique snapshots:", stats["snapshots"],
                            "| skipped duplicates:", stats["skipped"],
                            "| size:", round(size_mb, 2), "MB",
                            flush=True,
                        )
                        last_report = now

                    if stop_at_target and size_mb >= TARGET_MB:
                        stop = True
                        break

        output_file.flush()

    stats["minutes"] = round((time.perf_counter() - start_time) / 60, 1)
    return stats, latest


def run_smoke():
    cutoff = FULL_LOAD_CUTOFF - timedelta(days=1)
    result = fetch_snapshot((BACKENDS[0], cutoff))
    if not result["ok"]:
        raise RuntimeError("IBM smoke test failed: " + result["error"])
    print(
        "Smoke test passed | backend:", result["backend"],
        "| calibration:", result["calibration"],
        "| rows:", len(result["rows"]),
        flush=True,
    )


def main():
    global runtime_client

    parser = argparse.ArgumentParser(description="Quantalytics IBM collector")
    parser.add_argument("--mode", choices=["smoke", "full", "incremental"], required=True)
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--watermark-in", default="state/watermark.json")
    args = parser.parse_args()

    load_env_file()
    runtime_client = connect()

    if args.mode == "smoke":
        run_smoke()
        return 0

    os.makedirs(args.out_dir, exist_ok=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(args.out_dir, "ibm_" + args.mode + "_" + stamp + ".csv")
    watermark_out = os.path.join(args.out_dir, "watermark.json")

    if args.mode == "full":
        old_watermark = {}
        jobs = build_full_jobs()
    else:
        old_watermark = read_watermark(args.watermark_in)
        jobs = build_incremental_jobs(now, old_watermark)

    stats, latest = collect(jobs, out_path, old_watermark, stop_at_target=(args.mode == "full"))

    print("Finished | queries:", stats["queries"], "| failed:", stats["failed"],
          "| duplicate cutoffs skipped:", stats["skipped"],
          "| new snapshots:", stats["snapshots"], "| minutes:", stats["minutes"], flush=True)

    if args.mode == "incremental" and stats["failed"] > 0:
        os.remove(out_path)
        print("Some requests failed. Nothing was saved, so the next run will try the same period again.",
              flush=True)
        return 1

    if stats["snapshots"] == 0:
        os.remove(out_path)
        print("No new snapshots found. Nothing to upload.", flush=True)
        return 1 if args.mode == "full" else 0

    new_watermark = dict(old_watermark)
    new_watermark.update(latest)
    missing = [name for name in BACKENDS if name not in new_watermark]
    if missing:
        print("No data was collected for: " + ", ".join(missing) + ". Check the errors above.", flush=True)
        os.remove(out_path)
        return 1

    write_watermark(watermark_out, new_watermark)
    print("Output:", out_path, "|", round(os.path.getsize(out_path) / (1024.0 * 1024.0), 2), "MB", flush=True)
    for name in BACKENDS:
        print("Watermark", name, "->", to_text(new_watermark[name]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

</details>

### File 2 – `requirements.txt` (the list of Python packages)

First find the version you used when your script worked:
1. Open the terminal in VS Code (the same environment where the script ran).
2. Type: `pip show qiskit-ibm-runtime`
3. Look for the line `Version: x.y.z` and note it.

Now create `requirements.txt` in the **main folder** of the repo (not inside another folder) with this content, replacing the version:

```text
qiskit-ibm-runtime==REPLACE_WITH_YOUR_VERSION
```

For example, if your version is `0.40.1`, the line becomes `qiskit-ibm-runtime==0.40.1`.

> Why the exact version? The script uses some inner parts of the IBM package that can change between versions. Locking the version makes sure GitHub uses the same one that worked for you.

### File 3 – `.gitignore` (the "never upload these" list)

If your repo already has a `.gitignore`, open it and add these lines at the bottom. Otherwise create it:

```text
# Secrets - never upload these
.env
*.env

# Files made while testing
output/
state/
__pycache__/
*.pyc
.venv/
venv/
```

### File 4 – `.github/workflows/ibm-collect.yml` (the robot's instruction sheet)

The path must be exactly `.github/workflows/ibm-collect.yml` (with the dot at the start of `.github`). Spaces at the start of lines matter in this file, so paste it as it is.

```yaml
name: IBM data collector

on:
  # Every Monday at 03:00 UTC (08:00 Pakistan time)
  schedule:
    - cron: "0 3 * * 1"
  # The "Run workflow" button on the Actions page
  workflow_dispatch:
    inputs:
      mode:
        description: "smoke = test only | full = one-time history | incremental = new data only"
        type: choice
        options:
          - smoke
          - incremental
          - full
        default: smoke

permissions:
  contents: read

concurrency:
  group: ibm-collect
  cancel-in-progress: false

env:
  # Scheduled runs have no button input, so they always do an incremental load
  MODE: ${{ github.event.inputs.mode || 'incremental' }}
  # Where the files go in Databricks (change "workspace" if your catalog has another name)
  VOLUME: dbfs:/Volumes/workspace/landing/staging/ibm

jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 350
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
          cache: pip

      - name: Install Python packages
        run: pip install -r requirements.txt

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Check the Databricks connection
        run: databricks fs ls "$VOLUME"

      - name: Full load safety check (only allowed once)
        if: env.MODE == 'full'
        run: |
          if databricks fs ls "$VOLUME/_state/watermark.json" > /dev/null 2>&1; then
            echo "A full load was already done (watermark.json exists). Stopping so data is not loaded twice."
            exit 1
          fi

      - name: Smoke test (IBM connection only)
        if: env.MODE == 'smoke'
        env:
          IBM_QUANTUM_API_KEY: ${{ secrets.IBM_QUANTUM_API_KEY }}
          IBM_QUANTUM_INSTANCE: ${{ secrets.IBM_QUANTUM_INSTANCE }}
        run: python ingestion/ibm_collect.py --mode smoke

      - name: Download the last watermark
        if: env.MODE == 'incremental'
        run: |
          mkdir -p state
          databricks fs cp "$VOLUME/_state/watermark.json" state/watermark.json

      - name: Collect data from IBM
        if: env.MODE != 'smoke'
        env:
          IBM_QUANTUM_API_KEY: ${{ secrets.IBM_QUANTUM_API_KEY }}
          IBM_QUANTUM_INSTANCE: ${{ secrets.IBM_QUANTUM_INSTANCE }}
        run: python ingestion/ibm_collect.py --mode "$MODE" --out-dir output

      - name: Upload the CSV to Databricks
        if: env.MODE != 'smoke'
        run: |
          shopt -s nullglob
          files=(output/ibm_*.csv)
          if [ ${#files[@]} -eq 0 ]; then
            echo "No new data this time. Nothing to upload."
            exit 0
          fi
          for f in "${files[@]}"; do
            databricks fs cp "$f" "$VOLUME/new/$(basename "$f")"
          done
          # The watermark is saved LAST, only after the CSV upload worked
          databricks fs cp output/watermark.json "$VOLUME/_state/watermark.json" --overwrite

      - name: Keep a copy of the CSV for 14 days
        if: env.MODE != 'smoke'
        uses: actions/upload-artifact@v4
        with:
          name: ibm-${{ env.MODE }}-csv
          path: output/
          retention-days: 14
          if-no-files-found: ignore
```

In plain words, this file says:
1. **When:** every Monday at 03:00 UTC (08:00 Pakistan time), or when you press the **Run workflow** button yourself.
2. **Steps:** install Python and the Databricks tool → check that Databricks answers → (depending on the mode) fetch data from IBM → upload the CSV into Databricks → save the new bookmark. The bookmark is saved **last**, so a failed upload never moves it forward.
3. **Safety:** the full load refuses to run a second time. A copy of the CSV is also kept in GitHub for 14 days.

✅ **Checkpoint:** in your repo you can see the folders `ingestion` and `.github/workflows`, and the files `requirements.txt` and `.gitignore`.

---

## Part 4 – Save the secrets in GitHub

1. Open your repo on GitHub → **Settings** (top row of the repo page).
2. In the left menu click **Secrets and variables** → **Actions**.
3. Click **New repository secret**.
4. Type the **Name**, paste the **Secret** value, click **Add secret**.
5. Repeat for all four:

| Name (exactly) | Value |
|---|---|
| `IBM_QUANTUM_API_KEY` | your IBM key (Step 1.1) |
| `IBM_QUANTUM_INSTANCE` | the `crn:v1:...` text (Step 1.2) |
| `DATABRICKS_HOST` | the web address, nothing after `.com` (Step 1.3) |
| `DATABRICKS_TOKEN` | the token (Step 1.4) |

Rules: no quotes, no spaces before or after, no line breaks.

✅ **Checkpoint:** the page lists four secrets. (GitHub never shows their values again. That is normal.)

---

## Part 5 – Test run (smoke test)

This tests the whole chain with almost no data: login to Databricks, login to IBM, and fetch **one** snapshot.

1. In your repo click the **Actions** tab. If GitHub asks, click **"I understand my workflows, go ahead and enable them"**.
2. In the left list click **IBM data collector**.
3. Click **Run workflow** (right side). Leave **mode = smoke**. Click the green **Run workflow** button.
4. Refresh after a few seconds. A new run appears. Click it, then click the job called **collect**.
5. Wait for it to finish (about 1–2 minutes).

✅ **Checkpoint – you should see:**
- A green tick on every step.
- In **Check the Databricks connection**: a list that includes `new`, `processed` and `_state`.
- In **Smoke test (IBM connection only)**: a line like
  `Smoke test passed | backend: ibm_kingston | calibration: 2026-09-24T... | rows: 156`

If a step is red, open Part 10 and look for the error text.

---

## Part 6 – Full load (run once)

This collects the history and puts it in Databricks. **Run it only once.**

What it does: it asks IBM for readings of all three machines (`ibm_kingston`, `ibm_fez`, `ibm_marrakesh`), starting from 25 Sep 2026 13:33 UTC and going backwards in time, until the file reaches about **200 MB** (or one year back, whichever comes first). It skips repeated readings.

1. **Actions** → **IBM data collector** → **Run workflow**.
2. Set **mode = full**. Click **Run workflow**.
3. Open the run → **collect** and watch the step **Collect data from IBM**. Every 10 seconds you will see a line like
   `Progress | queries: 1200 | unique snapshots: 310 | skipped duplicates: 880 | size: 41.5 MB`
4. Wait. The time is the same order as your run in VS Code (it may be a little different). GitHub stops any run after about 6 hours and this file allows 5 hours 50 minutes.
5. At the end you should see `Finished | ...`, then `Output: ... ibm_full_<time>.csv | ~200 MB`, then three `Watermark ...` lines. The next step uploads the file to Databricks (a few minutes for 200 MB).

✅ **Checkpoint – check in Databricks:**
**Catalog → workspace → landing → staging → ibm**
- In `new/` there is one file named like `ibm_full_20261010T031500Z.csv` (about 200 MB).
- In `_state/` there is `watermark.json`. (This is the bookmark. Please do not delete it.)

**If the run fails halfway:** nothing was uploaded and no bookmark was saved, so nothing is broken. Just run it again.

**If you press Run workflow with `full` a second time**, it stops on purpose with the message *"A full load was already done"*. This protects you from loading the history twice.

**For your project submission (sample data):** open the finished run, scroll to the bottom to **Artifacts**, and download `ibm-full-csv`. GitHub does not accept files bigger than 100 MB inside the repo, so for `sample_data/full_load/` commit a smaller piece of the file (for example the first rows) and say so in your README.

---

## Part 7 – Bronze table in Databricks

Now we copy the CSV into the first table, **Bronze**. Bronze keeps the raw values exactly as they came. Nothing is cleaned or converted here.

**How the table looks:** one row = one qubit in one snapshot.

| Column | Meaning |
|---|---|
| `backend_id` | Which IBM machine (e.g. `ibm_kingston`) |
| `collection_timestamp` | When the robot fetched the data (taken from the CSV file name) |
| `raw_calibration_timestamp` | IBM's own calibration time, as text |
| `raw_payload` | The whole original CSV row (all 23 values), saved as text in JSON form |
| `source` | Always `ibm_quantum_platform` |
| `source_file` | Which CSV file the row came from (so the same file is never loaded twice) |

### Step 7.1 – Create the notebook
1. In Databricks: **+ New** → **Notebook**.
2. Rename it to `ibm_bronze_load`.
3. Compute: **Serverless**.
4. Paste each block into its own cell (**+ Code** adds a cell).

**Cell 1**

```python
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F

CATALOG = "workspace"      # change only if your catalog has another name

ROOT = f"/Volumes/{CATALOG}/landing/staging/ibm"
NEW_DIR = f"{ROOT}/new"
DONE_DIR = f"{ROOT}/processed"
BRONZE_TABLE = f"{CATALOG}.bronze.ibm_calibration_raw"

spark.conf.set("spark.sql.session.timeZone", "UTC")
```

**Cell 2**

```python
# The Bronze table. Nothing is cleaned here - the original values are kept as text.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        backend_id                STRING,
        collection_timestamp      TIMESTAMP,
        raw_calibration_timestamp STRING,
        raw_payload               STRING,
        source                    STRING,
        source_file               STRING
    )
    USING DELTA
""")
```

**Cell 3**

```python
already_loaded = {row["source_file"] for row in spark.table(BRONZE_TABLE).select("source_file").distinct().collect()}
dbutils.fs.mkdirs(DONE_DIR)

csv_files = sorted([f for f in dbutils.fs.ls(NEW_DIR) if f.name.endswith(".csv")], key=lambda f: f.name)
print("CSV files waiting:", len(csv_files))

for f in csv_files:
    if f.name in already_loaded:
        print("Already in Bronze, only moving:", f.name)
    else:
        # When did the collector fetch this file? It is written in the file name.
        match = re.search(r"(\d{8}T\d{6}Z)", f.name)
        if match:
            collected_at = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ")
        else:
            collected_at = datetime.now(timezone.utc).replace(tzinfo=None)

        raw = spark.read.option("header", True).csv(f.path)       # every column stays text

        bronze = raw.select(
            F.col("Backend_ID").alias("backend_id"),
            F.lit(collected_at).cast("timestamp").alias("collection_timestamp"),
            F.col("Calibration_Timestamp").alias("raw_calibration_timestamp"),
            F.to_json(F.struct("*"), {"ignoreNullFields": "false"}).alias("raw_payload"),
            F.lit("ibm_quantum_platform").alias("source"),
            F.lit(f.name).alias("source_file"),
        )

        bronze.write.mode("append").saveAsTable(BRONZE_TABLE)
        print("Loaded:", f.name, "| rows:", raw.count())

    dbutils.fs.mv(f.path, f"{DONE_DIR}/{f.name}")
```

**Cell 4**

```python
# Quick health check
display(spark.sql(f"""
    SELECT backend_id,
           COUNT(*)                                             AS rows_loaded,
           COUNT(DISTINCT raw_calibration_timestamp)            AS snapshots,
           MIN(raw_calibration_timestamp)                       AS oldest_calibration,
           MAX(raw_calibration_timestamp)                       AS newest_calibration
    FROM {BRONZE_TABLE}
    GROUP BY backend_id
    ORDER BY backend_id
"""))
```

> If your catalog is not called `workspace`, change it in Cell 1, on the line `CATALOG = "workspace"`.

### Step 7.2 – Run it
Click **Run all**.

✅ **Checkpoint – you should see:**
- `CSV files waiting: 1`
- `Loaded: ibm_full_....csv | rows: <a big number>`
- A summary table with **3 rows** (one per IBM machine). In it, `rows_loaded` ÷ 156 should be equal to `snapshots` for each machine, because every snapshot has 156 qubits.
- In **Catalog → ... → ibm → processed** the CSV file is now there, and `new/` is empty.

### Step 7.3 – Look at the data
Add a new cell and run:

```sql
%sql
SELECT * FROM workspace.bronze.ibm_calibration_raw LIMIT 5
```

You will see the columns from the table above. Click a `raw_payload` value to see the original CSV row.

### Why you can run this notebook again safely
- If there is nothing in `new/`, it just prints `CSV files waiting: 0`.
- If a file was already loaded, it is **not loaded again**. It is only moved to `processed/`.

---

## Part 8 – Incremental load (test it by hand)

Before making it automatic, test it once by hand.

**What it does:** it downloads the bookmark from Databricks, asks IBM only for readings **newer than the bookmark**, uploads one small CSV to `new/`, and then saves the moved bookmark.

> The first incremental run covers everything from 25 Sep 2026 until today, so it is bigger than a normal weekly file. Later runs cover about one week.

### Step 8.1 – Run it
1. **Actions** → **IBM data collector** → **Run workflow**.
2. **mode = incremental** → **Run workflow**.
3. Open the run and watch it. At the end you should see something like
   `Finished | queries: ... | new snapshots: 40`
   `Output: output/ibm_incremental_<time>.csv | <a few> MB`

✅ **Checkpoint:** in Databricks, `new/` contains `ibm_incremental_<time>.csv`.

### Step 8.2 – Load it into Bronze
Open the `ibm_bronze_load` notebook and press **Run all**.

✅ **Checkpoint:**
- `Loaded: ibm_incremental_....csv | rows: ...`
- The summary table now shows a newer `newest_calibration` and more rows.

### Step 8.3 – The "no duplicates" tests (from your testing plan)

**Test A – nothing new.** Run the **incremental** workflow again right away (Step 8.1).
✅ The log says `No new snapshots found. Nothing to upload.` and no new file appears in `new/`.

**Test B – same notebook twice.** Run `ibm_bronze_load` again.
✅ It says `CSV files waiting: 0` and the row counts do not change.

**Test C – the same file arrives again.** In a new Databricks notebook cell, copy a file from `processed/` back to `new/` (replace `FILE_NAME` with the real name of the incremental file):

```python
base = "/Volumes/workspace/landing/staging/ibm"
dbutils.fs.cp(f"{base}/processed/FILE_NAME", f"{base}/new/FILE_NAME")
```

Then run `ibm_bronze_load` again.
✅ It says `Already in Bronze, only moving: ...` and the row counts do not change.

---

## Part 9 – Make everything automatic

### Step 9.1 – The GitHub robot (already done)
The workflow file already contains the schedule:

```yaml
schedule:
  - cron: "0 3 * * 1"
```

This means **every Monday at 03:00 UTC = 08:00 in Pakistan**. Scheduled runs always do an **incremental** load. The times in GitHub are always in UTC (Pakistan time minus 5 hours).

To change the day or time, edit the line in the workflow file. The 5 numbers are *minute, hour, day of month, month, day of week* (0 = Sunday, 1 = Monday ...). For example `30 4 * * 0` means every Sunday at 04:30 UTC.

### Step 9.2 – The Databricks job (loads the file into Bronze)
This starts the Bronze notebook on its own, a little after the robot has delivered the file.

1. In Databricks, click **Jobs & Pipelines** in the left menu.
2. Click **Create** → **Job**.
3. Name it `ibm_bronze_weekly`.
4. Add a task:
   - Task name: `bronze_load`
   - Type: **Notebook**
   - Path: choose your `ibm_bronze_load` notebook
   - Compute: **Serverless**
5. On the right side, click **Add trigger** → **Scheduled**.
   - Every **1 week**, on **Monday**, at **10:00**
   - Time zone: **Asia/Karachi**. (That is 05:00 UTC, two hours after the GitHub robot starts. The gap gives GitHub time to finish, even when it starts late.)
6. Click **Save**.
7. Click **Run now** once to test it.

> Menu names in Databricks can move a little between versions. The idea stays the same: *a job with one notebook task and a weekly schedule*.

✅ **Checkpoint:** the test run is green and prints `CSV files waiting: 0` (nothing new is waiting right now).

### Step 9.3 – Check the first real Monday
On the first Monday after setup, check these 3 things:
1. **GitHub → Actions:** the run called *IBM data collector* has a green tick.
2. **Databricks → Catalog → ... → ibm → processed:** a new `ibm_incremental_...csv` file is there.
3. **Bronze table:** the row count increased, and `newest_calibration` is recent.

**If the Databricks job runs before the file arrives**, nothing is lost. The file waits in `new/` and is loaded in the next run.

---

## Part 10 – If something goes wrong

Open the failed run in GitHub (**Actions** → click the run → click **collect** → click the red step) and compare the message.

| What you see | What it means | What to do |
|---|---|---|
| `Run workflow` button is missing | GitHub did not find the workflow file | Check the path is exactly `.github/workflows/ibm-collect.yml` and that it is on the `main` branch |
| Red at **Install Python packages**, mentions `REPLACE_WITH_YOUR_VERSION` | The version was not replaced | Edit `requirements.txt` (Part 3, File 2) |
| Red at **Check the Databricks connection**, says `401`, `403`, `invalid token` or `Unauthorized` | The token is wrong or expired | Make a new token (Step 1.4) and update the `DATABRICKS_TOKEN` secret |
| Same step, says something about the host or URL | `DATABRICKS_HOST` has a slash or extra text at the end | Fix the secret (Step 1.3) |
| Same step, says `not found` or `does not exist` for the Volume | Part 2 was not done, or your catalog is not `workspace` | Run the setup notebook; fix the `VOLUME:` line in the workflow if the catalog name differs |
| `IBM_QUANTUM_API_KEY is missing` | The secret name was typed differently | Check the spelling in Part 4 (capital letters matter) |
| `IBM_QUANTUM_INSTANCE must be the full CRN` | The value does not start with `crn:v1:` | Copy the full CRN again (Step 1.2) |
| `IBM smoke test failed: ...` | IBM rejected the key or the instance | Check both values. If you created a new key, update the secret |
| `A full load was already done` | You ran **full** a second time. This is a safety stop | Nothing to fix. Use **incremental** from now on |
| `No watermark file found ... Run the FULL load once` | Incremental was run before the full load | Do Part 6 first |
| `Some requests failed. Nothing was saved...` | IBM or the network had a problem during an incremental run | Press **Run workflow → incremental** again. Nothing was lost, because the bookmark did not move |
| Scheduled run did not start on Monday | GitHub can start scheduled runs late. Also, **GitHub switches off schedules in public repos after 60 days without any activity** | Open the Actions tab and re-enable the workflow if asked. Make a small change in the repo (even in the README) now and then |
| Bronze notebook: `Path does not exist` | The Volume path is different | Check the catalog name in Cell 1 and that the setup notebook (Part 2) was run |
| Databricks says you reached a usage limit | Free Edition has daily limits and shuts compute down for the rest of the day | Wait for the next day and run the job again. Nothing is lost |
| Token stopped working suddenly | It expired, or it was not used for 90 days | New token, update the secret |

**If you want to start again from zero** (only if you really have to):
1. In Databricks run: `DROP TABLE IF EXISTS workspace.bronze.ibm_calibration_raw`
2. In **Catalog → ... → ibm**, delete every file in `new/`, `processed/` and the file `_state/watermark.json`.
3. Run the **full** workflow again (Part 6), then the Bronze notebook.

---

## Part 11 – Safety rules and what comes next

### Safety rules (important because your repo is public)
1. **Never paste a key or token into the code, the README, a chat or a screenshot.** Only into GitHub's secret form.
2. **Never upload a `.env` file.** The `.gitignore` from Part 3 helps, but check anyway.
3. **Anyone can read the logs of a public repo.** The collector never prints keys. GitHub also replaces known secret values in logs with `***`.
4. The workflow only starts by **schedule** or by **your button**. Strangers cannot start it, and secrets are not handed to outside contributors.
5. If you ever think a key was exposed: create a new IBM key, create a new Databricks token, and update the GitHub secrets.
6. **Calendar reminder:** the Databricks token expires. Renew it a few days before.

### Good to know
- **How often the robot asks IBM.** It asks 5 times per day (`SNAPSHOTS_PER_DAY = 5`, as in your original script) and keeps each *different* calibration once. If IBM ever publishes more than 5 different calibrations in one day, some could be missed. Raise this number at the top of `ibm_collect.py` if that happens. Repeated answers are skipped, so a higher number costs little.
- **GitHub Actions is free** for public repos. Databricks compute is only used by the small Bronze notebook, which keeps you well inside the Free Edition limits.
- **The first incremental run** is larger than the others. This is normal.

### What comes next
1. **Silver layer:** a notebook that reads `bronze.ibm_calibration_raw`, opens the `raw_payload`, fixes the data types and builds `silver_qubit_history` with the `MERGE` rule from your document.
2. **NOAA source:** the same pattern. Another workflow (or another job in this one) fetches the NOAA JSON files and drops them into `staging/noaa/new`, and a second Bronze notebook loads them.
3. **Gold layer** and the dashboard.

---

## Appendix A – How this matches your Phase 1 document

| Phase 1 document says | What this setup does |
|---|---|
| Section 6: *"GitHub Actions may be used for this collection layer so the Databricks workspace is not responsible for outbound API access"* | Exactly this. GitHub fetches, Databricks stores and processes |
| Full load of about 200 MB, multi-backend, walking backwards in time | `full` mode: three backends, newest to oldest from 25 Sep 2026, stops at 200 MB or 365 days |
| One incremental load **per week**, all newly available snapshots since the previous watermark | `incremental` mode, scheduled every Monday, uses the bookmark (`watermark.json`) per backend |
| The pipeline stores every new snapshot IBM exposes. 5 per day is only a planning number | The collector keeps every *different* calibration it finds. It does not make up snapshots |
| Tokens never stored in the dataset or repo; use repository secrets and `.gitignore` | GitHub Secrets for all four values, `.gitignore` for `.env` |
| Bronze fields: `backend_id`, `collection_timestamp`, `raw_calibration_timestamp`, `raw_payload`, `source` | All five exist. I added one more, `source_file`, so a file can never be loaded twice |
| Bronze keeps raw payloads "as is" | Nothing is cleaned. All 23 values are kept as text inside `raw_payload` |
| Process only new data in weekly runs (FinOps) | The robot fetches only newer-than-bookmark data. Bronze loads only files in `new/` |
| Small scheduled Spark jobs, no always-on compute | One weekly serverless job |
| Duplicate detection on re-runs (testing plan, item 4) | Tests A, B and C in Step 8.3 |
| Repo folders `ingestion/`, `notebooks/`, `spark/bronze/` | The collector is in `ingestion/`. The two notebooks go to `notebooks/` and `spark/bronze/` (Appendix B) |

### Choices I made where the document was silent
1. **One Bronze row per qubit per snapshot.** The document does not say how big one Bronze row is. Your raw data is already a table with one line per qubit, so this keeps Bronze simple and makes Silver's `MERGE` key `(backend_id, qubit_id, calibration_timestamp)` easy to build.
2. **The bookmark lives in the Databricks Volume** (`_state/watermark.json`), not inside GitHub. Databricks stays the one place that remembers the state, and the robot needs no write access to the repo.
3. **No partitioning on the Bronze table.** The document says to partition by backend and calibration date. For about half a million rows, hundreds of tiny partitions would make queries slower, not faster. I suggest applying the partitioning in Silver and Gold. Tell me if your TA wants it in Bronze as well. It is a small change.
4. **Duplicates in Bronze are blocked by file name, and duplicates in Silver will be handled by `MERGE`.** This gives two layers of protection.

---

## Appendix B – Final folder layout

```
Quantalytics/                            (your GitHub repo)
├── .github/
│   └── workflows/
│       └── ibm-collect.yml              <- the robot's instruction sheet (Part 3)
├── ingestion/
│   └── ibm_collect.py                   <- the collector (Part 3)
├── notebooks/
│   └── 00_setup_databricks.py           <- copy of the setup notebook (optional, see below)
├── spark/
│   └── bronze/
│       └── ibm_bronze_load.py           <- copy of the Bronze notebook (optional, see below)
├── sample_data/
│   ├── full_load/                       <- a small piece of the full-load CSV
│   └── incremental_load/                <- one weekly CSV
├── requirements.txt
├── .gitignore
└── README.md
```

**To save your notebooks in GitHub:** in Databricks open the notebook → **File** → **Export** → **Source file (.py)**. Then in GitHub use **Add file → Upload files** and put the file in the folder shown above.

**Inside Databricks:**

```
workspace (catalog)
├── landing  (schema)
│   └── staging  (volume)
│       └── ibm/
│           ├── new/         <- robot drops CSV files here
│           ├── processed/   <- loaded files are moved here
│           └── _state/      <- watermark.json (the bookmark)
├── bronze   (schema)
│   └── ibm_calibration_raw  (table)
├── silver   (schema)        <- next step
└── gold     (schema)        <- later
```
