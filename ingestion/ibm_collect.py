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
