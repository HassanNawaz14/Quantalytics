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
INCREMENTAL_CUTOFF = datetime(2026, 9, 25, 13, 33, 33, tzinfo=timezone.utc)
TARGET_MB = 200.0
LOOKBACK_DAYS = 365
SNAPSHOTS_PER_DAY = 5
MAX_WORKERS = 16
MAX_RETRIES = 3
REQUEST_TIMEOUT = 25
CHECKPOINT_EVERY = 50

OUTPUT_FILE = "Full_Load_Fast.csv"
CHECKPOINT_FILE = "full_load_fast_checkpoint.json"
CHECKPOINT_VERSION = 1

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


def load_env_file():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


load_env_file()
api_key = os.environ.get("IBM_QUANTUM_API_KEY", "").strip()
instance_crn = os.environ.get("IBM_QUANTUM_INSTANCE", "").strip()

if not api_key:
    raise RuntimeError("Set IBM_QUANTUM_API_KEY in the project .env file.")
if not instance_crn.startswith("crn:v1:"):
    raise RuntimeError(
        "Set IBM_QUANTUM_INSTANCE in .env to the full CRN beginning with 'crn:v1:'."
    )

print("Connecting to IBM Quantum...", flush=True)
service = QiskitRuntimeService(
    channel="ibm_quantum_platform",
    token=api_key,
    instance=instance_crn,
)
runtime_client = service._get_api_client(instance_crn)
runtime_client._session._timeout = (5, REQUEST_TIMEOUT)
print("Connected. Historical metadata requests do not submit quantum jobs.", flush=True)

cache_lock = Lock()
known_calibrations = {backend: [] for backend in BACKENDS}


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


if "--smoke-test" in sys.argv:
    smoke_cutoff = INCREMENTAL_CUTOFF - timedelta(days=1)
    smoke_result = fetch_snapshot((BACKENDS[0], smoke_cutoff))
    if not smoke_result["ok"]:
        raise RuntimeError("IBM smoke test failed: " + smoke_result["error"])
    print(
        "Smoke test passed | backend:", smoke_result["backend"],
        "| calibration:", smoke_result["calibration"],
        "| rows:", len(smoke_result["rows"]),
        flush=True,
    )
    raise SystemExit(0)


def load_checkpoint():
    empty = {"version": CHECKPOINT_VERSION, "completed": [], "calibrations": []}
    if not os.path.exists(CHECKPOINT_FILE):
        return empty
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        if data.get("version") != CHECKPOINT_VERSION:
            return empty
        return data
    except (OSError, json.JSONDecodeError):
        return empty


def save_checkpoint(completed, calibrations):
    temp_file = CHECKPOINT_FILE + ".tmp"
    data = {
        "version": CHECKPOINT_VERSION,
        "completed": sorted(completed),
        "calibrations": sorted(calibrations),
    }
    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(data, file, separators=(",", ":"))
    os.replace(temp_file, CHECKPOINT_FILE)


checkpoint = load_checkpoint()
completed_queries = set(checkpoint["completed"])
seen_calibrations = set(checkpoint["calibrations"])

jobs = []
interval_hours = 24.0 / SNAPSHOTS_PER_DAY
first_day = INCREMENTAL_CUTOFF.replace(hour=0, minute=0, second=0, microsecond=0)
for day_index in range(LOOKBACK_DAYS):
    day = first_day - timedelta(days=day_index)
    for slot_index in range(SNAPSHOTS_PER_DAY):
        cutoff = day + timedelta(hours=slot_index * interval_hours)
        if cutoff >= INCREMENTAL_CUTOFF:
            continue
        for backend in BACKENDS:
            query_key = backend + "|" + cutoff.isoformat()
            if query_key not in completed_queries:
                jobs.append((backend, cutoff))
jobs.sort(key=lambda job: job[1], reverse=True)

if os.path.exists(OUTPUT_FILE) and not os.path.exists(CHECKPOINT_FILE):
    raise RuntimeError(
        "Fast output already exists without its checkpoint; move it aside before restarting."
    )
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        csv.DictWriter(file, fieldnames=HEADERS).writeheader()

print("Pending requests:", len(jobs), flush=True)
print("Workers:", MAX_WORKERS, "| Checkpoint every:", CHECKPOINT_EVERY, "successful requests", flush=True)
print("Output:", OUTPUT_FILE, flush=True)

start_time = time.perf_counter()
last_report = start_time
completed_this_run = 0
successful_requests = 0
failed_requests = 0
skipped_duplicates = 0
new_snapshots = 0
uncheckpointed = 0
stop = False

with open(OUTPUT_FILE, "a", newline="", encoding="utf-8", buffering=1024 * 1024) as output_file:
    writer = csv.DictWriter(output_file, fieldnames=HEADERS)
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
                    result = {"ok": False, "backend": "unknown", "cutoff": "", "calibration": "", "rows": [], "error": str(exc)}

                completed_this_run += 1
                if result["ok"]:
                    backend = result["backend"]
                    cutoff = result["cutoff"]
                    query_key = backend + "|" + cutoff.replace("Z", "+00:00")
                    completed_queries.add(query_key)
                    successful_requests += 1
                    if result.get("skipped_duplicate"):
                        skipped_duplicates += 1
                    calibration = result["calibration"]
                    if calibration:
                        calibration_key = backend + "|" + calibration
                        if calibration_key not in seen_calibrations:
                            seen_calibrations.add(calibration_key)
                            if result["rows"]:
                                writer.writerows(result["rows"])
                                new_snapshots += 1
                    uncheckpointed += 1
                else:
                    failed_requests += 1
                    print("ERROR |", result["backend"], "|", result["error"], flush=True)

                now = time.perf_counter()
                file_size = output_file.tell()
                size_mb = file_size / (1024.0 * 1024.0)
                if uncheckpointed >= CHECKPOINT_EVERY or size_mb >= TARGET_MB:
                    output_file.flush()
                    save_checkpoint(completed_queries, seen_calibrations)
                    uncheckpointed = 0

                if completed_this_run == 1 or now - last_report >= 10:
                    elapsed = max(now - start_time, 0.001)
                    speed_kbps = file_size / elapsed / 1024.0
                    remaining = max(TARGET_MB * 1024 * 1024 - file_size, 0)
                    eta_seconds = remaining / max(file_size / elapsed, 1)
                    print(
                        "Progress | queries:", completed_this_run,
                        "| unique snapshots:", new_snapshots,
                        "| skipped duplicates:", skipped_duplicates,
                        "| size:", round(size_mb, 2), "MB",
                        "| speed:", round(speed_kbps, 1), "KB/s",
                        "| ETA:", round(eta_seconds / 60, 1), "min",
                        flush=True,
                    )
                    last_report = now

                if size_mb >= TARGET_MB:
                    stop = True
                    break

        output_file.flush()
        save_checkpoint(completed_queries, seen_calibrations)

print("Finished | output:", OUTPUT_FILE, flush=True)
print("Size:", round(os.path.getsize(OUTPUT_FILE) / (1024.0 * 1024.0), 2), "MB", flush=True)
print("Successful:", successful_requests, "| failed:", failed_requests, "| duplicate cutoffs skipped:", skipped_duplicates, flush=True)
print("Elapsed:", round((time.perf_counter() - start_time) / 60, 1), "min", flush=True)
