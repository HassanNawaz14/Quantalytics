import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Lock, local

import requests

BACKENDS = ["ibm_kingston", "ibm_fez", "ibm_marrakesh"]
FULL_LOAD_FILE = "Full_Load.csv"
STATE_FILE = "incremental_state.json"
LATEST_STATE_FILE = "Latest_State.csv"
STATE_VERSION = 1
REQUEST_TIMEOUT = 15
IAM_TIMEOUT = 15
MAX_RETRIES = 2
MAX_WORKERS = 16
PROBES_PER_DAY = 5

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


def parse_utc(value):
	parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
	if parsed.tzinfo is None:
		parsed = parsed.replace(tzinfo=timezone.utc)
	return parsed.astimezone(timezone.utc)


def format_utc(value):
	return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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


def load_state():
	if not os.path.exists(STATE_FILE):
		return None
	try:
		with open(STATE_FILE, "r", encoding="utf-8") as state_file:
			state = json.load(state_file)
	except (OSError, json.JSONDecodeError) as exc:
		raise RuntimeError(
			"Cannot read the incremental watermark file " + STATE_FILE
		) from exc
	if state.get("version") != STATE_VERSION or not state.get("watermark"):
		raise RuntimeError(
			"The incremental watermark file is invalid; inspect " + STATE_FILE
		)
	return state


def baseline_watermark(path):
	if not os.path.exists(path):
		raise RuntimeError(
			"No incremental state exists and the full-load baseline was not found: "
			+ path
			+ ". Pass --since with an ISO-8601 UTC timestamp to initialize it."
		)

	latest = None
	with open(path, "r", newline="", encoding="utf-8") as baseline_file:
		for row in csv.DictReader(baseline_file):
			value = row.get("Calibration_Timestamp", "").strip()
			if value:
				timestamp = parse_utc(value)
				latest = timestamp if latest is None else max(latest, timestamp)

	if latest is None:
		raise RuntimeError("No Calibration_Timestamp values found in " + path)
	return latest


def parameter_map(parameters):
	return {item.get("name"): item.get("value", "") for item in parameters}


class IBMMetadataClient:
	def __init__(self, api_key, instance_crn):
		from ibm_cloud_sdk_core import IAMTokenManager

		self.instance_crn = instance_crn
		self.token_manager = IAMTokenManager(api_key)
		self.token_manager.http_config = {"timeout": (5, IAM_TIMEOUT)}
		self.token_lock = Lock()
		self.thread_data = local()
		self.api_base = os.environ.get("IBM_QUANTUM_API_URL", "").strip()
		if not self.api_base:
			if "eu-de" in instance_crn.lower():
				self.api_base = "https://eu-de.quantum.cloud.ibm.com/api/v1"
			else:
				self.api_base = "https://quantum.cloud.ibm.com/api/v1"

	def authenticate(self):
		with self.token_lock:
			return self.token_manager.get_token()

	def get_session(self):
		if not hasattr(self.thread_data, "session"):
			session = requests.Session()
			session.headers.update({
				"Accept": "application/json",
				"IBM-API-Version": "2024-01-01",
			})
			self.thread_data.session = session
		return self.thread_data.session

	def backend_properties(self, backend_name, cutoff):
		with self.token_lock:
			token = self.token_manager.get_token()
		response = self.get_session().get(
			self.api_base + "/backends/" + backend_name + "/properties",
			params={"updated_before": cutoff.isoformat()},
			headers={
				"Authorization": "Bearer " + token,
				"Service-CRN": self.instance_crn,
			},
			timeout=(5, REQUEST_TIMEOUT),
		)
		response.raise_for_status()
		return response.json()


def snapshot_to_rows(payload, backend_name):
	timestamp = payload.get("last_update_date", "")
	if hasattr(timestamp, "isoformat"):
		timestamp = format_utc(timestamp)
	if not timestamp:
		return [], ""

	timestamp = format_utc(parse_utc(str(timestamp)))
	indexed_gates = {}
	for gate in payload.get("gates", []):
		gate_name = str(gate.get("gate", "")).lower()
		qubits = gate.get("qubits", [])
		for source_qubit in qubits:
			target_qubit = next(
				(qubit for qubit in qubits if qubit != source_qubit), None
			)
			for parameter in gate.get("parameters", []):
				key = (gate_name, source_qubit, parameter.get("name"))
				value = str(parameter.get("value", ""))
				if target_qubit is not None:
					value = str(target_qubit) + ":" + value
				indexed_gates.setdefault(key, []).append(value)

	rows = []
	for qubit_id, raw_parameters in enumerate(payload.get("qubits", [])):
		params = parameter_map(raw_parameters)

		def gate_value(gate_names, parameter_name):
			for gate_name in gate_names:
				values = indexed_gates.get((gate_name, qubit_id, parameter_name))
				if values:
					return ";".join(values)
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


def fetch_at(metadata_client, backend_name, cutoff):
	for attempt in range(MAX_RETRIES):
		try:
			return metadata_client.backend_properties(backend_name, cutoff) or {}
		except requests.HTTPError as exc:
			status = exc.response.status_code if exc.response is not None else None
			if status in (401, 403, 404) or attempt + 1 == MAX_RETRIES:
				raise
			time.sleep(0.5 * (2 ** attempt))
		except requests.RequestException as exc:
			print(
				"Retryable request failure |", backend_name,
				"|", str(exc),
				"| attempt:", attempt + 1, "/", MAX_RETRIES,
				flush=True,
			)
			if attempt + 1 == MAX_RETRIES:
				raise
			time.sleep(0.5 * (2 ** attempt))


def build_probe_jobs(watermark, until):
	jobs = []
	probe_spacing_hours = 24.0 / PROBES_PER_DAY
	day = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
	last_day = until.replace(hour=0, minute=0, second=0, microsecond=0)
	while day <= last_day:
		for slot_index in range(PROBES_PER_DAY):
			cutoff = day + timedelta(hours=slot_index * probe_spacing_hours)
			if watermark < cutoff <= until:
				for backend_name in BACKENDS:
					jobs.append((backend_name, cutoff))
		day += timedelta(days=1)
	jobs.sort(key=lambda job: job[1], reverse=True)
	return jobs


def fetch_snapshot(metadata_client, job):
	backend_name, cutoff = job
	payload = fetch_at(metadata_client, backend_name, cutoff)
	rows, timestamp_text = snapshot_to_rows(payload, backend_name)
	if not timestamp_text:
		raise RuntimeError(backend_name + " returned no calibration timestamp")
	timestamp = parse_utc(timestamp_text)
	if timestamp > cutoff:
		raise RuntimeError(backend_name + " returned a calibration newer than the requested cutoff")
	return backend_name, cutoff, timestamp, timestamp_text, rows


def run_query_batch(metadata_client, jobs, phase, watermark, snapshots):
	if not jobs:
		return []

	started = time.perf_counter()
	completed = 0
	results = []
	errors = []
	with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
		future_map = {
			executor.submit(fetch_snapshot, metadata_client, job): job
			for job in jobs
		}
		for future in as_completed(future_map):
			job = future_map[future]
			try:
				result = future.result()
				results.append(result)
				backend_name, _, timestamp, timestamp_text, rows = result
				if timestamp > watermark:
					snapshots[backend_name][timestamp_text] = (timestamp, rows)
			except Exception as exc:
				backend_name, cutoff = job
				errors.append(
					backend_name + " at " + format_utc(cutoff) + ": " + str(exc)
				)
			completed += 1
			elapsed = max(time.perf_counter() - started, 0.001)
			unique_count = sum(len(items) for items in snapshots.values())
			print(
				phase, "| requests:", completed, "/", len(jobs),
				"| unique snapshots:", unique_count,
				"| elapsed:", round(elapsed, 1), "sec",
				"| rate:", round(completed / elapsed, 2), "req/sec",
				flush=True,
			)

	if errors:
		raise RuntimeError("IBM history requests failed:\n" + "\n".join(errors[:5]))
	return results


def collect_all_snapshots(metadata_client, watermark, until, smoke_test=False):
	snapshots = {backend_name: {} for backend_name in BACKENDS}
	if smoke_test:
		jobs = [(backend_name, until) for backend_name in BACKENDS]
		run_query_batch(metadata_client, jobs, "Smoke test", watermark, snapshots)
		return snapshots

	probe_jobs = build_probe_jobs(watermark, until)
	print(
		"Calendar-aligned probes | planned requests:", len(probe_jobs),
		"| workers:", MAX_WORKERS,
		"| spacing:", round(24.0 / PROBES_PER_DAY, 2), "hours",
		flush=True,
	)
	run_query_batch(
		metadata_client, probe_jobs, "Weekly probes", watermark, snapshots
	)
	return snapshots


def write_csv_atomic(path, rows):
	temp_path = path + ".tmp"
	try:
		with open(temp_path, "w", newline="", encoding="utf-8") as output_file:
			writer = csv.DictWriter(output_file, fieldnames=HEADERS)
			writer.writeheader()
			writer.writerows(rows)
			output_file.flush()
			os.fsync(output_file.fileno())
		os.replace(temp_path, path)
	finally:
		if os.path.exists(temp_path):
			os.remove(temp_path)


def upsert_latest_state(new_rows):
	latest = {}
	if os.path.exists(LATEST_STATE_FILE):
		with open(LATEST_STATE_FILE, "r", newline="", encoding="utf-8") as state_file:
			for row in csv.DictReader(state_file):
				latest[(row["Backend_ID"], row["Qubit"])] = row

	for row in new_rows:
		key = (row["Backend_ID"], str(row["Qubit"]))
		current = latest.get(key)
		if current is None or parse_utc(row["Calibration_Timestamp"]) >= parse_utc(
			current["Calibration_Timestamp"]
		):
			latest[key] = row

	ordered = sorted(
		latest.values(),
		key=lambda row: (row["Backend_ID"], int(row["Qubit"])),
	)
	write_csv_atomic(LATEST_STATE_FILE, ordered)


def save_state(watermark):
	temp_path = STATE_FILE + ".tmp"
	with open(temp_path, "w", encoding="utf-8") as state_file:
		json.dump(
			{"version": STATE_VERSION, "watermark": format_utc(watermark)},
			state_file,
			separators=(",", ":"),
		)
		state_file.flush()
		os.fsync(state_file.fileno())
	os.replace(temp_path, STATE_FILE)


def parse_args():
	parser = argparse.ArgumentParser(
		description="Collect every IBM calibration snapshot newer than the prior weekly watermark."
	)
	parser.add_argument(
		"--since",
		help="Override the starting watermark (ISO-8601, for example 2026-09-25T13:33:33Z).",
	)
	parser.add_argument(
		"--until",
		help="Set the end of the collection window (ISO-8601 UTC); defaults to now.",
	)
	parser.add_argument(
		"--baseline",
		default=FULL_LOAD_FILE,
		help="Full-load CSV used to seed the first watermark.",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Print the collection window without connecting to IBM.",
	)
	parser.add_argument(
		"--smoke-test",
		action="store_true",
		help="Fetch one current calibration from each backend without writing files or advancing the watermark.",
	)
	return parser.parse_args()


def main():
	args = parse_args()
	state = load_state()

	if args.since:
		watermark = parse_utc(args.since)
	elif state:
		watermark = parse_utc(state["watermark"])
	else:
		watermark = baseline_watermark(args.baseline)

	until = parse_utc(args.until) if args.until else datetime.now(timezone.utc)
	if until <= watermark:
		raise RuntimeError("The end time must be later than the starting watermark.")

	print("Weekly collection window:", format_utc(watermark), "through", format_utc(until), flush=True)
	print("Backends:", ", ".join(BACKENDS), flush=True)
	probe_count = len(build_probe_jobs(watermark, until))
	print(
		"Parallel probe plan:", probe_count, "requests |",
		MAX_WORKERS, "workers | duplicate calibrations deduplicated",
		flush=True,
	)
	if args.dry_run:
		print("Dry run: no IBM request was made and no files were changed.", flush=True)
		return

	load_env_file()

	api_key = os.environ.get("IBM_QUANTUM_API_KEY", "").strip()
	instance_crn = os.environ.get("IBM_QUANTUM_INSTANCE", "").strip()
	if not api_key:
		raise RuntimeError("Set IBM_QUANTUM_API_KEY in the project .env file.")
	if not instance_crn.startswith("crn:v1:"):
		raise RuntimeError("Set IBM_QUANTUM_INSTANCE to the full CRN beginning with 'crn:v1:'.")

	print("Connecting to IBM Quantum...", flush=True)
	metadata_client = IBMMetadataClient(api_key, instance_crn)
	print("Requesting IBM IAM access token (15-second timeout)...", flush=True)
	metadata_client.authenticate()
	print("IBM IAM authentication complete.", flush=True)

	started = time.perf_counter()
	collected = collect_all_snapshots(
		metadata_client,
		watermark,
		until,
		args.smoke_test,
	)

	if args.smoke_test:
		print(
			"Smoke test passed | snapshots:",
		sum(len(entries) for entries in collected.values()),
		"| elapsed:", round(time.perf_counter() - started, 1), "sec",
		"| no output or watermark files were changed.",
		flush=True,
		)
		return

	all_rows = []
	for backend in BACKENDS:
		ordered_snapshots = sorted(
			collected[backend].values(),
			key=lambda item: item[0],
		)
		for _, rows in ordered_snapshots:
			all_rows.extend(rows)

	batch_name = (
		"Incremental_"
		+ watermark.strftime("%Y%m%dT%H%M%SZ")
		+ "_to_"
		+ until.strftime("%Y%m%dT%H%M%SZ")
		+ ".csv"
	)
	write_csv_atomic(batch_name, all_rows)
	upsert_latest_state(all_rows)
	save_state(until)

	size_mb = os.path.getsize(batch_name) / (1024.0 * 1024.0)
	snapshots = sum(len(collected[backend]) for backend in BACKENDS)
	print("Weekly batch:", batch_name, flush=True)
	print("New snapshots:", snapshots, "| rows:", len(all_rows), flush=True)
	print("Batch size:", round(size_mb, 2), "MB", flush=True)
	print("Latest-state upsert:", LATEST_STATE_FILE, flush=True)
	print("New watermark:", format_utc(until), flush=True)
	if size_mb < 1.0:
		print(
			"WARNING: This week's real source data is below 1 MB; no rows were fabricated.",
			flush=True,
		)


if __name__ == "__main__":
	main()
