# ingestion/noaa_collect.py

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, time as datetime_time, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

START_UTC = datetime(2026, 3, 1, tzinfo=timezone.utc)
CUTOFF_UTC = datetime(2026, 9, 25, 13, 33, 33, tzinfo=timezone.utc)

DGD_BASE = (
    "https://www.ngdc.noaa.gov/stp/space-weather/"
    "swpc-products/annual_reports/daily_solar_indices_summaries/"
    "daily_geomagnetic_data"
)

DSD_BASE = (
    "https://www.ngdc.noaa.gov/stp/space-weather/"
    "swpc-products/annual_reports/daily_solar_indices_summaries/"
    "daily_solar_data"
)

LIVE_ENDPOINTS = {
    "noaa_swpc_kp":
        "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    "noaa_swpc_f107":
        "https://services.swpc.noaa.gov/products/10cm-flux-30-day.json",
}


# ------------------------------------------------------------
# HTTP and timestamp helpers
# ------------------------------------------------------------

def download(url, attempts=3):
    """Download a NOAA file with basic retry handling."""
    last_error = None

    for attempt in range(attempts):
        try:
            request = Request(
                url,
                headers={"User-Agent": "Quantalytics-NOAA-collector/1.0"},
            )

            with urlopen(request, timeout=60) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"Unexpected HTTP status {response.status}: {url}"
                    )
                return response.read()

        except (HTTPError, URLError, TimeoutError, OSError,
                RuntimeError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"Could not download {url}: {last_error}")


def utc_timestamp(value):
    """Return (UTC ISO timestamp, aware datetime)."""
    value = str(value).strip()

    if not value:
        raise ValueError("Empty timestamp")

    # NOAA daily archive dates have no time component. Keep their daily
    # granularity by assigning midnight UTC, not a fabricated minute.
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        dt = datetime.combine(
            date.fromisoformat(value),
            datetime_time(0, 0),
            tzinfo=timezone.utc,
        )
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        dt = dt.astimezone(timezone.utc)

    formatted = dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    return formatted, dt


def quarter_names():
    """Quarter files needed to cover March through September 2026."""
    return ("2026Q1", "2026Q2", "2026Q3")


# ------------------------------------------------------------
# NOAA archive parsers
# ------------------------------------------------------------

def parse_dgd(text, filename):
    """
    Parse quarterly Daily Geomagnetic Data.

    The DGD format contains date columns followed by geomagnetic indices.
    The final eight fields are treated as the estimated planetary
    3-hour K-index values. Verify this against NOAA's file header before
    using the resulting values for scientific analysis.
    """
    output = []

    for line in text.splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if not re.match(r"^\d{4}\s+\d{1,2}\s+\d{1,2}\s+", stripped):
            continue

        fields = stripped.split()

        if len(fields) < 30:
            continue

        try:
            day = date(
                int(fields[0]),
                int(fields[1]),
                int(fields[2]),
            )
        except ValueError:
            continue

        # This parser intentionally fails closed if the expected data
        # columns are missing. The full original line is preserved below.
        planetary_k_values = fields[-8:]

        for interval, k_value in enumerate(planetary_k_values):
            dt = datetime.combine(
                day,
                datetime_time(interval * 3, 0),
                tzinfo=timezone.utc,
            )

            output.append({
                "source": "noaa_swpc_kp",
                "dt": dt,
                "timestamp": (
                    dt.isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                ),
                "original": stripped,
                "parsed": {
                    "estimated_planetary_k_index": k_value,
                    "interval_start_utc": (
                        dt.isoformat(timespec="seconds")
                        .replace("+00:00", "Z")
                    ),
                    "archive_file": filename,
                },
            })

    return output


def parse_dsd(text, filename):
    """
    Parse NOAA Daily Solar Data.

    NOAA's DSD file columns begin with YYYY MM DD, followed by the
    10.7 cm radio flux. The source line is retained for traceability.
    """
    output = []

    for line in text.splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if not re.match(r"^\d{4}\s+\d{1,2}\s+\d{1,2}\s+", stripped):
            continue

        fields = stripped.split()

        if len(fields) < 4:
            continue

        try:
            day = date(
                int(fields[0]),
                int(fields[1]),
                int(fields[2]),
            )
        except ValueError:
            continue

        # NOAA DSD column 4 is the daily 10.7 cm radio flux.
        flux = fields[3]
        dt = datetime.combine(
            day,
            datetime_time(0, 0),
            tzinfo=timezone.utc,
        )

        output.append({
            "source": "noaa_swpc_f107",
            "dt": dt,
            "timestamp": (
                dt.isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            ),
            "original": stripped,
            "parsed": {
                "f10_7_radio_flux": flux,
                "archive_file": filename,
                "timestamp_precision": "daily",
            },
        })

    return output


def historical_records():
    """Fetch the official NOAA quarterly archive files."""
    records = []

    for quarter in quarter_names():
        filename = f"{quarter}_DGD.txt"
        url = f"{DGD_BASE}/{filename}"
        text = download(url).decode("utf-8", errors="replace")
        parsed = parse_dgd(text, filename)

        if not parsed:
            raise RuntimeError(
                f"No DGD records parsed from {filename}. "
                "Check the source file format before continuing."
            )

        records.extend(parsed)

    for quarter in quarter_names():
        filename = f"{quarter}_DSD.txt"
        url = f"{DSD_BASE}/{filename}"
        text = download(url).decode("utf-8", errors="replace")
        parsed = parse_dsd(text, filename)

        if not parsed:
            raise RuntimeError(
                f"No DSD records parsed from {filename}. "
                "Check the source file format before continuing."
            )

        records.extend(parsed)

    return [
        record for record in records
        if START_UTC <= record["dt"] <= CUTOFF_UTC
    ]


# ------------------------------------------------------------
# Live feed parsing
# ------------------------------------------------------------

def normalize_live_payload(source, payload):
    """
    Accept either a list of dictionaries or a NOAA products-style
    table whose first row contains column names.
    """
    if not isinstance(payload, list):
        raise RuntimeError(
            f"{source} returned {type(payload).__name__}, not a list"
        )

    if not payload:
        return []

    if isinstance(payload[0], dict):
        return payload

    if isinstance(payload[0], list):
        headers = [str(value).strip() for value in payload[0]]
        rows = []

        for values in payload[1:]:
            if not isinstance(values, list):
                continue

            row = dict(zip(headers, values))
            rows.append(row)

        return rows

    raise RuntimeError(f"Unrecognized JSON structure from {source}")


def first_value(record, *names):
    """Get the first non-empty field from a set of possible field names."""
    lowered = {str(key).lower(): value for key, value in record.items()}

    for name in names:
        value = lowered.get(name.lower())
        if value is not None and str(value).strip() != "":
            return value

    return None


def live_records(smoke=False):
    records = []

    for source, url in LIVE_ENDPOINTS.items():
        payload = json.loads(download(url).decode("utf-8"))
        rows = normalize_live_payload(source, payload)

        if smoke:
            rows = rows[-5:]

        for record in rows:
            timestamp_value = first_value(
                record,
                "time_tag",
                "time",
                "date",
            )

            if timestamp_value is None:
                continue

            timestamp, dt = utc_timestamp(timestamp_value)

            records.append({
                "source": source,
                "dt": dt,
                "timestamp": timestamp,
                "original": record,
                "parsed": {
                    "field_note": (
                        "Original NOAA record retained for traceability"
                    )
                },
            })

    return records


# ------------------------------------------------------------
# Watermark and JSONL output
# ------------------------------------------------------------

def load_watermark(path):
    if not path:
        return {}

    with open(path, "r", encoding="utf-8") as file:
        content = json.load(file)

    sources = content.get("sources", {})

    if not isinstance(sources, dict):
        raise ValueError(
            "Invalid watermark: expected a 'sources' object"
        )

    return sources


def jsonl_row(record, output_filename):
    return {
        "observation_timestamp": record["timestamp"],
        "source": record["source"],
        "raw_payload": json.dumps(
            {
                "source_record": record["original"],
                "parsed_fields": record.get("parsed", {}),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "source_file": output_filename,
    }


def write_batch(records, out_dir, mode, old_watermark):
    os.makedirs(out_dir, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    filename = f"noaa_{mode}_{run_timestamp}.jsonl"
    output_path = os.path.join(out_dir, filename)

    # Deduplicate within the batch using the same key as the Bronze MERGE.
    unique = {}
    for record in records:
        unique[(record["source"], record["timestamp"])] = record

    records = sorted(
        unique.values(),
        key=lambda record: (
            record["dt"],
            record["source"],
        ),
    )

    if not records:
        print("No new records. No batch or watermark update is needed.")
        return False

    next_watermark = dict(old_watermark)

    with open(output_path, "w", encoding="utf-8", newline="\n") as file:
        for record in records:
            row = jsonl_row(record, filename)
            file.write(json.dumps(row, ensure_ascii=False) + "\n")

            previous = next_watermark.get(record["source"])

            if previous:
                _, previous_dt = utc_timestamp(previous)
            else:
                previous_dt = None

            if previous_dt is None or record["dt"] > previous_dt:
                next_watermark[record["source"]] = record["timestamp"]

    watermark_path = os.path.join(out_dir, "noaa_watermark.json")

    watermark = {
        "version": 1,
        "window_start_utc": (
            START_UTC.isoformat().replace("+00:00", "Z")
        ),
        "historical_cutoff_utc": (
            CUTOFF_UTC.isoformat().replace("+00:00", "Z")
        ),
        "sources": next_watermark,
        "updated_at_utc": (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        ),
    }

    with open(watermark_path, "w", encoding="utf-8") as file:
        json.dump(watermark, file, indent=2)
        file.write("\n")

    counts = {}
    for record in records:
        counts[record["source"]] = counts.get(record["source"], 0) + 1

    print(f"Mode: {mode}")
    print(f"Batch: {output_path}")
    print(f"Rows: {len(records)}")

    for source, count in sorted(counts.items()):
        print(f"{source}: {count} rows")

    print(
        "Watermark candidate written locally. "
        "The workflow must upload the data batch first, then commit the watermark."
    )

    return True


# ------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="NOAA historical and incremental data collector"
    )
    parser.add_argument(
        "--mode",
        choices=("smoke", "full", "incremental"),
        required=True,
    )
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--watermark-in", default=None)
    args = parser.parse_args()

    try:
        if args.mode == "smoke":
            records = live_records(smoke=True)

            if not records:
                raise RuntimeError("Smoke test returned zero records")

            for source in sorted({row["source"] for row in records}):
                print(f"Smoke test parsed records from {source}")

            print(f"Smoke test passed: {len(records)} records.")
            print("Smoke test does not write or update a watermark.")
            return 0

        if args.mode == "full":
            # This guard is repeated in the workflow; it protects against
            # accidentally using the same runner directory/state incorrectly.
            records = historical_records()
            old_watermark = {}

        else:
            if not args.watermark_in:
                raise ValueError(
                    "Incremental mode requires --watermark-in"
                )

            old_watermark = load_watermark(args.watermark_in)
            records = live_records()

            # Filter separately for each source; one feed must not advance
            # another feed's watermark.
            filtered = []

            for record in records:
                saved = old_watermark.get(record["source"])

                if saved:
                    _, saved_dt = utc_timestamp(saved)
                    if record["dt"] <= saved_dt:
                        continue

                filtered.append(record)

            records = filtered

        # Apply the historical window to full runs only.
        if args.mode == "full":
            records = [
                record for record in records
                if START_UTC <= record["dt"] <= CUTOFF_UTC
            ]

            if not records:
                raise RuntimeError(
                    "Historical backfill returned no records inside "
                    "the required date window."
                )

        wrote = write_batch(
            records,
            args.out_dir,
            args.mode,
            old_watermark,
        )

        if args.mode == "full" and not wrote:
            raise RuntimeError("Full backfill did not produce a data batch")

        return 0

    except Exception as exc:
        print(f"NOAA collector failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
