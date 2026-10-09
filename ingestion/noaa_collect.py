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
