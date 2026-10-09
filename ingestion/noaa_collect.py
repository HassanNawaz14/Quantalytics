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

def fetch_gfz_history(index_name, source_name, output, filename, start, end):
    """Fetch historical 3-hour Kp or observed F10.7 from GFZ."""
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    params = urlencode({
        "start": start,
        "end": end,
        "index": index_name,
    })

    url = "https://kp.gfz.de/app/json/?" + params
    request = Request(url, headers={"User-Agent": "Quantalytics-Research/1.0"})

    with urlopen(request, timeout=60) as response:
        data = json.loads(response.read().decode("utf-8"))

    # Handle the API's record-list or column-oriented JSON response.
    if isinstance(data, dict):
        times = data.get("time", data.get("datetime", data.get("date")))
        values = data.get("index", data.get(index_name, data.get("value")))
        if not isinstance(times, list) or not isinstance(values, list):
            raise RuntimeError(
                f"Unexpected GFZ response format for {index_name}: "
                f"{list(data.keys())}"
            )
        records = [
            {"time_tag": t, "value": v}
            for t, v in zip(times, values)
        ]
    elif isinstance(data, list):
        records = []
        for item in data:
            if isinstance(item, dict):
                timestamp = (
                    item.get("time")
                    or item.get("datetime")
                    or item.get("date")
                    or item.get("time_tag")
                )
                value = (
                    item.get("index")
                    or item.get(index_name)
                    or item.get("value")
                )
                if timestamp is not None:
                    records.append({"time_tag": timestamp, "value": value})
            elif isinstance(item, list) and len(item) >= 2:
                records.append({"time_tag": item[0], "value": item[1]})
    else:
        raise RuntimeError("Unexpected GFZ JSON response type")

    count = 0
    for record in records:
        timestamp = str(record["time_tag"])
        row = {
            "observation_timestamp": timestamp,
            "source": source_name,
            "raw_payload": json.dumps(record, ensure_ascii=False),
            "source_file": filename,
        }
        output.write(json.dumps(row, ensure_ascii=False) + "\n")
        count += 1

    if count == 0:
        raise RuntimeError(f"No historical records returned for {index_name}")

    print(f"Fetched historical {source_name}: {count} records")
    return count

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
        
        start = "2026-03-01T00:00:00Z"
        end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        total += fetch_gfz_history(
            "Kp", "gfz_kp_3h", output,
            os.path.basename(out_path), start, end
        )

        total += fetch_gfz_history(
            "Fobs", "gfz_f10_7_observed", output,
            os.path.basename(out_path), start, end
        )
        
        return 0

    
    except Exception as exc:
        if os.path.exists(out_path):
            os.remove(out_path)
        print("NOAA collection failed:", str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
