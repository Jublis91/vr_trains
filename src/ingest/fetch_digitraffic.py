from datetime import datetime, timedelta
from pathlib import Path

import argparse
import json
import requests

BASE_URL = "https://rata.digitraffic.fi/api/v1"

def fetch_live_trains(station: str, days: int) -> list[dict]:
    params = {
        "station": station,
        "arrived_trains": 50,
        "departed_trains": 50,
        "include_nonstopping": False,
    }
    response = requests.get(f"{BASE_URL}/live-trains", params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    cutoff = datetime.utcnow() - timedelta(days=days)
    return [
        item
        for item in payload
        if datetime.fromisoformat(item["departureDate"]) >= cutoff
    ]

def fetch_train_location(train_numbers: list[int]) -> list[dict]:
    locations = []
    for train_number in train_numbers:
        response = requests.get(
            f"{BASE_URL}/train-locations/latest/{train_number}", timeout=30
        )
        if response.status_code == 200:
            locations.append(response.json())
    return locations

def write_json(path: Path, payload: list[dict]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def write_ndjson(path: Path, payload: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for item in payload:
            handle.write(json.dumps(item, ensure_ascii=False))
            handle.write("\n")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="HKI")
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    output_dir = Path("data/staging")
    output_dir.mkdir(parents=True, exist_ok=True)

    live_trains = fetch_live_trains(args.station, args.days)
    train_numbers = [train["trainNumber"] for train in live_trains]
    locations = fetch_train_location(train_numbers[:20])

    write_json(output_dir / "live_trains.json", live_trains)
    write_ndjson(output_dir / "live_trains.ndjson", live_trains)
    write_json(output_dir / "train_locations.json", locations)
    write_ndjson(output_dir / "train_locations.ndjson", locations)


    print(f"Saved {len(live_trains)} live trains to {output_dir}")
    print(f"Saved {len(locations)} train locations to {output_dir}")

if __name__ == "__main__":
    main()