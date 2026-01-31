from datetime import datetime, timedelta
from pathlib import Path

import argparse
import json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://rata.digitraffic.fi/api/v1"
DEFAULT_TIMEOUT = 30
LOG = logging.getLogger(__name__)

def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "vr-trains-ingest/1.0"})
    return session


def fetch_live_trains(session: requests.Session, station: str, days: int) -> list[dict]:
    params = {
        "station": station,
        "arrived_trains": 50,
        "departed_trains": 50,
        "include_nonstopping": False,
    }
    response = session.get(
        f"{BASE_URL}/live-trains", params=params, timeout=DEFAULT_TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    cutoff = datetime.utcnow() - timedelta(days=days)
    return [
        item
        for item in payload
        if datetime.fromisoformat(item["departureDate"]) >= cutoff
    ]


def fetch_train_location(
    session: requests.Session, train_numbers: list[int]
) -> list[dict]:
    locations = []
    for train_number in train_numbers:
        try:
            response = session.get(
                f"{BASE_URL}/train-locations/latest/{train_number}",
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            locations.append(response.json())
        except requests.RequestException as exc:
            LOG.warning("Failed to fetch train location %s: %s", train_number, exc)
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
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    output_dir = Path("data/staging")
    output_dir.mkdir(parents=True, exist_ok=True)

    session = build_session()
    live_trains = fetch_live_trains(session, args.station, args.days)
    train_numbers = [train["trainNumber"] for train in live_trains]
    locations = fetch_train_location(session, train_numbers[: args.limit])

    write_json(output_dir / "live_trains.json", live_trains)
    write_ndjson(output_dir / "live_trains.ndjson", live_trains)
    write_json(output_dir / "train_locations.json", locations)
    write_ndjson(output_dir / "train_locations.ndjson", locations)

    print(f"Saved {len(live_trains)} live trains to {output_dir}")
    print(f"Saved {len(locations)} train locations to {output_dir}")


if __name__ == "__main__":
    main()
