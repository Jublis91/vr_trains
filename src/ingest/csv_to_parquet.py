from pathlib import Path

import duckdb

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "vr_trains.duckdb"


def main() -> None:
    csv_path = DATA_DIR / "sample_train_events.csv"
    parquet_path = DATA_DIR / "parquet" / "train_events.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(
        "create or replace table bronze.sample_train_events as select * from read_csv_auto(?)",
        [str(csv_path)],
    )
    conn.execute(
        "copy bronze.sample_train_events to ? (format parquet)",
        [str(parquet_path)],
    )
    conn.close()
    print(f"Wrote {parquet_path}")


if __name__ == "__main__":
    main()
