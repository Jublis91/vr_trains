from pathlib import Path

import duckdb

DATA_DIR = Path("data/staging")
DB_PATH = Path("data/vr_trains.duckdb")


def merge_live_trains(conn: duckdb.DuckDBPyConnection, json_path: Path) -> None:
    conn.execute("create schema if not exists bronze")
    conn.execute(
        """
        create table if not exists bronze.live_trains as
        select * from read_json_auto(?) limit 0
        """,
        [str(json_path)],
    )
    conn.execute(
        """
        insert into bronze.live_trains
        select *
        from read_json_auto(?) as incoming
        where not exists (
            select 1
            from bronze.live_trains existing
            where existing.trainNumber = incoming.trainNumber
              and existing.departureDate = incoming.departureDate
              and existing.version = incoming.version
        )
        """,
        [str(json_path)],
    )


def merge_train_locations(conn: duckdb.DuckDBPyConnection, json_path: Path) -> None:
    conn.execute("create schema if not exists bronze")
    conn.execute(
        """
        create table if not exists bronze.train_locations as
        select * from read_json_auto(?) limit 0
        """,
        [str(json_path)],
    )
    conn.execute(
        """
        insert into bronze.train_locations
        select *
        from read_json_auto(?) as incoming
        where not exists (
            select 1
            from bronze.train_locations existing
            where existing.trainNumber = incoming.trainNumber
              and existing.departureDate = incoming.departureDate
        )
        """,
        [str(json_path)],
    )


def main() -> None:
    live_trains_path = DATA_DIR / "live_trains.json"
    train_locations_path = DATA_DIR / "train_locations.json"

    if not live_trains_path.exists():
        raise FileNotFoundError(f"Missing {live_trains_path}")

    if not train_locations_path.exists():
        raise FileNotFoundError(f"Missing {train_locations_path}")

    conn = duckdb.connect(str(DB_PATH))
    merge_live_trains(conn, live_trains_path)
    merge_train_locations(conn, train_locations_path)
    conn.close()

    print("Bronze merge complete")


if __name__ == "__main__":
    main()
